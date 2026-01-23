import io
from io import BytesIO
import json
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from psycopg2.extras import execute_batch, execute_values


SILVER_SCHEMA_V1 = pa.schema([
    pa.field("coin_id", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("price_usd", pa.float64(), nullable=False),
    pa.field("market_cap", pa.float64(), nullable=False),
    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
])




# -----------------------------
# Constants
# -----------------------------
DATA_DIR = "/opt/airflow/datasets"
GE_CONTEXT_DIR = "/opt/airflow/gx"

# default_args = {
#     "owner": "lokesh",
#     "start_date": days_ago(1),
#     "retries": 0,
# }

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="coin_data_pipeline_optimized",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    description="Crypto ETL with Great Expectations validation",
)
# -----------------------------
# 0 DB CONNECITON
# -----------------------------
def get_pg_conn():
            conn = BaseHook.get_connection("postgres_warehouse")
            return psycopg2.connect(
                dbname=conn.schema,
                user=conn.login,
                password=conn.password,
                host=conn.host,
                port=conn.port,
            )
            
## ====================
    # 4.1 TABLE CREATION
## ====================

def create_tables():
    import logging

    logging.info("Creating tables if not exist")

    conn = get_pg_conn()
    conn.autocommit = True  # 🔥 CRITICAL
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_dimension (
            coin_id TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            category TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_prices_fact (
            id SERIAL PRIMARY KEY,
            coin_id TEXT,
            price_usd NUMERIC,
            market_cap NUMERIC,
            timestamp TIMESTAMP
        );
    """)

    cur.close()
    conn.close()

    logging.info("Tables created / verified")

    
create_tables_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag,
)
                

# -----------------------------
# 1️⃣ Extract
# -----------------------------
# def extract_data():
#     url = "https://api.coingecko.com/api/v3/coins/markets"
#     params = {"vs_currency": "usd", "order": "market_cap_desc"}

#     logging.info("Fetching crypto data from CoinGecko")
#     response = requests.get(url, params=params, timeout=10)
#     response.raise_for_status()

#     df = pd.DataFrame(response.json())
#     # df.to_csv(f"{DATA_DIR}/coin_raw.csv", index=False)
#     df = pd.DataFrame(response.json())

#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)

#     s3 = S3Hook(aws_conn_id="minio_s3")

#     today = datetime.utcnow().strftime("%Y%m%d")

#     s3.load_string(
#         string_data=csv_buffer.getvalue(),
#         key=f"raw/coin_raw_{today}.csv",
#         bucket_name="crypto-data",
#         replace=True
#     )
    
#     obj = s3.get_key(
#     key=f"raw/coin_raw_{today}.csv",
#     bucket_name="crypto-data"
#     )

#     df = pd.read_csv(obj.get()["Body"])


#     logging.info("Saved coin_raw.csv")

def extract_data():
    import logging

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc"}

    logging.info("Fetching crypto data from CoinGecko")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    df = pd.DataFrame(response.json())

    # Convert dataframe to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to MinIO using S3Hook
    s3_hook = S3Hook(aws_conn_id="minio_s3")
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key="raw/coin_raw.csv",
        bucket_name="crypto-data",
        replace=True,
    )

    logging.info("Uploaded raw data to MinIO: s3://crypto-data/raw/coin_raw.csv")



extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

# -----------------------------
# 2️⃣ upload_raw_to_s3
# -----------------------------

# def upload_raw_to_s3():
#     s3 = S3Hook(aws_conn_id="minio_s3")

#     bucket = "crypto-data"
#     key = "raw/coin_raw.csv"
#     local_path = f"{DATA_DIR}/coin_raw.csv"

#     # Ensure bucket exists
#     if not s3.check_for_bucket(bucket):
#         s3.create_bucket(bucket)

#     s3.load_file(
#         filename=local_path,
#         key=key,
#         bucket_name=bucket,
#         replace=True,
#     )

def upload_raw_to_s3(**context):
    logging.info("Uploading RAW data to MinIO (Bronze layer)")

    ds = context["ds"]  # execution date
    local_path = f"{DATA_DIR}/coin_raw.csv"

    df = pd.read_csv(local_path)
    records = df.to_dict(orient="records")

    s3_key = f"bronze/coins/dt={ds}/coin_raw.json"

    s3 = S3Hook(aws_conn_id="minio_s3")

    s3.load_string(
        string_data=json.dumps(records),
        key=s3_key,
        bucket_name="crypto-lake",
        replace=True,
    )

    logging.info(f"Uploaded RAW data to s3://crypto-lake/{s3_key}")


# upload_raw_task = PythonOperator(
#     task_id="upload_raw_to_s3",
#     python_callable=upload_raw_to_s3,
#     dag=dag,
# )

upload_raw_to_s3_task = PythonOperator(
    task_id="upload_raw_to_s3",
    python_callable=upload_raw_to_s3,
    provide_context=True,
    dag=dag,
)



# -----------------------------
# 2️⃣ Transform
# -----------------------------
# def transform_data():
#     df = pd.read_csv(f"{DATA_DIR}/coin_raw.csv")

#     df = df[
#         ["id", "symbol", "name", "current_price", "market_cap", "last_updated"]
#     ]

#     df = df.rename(
#         columns={
#             "id": "coin_id",
#             "current_price": "price_usd",
#             "last_updated": "timestamp",
#         }
#     )

#     df.to_csv(f"{DATA_DIR}/coin_transformed.csv", index=False)

#     logging.info("Saved coin_transformed.csv")

# def transform_data():
#     import logging

#     s3_hook = S3Hook(aws_conn_id="minio_s3")

#     # Read RAW CSV from MinIO
#     raw_obj = s3_hook.read_key(
#         key="raw/coin_raw.csv",
#         bucket_name="crypto-data"
#     )

#     df = pd.read_csv(io.StringIO(raw_obj))

#     # Transform
#     df = df[
#         ["id", "symbol", "name", "current_price", "market_cap", "last_updated"]
#     ]

#     df = df.rename(columns={
#         "id": "coin_id",
#         "current_price": "price_usd",
#         "last_updated": "timestamp",
#     })

#     # Write transformed CSV back to MinIO
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)

#     s3_hook.load_string(
#         string_data=csv_buffer.getvalue(),
#         key="processed/coin_transformed.csv",
#         bucket_name="crypto-data",
#         replace=True,
#     )

#     logging.info("Uploaded transformed data to MinIO: s3://crypto-data/processed/coin_transformed.csv")

# transform_task = PythonOperator(
#     task_id="transform_data",
#     python_callable=transform_data,
#     dag=dag,
# )


# def transform_bronze_to_silver():
# def transform_bronze_to_silver(**context):
#     execution_date = context["ds"]
#     logging.info("Starting Bronze ➜ Silver transformation")

#     s3 = S3Hook(aws_conn_id="minio_s3")

#     # execution_date = datetime.utcnow().date().isoformat()

#     bronze_key = f"bronze/coins/dt={execution_date}/coin_raw.json"
#     silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"
#     bucket = "crypto-lake"

#     # 1️⃣ Read raw JSON from Bronze
#     raw_json = s3.read_key(key=bronze_key, bucket_name=bucket)
#     df = pd.read_json(io.StringIO(raw_json))

#     # 2️⃣ Clean & select columns
#     df = df[
#         [
#             "id",
#             "symbol",
#             "name",
#             "current_price",
#             "market_cap",
#             "last_updated",
#         ]
#     ]

#     df = df.rename(
#         columns={
#             "id": "coin_id",
#             "current_price": "price_usd",
#             "last_updated": "timestamp",
#         }
#     )

#     # 3️⃣ Type casting
#     df["timestamp"] = pd.to_datetime(df["timestamp"])
#     df["price_usd"] = df["price_usd"].astype(float)
#     df["market_cap"] = df["market_cap"].astype(float)

#     # 4️⃣ Convert to Parquet (in-memory)
#     table = pa.Table.from_pandas(df)
#     buffer = io.BytesIO()
#     pq.write_table(table, buffer)
#     buffer.seek(0)

#     # 5️⃣ Write to Silver
#     s3.load_bytes(
#         bytes_data=buffer.getvalue(),
#         key=silver_key,
#         bucket_name=bucket,
#         replace=True,
#     )

#     logging.info(f"Silver data written to {silver_key}")

def transform_bronze_to_silver(**context):
    import logging
    import io
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Starting Bronze ➜ Silver transformation (atomic)")

    s3 = S3Hook(aws_conn_id="minio_s3")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    bronze_key = f"bronze/coins/dt={execution_date}/coin_raw.json"
    tmp_silver_key = f"silver/coins/_tmp_dt={execution_date}/coin_clean.parquet"
    final_silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"

    # 1️⃣ Read Bronze
    raw_json = s3.read_key(key=bronze_key, bucket_name=bucket)
    df = pd.read_json(io.StringIO(raw_json))

    # 2️⃣ Transform
    df = df[
        ["id", "symbol", "name", "current_price", "market_cap", "last_updated"]
    ].rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "last_updated": "timestamp",
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["price_usd"] = df["price_usd"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)

    # # 3️⃣ Write Parquet to TEMP location
    # table = pa.Table.from_pandas(df)
    
    # 3️⃣ Enforce Silver schema (HARD CONTRACT)
    try:
        table = pa.Table.from_pandas(
            df,
            schema=SILVER_SCHEMA_V1,
            preserve_index=False,
            safe=True
        )
    except Exception as e:
        logging.error("❌ Silver schema validation failed")
        logging.error(str(e))
        raise
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=tmp_silver_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"Temporary Silver written: {tmp_silver_key}")

    # 4️⃣ Promote TEMP ➜ FINAL (atomic visibility)
    if s3.check_for_key(final_silver_key, bucket):
        s3.delete_objects(bucket=bucket, keys=[final_silver_key])

    s3.copy_object(
        source_bucket_key=tmp_silver_key,
        dest_bucket_key=final_silver_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )

    s3.delete_objects(bucket=bucket, keys=[tmp_silver_key])

    logging.info(f"Silver partition committed: {final_silver_key}")
    
    # 5️⃣ Write Silver metadata (schema versioning)
    SILVER_SCHEMA_VERSION = "v1"

    metadata = {
        "dataset": "coins",
        "schema_version": SILVER_SCHEMA_VERSION,
        "execution_date": execution_date,
        "row_count": len(df),
        "source_bronze_path": bronze_key,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    metadata_key = f"silver/coins/dt={execution_date}/_metadata.json"

    s3.load_string(
        string_data=json.dumps(metadata, indent=2),
        key=metadata_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"Silver metadata written: {metadata_key}")



transform_bronze_to_silver_task = PythonOperator(
    task_id="transform_bronze_to_silver",
    python_callable=transform_bronze_to_silver,
    provide_context=True,
    dag=dag,
)



# -----------------------------
# 3️⃣ Validate (Great Expectations - Fluent API)
# -----------------------------

def validate_data():
    import great_expectations as gx
    import pandas as pd
    import logging

    logging.info("Starting FAST in-memory validation")

    # NOTE:
    # This validation currently reads from a legacy local CSV.
    # Silver validation is now enforced via PyArrow schema + atomic writes.
    # This task will be refactored or removed in Step C (Observability & Reliability).

    df = pd.read_csv("/opt/airflow/datasets/coin_transformed.csv")

    # Create ephemeral GE context (no filesystem, no gx folder)
    context = gx.get_context()

    # Add in-memory pandas datasource
    datasource = context.sources.add_pandas(name="in_memory_pandas")

    # Create dataframe asset
    asset = datasource.add_dataframe_asset(name="coin_df")

    # Create batch (THIS IS THE KEY FIX)
    batch_request = asset.build_batch_request(dataframe=df)

    # Create / reuse suite
    suite_name = "coin_suite"
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name
    )

    # Get validator (WITH ACTIVE BATCH)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Expectations
    validator.expect_column_values_to_not_be_null("coin_id")
    validator.expect_column_values_to_be_between("price_usd", min_value=0)
    validator.expect_column_values_to_not_be_null("market_cap")
    validator.expect_column_values_to_not_be_null("timestamp")

    # Validate
    result = validator.validate()

    if not result.success:
        raise ValueError("❌ Data validation FAILED")

    logging.info("✅ Data validation PASSED")


validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)



# -----------------------------
# 4️⃣ Load Dimension Table
# -----------------------------
def load_dimension():
    df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

    # conn = psycopg2.connect(
    #     "dbname=airflow user=airflow password=airflow host=postgres"
    # )
    conn = get_pg_conn()
    cur = conn.cursor()

    # for _, row in df.iterrows():
    #     cur.execute(
    #         """
    #         INSERT INTO coin_dimension (coin_id, name, symbol, category)
    #         VALUES (%s, %s, %s, %s)
    #         ON CONFLICT (coin_id) DO NOTHING;
    #         """,
    #         (row["coin_id"], row["name"], row["symbol"], "cryptocurrency"),
    #     )
    execute_batch(
    cur,
    """
    INSERT INTO coin_dimension (coin_id, name, symbol, category)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (coin_id) DO NOTHING;
    """,
    [
        (r.coin_id, r.name, r.symbol, "cryptocurrency")
        for r in df.itertuples(index=False)
    ],
)

    conn.commit()
    cur.close()
    conn.close()

    logging.info("Loaded coin_dimension")


load_dim_task = PythonOperator(
    task_id="load_dimension_table",
    python_callable=load_dimension,
    dag=dag,
)

# -----------------------------
# 5️⃣ Load Fact Table
# -----------------------------
# def load_fact():
#     df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

#     # conn = psycopg2.connect(
#     #     "dbname=airflow user=airflow password=airflow host=postgres"
#     # )
#     conn = get_pg_conn()
#     cur = conn.cursor()

#     # for _, row in df.iterrows():
#     #     cur.execute(
#     #         """
#     #         INSERT INTO coin_prices_fact
#     #         (coin_id, price_usd, market_cap, timestamp)
#     #         VALUES (%s, %s, %s, %s);
#     #         """,
#     #         (
#     #             row["coin_id"],
#     #             row["price_usd"],
#     #             row["market_cap"],
#     #             row["timestamp"],
#     #         ),
#     #     )
#     execute_batch(
#         cur,
#         """
#         INSERT INTO coin_prices_fact
#         (coin_id, price_usd, market_cap, timestamp)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT ON CONSTRAINT uq_coin_time
#         DO NOTHING;
#         """,
#         [
#             (r.coin_id, r.price_usd, r.market_cap, r.timestamp)
#             for r in df.itertuples(index=False)
#         ],
#     )
#     conn.commit()
#     cur.close()
#     conn.close()

#     logging.info("Loaded coin_prices_fact")



def load_fact():
    logging.info("Loading coin_prices_fact (idempotent)")

    df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

    conn = get_pg_conn()
    cur = conn.cursor()

    records = [
        (r.coin_id, r.price_usd, r.market_cap, r.timestamp)
        for r in df.itertuples(index=False)
    ]

    sql = """
        INSERT INTO coin_prices_fact
        (coin_id, price_usd, market_cap, timestamp)
        VALUES %s
        ON CONFLICT (coin_id, timestamp) DO NOTHING
    """

    execute_values(
        cur,
        sql,
        records,
        page_size=1000
    )

    conn.commit()
    cur.close()
    conn.close()

    logging.info("Loaded coin_prices_fact successfully")


load_fact_task = PythonOperator(
    task_id="load_fact_table",
    python_callable=load_fact,
    dag=dag,
)


# -----------------------------
# 6 load_gold_metrics ( PGSQL - TABLE STORE )
# -----------------------------



# def load_gold_metrics():
#     conn = get_pg_conn()
#     cur = conn.cursor()

#     cur.execute("""
#         INSERT INTO coin_daily_metrics
#         SELECT
#             coin_id,
#             DATE(timestamp) AS date,
#             AVG(price_usd),
#             MIN(price_usd),
#             MAX(price_usd),
#             AVG(market_cap)
#         FROM coin_prices_fact
#         GROUP BY coin_id, DATE(timestamp)
#         ON CONFLICT (coin_id, date) DO NOTHING;
#     """)

#     conn.commit()
#     cur.close()
#     conn.close()
    
# load_gold_task = PythonOperator(
#     task_id="load_gold_metrics",
#     python_callable=load_gold_metrics,
#     dag=dag,
# )

## =================================

## build_gold_coin_daily_minio

## =================================

# def build_gold_coin_daily_minio(**context):
#     logging.info("Building GOLD layer (MinIO)")

#     execution_date = context["ds"]  # YYYY-MM-DD
#     silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"
#     gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

#     s3 = S3Hook(aws_conn_id="minio_s3")

#     # Read Silver parquet
#     silver_obj = s3.get_key(silver_key, bucket_name="crypto-lake")
#     df = pd.read_parquet(BytesIO(silver_obj.get()["Body"].read()))

#     # Aggregate
#     gold_df = (
#         df.groupby("coin_id")
#         .agg(
#             avg_price_usd=("price_usd", "mean"),
#             min_price_usd=("price_usd", "min"),
#             max_price_usd=("price_usd", "max"),
#             avg_market_cap=("market_cap", "mean"),
#         )
#         .reset_index()
#     )

#     gold_df["dt"] = execution_date

#     # Write Parquet back to MinIO
#     buffer = BytesIO()
#     gold_df.to_parquet(buffer, index=False)
#     buffer.seek(0)

#     s3.load_file_obj(
#         buffer,
#         key=gold_key,
#         bucket_name="crypto-lake",
#         replace=True,
#     )

#     logging.info("Gold layer written to MinIO successfully")

def build_gold_coin_daily_minio(**context):
    import logging
    from io import BytesIO
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Building GOLD layer (coin daily metrics)")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"
    gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

    s3 = S3Hook(aws_conn_id="minio_s3")

    # 1️⃣ Read Silver Parquet
    silver_obj = s3.get_key(silver_key, bucket_name=bucket)
    df = pd.read_parquet(BytesIO(silver_obj.get()["Body"].read()))

    # 2️⃣ Aggregate (Gold logic)
    gold_df = (
        df.groupby("coin_id")
        .agg(
            avg_price_usd=("price_usd", "mean"),
            min_price_usd=("price_usd", "min"),
            max_price_usd=("price_usd", "max"),
            avg_market_cap=("market_cap", "mean"),
        )
        .reset_index()
    )

    # 3️⃣ Add business date
    gold_df["dt"] = execution_date

    # 4️⃣ Write Gold Parquet (idempotent overwrite)
    buffer = BytesIO()
    gold_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.load_file_obj(
        buffer,
        key=gold_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"GOLD dataset written to s3://{bucket}/{gold_key}")


build_gold_minio_task = PythonOperator(
    task_id="build_gold_minio",
    python_callable=build_gold_coin_daily_minio,
    provide_context=True,
    dag=dag,
)

## =================================

## load_gold_to_postgres

## =================================


# def load_gold_to_postgres(**context):
#     logging.info("Loading GOLD layer into Postgres")

#     execution_date = context["ds"]
#     gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

#     s3 = S3Hook(aws_conn_id="minio_s3")

#     # Read gold parquet from MinIO
#     obj = s3.get_key(gold_key, bucket_name="crypto-lake")
#     df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))

#     conn = get_pg_conn()
#     cur = conn.cursor()

#     records = [
#         (
#             execution_date,
#             r.coin_id,
#             r.avg_price_usd,
#             r.min_price_usd,
#             r.max_price_usd,
#             r.avg_market_cap,
#         )
#         for r in df.itertuples(index=False)
#     ]

#     sql = """
#         INSERT INTO gold_coin_daily_metrics
#         (dt, coin_id, avg_price_usd, min_price_usd, max_price_usd, avg_market_cap)
#         VALUES %s
#         ON CONFLICT (dt, coin_id)
#         DO UPDATE SET
#             avg_price_usd = EXCLUDED.avg_price_usd,
#             min_price_usd = EXCLUDED.min_price_usd,
#             max_price_usd = EXCLUDED.max_price_usd,
#             avg_market_cap = EXCLUDED.avg_market_cap;
#     """

#     execute_values(cur, sql, records, page_size=1000)

#     conn.commit()
#     cur.close()
#     conn.close()

#     logging.info("Gold layer loaded into Postgres successfully")
def load_gold_to_postgres(**context):
    import logging
    from io import BytesIO
    import pandas as pd
    from psycopg2.extras import execute_values
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Loading GOLD dataset into Postgres")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

    # 1️⃣ Read Gold Parquet from MinIO
    s3 = S3Hook(aws_conn_id="minio_s3")
    obj = s3.get_key(gold_key, bucket_name=bucket)
    df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))

    if df.empty:
        raise ValueError("❌ Gold dataset is empty — aborting Postgres load")

    # 2️⃣ Prepare records
    records = [
        (
            execution_date,
            r.coin_id,
            r.avg_price_usd,
            r.min_price_usd,
            r.max_price_usd,
            r.avg_market_cap,
        )
        for r in df.itertuples(index=False)
    ]

    # 3️⃣ Insert / upsert into Postgres
    conn = get_pg_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO gold_coin_daily_metrics
        (dt, coin_id, avg_price_usd, min_price_usd, max_price_usd, avg_market_cap)
        VALUES %s
        ON CONFLICT (dt, coin_id)
        DO UPDATE SET
            avg_price_usd   = EXCLUDED.avg_price_usd,
            min_price_usd   = EXCLUDED.min_price_usd,
            max_price_usd   = EXCLUDED.max_price_usd,
            avg_market_cap  = EXCLUDED.avg_market_cap;
    """

    execute_values(cur, sql, records, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

    logging.info("GOLD dataset successfully loaded into Postgres")
  
    
load_gold_postgres_task = PythonOperator(
    task_id="load_gold_postgres",
    python_callable=load_gold_to_postgres,
    provide_context=True,
    dag=dag,
) ## =================================

## validate_gold_metrics

## =================================
    
def validate_gold_metrics():
    logging.info("Validating GOLD metrics")

    conn = get_pg_conn()
    cur = conn.cursor()

    checks = {
        "row_count": """
            SELECT COUNT(*) FROM gold_coin_daily_metrics
            WHERE dt = CURRENT_DATE
        """,
        "no_null_prices": """
            SELECT COUNT(*) FROM gold_coin_daily_metrics
            WHERE avg_price_usd IS NULL
        """,
        "price_positive": """
            SELECT COUNT(*) FROM gold_coin_daily_metrics
            WHERE avg_price_usd <= 0
        """,
    }

    for name, sql in checks.items():
        cur.execute(sql)
        result = cur.fetchone()[0]

        if name == "row_count" and result == 0:
            raise ValueError("❌ No GOLD data for today")

        if name != "row_count" and result > 0:
            raise ValueError(f"❌ GOLD quality check failed: {name}")

    cur.close()
    conn.close()

    logging.info("✅ Gold data quality checks passed")

validate_gold_task = PythonOperator(
    task_id="validate_gold_metrics",
    python_callable=validate_gold_metrics,
    dag=dag,
)
def validate_gold_row_count(**context):
    import logging
    from io import BytesIO
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Validating GOLD row count consistency")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

    # 1️⃣ Count rows in MinIO Gold
    s3 = S3Hook(aws_conn_id="minio_s3")
    obj = s3.get_key(gold_key, bucket_name=bucket)
    gold_df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))
    minio_count = len(gold_df)

    logging.info(f"MinIO Gold row count: {minio_count}")

    if minio_count == 0:
        raise ValueError("❌ MinIO Gold dataset is empty")

    # 2️⃣ Count rows in Postgres Gold
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_coin_daily_metrics
        WHERE dt = %s
        """,
        (execution_date,),
    )

    postgres_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    logging.info(f"Postgres Gold row count: {postgres_count}")

    # 3️⃣ Compare
    if minio_count != postgres_count:
        raise ValueError(
            f"❌ Gold row count mismatch: "
            f"MinIO={minio_count}, Postgres={postgres_count}"
        )

    logging.info("✅ Gold row count validation passed")

validate_gold_row_count_task = PythonOperator(
    task_id="validate_gold_row_count",
    python_callable=validate_gold_row_count,
    provide_context=True,
    dag=dag,
)
    
def validate_gold_sanity(**context):
    import logging

    logging.info("Running GOLD sanity checks")

    execution_date = context["ds"]

    conn = get_pg_conn()
    cur = conn.cursor()

    # 1️⃣ Non-empty check
    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_coin_daily_metrics
        WHERE dt = %s
        """,
        (execution_date,),
    )
    row_count = cur.fetchone()[0]

    if row_count == 0:
        raise ValueError("❌ Gold sanity check failed: no rows for execution date")

    logging.info(f"Gold sanity: row_count={row_count}")

    # 2️⃣ NULL coin_id check
    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_coin_daily_metrics
        WHERE dt = %s
          AND coin_id IS NULL
        """,
        (execution_date,),
    )
    null_coin_ids = cur.fetchone()[0]

    if null_coin_ids > 0:
        raise ValueError(
            f"❌ Gold sanity check failed: {null_coin_ids} NULL coin_id values"
        )

    # 3️⃣ Metric validity checks
    cur.execute(
        """
        SELECT COUNT(*)
        FROM gold_coin_daily_metrics
        WHERE dt = %s
          AND (
                avg_price_usd <= 0
             OR min_price_usd < 0
             OR max_price_usd < min_price_usd
             OR avg_market_cap < 0
          )
        """,
        (execution_date,),
    )
    invalid_metrics = cur.fetchone()[0]

    if invalid_metrics > 0:
        raise ValueError(
            f"❌ Gold sanity check failed: {invalid_metrics} rows with invalid metrics"
        )

    cur.close()
    conn.close()

    logging.info("✅ Gold sanity checks passed")

validate_gold_sanity_task = PythonOperator(
    task_id="validate_gold_sanity",
    python_callable=validate_gold_sanity,
    provide_context=True,
    dag=dag,
)

def validate_gold_freshness(**context):
    import logging
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Validating GOLD data freshness")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

    s3 = S3Hook(aws_conn_id="minio_s3")

    if not s3.check_for_key(gold_key, bucket_name=bucket):
        raise ValueError(
            f"❌ Gold freshness check failed: "
            f"missing Gold partition for {execution_date}"
        )

    logging.info("✅ Gold freshness check passed")

validate_gold_freshness_task = PythonOperator(
    task_id="validate_gold_freshness",
    python_callable=validate_gold_freshness,
    provide_context=True,
    dag=dag,
)

# def validate_gold_sla(**context):
#     import logging
#     from datetime import datetime, time, timezone

#     logging.info("Validating GOLD SLA (lateness check)")

#     execution_date_str = context["ds"]  # YYYY-MM-DD
#     execution_date = datetime.fromisoformat(execution_date_str).date()

#     # SLA definition: Gold must be ready by 09:00 UTC
#     sla_deadline = datetime.combine(
#         execution_date,
#         time(hour=9, minute=0),
#         tzinfo=timezone.utc,
#     )

#     now_utc = datetime.now(timezone.utc)

#     logging.info(f"SLA deadline: {sla_deadline.isoformat()}")
#     logging.info(f"Current time: {now_utc.isoformat()}")

#     if now_utc > sla_deadline:
#         raise ValueError(
#             f"❌ GOLD SLA missed: "
#             f"completed at {now_utc.isoformat()}, "
#             f"expected by {sla_deadline.isoformat()}"
#         )

#     logging.info("✅ GOLD SLA met")

def validate_gold_sla(**context):
    import logging
    from datetime import datetime, time, timezone
    from airflow.utils.types import DagRunType

    logging.info("Validating GOLD SLA (lateness check)")

    dag_run = context["dag_run"]

    # ✅ Only enforce SLA for scheduled runs
    if dag_run.run_type != DagRunType.SCHEDULED:
        logging.info(
            f"Skipping SLA check for run_type={dag_run.run_type}"
        )
        return

    execution_date_str = context["ds"]
    execution_date = datetime.fromisoformat(execution_date_str).date()

    sla_deadline = datetime.combine(
        execution_date,
        time(hour=9, minute=0),
        tzinfo=timezone.utc,
    )

    now_utc = datetime.now(timezone.utc)

    logging.info(f"SLA deadline: {sla_deadline.isoformat()}")
    logging.info(f"Current time: {now_utc.isoformat()}")

    if now_utc > sla_deadline:
        raise ValueError(
            f"❌ GOLD SLA missed: "
            f"completed at {now_utc.isoformat()}, "
            f"expected by {sla_deadline.isoformat()}"
        )

    logging.info("✅ GOLD SLA met")


validate_gold_sla_task = PythonOperator(
    task_id="validate_gold_sla",
    python_callable=validate_gold_sla,
    provide_context=True,
    dag=dag,
)

   

# -----------------------------
# DAG Dependencies
# -----------------------------

create_tables_task >> extract_task >> upload_raw_to_s3_task >> transform_bronze_to_silver_task  >> validate_task >> load_dim_task >> load_fact_task >> build_gold_minio_task >> load_gold_postgres_task >> validate_gold_row_count_task >> validate_gold_sanity_task >> validate_gold_freshness_task >> validate_gold_sla_task >> validate_gold_task 
# >> load_gold_task


# extract_task >> transform_task >> validate_task >> create_tables_task >> load_dim_task >> load_fact_task

