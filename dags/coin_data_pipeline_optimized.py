from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd
import requests
import psycopg2
import logging

import great_expectations as gx
from airflow.hooks.base import BaseHook
from psycopg2.extras import execute_batch
from psycopg2.extras import execute_values

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# minio - connection  
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from datetime import datetime
import io





# -----------------------------
# Constants
# -----------------------------
DATA_DIR = "/opt/airflow/datasets"
GE_CONTEXT_DIR = "/opt/airflow/gx"

default_args = {
    "owner": "lokesh",
    "start_date": days_ago(1),
    "retries": 0,
}

dag = DAG(
    dag_id="coin_data_pipeline_optimized",
    default_args=default_args,
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


def transform_bronze_to_silver():
    logging.info("Starting Bronze ➜ Silver transformation")

    s3 = S3Hook(aws_conn_id="minio_s3")

    execution_date = datetime.utcnow().date().isoformat()

    bronze_key = f"bronze/coins/dt={execution_date}/coin_raw.json"
    silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"
    bucket = "crypto-lake"

    # 1️⃣ Read raw JSON from Bronze
    raw_json = s3.read_key(key=bronze_key, bucket_name=bucket)
    df = pd.read_json(io.StringIO(raw_json))

    # 2️⃣ Clean & select columns
    df = df[
        [
            "id",
            "symbol",
            "name",
            "current_price",
            "market_cap",
            "last_updated",
        ]
    ]

    df = df.rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "last_updated": "timestamp",
        }
    )

    # 3️⃣ Type casting
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["price_usd"] = df["price_usd"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)

    # 4️⃣ Convert to Parquet (in-memory)
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # 5️⃣ Write to Silver
    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=silver_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"Silver data written to {silver_key}")

transform_bronze_to_silver_task = PythonOperator(
    task_id="transform_bronze_to_silver",
    python_callable=transform_bronze_to_silver,
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

    # Load data
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
# 6 load_gold_metrics
# -----------------------------



def load_gold_metrics():
    conn = get_pg_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO coin_daily_metrics
        SELECT
            coin_id,
            DATE(timestamp) AS date,
            AVG(price_usd),
            MIN(price_usd),
            MAX(price_usd),
            AVG(market_cap)
        FROM coin_prices_fact
        GROUP BY coin_id, DATE(timestamp)
        ON CONFLICT (coin_id, date) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()
    
load_gold_task = PythonOperator(
    task_id="load_gold_metrics",
    python_callable=load_gold_metrics,
    dag=dag,
)
    

# -----------------------------
# DAG Dependencies
# -----------------------------

create_tables_task >> extract_task >> upload_raw_to_s3_task >> transform_bronze_to_silver_task  >> validate_task >> load_dim_task >> load_fact_task  >> load_gold_task


# extract_task >> transform_task >> validate_task >> create_tables_task >> load_dim_task >> load_fact_task
