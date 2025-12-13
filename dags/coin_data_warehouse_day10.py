from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import psycopg2
import logging

# GE Fluent (v0.16+)
import great_expectations as gx


DATA_DIR = "/opt/airflow/datasets"

default_args = {
    "owner": "lokesh",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    dag_id="coin_data_pipeline_optimized",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)


# -----------------------------------------------------
# 1) Extract
# -----------------------------------------------------
def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc"}

    logging.info("Fetching data from API…")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    df.to_csv(f"{DATA_DIR}/coin_raw.csv", index=False)
    logging.info("Saved coin_raw.csv")


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag
)


# -----------------------------------------------------
# 2) Transform
# -----------------------------------------------------
def transform_data():
    df = pd.read_csv(f"{DATA_DIR}/coin_raw.csv")

    df = df[["id", "symbol", "name", "current_price", "market_cap", "last_updated"]]

    df = df.rename(columns={
        "id": "coin_id",
        "current_price": "price_usd",
        "last_updated": "timestamp"
    })

    df.to_csv(f"{DATA_DIR}/coin_transformed.csv", index=False)
    logging.info("Saved coin_transformed.csv")


transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)


# -----------------------------------------------------
# 3) Validate with GE Fluent API (FAST)
# -----------------------------------------------------
def validate_data():
    df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

    logging.info("Starting Great Expectations validation (fluent)…")

    context = gx.get_context()  # No filesystem context

    source = context.sources.add_pandas(name="pandas_src")
    asset = source.add_dataframe_asset(name="coin_asset")
    batch = asset.add_batch(df=df)

    validator = context.get_validator(
        batch=batch,
        expectation_suite_name="coin_suite"
    )

    # Expectations
    validator.expect_column_values_to_not_be_null("coin_id")
    validator.expect_column_values_to_be_between("price_usd", min_value=0)
    validator.expect_column_values_to_not_be_null("market_cap")
    validator.expect_column_values_to_not_be_null("timestamp")

    # Run validation
    result = validator.validate()

    if not result.success:
        logging.error("Validation FAILED!")
        raise ValueError("GE validation failed.")

    logging.info("Validation PASSED!")


validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag
)


# -----------------------------------------------------
# 4) Load Dimension Table
# -----------------------------------------------------
def load_dim():
    df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

    conn = psycopg2.connect(
        "dbname=warehouse user=postgres password=postgres host=postgres"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO coin_dimension (coin_id, name, symbol, category)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (coin_id) DO NOTHING;
        """, (row["coin_id"], row["name"], row["symbol"], "cryptocurrency"))

    conn.commit()
    cur.close()
    conn.close()


load_dim_task = PythonOperator(
    task_id="load_dimension_table",
    python_callable=load_dim,
    dag=dag
)


# -----------------------------------------------------
# 5) Load Fact Table
# -----------------------------------------------------
def load_fact():
    df = pd.read_csv(f"{DATA_DIR}/coin_transformed.csv")

    conn = psycopg2.connect(
        "dbname=warehouse user=postgres password=postgres host=postgres"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO coin_prices_fact (coin_id, price_usd, market_cap, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (row["coin_id"], row["price_usd"], row["market_cap"], row["timestamp"]))

    conn.commit()
    cur.close()
    conn.close()


load_fact_task = PythonOperator(
    task_id="load_fact_table",
    python_callable=load_fact,
    dag=dag
)


# -----------------------------------------------------
# DAG dependencies
# -----------------------------------------------------
extract_task >> transform_task >> validate_task >> load_dim_task >> load_fact_task
