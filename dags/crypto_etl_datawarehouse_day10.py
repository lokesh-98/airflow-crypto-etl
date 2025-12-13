from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import great_expectations as ge

import pandas as pd
import psycopg2
import requests
from datetime import datetime

# -----------------------------
# Default DAG arguments
# -----------------------------
default_args = {
    "owner": "data_engineer",
    "start_date": days_ago(1),
    "retries": 1
}

dag = DAG(
    "crpyto_coin_data_warehouse_day10",
    default_args=default_args,
    description="Day 10: ETL + GE Validation + Warehouse Load",
    schedule_interval="@daily",
    catchup=False,
)

# -----------------------------
# 1️⃣ Extract Task
# -----------------------------
def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc"}

    response = requests.get(url, params=params, timeout=10)
    df = pd.DataFrame(response.json())
    df.to_csv("/opt/airflow/datasets/coin_raw.csv", index=False)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag
)

# -----------------------------
# 2️⃣ Transform Task
# -----------------------------
def transform_data():
    df = pd.read_csv("/opt/airflow/datasets/coin_raw.csv")

    df = df[["id", "symbol", "name", "current_price", "market_cap", "last_updated"]]

    df = df.rename(columns={
        "id": "coin_id",
        "current_price": "price_usd",
        "last_updated": "timestamp"
    })

    df.to_csv("/opt/airflow/datasets/coin_transformed.csv", index=False)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)

# -----------------------------
# 3️⃣ Validation (Great Expectations)
# -----------------------------
def run_ge_validation():
    context = ge.DataContext("/opt/airflow/great_expectations")

    result = context.run_checkpoint(
        checkpoint_name="coin_checkpoint"
    )

    if not result["success"]:
        raise ValueError("Great Expectations validation failed")

validate_task = PythonOperator(
    task_id="validate_coin_data",
    python_callable=run_ge_validation,
    dag=dag
)

# -----------------------------
# 4️⃣ Load Dimension Table
# -----------------------------
def load_dim():
    df = pd.read_csv("/opt/airflow/datasets/coin_transformed.csv")

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

# -----------------------------
# 5️⃣ Load Fact Table
# -----------------------------
def load_fact():
    df = pd.read_csv("/opt/airflow/datasets/coin_transformed.csv")

    conn = psycopg2.connect(
        "dbname=warehouse user=postgres password=postgres host=postgres"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO coin_prices_fact (coin_id, price_usd, market_cap, timestamp)
            VALUES (%s, %s, %s, %s);
        """, (row["coin_id"], row["price_usd"], row["market_cap"], row["timestamp"]))

    conn.commit()
    cur.close()
    conn.close()

load_fact_task = PythonOperator(
    task_id="load_fact_table",
    python_callable=load_fact,
    dag=dag
)

# -----------------------------
# DAG Dependencies
# -----------------------------
extract_task >> transform_task >> validate_task >> load_dim_task >> load_fact_task
