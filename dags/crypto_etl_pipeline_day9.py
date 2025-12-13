from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import psycopg2
from psycopg2 import sql
from minio import Minio
from datetime import datetime as dt

default_args = {
    "owner": "lokesh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_etl_pipeline_day9",
    default_args=default_args,
    description="ETL pipeline with MinIO upload",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 3),
    catchup=False
) as dag:

    # ---------------------------
    # 1️⃣ Extract
    # ---------------------------
    def extract():
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "per_page": 10}

        response = requests.get(url, params=params, timeout=15, verify=False)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df.to_csv("/opt/airflow/datasets/crypto_raw.csv", index=False)

        print("✅ Extracted crypto data")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    # ---------------------------
    # 2️⃣ Transform
    # ---------------------------
    def transform():
        df = pd.read_csv("/opt/airflow/datasets/crypto_raw.csv")

        df = df[[
            "id", "symbol", "name",
            "current_price", "market_cap",
            "price_change_percentage_24h"
        ]]
        df["last_updated"] = dt.now()

        df.to_csv("/opt/airflow/datasets/crypto_transformed.csv", index=False)

        print("✅ Transformed crypto data")

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    # ---------------------------
    # 3️⃣ Load to PostgreSQL
    # ---------------------------
    def load():
        df = pd.read_csv("/opt/airflow/datasets/crypto_transformed.csv")

        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port=5432
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_data_pipeline (
                id TEXT,
                symbol TEXT,
                name TEXT,
                current_price FLOAT,
                market_cap BIGINT,
                price_change_percentage_24h FLOAT,
                last_updated TIMESTAMP
            );
        """)

        for _, row in df.iterrows():
            cur.execute(sql.SQL("""
                INSERT INTO crypto_data_pipeline 
                (id, symbol, name, current_price, market_cap, 
                 price_change_percentage_24h, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """), tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        print("✅ Loaded into PostgreSQL")

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    # ---------------------------
    # 4️⃣ Upload to MinIO
    # ---------------------------
    def upload_file_to_minio():
        local_file = "/opt/airflow/datasets/crypto_transformed.csv"

        client = Minio(
            "minio:9000",                # ✔ FIXED (use Docker service name)
            access_key="admin",
            secret_key="admin123",
            secure=False
        )

        bucket = "processed"
        object_name = "crypto/crypto_transformed.csv"

        # Create bucket if missing
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print("🪣 Created bucket:", bucket)

        # Upload file
        client.fput_object(bucket, object_name, local_file)

        print("✅ Uploaded crypto_transformed.csv to MinIO")

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_file_to_minio
    )

    # ---------------------------
    # DAG ORDER
    # ---------------------------
    extract_task >> transform_task >> load_task >> upload_task
