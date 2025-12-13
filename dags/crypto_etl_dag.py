from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime as dt

# Default arguments
default_args = {
    "owner": "lokesh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define DAG
with DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for crypto data",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 3),
    catchup=False
) as dag:

    # --------------------
    # Extract
    # --------------------
    def extract():
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {"vs_currency": "usd", "per_page": 10}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data)
            df.to_csv("C:/Users/Lokesh/OneDrive/Documents/DataEngineering/datasets/crypto_raw.csv", index=False)
            print("✅ Data extracted and saved locally.")
        except requests.exceptions.RequestException as e:
            print(f"❌ API Error: {e}")
            raise e
        except Exception as e:
            print(f"❌ Unexpected Extract Error: {e}")
            raise e    

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    # --------------------
    # Transform
    # --------------------
    def transform():
        try:
            df = pd.read_csv("C:/Users/Lokesh/OneDrive/Documents/DataEngineering/datasets/crypto_raw.csv")
            df = df[["id", "symbol", "name", "current_price", "market_cap", "price_change_percentage_24h"]]
            df["last_updated"] = dt.now()
            df.to_csv("C:/Users/Lokesh/OneDrive/Documents/DataEngineering/datasets/crypto_transformed.csv", index=False)
            print("✅ Data transformed and saved locally.")
        except FileNotFoundError:
            print("❌ Raw data file not found for transform.")
            raise
        except Exception as e:
            print(f"❌ Unexpected Transform Error: {e}")
            raise
    
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    # --------------------
    # Load
    # --------------------
    def load():
        try:
            df = pd.read_csv("C:/Users/Lokesh/OneDrive/Documents/DataEngineering/datasets/crypto_transformed.csv")
            conn = psycopg2.connect(
                dbname="airflow",
                user="airflow",
                password="airflow",  # use your password
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
                    INSERT INTO crypto_data_pipeline (id, symbol, name, current_price, market_cap, price_change_percentage_24h, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """), tuple(row))

            conn.commit()
            cur.close()
            conn.close()
            print("✅ Data loaded into PostgreSQL successfully.")
        except psycopg2.Error as e:
            print(f"❌ Database Error: {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected Load Error: {e}")
            raise
        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass    

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
