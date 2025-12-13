from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
from minio import Minio
from minio.error import S3Error

# --- Default Airflow args ---
default_args = {
    "owner": "lokesh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Connect to MinIO ---
client = Minio(
    "host.docker.internal:9000",  # Airflow container -> host MinIO
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# --- Upload function ---
def upload_to_minio():
    local_file = "C:/Users/Lokesh/OneDrive/Documents/DataEngineering/datasets/crypto_data.csv"
    bucket = "processed"
    object_name = "crypto/crypto_transformed.csv"

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"✅ Created bucket: {bucket}")
        client.fput_object(bucket, object_name, local_file)
        print(f"✅ Uploaded {local_file} → {bucket}/{object_name}")

    except S3Error as e:
        print(f"❌ MinIO error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# --- ETL Function ---
def run_etl():
    try:
        subprocess.run(
            ["python", "C:/Users/Lokesh/OneDrive/Documents/DataEngineering/scripts/etl_pipeline.py"],
            check=True
        )
        print("✅ ETL script executed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ ETL process failed: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise

# --- DAG Definition ---
with DAG(
    dag_id="crypto_etl_pipeline_v4",
    default_args=default_args,
    description="Daily ETL + Upload to MinIO",
    start_date=datetime(2025, 11, 3),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio
    )

    etl_task >> upload_task
