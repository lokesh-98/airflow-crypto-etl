from minio import Minio
from minio.error import S3Error
import os

# 1️⃣ Connect to MinIO
client = Minio(
    "host.docker.internal:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# 2️⃣ Upload function
def upload_to_minio(local_file, bucket_name, object_name):
    try:
        # Debug: Print the file path to check if it's correct
        print(f"Attempting to upload file from path: {local_file}")
        
        # Check if the file exists
        if not os.path.exists(local_file):
            print(f"❌ File not found at {local_file}")
            return
        
        # Check if the bucket exists, otherwise create it
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        
        # Upload the file
        client.fput_object(bucket_name, object_name, local_file)
        print(f"✅ Uploaded {local_file} → {bucket_name}/{object_name}")

    except S3Error as e:
        print(f"❌ MinIO error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Example call — upload your transformed CSV
local_file = r"C:\Users\Lokesh\OneDrive\Documents\DataEngineering\datasets\crypto_data.csv"  # Ensure path is correct

upload_to_minio(
    local_file=local_file,
    bucket_name="processed",
    object_name="crypto/crypto_transformed.csv"
)
