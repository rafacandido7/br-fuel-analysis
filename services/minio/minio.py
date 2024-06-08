import os
import boto3
from dotenv import load_dotenv
from botocore.client import Config

load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_ENDPOINT'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    config=Config(signature_version='s3v4'),
    region_name=os.getenv('AWS_REGION')
)

def send_file_to_minio(file_path, bucket_name, object_name):
    s3.upload_file(file_path, bucket_name, object_name)
