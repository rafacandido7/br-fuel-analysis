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

bucket_name = os.getenv('AWS_BUCKET_NAME')

def save_raw_data(file_paths):
    for file_path in file_paths:
        object_name = os.path.basename(file_path)
        print(object_name)
        # send_file_to_minio(file_path, object_name)

def send_file_to_minio(file_path, object_name):
    s3.upload_file(file_path, object_name)

# def file_exists(object_name):
#     try:
#         s3.head_object(Bucket=bucket_name, Key=object_name)
#         return True
#     except Error as e:
#         if e.response['Error']['Code'] == '404':
#             return False
#         else:
#             raise e
