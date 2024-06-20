import os
import boto3
from dotenv import load_dotenv
from botocore.client import Config
from rich.console import Console
from rich.logging import RichHandler
import logging

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY_ID = os.getenv('MINIO_ACCESS_KEY_ID')
MINIO_SECRET_ACCESS_KEY = os.getenv('MINIO_SECRET_ACCESS_KEY')
MINIO_REGION = os.getenv('MINIO_REGION')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True)]
)
logger = logging.getLogger("MinIOOperations")

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY_ID,
    aws_secret_access_key=MINIO_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4'),
    region_name=MINIO_REGION
)

def bucket_exists(bucket_name):
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    return bucket_name in buckets

def create_bucket_if_not_exists(bucket_name):
    if not bucket_exists(bucket_name):
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"[bold green]Criando bucket:[/bold green] [cyan]{bucket_name}[/cyan]")

def save_raw_data(file_paths):
    create_bucket_if_not_exists(MINIO_BUCKET_NAME)

    for file_path in file_paths:
        object_name = file_path.split('/').pop()
        logger.info(f">>> [bold green]Uploading[/bold green] [cyan]{file_path}[/cyan] [bold green]as[/bold green] [cyan]{object_name}[/cyan]")
        s3.upload_file(file_path, MINIO_BUCKET_NAME, object_name)

    logger.info("[+] [bold green]Arquivos salvos com sucesso![/bold green]")

    return list_files_in_bucket(MINIO_BUCKET_NAME)

def list_files_in_bucket(bucket_name):
    objects = s3.list_objects_v2(Bucket=bucket_name)
    files = [obj['Key'] for obj in objects.get('Contents', [])]

    file_paths = [f"s3a://{MINIO_BUCKET_NAME}/{file}" for file in files]

    return file_paths
