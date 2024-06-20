import os
import logging
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY_ID = os.getenv('MINIO_ACCESS_KEY_ID')
MINIO_SECRET_ACCESS_KEY = os.getenv('MINIO_SECRET_ACCESS_KEY')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

estimated_partitions = 200
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

spark = SparkSession.builder \
    .appName("Data Loader") \
    .master(SPARK_MASTER_URL) \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3..0") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY_ID)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

def etl(file_paths):
    print("\033[1m\033[94m ETL Service Started \n\033[0m")

    print(file_paths)

    df = extract_data(file_paths)

    df.show(5, False)

    print("\033[1m\033[92m ETL Service Completed \n\033[0m")

def extract_data(file_paths):
    df = spark.read.csv(file_paths, sep=';', inferSchema=True, header=True)
    return df
