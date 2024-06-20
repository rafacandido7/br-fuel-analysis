from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Data Loader") \
    .master("spark://localhost:7077") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAZI2L3L76DUPLVOCOO") \
    .config("spark.hadoop.fs.s3a.secret.key", "SKumX18fA5Agfdf4EYS2SGWMpgw9teYI4m/dVpf") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Get the SparkContext from the SparkSession
sc = spark.sparkContext


# Additional configuration for SparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZI2L3L76DUPLVOCOO")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "SKumX18fA5Agfdf4EYS2SGWMpgw9teYI4m/dVpf")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Your Spark code goes here
sc.setLogLevel("INFO")


# Read a JSON file from an MinIO bucket using the access key, secret key,
# and endpoint configured above
df = spark.read.csv('s3a://bi-fuel-data/test.csv', sep=';', inferSchema=True, header=True)
# show data
df.show(5, False)
