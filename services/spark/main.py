import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f
from pyspark.sql.functions import expr

estimated_partitions = 200

spark = SparkSession.builder \
    .appName("Data Loader") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.24.jar") \
    .config("spark.sql.shuffle.partitions", estimated_partitions) \
    .getOrCreate()

def etl(file_paths):
    print("\033[1m\033[94m ETL Service Started \n\033[0m")
    df = extract_data(file_paths)
    df_sanitized = handle_data(df)
    save_to_postgresql(df_sanitized)
    print("\033[1m\033[92m ETL Service Completed \n\033[0m")

def extract_data(file_paths):
    df = spark.read.csv(file_paths, sep=';', inferSchema=True, header=True)
    return df

def handle_data(df):
    df = df.withColumn(
        "Data da Coleta",
        f.to_date(f.col("Data da Coleta").cast(StringType()), 'dd/MM/yyyy')
    ).withColumn('Valor de Venda', f.regexp_replace('Valor de Venda', ',', '.')) \
    .withColumn('Valor de Venda', f.col('Valor de Venda').cast(DoubleType())) \
    .withColumn('Valor de Compra', f.regexp_replace('Valor de Compra', ',', '.')) \
    .withColumn('Valor de Compra', f.col('Valor de Compra').cast(DoubleType())) \
    .withColumn("Tempo_ID", expr("uuid()")) \
    .withColumn("Localizacao_ID", expr("uuid()")) \
    .withColumn("Produto_ID", expr("uuid()")) \
    .withColumn("Revenda_ID", expr("uuid()")) \
    .withColumn("Endereco_ID", expr("uuid()")) \
    .withColumn("Venda_ID", expr("uuid()")) \
    .fillna({'Complemento': 'S/C'})

    df = df.repartition(40)

    return df

def save_to_postgresql(df):
    properties = {
          "user": os.getenv("POSTGRES_USER"),
          "password": os.getenv("POSTGRES_PASSWORD"),
          "driver": "org.postgresql.Driver"
      }
    url = f"jdbc:postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}?schema=public"

    save_time(df, url, properties)
    save_localizacao(df, url, properties)
    save_produto(df, url, properties)
    save_revenda(df, url, properties)
    save_endereco(df, url, properties)
    save_venda(df, url, properties)

def save_time(df, url, properties):
    df.select(
        f.col("Tempo_ID").alias("tempo_id"),
        f.col("Data da Coleta").alias("data_coleta"),
        f.dayofmonth(f.col("Data da Coleta")).alias("dia"),
        f.month(f.col("Data da Coleta")).alias("mes"),
        f.year(f.col("Data da Coleta")).alias("ano"),
        f.dayofweek(f.col("Data da Coleta")).alias("dia_semana")
    ).write.jdbc(url, "DIM_Tempo", mode="append", properties=properties)

def save_localizacao(df, url, properties):
    df.select(
        f.col("Localizacao_ID").alias("localizacao_id"),
        f.col("Regiao - Sigla").alias("regiao_sigla"),
        f.col("Estado - Sigla").alias("estado_sigla"),
        f.col("Municipio").alias("municipio"),
        f.col("Bairro").alias("bairro"),
        f.col("Cep").alias("cep")
    ).write.jdbc(url, "DIM_Localizacao", mode="append", properties=properties)

def save_produto(df, url, properties):
    df.select(
        f.col("Produto_ID").alias("produto_id"),
        f.col("Produto").alias("produto"),
        f.col("Bandeira").alias("bandeira")
    ).write.jdbc(url, "DIM_Produto", mode="append", properties=properties)

def save_revenda(df, url, properties):
    df.select(
        f.col("Revenda_ID").alias("revenda_id"),
        f.col("CNPJ da Revenda").alias("cnpj_revenda"),
        f.col("Revenda").alias("nome_revenda")
    ).write.jdbc(url, "DIM_Revenda", mode="append", properties=properties)

def save_endereco(df, url, properties):
    df.select(
        f.col("Endereco_ID").alias("endereco_id"),
        f.col("Nome da Rua").alias("nome_rua"),
        f.col("Numero Rua").alias("numero_rua"),
        f.col("Complemento").alias("complemento")
    ).write.jdbc(url, "DIM_Endereco", mode="append", properties=properties)

def save_venda(df, url, properties):
    df.select(
        f.col("Venda_ID").alias("venda_id"),
        f.col("Tempo_ID").alias("tempo_id"),
        f.col("Localizacao_ID").alias("localizacao_id"),
        f.col("Produto_ID").alias("produto_id"),
        f.col("Revenda_ID").alias("revenda_id"),
        f.col("Endereco_ID").alias("endereco_id"),
        f.col("Valor de Venda").alias("valor_venda"),
        f.col("Valor de Compra").alias("valor_compra")
    ).write.jdbc(url, "FATO_Venda", mode="append", properties=properties)

