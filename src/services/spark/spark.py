import os
from uuid import uuid4
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f
from pyspark.sql.functions import expr

load_dotenv()

estimated_partitions = 200

# Caminho completo do JAR
SPARK_PSQL_JAR_PATH = os.getenv("SPARK_PSQL_JAR_PATH")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

spark = SparkSession.builder \
    .appName("Data Loader") \
    .master(SPARK_MASTER_URL) \
    .config("spark.sql.shuffle.partitions", estimated_partitions) \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "4") \
    .getOrCreate()
    # .config("spark.jars", SPARK_PSQL_JAR_PATH) \

# Função para carregar dados das dimensões existentes
def load_dimensions():
    url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    
    dim_produto = spark.read.jdbc(url=url, table="dim_produto", properties=properties)
    dim_localizacao = spark.read.jdbc(url=url, table="dim_localizacao", properties=properties)
    
    return dim_produto, dim_localizacao

# Função para gerar UUID
def generate_uuid():
    return str(uuid4())

# UDF para gerar UUID
udf_generate_uuid = f.udf(generate_uuid, StringType())

def etl(file_paths):
    print("\033[1m\033[94m ETL Service Started \n\033[0m")
    
    # Carregar dimensões
    # dim_produto, dim_localizacao = load_dimensions()
    
    # df = extract_data(file_paths)
    data = [
        Row(category='A', amount=100),
        Row(category='B', amount=150),
        Row(category='A', amount=200),
        Row(category='C', amount=120),
        Row(category='B', amount=100),
        Row(category='C', amount=180)
    ]
    
    # Cria um DataFrame a partir dos dados em memória
    df = spark.createDataFrame(data)
    
    # Mostra o conteúdo inicial do DataFrame
    df.show()
    
    # Realiza o agrupamento e a soma
    result = df.groupBy("category").sum("amount")
    print(result)
    # df_sanitized = handle_data(df, dim_produto, dim_localizacao)
    # save_to_postgresql(df_sanitized)
    
    print("\033[1m\033[92m ETL Service Completed \n\033[0m")

def extract_data(file_paths):
    df = spark.read.csv(file_paths, sep=';', inferSchema=True, header=True)
    return df

def handle_data(df, dim_produto, dim_localizacao):
    df = df.withColumn(
        "data_coleta",
        f.to_date(f.col("Data da Coleta").cast(StringType()), 'dd/MM/yyyy')
    ).withColumn('valor_venda', f.regexp_replace('Valor de Venda', ',', '.')) \
     .withColumn('valor_venda', f.col('Valor de Venda').cast(DoubleType())) \
     .withColumn('valor_compra', f.regexp_replace('Valor de Compra', ',', '.')) \
     .withColumn('valor_compra', f.col('Valor de Compra').cast(DoubleType())) \
     .fillna({'complemento': 'S/C'})

    df = df.withColumn("tempo_id", expr("uuid()")) \
           .withColumn("localizacao_id", expr("uuid()")) \
           .withColumn("produto_id", expr("uuid()")) \
           .withColumn("revenda_id", expr("uuid()")) \
           .withColumn("endereco_id", expr("uuid()")) \
           .withColumn("venda_id", expr("uuid()"))
           
    df = df.repartition(40)
    
    # Manter apenas as novas entradas para a dimensão produto
    new_produto_df = df.select("produto").distinct().subtract(dim_produto.select("produto")).withColumn("produto_id", udf_generate_uuid())
    
    # Atualizar o DataFrame principal para usar os produto_id existentes ou novos
    df = df.join(dim_produto, ["produto"], "left_outer") \
           .withColumn("produto_id", f.coalesce(dim_produto["produto_id"], new_produto_df["produto_id"]))
    
    # Manter apenas as novas entradas para a dimensão localizacao
    new_localizacao_df = df.select("regiao_sigla", "estado_sigla", "municipio", "bairro", "cep").distinct().subtract(dim_localizacao.select("regiao_sigla", "estado_sigla", "municipio", "bairro", "cep")).withColumn("localizacao_id", udf_generate_uuid())
    
    # Atualizar o DataFrame principal para usar os localizacao_id existentes ou novos
    df = df.join(dim_localizacao, ["regiao_sigla", "estado_sigla", "municipio", "bairro", "cep"], "left_outer") \
           .withColumn("localizacao_id", f.coalesce(dim_localizacao["localizacao_id"], new_localizacao_df["localizacao_id"]))
    
    return df

def save_to_postgresql(df):
    properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}?schema=public"

    save_time_dimension(df, url, properties)
    save_localizacao(df, url, properties)
    save_produto(df, url, properties)
    save_revenda(df, url, properties)
    save_endereco(df, url, properties)
    save_venda(df, url, properties)

def save_time_dimension(df, url, properties):
    df.select(
        f.col("tempo_id"),
        f.col("data_coleta"),
        f.dayofmonth(f.col("data_coleta")).alias("dia"),
        f.month(f.col("data_coleta")).alias("mes"),
        f.year(f.col("data_coleta")).alias("ano"),
        f.dayofweek(f.col("data_coleta")).alias("dia_semana")
    ).write.jdbc(url, "dim_tempo", mode="append", properties=properties)

def save_localizacao(df, url, properties):
    new_localizacao_df = df.select(
        f.col("localizacao_id"),
        f.col("regiao_sigla"),
        f.col("estado_sigla"),
        f.col("municipio"),
        f.col("bairro"),
        f.col("cep")
    ).distinct()
    
    new_localizacao_df.write.jdbc(url, "dim_localizacao", mode="append", properties=properties)

def save_produto(df, url, properties):
    new_produto_df = df.select(
        f.col("produto_id"),
        f.col("produto"),
        f.col("bandeira")
    ).distinct()
    
    new_produto_df.write.jdbc(url, "dim_produto", mode="append", properties=properties)

def save_revenda(df, url, properties):
    df.select(
        f.col("revenda_id"),
        f.col("cnpj_revenda"),
        f.col("nome_revenda")
    ).write.jdbc(url, "dim_revenda", mode="append", properties=properties)

def save_endereco(df, url, properties):
    df.select(
        f.col("endereco_id"),
        f.col("nome_rua"),
        f.col("numero_rua"),
        f.col("complemento")
    ).write.jdbc(url, "dim_endereco", mode="append", properties=properties)

def save_venda(df, url, properties):
    df.select(
        f.col("venda_id"),
        f.col("tempo_id"),
        f.col("localizacao_id"),
        f.col("produto_id"),
        f.col("revenda_id"),
        f.col("endereco_id"),
        f.col("valor_venda"),
        f.col("valor_compra")
    ).write.jdbc(url, "fato_venda", mode="append", properties=properties)