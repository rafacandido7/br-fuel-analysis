from rich.console import Console
from rich.logging import RichHandler
from services.minio.minio import save_raw_data
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f
from db.tables import Tables
import logging

# Configuração básica do logging com rich
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Remove prefixos padrão do logging
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True)]
)
logger = logging.getLogger("SparkAnalysis")

class SparkAnalysisService:
    def __init__(self, app_name, db_url, properties):
        self.spark = SparkSession.builder \
          .appName(app_name) \
          .master("local[*]") \
          .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
          .getOrCreate()
        self.db_url = db_url
        self.properties = properties

        sc = self.spark.sparkContext
        sc.setLogLevel("ERROR")

    def extract_data(self, path):
        """
        Extrai dados de um arquivo CSV.

        Args:
            path (str): Caminho do arquivo CSV.

        Returns:
            DataFrame: DataFrame contendo os dados extraídos.
        """
        logger.info(f"[bold green]Lendo dados do arquivo:[/bold green] [cyan]{path}[/cyan]")
        df = self.spark.read.csv(path, sep=';', inferSchema=True, header=True)
        logger.info("[bold green]Dados lidos com sucesso.[/bold green]")
        return df

    def load_from_db_table(self, table_name):
        """
        Carrega uma tabela de dimensões do banco de dados.

        Args:
            table_name (Tables): Nome da tabela de dimensões.

        Returns:
            DataFrame: DataFrame contendo os dados da tabela de dimensões.
        """
        logger.info(f"[bold magenta]Carregando tabela de dimensão:[/bold magenta] [cyan]{table_name.value}[/cyan]")
        return self.spark.read.jdbc(url=self.db_url, table=table_name.value, properties=self.properties)

    def upsert_dim_table(self, df, table_name, key_columns):
        """
        Realiza upsert (inserção/atualização) de dados na tabela de dimensões.

        Args:
            df (DataFrame): DataFrame contendo os dados a serem upsertados.
            table_name (Tables): Nome da tabela de dimensões.
            key_columns (list): Lista de colunas-chave para verificação de duplicidade.
        """
        logger.info(f"[bold magenta]Realizando upsert na tabela de dimensão:[/bold magenta] [cyan]{table_name.value}[/cyan]")
        existing_df = self.load_from_db_table(table_name)
        new_df = df.join(existing_df, key_columns, 'leftanti')
        if new_df.count() > 0:
            self.load_data_to_db(new_df, table_name)
        logger.info(f"[bold magenta]Upsert realizado na tabela de dimensão:[/bold magenta] [cyan]{table_name.value}[/cyan]")

    def load_data_to_db(self, df, table_name):
        """
        Carrega dados em uma tabela no banco de dados.

        Args:
            df (DataFrame): DataFrame contendo os dados a serem carregados.
            table_name (Tables): Nome da tabela no banco de dados.
        """
        logger.info(f"[bold magenta]Carregando dados na tabela:[/bold magenta] [cyan]{table_name.value}[/cyan]")
        df.write.jdbc(url=self.db_url, table=table_name.value, mode="append", properties=self.properties)
        logger.info(f"[bold magenta]Dados carregados na tabela:[/bold magenta] [cyan]{table_name.value}[/cyan]")

    def transform_ca_data(self, df):
        """
        Transforma dados de combustível.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame transformado.
        """
        logger.info("[bold green]Transformando dados de combustível.[/bold green]")
        df = df.withColumn(
            "Data da Coleta",
            f.to_date(f.col("Data da Coleta").cast(StringType()), 'dd/MM/yyyy')
        ).withColumn('Valor de Venda', f.regexp_replace('Valor de Venda', ',', '.')) \
            .withColumn('Valor de Venda', f.col('Valor de Venda').cast(DoubleType())) \
            .withColumn('Valor de Compra', f.regexp_replace('Valor de Compra', ',', '.')) \
            .withColumn('Valor de Compra', f.col('Valor de Compra').cast(DoubleType())) \
            .dropna(subset=['Valor de Venda']) \
            .fillna({'Complemento': 'S/C'})

        df = df.withColumnRenamed('Regiao - Sigla', 'Regiao_Sigla') \
            .withColumnRenamed('Estado - Sigla', 'Estado_Sigla') \
            .withColumnRenamed('CNPJ da Revenda', 'CNPJ_Revenda') \
            .withColumnRenamed('Data da Coleta', 'Data_Coleta') \
            .withColumnRenamed('Nome da Rua', 'Nome_Rua') \
            .withColumnRenamed('Numero Rua', 'Numero_Rua') \
            .withColumnRenamed('Valor de Venda', 'Valor_Venda') \
            .withColumnRenamed('Valor de Compra', 'Valor_Compra')

        logger.info("[bold green]Dados de combustível transformados.[/bold green]")
        return df

    def transform_dollar_data(self, df):
        """
        Transforma dados da cotação do dólar.

        Args:
            df (DataFrame): DataFrame contendo os dados da cotação do dólar.

        Returns:
            DataFrame: DataFrame transformado.
        """
        logger.info("[bold green]Transformando dados da cotação do dólar.[/bold green]")
        df = df.withColumn(
            "data",
            f.to_date(f.col("data").cast(StringType()), 'dd/MM/yyyy')
        ).withColumn('valor', f.regexp_replace('valor', ',', '.')) \
            .withColumn('valor', f.col('valor').cast(DoubleType())) \
            .withColumn('dia', f.dayofmonth(f.col('data'))) \
            .withColumn('mes', f.month(f.col('data'))) \
            .withColumn('ano', f.year(f.col('data'))) \
            .withColumn('dia_semana', f.dayofweek(f.col('data')))

        logger.info("[bold green]Dados da cotação do dólar transformados.[/bold green]")
        return df

    def create_dim_tempo(self, df):
        """
        Cria a tabela de dimensões de tempo.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame contendo as dimensões de tempo.
        """
        logger.info("[bold green]Criando tabela de dimensão de tempo.[/bold green]")
        dim_tempo = df.select(
            f.col("Data_Coleta").alias("data_coleta"),
            f.dayofmonth("Data_Coleta").alias("dia"),
            f.month("Data_Coleta").alias("mes"),
            f.year("Data_Coleta").alias("ano"),
            f.dayofweek("Data_Coleta").alias("dia_semana")
        ).distinct()
        logger.info("[bold green]Tabela de dimensão de tempo criada.[/bold green]")
        return dim_tempo

    def create_dim_localizacao(self, df):
        """
        Cria a tabela de dimensões de localização.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame contendo as dimensões de localização.
        """
        logger.info("[bold green]Criando tabela de dimensão de localização.[/bold green]")
        dim_localizacao = df.select(
            f.col("Regiao_Sigla").alias("regiao_Sigla"),
            f.col("Estado_Sigla").alias("estado_Sigla"),
            f.col("Municipio").alias("municipio"),
            f.col("Bairro").alias("bairro"),
            f.col("Cep").alias("cep")
        ).distinct()
        logger.info("[bold green]Tabela de dimensão de localização criada.[/bold green]")
        return dim_localizacao

    def create_dim_produto(self, df):
        """
        Cria a tabela de dimensões de produto.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame contendo as dimensões de produto.
        """
        logger.info("[bold green]Criando tabela de dimensão de produto.[/bold green]")
        dim_produto = df.select(f.col("Produto").alias("produto")).distinct()
        logger.info("[bold green]Tabela de dimensão de produto criada.[/bold green]")
        return dim_produto

    def create_dim_revenda(self, df):
        """
        Cria a tabela de dimensões de revenda.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame contendo as dimensões de revenda.
        """
        logger.info("[bold green]Criando tabela de dimensão de revenda.[/bold green]")
        dim_revenda = df.select(
            f.col("CNPJ_Revenda").alias("cnpj_revenda"),
            f.col("Revenda").alias("nome_revenda")
        ).distinct()
        logger.info("[bold green]Tabela de dimensão de revenda criada.[/bold green]")
        return dim_revenda

    def create_dim_endereco(self, df):
        """
        Cria a tabela de dimensões de endereço.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.

        Returns:
            DataFrame: DataFrame contendo as dimensões de endereço.
        """
        logger.info("[bold green]Criando tabela de dimensão de endereço.[/bold green]")
        dim_endereco = df.select(
            f.col("Nome_Rua").alias("nome_rua"),
            f.col("Numero_Rua").alias("numero_rua"),
            f.col("Complemento").alias("complemento")
        ).distinct()
        logger.info("[bold green]Tabela de dimensão de endereço criada.[/bold green]")
        return dim_endereco

    def create_fato_venda(self, df, dim_tempo, dim_localizacao, dim_produto, dim_revenda, dim_endereco):
        """
        Cria a tabela de fatos de vendas.

        Args:
            df (DataFrame): DataFrame contendo os dados de combustível.
            dim_tempo (DataFrame): DataFrame contendo as dimensões de tempo.
            dim_localizacao (DataFrame): DataFrame contendo as dimensões de localização.
            dim_produto (DataFrame): DataFrame contendo as dimensões de produto.
            dim_revenda (DataFrame): DataFrame contendo as dimensões de revenda.
            dim_endereco (DataFrame): DataFrame contendo as dimensões de endereço.

        Returns:
            DataFrame: DataFrame contendo os fatos de vendas.
        """
        logger.info("[bold green]Criando tabela de fatos de vendas.[/bold green]")
        fato_venda = df.join(dim_tempo, df.Data_Coleta == dim_tempo.data_coleta, 'left') \
                      .join(dim_localizacao, (df.Regiao_Sigla == dim_localizacao.regiao_sigla) &
                                            (df.Estado_Sigla == dim_localizacao.estado_sigla) &
                                            (df.Municipio == dim_localizacao.municipio) &
                                            (df.Bairro == dim_localizacao.bairro) &
                                            (df.Cep == dim_localizacao.cep), 'left') \
                      .join(dim_produto, df.Produto == dim_produto.produto, 'left') \
                      .join(dim_revenda, (df.CNPJ_Revenda == dim_revenda.cnpj_revenda) &
                                          (df.Revenda == dim_revenda.nome_revenda), 'left') \
                      .join(dim_endereco, (df.Nome_Rua == dim_endereco.nome_rua) &
                                          (df.Numero_Rua == dim_endereco.numero_rua) &
                                          (df.Complemento == dim_endereco.complemento), 'left') \
                      .select(
                          "tempo_id",
                          "localizacao_id",
                          "produto_id",
                          "revenda_id",
                          "endereco_id",
                          f.col("Valor_Venda").alias('valor_venda'),
                          f.col("Valor_Compra").alias('valor_compra'),
                      )
        logger.info("[bold green]Tabela de fatos de vendas criada.[/bold green]")
        return fato_venda

    def run_fuel_data(self, paths):
        """
        ETL dos dados de combustível.

        Args:
            paths: Caminho dos arquivos CSVs.
        """
        logger.info("[bold blue]Iniciando processo ETL dos dados de combustível.[/bold blue]")
        df = self.extract_data(paths)
        df = self.transform_ca_data(df)

        dim_tempo = self.create_dim_tempo(df)
        dim_localizacao = self.create_dim_localizacao(df)
        dim_produto = self.create_dim_produto(df)
        dim_revenda = self.create_dim_revenda(df)
        dim_endereco = self.create_dim_endereco(df)

        self.upsert_dim_table(dim_tempo, Tables.DIM_TEMPO, ["data_coleta", "dia", "mes", "ano", "dia_semana"])
        self.upsert_dim_table(dim_localizacao, Tables.DIM_LOCALIZACAO, ["regiao_sigla", "estado_sigla", "municipio", "bairro", "cep"])
        self.upsert_dim_table(dim_produto, Tables.DIM_PRODUTO, ["produto"])
        self.upsert_dim_table(dim_revenda, Tables.DIM_REVENDA, ["cnpj_revenda", "nome_revenda"])
        self.upsert_dim_table(dim_endereco, Tables.DIM_ENDERECO, ["nome_rua", "numero_rua", "complemento"])

        dim_tempo_db = self.load_from_db_table(Tables.DIM_TEMPO)
        dim_localizacao_db = self.load_from_db_table(Tables.DIM_LOCALIZACAO)
        dim_produto_db = self.load_from_db_table(Tables.DIM_PRODUTO)
        dim_revenda_db = self.load_from_db_table(Tables.DIM_REVENDA)
        dim_endereco_db = self.load_from_db_table(Tables.DIM_ENDERECO)

        fato_venda = self.create_fato_venda(df, dim_tempo_db, dim_localizacao_db, dim_produto_db, dim_revenda_db, dim_endereco_db)
        self.load_data_to_db(fato_venda, Tables.FATO_VENDA)
        logger.info("[bold blue]Processo ETL dos dados de combustível finalizado.[/bold blue]")

    def test(self):
        return print(self.load_from_db_table(Tables.DOLLAR_INFO).count())

    def run_dollar_data(self, paths):
        logger.info("[bold blue]Processando dados da cotação do dólar.[/bold blue]")
        df = self.extract_data(paths)
        cleaned_df = self.transform_dollar_data(df)

        self.load_data_to_db(cleaned_df, Tables.DOLLAR_INFO)
        logger.info("[bold blue]Dados da cotação do dólar processados.[/bold blue]")
