from pyspark.sql import SparkSession
from pyspark.sql import Row

def main():
    # Inicializa a SparkSession
    spark = SparkSession.builder \
        .appName("InMemorySparkJob") \
        .master("spark://172.17.250.14:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .getOrCreate()
    
    # Cria dados em memória
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
    
    # Mostra o resultado do agrupamento e soma
    result.show()
    
    # Encerra a SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
