import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from services.spark.__main__ import SparkAnalysisService
from pyspark.sql import functions as f
from db.tables import Tables

APP_NAME = "Spark Analysis"
URL = "jdbc:postgresql://db:5432/fuel_analysis"
DB_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

def correlation_analysis_page():
    st.title("Análise da Correlação com o Dólar")

    spark_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

    df_venda = spark_service.load_from_db_table(Tables.FATO_VENDA)
    df_tempo = spark_service.load_from_db_table(Tables.DIM_TEMPO)
    df_produto = spark_service.load_from_db_table(Tables.DIM_PRODUTO)
    df_dollar = spark_service.load_from_db_table(Tables.DOLLAR_INFO)

    df_filtered = df_venda.join(df_tempo, "tempo_id").join(df_produto, "produto_id")
    df_gasolina = df_filtered.filter(df_produto.produto == "GASOLINA")
    df_gasolina = df_gasolina.select("data_coleta", "valor_venda").withColumnRenamed("data_coleta", "data")

    df_dollar = df_dollar.select("data", "valor").withColumnRenamed("valor", "valor_dollar")

    df_merged = df_gasolina.join(df_dollar, "data")

    df_pandas = df_merged.toPandas()

    df_pandas["valor_venda"] = df_pandas["valor_venda"].astype(float)
    df_pandas["valor_dollar"] = df_pandas["valor_dollar"].astype(float)

    correlation = df_pandas["valor_venda"].corr(df_pandas["valor_dollar"])
    st.write(f"Correlação entre o preço da gasolina e o valor do dólar: {correlation:.2f}")

    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df_pandas, x="valor_dollar", y="valor_venda", alpha=0.5)
    sns.regplot(data=df_pandas, x="valor_dollar", y="valor_venda", scatter=False, color="r")
    plt.title("Correlação entre o Preço da Gasolina e o Valor do Dólar")
    plt.xlabel("Valor do Dólar")
    plt.ylabel("Preço da Gasolina (R$)")
    plt.grid(True)
    st.pyplot(plt)

    st.header("Observações")
    st.markdown("""
    - **Correlação Negativa:** A análise mostra uma correlação negativa inesperada entre o preço da gasolina e o valor do dólar. Este fenômeno sugere que outros fatores além do valor do dólar estão influenciando os preços da gasolina no Brasil.
    - **Variabilidade dos Preços:** A grande dispersão dos pontos indica que há uma alta variabilidade nos preços da gasolina que não é explicada apenas pelo valor do dólar, sugerindo a necessidade de considerar outros fatores econômicos e regulatórios.
    """)

if __name__ == "__main__":
    correlation_analysis_page()
