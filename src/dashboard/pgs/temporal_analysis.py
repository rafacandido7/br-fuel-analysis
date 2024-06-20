import streamlit as st
import matplotlib.pyplot as plt
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

def temporal_analysis_page():
    st.title("Análise Temporal de Preços de Combustíveis")

    st.header("Selecione o Ano e Tipo de Combustível")
    selected_year = st.slider("Ano", 2004, 2023, 2023)
    fuel_types = ["GASOLINA", "GASOLINA ADITIVADA", "DIESEL S10", "DIESEL", "GNV", "ETANOL"]
    selected_fuel = st.selectbox("Tipo de Combustível", fuel_types)

    st.write(f"Ano selecionado: {selected_year}")
    st.write(f"Tipo de combustível selecionado: {selected_fuel}")

    st.header("Resultados da Análise Temporal")

    spark_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

    df_venda = spark_service.load_from_db_table(Tables.FATO_VENDA)
    df_tempo = spark_service.load_from_db_table(Tables.DIM_TEMPO)
    df_produto = spark_service.load_from_db_table(Tables.DIM_PRODUTO)

    df_filtered = df_venda.join(df_tempo, "tempo_id").join(df_produto, "produto_id")
    df_filtered = df_filtered.filter((df_produto.produto == selected_fuel) & (df_tempo.ano == selected_year))

    st.write("Dados Filtrados:")
    st.dataframe(df_filtered.limit(5).toPandas())

    df_grouped = df_filtered.groupBy("mes").agg(f.mean("valor_venda").alias("preco_medio"))

    # Debug: Verificar dados agrupados
    st.write("Dados Agrupados:")
    st.dataframe(df_grouped.limit(5).toPandas())

    # Converter para Pandas para plotagem
    df_pandas = df_grouped.toPandas()
    df_pandas = df_pandas.sort_values(by="mes")

    # Plotar os dados
    plt.figure(figsize=(10, 6))
    plt.plot(df_pandas["mes"], df_pandas["preco_medio"], marker='o')
    plt.title(f"Preços Médios de {selected_fuel} em {selected_year}")
    plt.xlabel("Mês")
    plt.ylabel("Preço Médio de Venda")
    plt.grid(True)
    plt.xticks(range(1, 13))
    plt.tight_layout()

    # Exibir o gráfico no Streamlit
    st.pyplot(plt)

if __name__ == "__main__":
    temporal_analysis_page()
