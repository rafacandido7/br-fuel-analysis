import streamlit as st
import plotly.express as px
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

def geographic_analysis_page():
    st.title("Análise Geográfica de Preços de Combustíveis")

    st.header("Comparação de Preços entre Diferentes Regiões")
    selected_year = st.slider("Ano", 2004, 2023, 2023)
    fuel_types = ["GASOLINA", "GASOLINA ADITIVADA", "DIESEL S10", "DIESEL", "GNV", "ETANOL"]
    selected_fuel = st.selectbox("Tipo de Combustível", fuel_types)

    spark_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

    df_venda = spark_service.load_from_db_table(Tables.FATO_VENDA)
    df_tempo = spark_service.load_from_db_table(Tables.DIM_TEMPO)
    df_localizacao = spark_service.load_from_db_table(Tables.DIM_LOCALIZACAO)
    df_produto = spark_service.load_from_db_table(Tables.DIM_PRODUTO)

    df_filtered = df_venda.join(df_tempo, "tempo_id").join(df_localizacao, "localizacao_id").join(df_produto, "produto_id")
    df_filtered = df_filtered.filter((df_tempo.ano == selected_year) & (df_produto.produto == selected_fuel))

    df_grouped = df_filtered.groupBy("regiao_sigla").agg(f.mean("valor_venda").alias("preco_medio"))

    df_pandas = df_grouped.toPandas()

    fig = px.bar(df_pandas, x="regiao_sigla", y="preco_medio",
                 title=f"Comparação de Preços de {selected_fuel} por Região em {selected_year}",
                 labels={"regiao_sigla": "Região", "preco_medio": "Preço Médio de Venda"},
                 color="regiao_sigla")

    fig.update_layout(
        xaxis_title="Região",
        yaxis_title="Preço Médio de Venda"
    )

    st.plotly_chart(fig)

    st.header("Observações")
    st.markdown(f"""
    - **Fatores Influentes**: Fatores como custos de transporte, impostos regionais e demanda local podem influenciar essas variações de preços.
    """)

if __name__ == "__main__":
    geographic_analysis_page()
