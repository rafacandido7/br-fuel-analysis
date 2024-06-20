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

    st.header("Comparação de Preços entre Diferentes Estados")
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

    df_grouped = df_filtered.groupBy("estado_sigla").agg(f.mean("valor_venda").alias("preco_medio"))

    df_pandas = df_grouped.toPandas()

    df_pandas = df_pandas.dropna()
    df_pandas["preco_medio"] = df_pandas["preco_medio"].astype(float)

    df_pandas = df_pandas.sort_values(by='estado_sigla')

    fig = px.bar(df_pandas, x='estado_sigla', y='preco_medio',
                 title=f'Comparação de Preços de {selected_fuel} por Estado em {selected_year}',
                 labels={'estado_sigla': 'Estado', 'preco_medio': 'Preço Médio de Venda'},
                 color='estado_sigla',
                 height=600)

    fig.update_layout(
        xaxis_title="Estado",
        yaxis_title="Preço Médio de Venda",
        yaxis=dict(tickformat=".2f"),
        xaxis=dict(categoryorder='category ascending')
    )

    st.plotly_chart(fig)

    st.header("Observações")
    st.markdown(f"""
    - **Variações por Estado**: Há variações significativas nos preços de {selected_fuel} entre os estados, influenciadas por fatores regionais e locais.
    - **Fatores Influentes**: Custos de transporte, impostos estaduais e demanda local são fatores que podem contribuir para essas variações de preços.
    """)

if __name__ == "__main__":
    geographic_analysis_page()
