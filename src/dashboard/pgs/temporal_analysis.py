import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
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

    spark_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

    df_venda = spark_service.load_from_db_table(Tables.FATO_VENDA)
    df_tempo = spark_service.load_from_db_table(Tables.DIM_TEMPO)
    df_produto = spark_service.load_from_db_table(Tables.DIM_PRODUTO)

    df_filtered_all = df_venda.join(df_tempo, "tempo_id").join(df_produto, "produto_id")
    df_filtered_all = df_filtered_all.filter(df_tempo.ano == selected_year)

    df_grouped_all = df_filtered_all.groupBy("mes", "produto").agg(f.mean("valor_venda").alias("preco_medio"))

    df_pandas_all = df_grouped_all.toPandas()
    df_pandas_all = df_pandas_all.sort_values(by=["mes", "produto"])

    fig_all = px.line(df_pandas_all, x="mes", y="preco_medio", color="produto",
                      title=f"Preços Médios de Combustíveis em {selected_year}",
                      labels={"mes": "Mês", "preco_medio": "Preço Médio de Venda", "produto": "Tipo de Combustível"})

    fig_all.update_layout(
        xaxis=dict(
            tickmode='linear',
            dtick=1,
            title="Mês"
        ),
        yaxis=dict(
            title="Preço Médio de Venda"
        )
    )

    st.plotly_chart(fig_all)

    fuel_types = ["GASOLINA", "GASOLINA ADITIVADA", "DIESEL S10", "DIESEL", "GNV", "ETANOL"]
    selected_fuel = st.selectbox("Tipo de Combustível", fuel_types)

    df_filtered_specific = df_filtered_all.filter(df_produto.produto == selected_fuel)

    df_grouped_specific = df_filtered_specific.groupBy("mes").agg(f.mean("valor_venda").alias("preco_medio"))

    df_pandas_specific = df_grouped_specific.toPandas()
    df_pandas_specific = df_pandas_specific.sort_values(by="mes")

    fig_specific = go.Figure()
    fig_specific.add_trace(go.Scatter(x=df_pandas_specific["mes"], y=df_pandas_specific["preco_medio"], mode='lines+markers', name=selected_fuel))
    fig_specific.update_layout(
        title=f"Preços Médios de {selected_fuel} em {selected_year}",
        xaxis_title="Mês",
        yaxis_title="Preço Médio de Venda",
        xaxis=dict(
            tickmode='linear',
            dtick=1
        )
    )

    st.plotly_chart(fig_specific)

    # Boxplot para todos os combustíveis
    st.header("Boxplot dos Preços de Todos os Combustíveis")
    df_boxplot = df_filtered_all.toPandas()

    # Converter a coluna 'valor_venda' para float
    df_boxplot['valor_venda'] = df_boxplot['valor_venda'].astype(float)

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    df_boxplot.boxplot(column='valor_venda', by='produto', ax=ax)
    plt.title("Boxplot dos Preços de Todos os Combustíveis")
    plt.suptitle("")
    plt.xlabel("Combustíveis")
    plt.ylabel("Preço de Venda")
    plt.xticks(rotation=45)
    plt.tight_layout()

    st.pyplot(plt)

if __name__ == "__main__":
    temporal_analysis_page()
