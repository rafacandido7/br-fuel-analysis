import streamlit as st
import pandas as pd
import streamlit_mermaid as stmd

from services.spark.__main__ import SparkAnalysisService

APP_NAME = "Spark Analysis"
URL = "jdbc:postgresql://db:5432/fuel_analysis"
DB_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

def main():
    spark_analysis_service = SparkAnalysisService(APP_NAME, URL, DB_PROPERTIES)

    st.sidebar.title("Perguntas Analíticas")

    st.title("Análise de Dados sobre Preços de Combustíveis no Brasil")
    st.markdown("## Descrição Geral do Projeto")
    st.markdown("""
    Este projeto visa analisar os dados históricos de preços de combustíveis no Brasil, utilizando um pipeline de Big Data com PySpark e Streamlit para criar um relatório analítico interativo. O objetivo é identificar padrões, tendências e desenvolver um modelo preditivo para estimar os preços futuros dos combustíveis.
    """)

    st.header("Escopo do Projeto")

    st.markdown("### Objetivo do Projeto")
    st.markdown("""
    Elaborar um relatório analítico interativo utilizando um pipeline de Big Data, PySpark e Streamlit combinados.
    """)

    st.markdown("### Qualificações do Projeto")
    st.markdown("""
    - Dados em grande volumetria/contexto real.
    - Inclusão de metadados de modelagem na pipeline.
    """)

    st.markdown("### Motivação")
    st.markdown("""
    O setor de combustíveis no Brasil desempenha um papel fundamental na economia e na vida cotidiana dos cidadãos. A variação dos preços dos combustíveis pode impactar diretamente o orçamento das famílias, a competitividade das empresas e a formulação de políticas públicas. Diante disso, é essencial realizar uma análise abrangente dos dados disponíveis para compreender os padrões e tendências dos preços dos combustíveis ao longo do tempo e em diferentes regiões do país.
    """)

    st.markdown("### Descrição dos Dados")
    st.markdown("""
    Os dados disponíveis consistem em um conjunto de informações sobre preços de combustíveis coletados em diferentes regiões do Brasil. As colunas incluem:
    - Região (sigla)
    - Estado (sigla)
    - Município
    - Revenda
    - CNPJ da Revenda
    - Nome da Rua
    - Número da Rua
    - Complemento
    - Bairro
    - CEP
    - Produto
    - Data da Coleta
    - Valor de Venda
    - Valor de Compra
    - Unidade de Medida
    - Bandeira
    """)

    st.markdown("Os dados são fornecidos pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) e abrangem um período de tempo significativo, permitindo uma análise histórica dos preços dos combustíveis.")

    st.header("Diagrama ERD")
    stmd.st_mermaid("""
    erDiagram
    DIM_Tempo {
        Tempo_ID varchar PK
        Data_Coleta date
        Dia int
        Mes int
        Ano int
        Dia_Semana int
    }
    DIM_Localizacao {
        Localizacao_ID varchar PK
        Regiao_Sigla varchar
        Estado_Sigla varchar
        Municipio varchar
        Bairro varchar
        CEP varchar
        Localizacao_Geografica_ID varchar FK
    }
    DIM_Localizacao_Geografica {
        Localizacao_Geografica_ID varchar PK
        Latitude decimal
        Longitude decimal
    }
    DIM_Produto {
        Produto_ID varchar PK
        Produto varchar
    }
    DIM_Revenda {
        Revenda_ID varchar PK
        CNPJ_Revenda varchar
        Nome_Revenda varchar
    }
    DIM_Endereco {
        Endereco_ID varchar PK
        Nome_Rua varchar
        Numero_Rua varchar
        Complemento varchar
    }
    FATO_Venda {
        Venda_ID varchar PK
        Tempo_ID varchar FK
        Localizacao_ID varchar FK
        Produto_ID varchar FK
        Revenda_ID varchar FK
        Endereco_ID varchar FK
        Valor_Venda decimal
        Valor_Compra decimal
    }

    DIM_Tempo ||--|| FATO_Venda : Tempo_ID
    DIM_Localizacao ||--|{ FATO_Venda : Localizacao_ID
    DIM_Localizacao_Geografica ||--|| DIM_Localizacao : Localizacao_Geografica_ID
    DIM_Produto ||--|| FATO_Venda : Produto_ID
    DIM_Revenda ||--|| FATO_Venda : Revenda_ID
    DIM_Endereco ||--|| FATO_Venda : Endereco_ID
    """)
    st.header("Amostra dos Dados (Não tratados)")
    data = pd.read_csv('dashboard/samples_data/test.csv', sep=";")
    st.dataframe(data)

    st.header("PRODUTOS DO PYSPARKK")
    prod = spark_analysis_service.load_dim_table('dim_produto')

    st.dataframe(prod)


    st.header("Navegação")
    st.markdown("[Análise Temporal](exploratory_analysis.py)")
    st.markdown("[Modelo Preditivo](predictive_model.py)")

if __name__ == "__main__":
    main()
