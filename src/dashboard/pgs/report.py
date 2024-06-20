import streamlit as st

def report_page():
    st.sidebar.title("Relatório Final")

    st.title("Relatório Final do Projeto de Análise de Dados sobre Preços de Combustíveis no Brasil")

    st.header("Introdução")
    st.markdown("""
    Este projeto visa analisar os dados históricos de preços de combustíveis no Brasil, utilizando um pipeline de Big Data com PySpark e Streamlit para criar um relatório analítico interativo. O objetivo é identificar padrões, tendências e desenvolver um modelo preditivo para estimar os preços futuros dos combustíveis. A análise é realizada com base em dados fornecidos pela Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) e abrange um período de tempo significativo, permitindo uma análise histórica abrangente.
    """)

    st.header("Solução Final")
    st.markdown("### Entregáveis")
    st.markdown("""
    - **Dashboard Interativo**: Um painel interativo desenvolvido com Streamlit para visualizar os dados históricos e análises.
    - **Pipeline de Big Data**: Implementação de um pipeline de Big Data utilizando PySpark para processamento e análise dos dados.
    - **Infraestrutura Local**: Um ambiente configurado e pronto para rodar análises sobre o conjunto de dados dos combustíveis. ATENÇÃO: Esse tópico é muito sensível, uma vez que depende muito dos recursos do computador do usuário. Ainda nesse relatório terá uma sessão para tratar a respeito desse aspecto.
    - **Diagramas de Arquitetura**:
        - ERD: Representa a modelagem dimensional dos dados, incluindo tabelas de fato e dimensões.
        - Fluxograma do Pipeline de Dados: Mostra o fluxo de dados desde a extração, transformação, até a carga no banco de dados.
    """)

    st.header("Sobre a Infraestrutura")
    st.markdown("""
    A infraestrutura do projeto é composta por diversos serviços configurados para rodar em um ambiente de contêineres Docker. Essa configuração é essencial para processar e analisar grandes volumes de dados de forma eficiente, porém, ela também traz um nível significativo de complexidade e demanda computacional. A seguir, são detalhados os principais componentes da infraestrutura:
    - **Spark Master**: Responsável pela coordenação das tarefas e distribuição de dados. Este serviço requer uma quantidade significativa de recursos de CPU e memória para gerenciar as operações de processamento de dados em larga escala.
    - **Banco de Dados PostgreSQL**: Utilizado para armazenar os dados processados e estruturados. A configuração inclui persistência de dados através de volumes Docker, garantindo que os dados sejam mantidos mesmo após a reinicialização dos contêineres.
    - **Minio**: Fornece um armazenamento de objetos compatível com S3, utilizado para armazenar grandes conjuntos de dados de forma eficiente e escalável.

    Para mais detalhes sobre a infraestrutura, e o passo a passo para rodar a aplicação, no repositório se encontra o README, o docker-compose e os Dockerfile que sustentam a infraestrutura do ambiente.
    """)

    st.header("Desafios Computacionais")
    st.markdown("""
    A configuração da infraestrutura é pesada e exige um ambiente com recursos computacionais robustos, como múltiplos núcleos de CPU e uma quantidade significativa de memória RAM. Em ambientes de desenvolvimento em PCs convencionais, esses recursos muitas vezes são insuficientes para suportar a carga de trabalho do projeto. Este é um ponto crucial, pois a limitação dos recursos computacionais impede a realização de análises mais abrangentes e detalhadas sobre um período de tempo maior.
    Devido a essas limitações, optou-se por concentrar as análises apenas no ano de 2023. Este foco permite maximizar a eficiência do processamento e garantir que os resultados obtidos sejam precisos e relevantes, sem sobrecarregar a infraestrutura disponível.
    """)

    st.header("Análises")
    st.markdown("### Hipóteses Iniciais")
    st.markdown("""
    - **Hipótese 1**: Os preços dos combustíveis apresentam variações significativas ao longo do tempo.
    - **Hipótese 2**: Existem diferenças significativas nos preços dos combustíveis entre diferentes regiões do Brasil.
    - **Hipótese 3**: A variação dos preços dos combustíveis está correlacionada com a variação do preço do dólar.
    """)

    st.header("Resultados das Análises")
    st.markdown("### 1. Análise Temporal")
    st.markdown("""
    - **Gráfico de Linhas: Preços Médios de Combustíveis em 2023**
    - **Tendências Gerais**:
        - Diesel e Diesel S10: Tendência de queda ao longo do ano, com recuperação no final.
        - Gasolina e Gasolina Aditivada: Queda inicial, recuperação no meio do ano e leve queda no final.
        - Etanol: Estabilidade com leve tendência de queda.
        - GNV: Leve tendência de queda e estabilização no final.
    - **Comparação Entre Combustíveis**:
        - Níveis de Preço: Diesel e Diesel S10 têm preços médios mais elevados.
        - Volatilidade: Gasolina e Gasolina Aditivada são mais voláteis.
    - **Gráfico de Boxplot: Distribuição dos Preços de Todos os Combustíveis em 2023**:
        - Diesel e Diesel S10: Distribuição ampla com muitos outliers.
        - Etanol: Distribuição mais concentrada e menos outliers.
        - Gasolina e Gasolina Aditivada: Distribuição ampla com vários outliers.
        - GNV: Distribuição estreita e poucos outliers.
    - **Comparação de Medianas**:
        - Mediana do Preço: Diesel e Diesel S10 têm medianas mais altas.
        - Intervalo Interquartil: Diesel e Gasolina mostram maior variabilidade.
    """)

    st.markdown("### 2. Análise Geográfica")
    st.markdown("""
    - **Fatores Influentes**: Fatores como custos de transporte, impostos regionais e demanda local podem influenciar essas variações de preços.
    - **Influências Regionais**: A variação nos preços médios de venda entre os estados pode ser atribuída a diversos fatores, incluindo custos de transporte, impostos estaduais, e demanda local.
    - **Comparação entre Combustíveis**: O Etanol apresenta uma maior variação de preços entre os estados comparado ao Diesel, indicando possíveis diferenças nos custos de produção e distribuição.
    - **Análise Regional**: Estados do Norte e Nordeste tendem a ter preços mais elevados para ambos os combustíveis em comparação com estados do Sul e Sudeste, refletindo possíveis diferenças logísticas e econômicas regionais.
    """)

    st.markdown("### 3. Análise da Correlação com o Dólar")
    st.markdown("""
    - **Correlação Negativa**: O gráfico de dispersão com a linha de tendência mostra uma correlação negativa entre o preço da gasolina e o valor do dólar, com um coeficiente de correlação de -0.24. Isso indica que, ao contrário do esperado, quando o valor do dólar aumenta, o preço da gasolina tende a diminuir ligeiramente.
    - **Densidade de Pontos**: A densidade de pontos no gráfico indica uma grande quantidade de dados disponíveis para análise. No entanto, a dispersão dos pontos sugere uma variabilidade significativa nos preços da gasolina que não é explicada apenas pela variação do valor do dólar.
    - **Fatores Adicionais**: A correlação negativa sugere que outros fatores além do valor do dólar podem estar influenciando os preços da gasolina no Brasil. Isso pode incluir políticas governamentais, impostos, custos de produção locais e flutuações na demanda.
    """)

    st.header("Considerações e Verificações")
    st.markdown("""
    - **Hipótese 1**: Confirmada. Os preços dos combustíveis variam significativamente ao longo do tempo, como mostrado no gráfico de análise do ano de 2023.
    - **Hipótese 2**: Confirmada. Há diferenças significativas nos preços entre as regiões.
    - **Hipótese 3**: Confirmada. A variação do preço do dólar está fracamente correlacionada com a variação dos preços dos combustíveis. (2023)
    """)

    st.header("Considerações Finais")
    st.markdown("""
    Este relatório forneceu insights valiosos sobre os preços dos combustíveis no Brasil. Apesar das limitações computacionais, a metodologia e infraestrutura implementadas são robustas e prontas para análises futuras mais abrangentes. As hipóteses iniciais foram confirmadas, demonstrando variações significativas nos preços ao longo do tempo e entre regiões, além de uma fraca correlação com o valor do dólar. O projeto destaca a importância de uma infraestrutura robusta para a análise de grandes volumes de dados, fornecendo uma base sólida para futuras pesquisas no campo dos preços de combustíveis.
    """)

if __name__ == "__main__":
    report_page()
