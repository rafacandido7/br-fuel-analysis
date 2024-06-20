import streamlit as st

def report_page():
    st.sidebar.title("Relatório Final")

    st.title("Relatório Final do Projeto de Análise de Dados sobre Preços de Combustíveis no Brasil")

    st.header("Introdução")
    st.markdown("""
    Contextualizar o seu projeto na versão final. De preferência incluir referências bibliográficas associadas com os conceitos estudados na disciplina.
    """)

    st.header("Solução Final")

    st.markdown("### Entregáveis")
    st.markdown("""
    Descrever todos os arquivos inseridos:
    - Arquivo 1: Descrição
    - Arquivo 2: Descrição
    """)

    st.header("Análises")
    st.markdown("""
    Suas análises podem ser demonstradas de várias formas. Em geral você vai incluir:
    - Imagens extraídas do painel
    - Hipóteses iniciais acerca dos dados
    - Considerações sobre algum aspecto estudado
    - Verificar se a hipótese inicial se confirma, apresentando informações em forma de tabela e gráficos para sustentar seu argumento.
    """)


    st.markdown("### Hipótese Inicial")
    st.markdown("A hipótese inicial é que os preços dos combustíveis apresentam variações significativas ao longo do tempo e entre diferentes regiões.")

    st.markdown("### Confirmação da Hipótese")
    st.markdown("Analisando os dados, observa-se que... (detalhar as observações)")

    st.header("Conclusões")
    st.markdown("""
    Identificar os principais pontos do seu estudo. Apontar quais foram as conclusões mais relevantes. Por exemplo:
    - Conclusão 1: Os preços dos combustíveis variam significativamente entre as regiões.
    - Conclusão 2: A tendência geral dos preços dos combustíveis ao longo do tempo é de aumento/diminuição.
    - Conclusão 3: A análise preditiva indica que os preços futuros dos combustíveis tendem a...
    """)

if __name__ == "__main__":
    report_page()
