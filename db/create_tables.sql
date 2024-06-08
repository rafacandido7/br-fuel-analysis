-- Criação da tabela DIM_Tempo
CREATE TABLE DIM_Tempo (
    Tempo_ID UUID PRIMARY KEY,
    Data_Coleta DATE,
    Dia INT,
    Mes INT,
    Ano INT,
    Dia_Semana INT
);

CREATE TABLE DIM_Localizacao_Geografica (
    Localizacao_Geografica_ID UUID PRIMARY KEY,
    Latitude DECIMAL,
    Longitude DECIMAL
);

CREATE TABLE DIM_Localizacao (
    Localizacao_ID UUID PRIMARY KEY,
    Regiao_Sigla VARCHAR,
    Estado_Sigla VARCHAR,
    Municipio VARCHAR,
    Bairro VARCHAR,
    CEP VARCHAR,
    Localizacao_Geografica_ID UUID REFERENCES DIM_Localizacao_Geografica(Localizacao_Geografica_ID)
);

CREATE TABLE DIM_Produto (
    Produto_ID UUID PRIMARY KEY,
    Produto VARCHAR,
    Bandeira VARCHAR
);

CREATE TABLE DIM_Revenda (
    Revenda_ID UUID PRIMARY KEY,
    CNPJ_Revenda VARCHAR,
    Nome_Revenda VARCHAR
);

CREATE TABLE DIM_Endereco (
    Endereco_ID UUID PRIMARY KEY,
    Nome_Rua VARCHAR,
    Numero_Rua VARCHAR,
    Complemento VARCHAR
);

CREATE TABLE FATO_Venda (
    Venda_ID UUID PRIMARY KEY,
    Tempo_ID UUID REFERENCES DIM_Tempo(Tempo_ID),
    Localizacao_ID UUID REFERENCES DIM_Localizacao(Localizacao_ID),
    Produto_ID UUID REFERENCES DIM_Produto(Produto_ID),
    Revenda_ID UUID REFERENCES DIM_Revenda(Revenda_ID),
    Endereco_ID UUID REFERENCES DIM_Endereco(Endereco_ID),
    Valor_Venda DECIMAL,
    Valor_Compra DECIMAL
);
