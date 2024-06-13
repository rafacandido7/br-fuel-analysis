CREATE TABLE dim_tempo (
    tempo_id UUID PRIMARY KEY,
    data_coleta DATE,
    dia INT,
    mes INT,
    ano INT,
    dia_semana INT
);

CREATE TABLE dim_localizacao_geografica (
    localizacao_geografica_id UUID PRIMARY KEY,
    latitude DECIMAL,
    longitude DECIMAL
);

CREATE TABLE dim_localizacao (
    localizacao_id UUID PRIMARY KEY,
    regiao_sigla VARCHAR,
    estado_sigla VARCHAR,
    municipio VARCHAR,
    bairro VARCHAR,
    cep VARCHAR,
    localizacao_geografica_id UUID REFERENCES dim_localizacao_geografica(localizacao_geografica_id)
);

CREATE TABLE dim_produto (
    produto_id UUID PRIMARY KEY,
    produto VARCHAR,
);

CREATE TABLE dim_revenda (
    revenda_id UUID PRIMARY KEY,
    cnpj_revenda VARCHAR,
    nome_revenda VARCHAR
);

CREATE TABLE dim_endereco (
    endereco_id UUID PRIMARY KEY,
    nome_rua VARCHAR,
    numero_rua VARCHAR,
    complemento VARCHAR
);

CREATE TABLE fato_venda (
    venda_id UUID PRIMARY KEY,
    tempo_id UUID REFERENCES dim_tempo(tempo_id),
    localizacao_id UUID REFERENCES dim_localizacao(localizacao_id),
    produto_id UUID REFERENCES dim_produto(produto_id),
    revenda_id UUID REFERENCES dim_revenda(revenda_id),
    endereco_id UUID REFERENCES dim_endereco(endereco_id),
    valor_venda DECIMAL,
    valor_compra DECIMAL
);
