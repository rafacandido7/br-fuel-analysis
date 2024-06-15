CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE dim_tempo (
    tempo_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data_coleta DATE,
    dia INT,
    mes INT,
    ano INT,
    dia_semana INT
);

CREATE TABLE dim_localizacao (
    localizacao_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    regiao_sigla VARCHAR,
    estado_sigla VARCHAR,
    municipio VARCHAR,
    bairro VARCHAR,
    cep VARCHAR
);

CREATE TABLE dim_produto (
    produto_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    produto VARCHAR
);

CREATE TABLE dim_revenda (
    revenda_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cnpj_revenda VARCHAR,
    nome_revenda VARCHAR
);

CREATE TABLE dim_endereco (
    endereco_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nome_rua VARCHAR,
    numero_rua VARCHAR,
    complemento VARCHAR
);

CREATE TABLE fato_venda (
    venda_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tempo_id UUID REFERENCES dim_tempo(tempo_id),
    localizacao_id UUID REFERENCES dim_localizacao(localizacao_id),
    produto_id UUID REFERENCES dim_produto(produto_id),
    revenda_id UUID REFERENCES dim_revenda(revenda_id),
    endereco_id UUID REFERENCES dim_endereco(endereco_id),
    valor_venda DECIMAL,
    valor_compra DECIMAL
);

CREATE TABLE dollar_info (
    dollar_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data DATE,
    dia INT,
    mes INT,
    ano INT,
    dia_semana INT,
    valor DECIMAL
);
