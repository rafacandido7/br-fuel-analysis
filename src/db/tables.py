from enum import Enum

class Tables(Enum):
    DIM_TEMPO = "dim_tempo"
    DIM_LOCALIZACAO = "dim_localizacao"
    DIM_PRODUTO = "dim_produto"
    DIM_REVENDA = "dim_revenda"
    DIM_ENDERECO = "dim_endereco"
    FATO_VENDA = "fato_venda"
    DOLLAR_INFO = "dollar_info"
