# Databricks notebook source
# ============================================================================
# GOLD COLUMN DOCS - comentários de tabela/coluna pro Unity Catalog
# ============================================================================
"""
Fonte única dos comentários de tabela e coluna da Gold: usada por
gold_utils.save_to_gold / GoldTableProcessor.save_gold_table (COMMENT ON
TABLE / ALTER COLUMN...COMMENT no Unity Catalog) - mesmo padrão de
silver_column_docs.py.

Descrições voltadas pro negócio (o que a coluna significa pra quem consome o
dado - analista, BI, Genie), não pra como ela foi calculada.
"""

GOLD_TABLES = {
    "TB_FATO_MERCADO_CARTAS": {
        "comment": "Visão única de mercado de cartas de Magic: The Gathering - combina o catálogo de cartas, a coleção de origem, o histórico de cotação de preço e o volume de esclarecimentos oficiais de regras. Uma linha por cotação de preço de uma impressão de carta. Feita para responder 'quanto vale essa carta, em que coleção ela está e o quanto ela é discutida em termos de regras', sem precisar conhecer Bronze/Silver.",
        "columns": {
            "ID_CARTA": "Id único da impressão/edição desta carta.",
            "ID_ORACLE": "Identificador da carta estável entre todas as suas impressões - use para agrupar todas as versões de uma carta independente da edição.",
            "NME_CARTA": "Nome da carta.",
            "NME_TIPO_CARTA": "Tipo principal da carta (Creature, Instant, Planeswalker...).",
            "NME_RARIDADE": "Raridade desta impressão da carta.",
            "NME_CATEGORIA_COR": "Categoria de cor da carta (Incolor, Monocolor, Bicolor, Multicolor) - facilita agrupar cartas por perfil de cor.",
            "COD_CORES": "Cores da carta.",
            "QTD_CUSTO_MANA": "Custo de mana convertido (CMC) desta carta.",
            "COD_COLECAO": "Código da coleção/edição desta impressão da carta.",
            "NME_COLECAO": "Nome da coleção/edição desta impressão da carta. 'Nao_Identificado' quando a coleção não foi encontrada no catálogo de coleções.",
            "NME_BLOCO": "Bloco de expansão da coleção desta impressão. 'Nao_Identificado' quando a coleção não foi encontrada no catálogo de coleções.",
            "DT_LANCAMENTO_COLECAO": "Data de lançamento da coleção desta impressão. Sentinela 1001-01-01 quando a coleção não foi encontrada no catálogo de coleções.",
            "DT_COTACAO": "Data/hora desta coleta de preço. Junto com ID_CARTA, é a chave única da tabela - a mesma carta aparece em várias linhas, uma por coleta.",
            "VLR_USD": "Preço em dólares americanos nesta coleta. NULO significa que não havia cotação em dólar nesta coleta, não que a carta vale zero.",
            "VLR_USD_FOIL": "Preço em dólares da versão foil desta mesma impressão nesta coleta - foil é outra cotação da mesma carta, não outra linha, e costuma valer várias vezes o não-foil. NULO significa sem foil ou sem cotação nesta coleta.",
            "VLR_USD_ETCHED": "Preço em dólares da versão etched foil desta mesma impressão nesta coleta. NULO na maioria esmagadora das cartas - poucos sets tiveram etched.",
            "VLR_EUR": "Preço em euros nesta coleta. NULO significa que não havia cotação em euro nesta coleta, não que a carta vale zero.",
            "VLR_EUR_FOIL": "Preço em euros da versão foil desta mesma impressão nesta coleta. NULO significa sem foil ou sem cotação nesta coleta.",
            "VLR_TIX": "Preço em MTGO tickets nesta coleta. NULO significa que não havia cotação em tix nesta coleta, não que a carta vale zero.",
            "QTD_ESCLARECIMENTOS": "Quantidade de esclarecimentos oficiais de regras (rulings) já publicados para esta carta, somando todas as impressões. Zero é um valor real (carta nunca teve ruling), não 'desconhecido'.",
            "DT_ULTIMO_ESCLARECIMENTO": "Data do esclarecimento de regras mais recente publicado para esta carta. Sentinela 1001-01-01 quando a carta nunca teve esclarecimento (QTD_ESCLARECIMENTOS = 0).",
            "ID_CARTA_CANONICO": "Id de impressão vigente desta carta. Igual a ID_CARTA quando a carta nunca foi migrada; aponta para o id substituto quando a Scryfall fundiu ou removeu este id. Use este campo (em vez de ID_CARTA) para agrupar/filtrar sem herdar ids obsoletos.",
            "FLG_ID_CARTA_MIGRADO": "'Sim' quando o ID_CARTA desta linha já foi substituído pela Scryfall (ver ID_CARTA_CANONICO para o id vigente); 'Nao' quando o id ainda é o vigente.",
            "ANO_COTACAO": "Ano da coleta de preço - usado só para particionamento físico da tabela.",
            "MES_COTACAO": "Mês da coleta de preço - usado só para particionamento físico da tabela.",
        },
    },
}


def get_table_comment(gold_table_name):
    return GOLD_TABLES.get(gold_table_name, {}).get("comment")


def get_column_comments(gold_table_name):
    return GOLD_TABLES.get(gold_table_name, {}).get("columns", {})


if __name__ == "__main__":
    for table_name in GOLD_TABLES:
        assert get_table_comment(table_name), f"{table_name} sem comment de tabela"
        assert get_column_comments(table_name), f"{table_name} sem comments de coluna"
    assert get_table_comment("inexistente") is None
    assert get_column_comments("inexistente") == {}
    print("gold_column_docs: OK")
