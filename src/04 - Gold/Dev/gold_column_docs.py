# Databricks notebook source
# ============================================================================
# GOLD COLUMN DOCS - comentários de tabela/coluna pro Unity Catalog
# ============================================================================
"""
Comentários de tabela e coluna da Gold, aplicados no Unity Catalog por
gold_utils.salvar_na_gold (mesmo padrão de silver_column_docs.py).

Descrições voltadas para quem consome o dado (analista, BI, Genie).
"""

TABELAS_GOLD = {
    "TB_FATO_MERCADO_CARTAS": {
        "comment": "Visão única de mercado de cartas de Magic: The Gathering - combina o catálogo de cartas, a coleção de origem, o histórico de cotação de preço e o volume de esclarecimentos oficiais de regras. Uma linha por cotação de preço de uma impressão de carta. Feita para responder 'quanto vale essa carta, em que coleção ela está e o quanto ela é discutida em termos de regras', sem precisar conhecer Bronze/Silver.",
        "columns": {
            "ID_CARTA": "Id único da impressão/edição desta carta.",
            "ID_ORACLE": "Identificador da carta estável entre todas as suas impressões - use para agrupar todas as versões de uma carta independente da edição.",
            "NME_CARTA": "Nome da carta, normalizado (Title_Case sem acento, espaço vira '_'; ex.: 'Lightning_Bolt').",
            "NME_TIPO_CARTA": "Linha de tipo antes do '—' (supertipos + tipos), em Title_Case com '_' - ex.: 'Creature', 'Legendary_Creature', 'Artifact_Creature', 'Basic_Land'; qualquer Planeswalker vira 'Planeswalker'; 'NA' se ausente. Para achar todas as criaturas use LIKE '%Creature%'.",
            "NME_RARIDADE": "Raridade desta impressão da carta.",
            "NME_CATEGORIA_COR": "Categoria de cor da carta (Colorless, Mono, Dual_Color, Multicolor) - facilita agrupar cartas por perfil de cor.",
            "COD_CORES": "Cores da carta como letras WUBRG separadas por vírgula (ex.: 'W, U'); 'Colorless' quando a carta não tem cor (nunca NULO/vazio).",
            "QTD_CUSTO_MANA": "Custo de mana convertido (CMC) desta carta.",
            "COD_COLECAO": "Código da coleção/edição desta impressão da carta.",
            "NME_COLECAO": "Nome da coleção/edição desta impressão da carta. 'Nao_Identificado' quando a coleção não foi encontrada no catálogo de coleções.",
            "NME_BLOCO": "Bloco de expansão da coleção desta impressão. 'Nao_Identificado' quando a coleção não foi encontrada no catálogo de coleções ou não pertence a nenhum bloco (block nulo na Scryfall).",
            "DT_LANCAMENTO_COLECAO": "Data de lançamento da coleção desta impressão. Sentinela 1001-01-01 quando a coleção não foi encontrada no catálogo de coleções.",
            "DT_COTACAO": "Data/hora desta coleta de preço. Junto com ID_CARTA, é a chave única da tabela - a mesma carta aparece em várias linhas, uma por coleta.",
            "VLR_USD": "Preço em dólares americanos nesta coleta. NULO significa que não havia cotação em dólar nesta coleta, não que a carta vale zero.",
            "VLR_USD_FOIL": "Preço em dólares da versão foil desta mesma impressão nesta coleta - foil é outra cotação da mesma carta, não outra linha, e costuma valer várias vezes o não-foil. NULO significa sem foil ou sem cotação nesta coleta.",
            "VLR_USD_ETCHED": "Preço em dólares da versão etched foil desta mesma impressão nesta coleta. NULO na maioria esmagadora das cartas - poucos sets tiveram etched.",
            "VLR_EUR": "Preço em euros nesta coleta. NULO significa que não havia cotação em euro nesta coleta, não que a carta vale zero.",
            "VLR_EUR_FOIL": "Preço em euros da versão foil desta mesma impressão nesta coleta. NULO significa sem foil ou sem cotação nesta coleta.",
            "VLR_TIX": "Preço em MTGO tickets nesta coleta. NULO significa que não havia cotação em tix nesta coleta, não que a carta vale zero.",
            "QTD_ESCLARECIMENTOS": "Quantidade de esclarecimentos oficiais de regras (rulings) já publicados para esta carta (contados por ID_ORACLE - o mesmo valor em todas as impressões dela). Zero é um valor real (carta nunca teve ruling), não 'desconhecido'.",
            "DT_ULTIMO_ESCLARECIMENTO": "Data do esclarecimento de regras mais recente publicado para esta carta. Sentinela 1001-01-01 quando a carta nunca teve esclarecimento (QTD_ESCLARECIMENTOS = 0).",
            "ID_CARTA_CANONICO": "Id de impressão vigente desta carta. Igual a ID_CARTA quando a carta nunca foi migrada; aponta para o id substituto quando a Scryfall fundiu este id (Unificacao); num id removido (Remocao) continua igual a ID_CARTA. Use este campo (em vez de ID_CARTA) para agrupar/filtrar sem herdar ids obsoletos.",
            "FLG_ID_CARTA_MIGRADO": "'Sim' quando o ID_CARTA desta linha foi fundido ou removido pela Scryfall (ver ID_CARTA_CANONICO; só a fusão tem id substituto); 'Nao' quando o id ainda é o vigente.",
            "ANO_COTACAO": "Ano da coleta de preço - usado só para particionamento físico da tabela.",
            "MES_COTACAO": "Mês da coleta de preço - usado só para particionamento físico da tabela.",
        },
    },
}


def obter_comentario_tabela(nome_tabela_gold):
    return TABELAS_GOLD.get(nome_tabela_gold, {}).get("comment")


def obter_comentarios_colunas(nome_tabela_gold):
    return TABELAS_GOLD.get(nome_tabela_gold, {}).get("columns", {})


if __name__ == "__main__":
    for nome_tabela in TABELAS_GOLD:
        assert obter_comentario_tabela(nome_tabela), f"{nome_tabela} sem comment de tabela"
        assert obter_comentarios_colunas(nome_tabela), f"{nome_tabela} sem comments de coluna"
    assert obter_comentario_tabela("inexistente") is None
    assert obter_comentarios_colunas("inexistente") == {}
    print("gold_column_docs: OK")
