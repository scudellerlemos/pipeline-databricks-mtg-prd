# Databricks notebook source
# ============================================================================
# SILVER COLUMN DOCS - comentários de tabela/coluna pro Unity Catalog
# ============================================================================
"""
Fonte única dos comentários de tabela e coluna da Silver: usada por
silver_utils.save_to_silver / SilverTableProcessor.save_silver_table
(COMMENT ON TABLE / ALTER COLUMN...COMMENT no Unity Catalog) e pelos READMEs
de cada tabela em Documentação/ - evita descrever a mesma coluna em dois
lugares que divergem com o tempo.

Descrições voltadas pro negócio (o que a coluna significa pra quem consome o
dado), não pra como ela foi calculada - isso já está no notebook.

Cada notebook de tabela chama get_table_comment(nome)/get_column_comments(nome)
e repassa pro save_silver_table. Não faz %run aninhado aqui (o lint estático
só resolve %run um nível) - importar via %run ./silver_column_docs direto no
notebook, sem dependência de dbutils/spark (é só dado estático), mesmo padrão de
bronze_column_docs.py.
"""

# Colunas técnicas presentes em toda tabela Silver (linhagem até a Stage/Bronze) -
# mesma descrição em qualquer tabela, uma vez só aqui.
COMMON_COLUMNS = {
    "DT_INGESTAO": "Data/hora em que o dado de origem foi coletado da fonte.",
    "NME_FONTE": "Fonte de dados de origem (ex.: 'scryfall').",
    "DESC_URL_ORIGEM": "Endpoint/URL da API de origem do dado.",
    "DESC_ARQUIVO_ORIGEM": "Caminho do arquivo de origem na camada Bronze - usado só para auditoria/rastreabilidade.",
    "ID_EXECUCAO_BRONZE": "Id da execução da Bronze que originou esta linha - usado só para auditoria/rastreabilidade.",
    "DT_INGESTAO_BRONZE": "Data/hora em que a Bronze processou o registro - usado só para auditoria/rastreabilidade.",
}

SILVER_TABLES = {
    "TB_FATO_CARTAS": {
        "comment": "Catálogo de cartas de Magic: The Gathering - uma linha por impressão/edição de carta, pronta para análise de gameplay, deckbuilding e coleção. Responde 'o que é essa carta': texto de regras, custo de mana, tipo, raridade, artista e em qual coleção ela saiu. Preço e histórico de migração de id ficam em tabelas próprias (TB_FATO_PRECOS_CARTAS, TB_MOV_MIGRACOES_CARTAS) - junte por ID_CARTA quando precisar combinar.",
        "columns": {
            "ID_CARTA": "Id único da impressão desta carta. Preservado como veio da fonte - nunca reatribuído, mesmo quando a Scryfall unifica cartas (ver TB_MOV_MIGRACOES_CARTAS para o id canônico pós-migração).",
            "ID_ORACLE": "Identificador da carta estável entre todas as suas impressões (diferentes edições da mesma carta compartilham este id). Use para agrupar todas as versões de uma carta.",
            "NME_CARTA": "Nome da carta.",
            "DESC_CUSTO_MANA": "Custo de mana para conjurar a carta, em notação de símbolos.",
            "QTD_CUSTO_MANA": "Custo de mana convertido (CMC) - número total de mana necessário, usado para curva de mana do deck.",
            "COD_CORES": "Cores da carta.",
            "COD_IDENTIDADE_COR": "Identidade de cor da carta - relevante para montar deck em formatos como Commander.",
            "NME_TIPO_CARTA": "Tipo principal da carta (Creature, Instant, Planeswalker...).",
            "DESC_DETALHE_TIPO_CARTA": "Complemento do tipo quando a carta tem subtítulo próprio (ex.: nome de um Planeswalker no lugar do subtipo).",
            "DESC_TIPOS": "Tipos da carta.",
            "DESC_SUBTIPOS": "Subtipos da carta (raça/classe da criatura, tipo de feitiço, etc.).",
            "NME_RARIDADE": "Raridade desta impressão da carta.",
            "COD_COLECAO": "Coleção/edição em que esta impressão da carta saiu. Junte com TB_DIM_COLECOES para detalhes da coleção.",
            "NME_COLECAO": "Nome da coleção/edição em que esta impressão da carta saiu.",
            "DESC_CARTA": "Texto de regras impresso na carta - o que ela faz.",
            "NME_ARTISTA": "Ilustrador responsável pela arte desta impressão.",
            "NUM_COLECIONADOR": "Número de colecionador desta carta dentro da coleção - usado para identificar a carta fisicamente num booster/pacote.",
            "NME_FORCA": "Força da criatura em combate.",
            "NME_RESISTENCIA": "Resistência da criatura em combate (dano que suporta antes de morrer).",
            "NME_DISPOSICAO_CARTA": "Formato físico da carta (carta simples, carta dupla/split, transformável, etc.).",
            "ID_MULTIVERSO": "Id desta carta no banco oficial de cartas da Wizards (Gatherer) - use para linkar a carta na fonte oficial.",
            "URL_IMAGEM": "Endereço da imagem desta impressão da carta.",
            "COD_VARIACOES": "Outras impressões/variações visuais da mesma carta.",
            "DESC_NOMES_ESTRANGEIROS": "Nome e texto desta carta traduzidos para outros idiomas.",
            "DESC_IMPRESSOES": "Todas as coleções em que esta carta já foi impressa.",
            "DESC_CARTA_ORIGINAL": "Texto de regras desta carta como impresso originalmente, antes de qualquer correção oficial (errata).",
            "NME_TIPO_ORIGINAL": "Linha de tipo original da carta, antes de reclassificações oficiais.",
            "DESC_LEGALIDADES": "Em quais formatos de jogo (Standard, Commander, Modern...) esta carta é permitida.",
            "NME_CATEGORIA_COR": "Categoria de cor da carta derivada do custo de mana (Incolor, Monocolor, Bicolor, Multicolor) - facilita agrupar cartas por perfil de cor.",
            "QTD_CORES": "Quantidade de cores distintas no custo de mana da carta.",
            "ANO_INGESTAO": "Ano da coleta do dado de origem - usado só para particionamento físico da tabela.",
            "MES_INGESTAO": "Mês da coleta do dado de origem - usado só para particionamento físico da tabela.",
        },
    },
    "TB_DIM_COLECOES": {
        "comment": "Catálogo das coleções/edições de Magic: The Gathering já lançadas, incluindo edições só digitais. Responde 'quando saiu, quantas cartas tem, a que bloco pertence e o que vem num pacote de booster' - use para organizar a coleção por edição ou situar uma carta na linha do tempo do jogo.",
        "columns": {
            "COD_COLECAO": "Código curto da coleção/edição (ex.: 'M19').",
            "NME_COLECAO": "Nome completo da coleção/edição.",
            "NME_TIPO_COLECAO": "Tipo da coleção (edição principal, masters, promocional, etc.).",
            "NME_COR_BORDA": "Cor de borda padrão das cartas desta coleção.",
            "ID_CARDMARKET": "Id desta coleção na Cardmarket - use para cruzar com dado de preço/mercado europeu.",
            "NME_CARDMARKET": "Nome desta coleção na Cardmarket - pode diferir do nome oficial.",
            "DT_LANCAMENTO": "Data de lançamento da coleção.",
            "COD_GATHERER": "Código desta coleção no banco oficial de cartas da Wizards (Gatherer).",
            "COD_MAGICCARDSINFO": "Código desta coleção no site magiccards.info.",
            "COD_ANTIGO": "Código anterior da coleção, se ela já foi renomeada.",
            "FLG_SOMENTE_ONLINE": "Indica se a coleção só existe em ambiente digital (Arena/MTGO), sem versão física.",
            "QTD_CARTAS": "Quantidade de cartas que compõem a coleção.",
            "COD_COLECAO_PAI": "Coleção 'pai', quando esta é uma sub-coleção (ex.: promoções vinculadas a uma edição principal).",
            "NME_BLOCO": "Bloco de expansão ao qual a coleção pertence.",
            "URL_ICONE": "Endereço do ícone que representa a coleção.",
            **{f"DESC_BOOSTER_SLOT_{i}": f"Tipo de carta possível na posição {i} de um pacote de booster desta coleção." for i in range(20)},
            "ANO_LANCAMENTO": "Ano de lançamento da coleção - usado só para particionamento físico da tabela.",
            "MES_LANCAMENTO": "Mês de lançamento da coleção - usado só para particionamento físico da tabela.",
        },
    },
    "TB_FATO_PRECOS_CARTAS": {
        "comment": "Histórico de cotações de preço de cartas de Magic: The Gathering em dólar, euro e MTGO ticket - uma linha por coleta de preço de uma impressão. Preço varia por impressão: a mesma carta reimpressa em outra coleção vale outro valor, e cada impressão tem sua própria cotação aqui. Use para acompanhar valorização/desvalorização ao longo do tempo, comparar preço entre impressões/coleções ou montar um indicador de valor de coleção. A mesma impressão tem várias linhas (uma por coleta) de propósito - é histórico, não é 'o preço atual'.",
        "columns": {
            "ID_CARTA": "Impressão de carta a que esta cotação se refere - junte com TB_FATO_CARTAS.ID_CARTA (1:1 por impressão). Junto com DT_INGESTAO, forma a chave única desta tabela.",
            "NME_CARTA": "Nome da carta cotada. Não é chave: o mesmo nome aparece em várias impressões, cada uma com seu próprio preço.",
            "COD_COLECAO": "Coleção/edição desta impressão cotada.",
            "NME_RARIDADE": "Raridade desta impressão cotada.",
            "VLR_USD": "Preço em dólares americanos. NULO significa que não havia cotação em dólar nesta coleta, não que a carta vale zero.",
            "VLR_USD_FOIL": "Preço em dólares da versão foil da MESMA impressão - foil é outra cotação da mesma carta, não outra impressão, e costuma valer várias vezes o não-foil. NULO significa que essa impressão não tem foil ou não tinha cotação nesta coleta.",
            "VLR_USD_ETCHED": "Preço em dólares da versão etched foil da mesma impressão. NULO na maioria esmagadora das cartas - poucos sets tiveram etched.",
            "VLR_EUR": "Preço em euros. NULO significa que não havia cotação em euro nesta coleta, não que a carta vale zero.",
            "VLR_EUR_FOIL": "Preço em euros da versão foil da mesma impressão. NULO significa que essa impressão não tem foil ou não tinha cotação nesta coleta.",
            "VLR_TIX": "Preço em MTGO tickets (moeda do Magic Online). NULO significa que não havia cotação em tix nesta coleta, não que a carta vale zero.",
            "URL_SCRYFALL": "Endereço da página desta carta na Scryfall.",
            "URL_IMAGEM": "Endereço da imagem desta impressão cotada.",
            "DT_LANCAMENTO": "Data de lançamento da coleção desta impressão cotada.",
            "ANO_INGESTAO": "Ano da coleta de preço - usado só para particionamento físico da tabela.",
            "MES_INGESTAO": "Mês da coleta de preço - usado só para particionamento físico da tabela.",
        },
    },
    "TB_MOV_MIGRACOES_CARTAS": {
        "comment": "Histórico de trocas de identificador de carta feitas pela Scryfall, quando duas cartas são unificadas em uma só ou uma é removida do catálogo. Use para reconciliar um id antigo de carta com o id vigente e não perder o vínculo em análises feitas antes da mudança - ID_CARTA_CANONICO já traz o id final, mesmo quando a carta passou por várias migrações em cadeia.",
        "columns": {
            "ID_MIGRACAO": "Id único deste registro de migração.",
            "URL_SCRYFALL": "Endereço da API da Scryfall para este registro de migração.",
            "DT_EXECUCAO": "Data em que esta migração de id foi executada.",
            "NME_ESTRATEGIA_MIGRACAO": "Como a migração foi feita: unificação de duas cartas em uma só, ou remoção de uma carta do catálogo.",
            "ID_CARTA_ANTIGO": "Id de carta que deixou de ser usado por causa desta migração.",
            "ID_CARTA_NOVO": "Id de carta que passou a valer no lugar do antigo, quando a migração foi uma unificação. Vazio quando a migração foi uma remoção.",
            "ID_CARTA_CANONICO": "Id de carta final, já resolvido até a última migração da cadeia (uma carta pode ser migrada mais de uma vez) - use este id, não ID_CARTA_NOVO, para sempre chegar na versão vigente.",
            "DESC_NOTA": "Explicação, quando fornecida, do motivo desta migração.",
            "ID_CARTA_ASSOCIADA": "Carta associada a este registro de migração.",
            "COD_IDIOMA": "Idioma da carta associada a este registro de migração.",
            "NME_CARTA_ASSOCIADA": "Nome da carta associada a este registro de migração.",
            "COD_COLECAO_ASSOCIADA": "Coleção da carta associada a este registro de migração.",
            "ID_ORACLE_ASSOCIADO": "Identificador estável (entre impressões) da carta associada a este registro de migração.",
            "NUM_COLECIONADOR_ASSOCIADO": "Número de colecionador da carta associada a este registro de migração.",
            "ANO_EXECUCAO": "Ano de execução da migração - usado só para particionamento físico da tabela.",
            "MES_EXECUCAO": "Mês de execução da migração - usado só para particionamento físico da tabela.",
        },
    },
    "TB_DOM_SIMBOLOS": {
        "comment": "Lista de referência dos símbolos de mana e custo que aparecem no texto e no custo de mana das cartas (ex.: símbolo de mana branca, símbolo de taps). Use para traduzir/exibir corretamente esses símbolos e para saber quanto cada um vale em custo de mana. Lista de apoio, praticamente estática - raramente ganha símbolo novo.",
        "columns": {
            "COD_SIMBOLO": "Código do símbolo, na mesma notação usada em DESC_CUSTO_MANA/DESC_CARTA de TB_FATO_CARTAS - junte por este código para traduzir um símbolo encontrado no texto de uma carta.",
            "URL_ICONE": "Endereço da imagem deste símbolo.",
            "DESC_VARIANTE_LIVRE": "Forma alternativa de escrever este símbolo em texto livre, quando existe.",
            "DESC_SIMBOLO": "Descrição deste símbolo em texto.",
            "FLG_TRANSPONIVEL": "Indica se este símbolo pode aparecer em ordem trocada dentro de um texto de regra.",
            "FLG_REPRESENTA_MANA": "Indica se este símbolo representa mana (nem todo símbolo representa - alguns são custos não-mana, como o de virar a carta).",
            "FLG_APARECE_CUSTO_MANA": "Indica se este símbolo pode aparecer no custo de mana de uma carta.",
            "QTD_VALOR_MANA": "Quanto este símbolo contribui para o custo convertido de mana de uma carta.",
            "FLG_HIBRIDO": "Indica se é um símbolo de mana híbrida (pode ser pago com qualquer uma de duas cores).",
            "FLG_PHYREXIANO": "Indica se é um símbolo de mana phyrexiana (pode ser pago com mana de uma cor ou com pontos de vida).",
            "QTD_CUSTO_CONVERTIDO": "Custo de mana convertido equivalente deste símbolo, quando difere de QTD_VALOR_MANA em casos especiais.",
            "FLG_HUMORISTICO": "Indica se este símbolo só aparece em cartas não-oficiais/humorísticas.",
            "COD_CORES": "Cor(es) de mana associada(s) a este símbolo.",
            "DESC_GRAFIAS_GATHERER": "Formas alternativas deste símbolo usadas no banco oficial de cartas da Wizards (Gatherer).",
        },
    },
    "TB_PONTE_CARTA_SIMBOLOS": {
        "comment": "Tabela ponte entre carta e símbolo de mana - uma linha por símbolo que compõe o custo de mana de uma carta (ex.: carta com custo '2 mana genérica + 2 azul' vira 3 linhas). Resolve a relação N:N escondida dentro do texto de TB_FATO_CARTAS.DESC_CUSTO_MANA. Use para analisar cartas por símbolo/cor de mana (curva de mana, distribuição de cor) - junte COD_SIMBOLO com TB_DOM_SIMBOLOS para nome/cor/valor do símbolo.",
        "columns": {
            "ID_CARTA": "Impressão de carta a que este símbolo pertence - junte com TB_FATO_CARTAS.ID_CARTA.",
            "NUM_ORDEM_SIMBOLO": "Posição deste símbolo dentro do custo de mana da carta (1 = primeiro símbolo à esquerda).",
            "COD_SIMBOLO": "Símbolo de mana nesta posição do custo, na mesma notação de TB_DOM_SIMBOLOS.COD_SIMBOLO - junte lá para nome/cor/valor do símbolo.",
        },
    },
    "TB_FATO_ESCLARECIMENTOS_CARTAS": {
        "comment": "Esclarecimentos oficiais de regras (rulings) publicados para cartas específicas de Magic: The Gathering, ligados por ID_ORACLE a todas as impressões da carta. Use para responder dúvida de interação entre cartas ou interpretação de regra que o texto da carta sozinha não deixa claro - uma carta pode acumular vários esclarecimentos ao longo do tempo.",
        "columns": {
            "ID_ESCLARECIMENTO": "Id único deste esclarecimento (gerado a partir do conteúdo, pois a fonte não fornece um id próprio).",
            "ID_ORACLE": "Carta a que este esclarecimento se aplica (mesmo valor para todas as impressões da carta) - junte com TB_FATO_CARTAS.ID_ORACLE.",
            "NME_EMISSOR": "Quem emitiu este esclarecimento: a própria Wizards (oficial) ou a Scryfall (complementar).",
            "DT_PUBLICACAO": "Data de publicação deste esclarecimento.",
            "DESC_ESCLARECIMENTO": "Texto do esclarecimento de regras.",
            "ANO_PUBLICACAO": "Ano de publicação do esclarecimento - usado só para particionamento físico da tabela.",
            "MES_PUBLICACAO": "Mês de publicação do esclarecimento - usado só para particionamento físico da tabela.",
        },
    },
}


def get_table_comment(silver_table_name):
    return SILVER_TABLES.get(silver_table_name, {}).get("comment")


def get_column_comments(silver_table_name):
    """COMMON_COLUMNS + colunas específicas da tabela (específica vence em conflito de chave)."""
    table_columns = SILVER_TABLES.get(silver_table_name, {}).get("columns", {})
    return {**COMMON_COLUMNS, **table_columns}


if __name__ == "__main__":
    for table_name in SILVER_TABLES:
        assert get_table_comment(table_name), f"{table_name} sem comment de tabela"
    cartas_comments = get_column_comments("TB_FATO_CARTAS")
    assert cartas_comments["DT_INGESTAO"] == COMMON_COLUMNS["DT_INGESTAO"]
    assert cartas_comments["NME_CARTA"] == SILVER_TABLES["TB_FATO_CARTAS"]["columns"]["NME_CARTA"]
    assert get_table_comment("inexistente") is None
    assert get_column_comments("inexistente") == COMMON_COLUMNS
    print("silver_column_docs: OK")
