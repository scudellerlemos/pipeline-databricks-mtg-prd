# Databricks notebook source
# ============================================================================
# BRONZE COLUMN DOCS - comentários de tabela/coluna pro Unity Catalog
# ============================================================================
"""
Comentários de tabela e coluna da Bronze, aplicados no Unity Catalog por
executar_ingestao_bronze. Importar com %run ./bronze_column_docs no notebook.

Os READMEs de Documentação/ repetem este texto à mão: ao mudar uma
descrição, atualizar os dois.
"""

# Colunas técnicas comuns a toda tabela Bronze. Uma tabela sobrescreve uma
# entrada declarando a mesma chave em TABELAS_BRONZE[tabela]["columns"].
COLUNAS_COMUNS = {
    "ingestion_timestamp": "Início da execução da Stage que gravou o registro (mesmo valor em todas as linhas da run) - distinto do bronze_ingestion_timestamp.",
    "source": "Nome da fonte de dados de origem (ex.: 'scryfall').",
    "endpoint": "Nome da tabela/endpoint lógico gravado pela Stage (= nome da tabela, ex.: 'cards'), não a URL da API.",
    "source_file": "Caminho completo do arquivo Parquet de origem na Stage (_metadata.file_path) - chave de idempotência da Bronze.",
    "bronze_run_id": "Id da execução da Bronze que gravou a linha.",
    "bronze_ingestion_timestamp": "Timestamp em que a Bronze processou o registro.",
}

TABELAS_BRONZE = {
    "cards": {
        "comment": "Catálogo de cartas de Magic: The Gathering - uma linha por impressão/edição de carta. Responde 'o que é essa carta': texto de regras, custo de mana, tipo, raridade, artista, em qual set saiu e em quais formatos de jogo (Standard, Commander, etc.) ela é legal. Base pra qualquer análise de deck, coleção ou busca de carta.",
        "columns": {
            "id": "Id da impressão na Scryfall (cada reimpressão da mesma carta tem o seu).",
            "name": "Nome da carta.",
            "manaCost": "Custo de mana em notação simbólica (ex.: '{2}{U}{U}').",
            "cmc": "Custo de mana convertido (soma numérica do custo de mana).",
            "colors": "Cores da carta.",
            "colorIdentity": "Identidade de cor da carta (usada em formatos como Commander).",
            "type": "Linha de tipo completa da carta (ex.: 'Creature — Human Wizard').",
            "types": "Tipos principais da carta (ex.: Creature, Instant). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "subtypes": "Subtipos da carta (ex.: Human, Wizard). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "rarity": "Raridade da impressão (common/uncommon/rare/mythic).",
            "set": "Código do set/edição desta impressão.",
            "setName": "Nome completo do set/edição.",
            "text": "Texto de regras oficial atual (Oracle text, com erratas) - não necessariamente o impresso nesta edição. Em dupla face, só a frente.",
            "artist": "Nome do ilustrador.",
            "number": "Número de colecionador dentro do set.",
            "power": "Força da criatura (texto, pode ser '*').",
            "toughness": "Resistência da criatura (texto, pode ser '*').",
            "layout": "Layout físico da carta (normal, split, transform, etc.).",
            "multiverseid": "Id da carta no Gatherer (banco oficial de cartas da Wizards) - usado pra linkar a carta na fonte oficial. Sempre nulo (campo legado da magicthegathering.io; a Scryfall tem multiverse_ids, lista, mas a Stage não mapeia).",
            "imageUrl": "URL da imagem da carta.",
            "variations": "Ids de outras impressões/variações visuais da mesma carta. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "foreignNames": "Nomes/textos traduzidos em outros idiomas. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "printings": "Códigos de todos os sets em que a carta já foi impressa. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "originalText": "Texto de regras como impresso originalmente (antes de errata). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "originalType": "Linha de tipo original antes de reclassificações. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "legalities": "Legalidade da carta por formato de jogo.",
            # Pode vir NULL em partições gravadas antes deste campo existir.
            "oracle_id": "Oracle id da carta na Scryfall - estável entre impressões (printings) da mesma carta, ao contrário de id (que identifica só esta impressão). Usado na Gold para ligar a carta aos esclarecimentos (rulings).",
        },
    },
    "sets": {
        "comment": "Sets/edições de Magic: The Gathering. Cada run da Stage traz os lançados a partir de 1º de janeiro de (ano atual - years_back, padrão 5) e a Bronze acumula as runs (append), incluindo edições só digitais e anunciadas ainda não lançadas. Responde 'quando saiu, quantas cartas tem e a que bloco pertence' - útil pra organizar coleção por edição ou situar uma carta na linha do tempo do jogo.",
        "columns": {
            "code": "Código curto do set/edição (ex.: 'dmu', minúsculo como na Scryfall).",
            "name": "Nome completo do set/edição.",
            "type": "Tipo de set (core, expansion, masters, promo, etc.).",
            "border": "Cor de borda padrão das cartas do set (black/white/silver). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "mkm_id": "Id do set na Cardmarket (MKM) - usado pra cruzar com dado de preço/mercado da Cardmarket. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "mkm_name": "Nome do set na Cardmarket (MKM) - pode diferir do nome oficial usado na Scryfall. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "releaseDate": "Data de lançamento do set.",
            "gathererCode": "Código do set usado no Gatherer (Wizards). Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "magicCardsInfoCode": "Código do set usado no site magiccards.info. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "oldCode": "Código antigo do set, se já foi renomeado. Sempre nulo (campo legado da magicthegathering.io sem equivalente na Scryfall).",
            "onlineOnly": "true se o set só existe em ambiente digital (Arena/MTGO).",
            "card_count": "Quantidade de cartas no set.",
            "parent_set_code": "Código do set 'pai', quando este é um sub-set (ex.: promos de um set principal).",
            "block": "Bloco de expansão ao qual o set pertence.",
            "icon_svg_uri": "URL do ícone SVG do set.",
            "booster": "Campo legado da magicthegathering.io (lista de booster serializada) sem equivalente na Scryfall - sempre nulo desde a migração pra Scryfall, mantido só por imutabilidade de schema.",
            # booster_0..19: legado da magicthegathering.io, sempre nulo.
            **{
                f"booster_{i}": f"Slot {i} do pacote de booster (legado da magicthegathering.io). Sempre nulo: a Scryfall não expõe booster; coluna mantida só por imutabilidade de schema."
                for i in range(20)
            },
        },
    },
    "card_prices": {
        "comment": "Histórico de cotações de preço de cartas em dólar, euro e MTGO ticket - cada linha é o preço de uma IMPRESSÃO de carta em um momento coletado. Preço em Magic varia por impressão: a mesma carta reimpressa em outro set tem cotação própria, e cada uma aparece aqui com seu `id`. Serve pra acompanhar valorização/desvalorização ao longo do tempo, comparar preço entre impressões/sets ou montar um indicador de valor de coleção. A mesma impressão tem várias linhas (uma por coleta) de propósito - é histórico, não é a cotação 'atual'.",
        "columns": {
            "id": "Id da impressão cotada - mesmo id da tabela `cards`. Chave de join entre preço e carta.",
            "name": "Nome da carta.",
            "set": "Código do set/edição desta impressão.",
            "rarity": "Raridade da impressão.",
            "usd": "Preço em dólares americanos da variante normal (não-foil) desta impressão, como veio da fonte.",
            "usd_foil": "Preço em dólares da variante foil da MESMA impressão - a fonte cota foil separado e costuma valer múltiplos do não-foil. Nulo quando a impressão não tem foil.",
            "usd_etched": "Preço em dólares da variante etched foil da mesma impressão. Nulo na esmagadora maioria - só alguns sets tiveram etched.",
            "eur": "Preço em euros da variante normal (não-foil) desta impressão, como veio da fonte.",
            "eur_foil": "Preço em euros da variante foil da mesma impressão. Nulo quando a impressão não tem foil.",
            "tix": "Preço em MTGO tickets, como veio da fonte.",
            "scryfall_uri": "URL da página da carta na Scryfall.",
            "image_url": "URL da imagem da carta. Nulo em cartas de dupla face (a Stage lê só `image_uris` da raiz, sem o fallback pra `card_faces[0]` de `cards.imageUrl`).",
            "releaseDate": "Data de lançamento da impressão/set desta carta.",
        },
    },
    "symbology": {
        "comment": "Catálogo de referência dos símbolos de mana e custo que aparecem no texto das cartas (ex.: {W}, {2/U}, {T}). Serve pra traduzir/renderizar corretamente esses símbolos e pra calcular quanto cada um vale em custo de mana - tabela de apoio, praticamente estática (raramente ganha símbolo novo).",
        "columns": {
            "symbol": "Símbolo de mana/custo em notação textual (ex.: '{W}', '{2/U}').",
            "svg_uri": "URL da imagem SVG do símbolo.",
            "loose_variant": "Variante alternativa de escrita do símbolo em texto livre, quando existe.",
            "english": "Descrição do símbolo em inglês.",
            "transposable": "true se o símbolo pode aparecer em ordem trocada em textos de regra.",
            "represents_mana": "true se o símbolo representa mana (nem todo símbolo representa - ex.: {T} de tap).",
            "appears_in_mana_costs": "true se o símbolo pode aparecer em custos de mana de cartas.",
            "mana_value": "Valor de mana que este símbolo contribui ao custo convertido de mana.",
            "hybrid": "true se é um símbolo de mana híbrida (ex.: {W/U}).",
            "phyrexian": "true se é um símbolo de mana phyrexiana (ex.: {U/P}).",
            "cmc": "Custo de mana convertido equivalente deste símbolo (pode diferir de mana_value em casos especiais).",
            "funny": "true se o símbolo só aparece em cartas não-oficiais/humorísticas (Un-sets).",
            "colors": "Cor(es) de mana associada(s) ao símbolo.",
            "gatherer_alternates": "Grafias alternativas do símbolo usadas no Gatherer.",
        },
    },
    "rulings": {
        "comment": "Esclarecimentos oficiais de regras (rulings) publicados pela Wizards/Scryfall pra cartas específicas, ligados por oracle_id. Serve pra responder dúvida de interação entre cartas ou interpretação de regra que o texto da carta sozinho não deixa claro - uma carta pode acumular várias rulings ao longo do tempo.",
        "columns": {
            "oracle_id": "Oracle id da carta a que esta ruling se aplica (mesmo valor para todas as impressões da carta).",
            # Sobrescreve COLUNAS_COMUNS["source"]: aqui é o emissor da ruling.
            "source": "Quem emitiu a ruling ('wotc' ou 'scryfall') - não é a fonte de linhagem. Partições gravadas antes da correção têm sempre 'scryfall' (a Stage sobrescrevia a coluna).",
            "published_at": "Data de publicação da ruling.",
            "comment": "Texto da ruling / esclarecimento de regras.",
        },
    },
    "migrations": {
        "comment": "Histórico de trocas de identificador de carta na Scryfall (quando duas cartas são unificadas ou uma é removida do catálogo). Serve pra reconciliar um id antigo com o novo e evitar perder o vínculo de uma carta em análises/joins feitos antes da mudança.",
        "columns": {
            "id": "Id único do registro de migração na Scryfall.",
            "uri": "URL da API da Scryfall para este registro de migração.",
            "performed_at": "Data em que a migração de id foi executada pela Scryfall.",
            "migration_strategy": "Estratégia da migração (ex.: 'merge', 'delete').",
            "old_scryfall_id": "Id Scryfall antigo, substituído pela migração.",
            "new_scryfall_id": "Novo id Scryfall quando a estratégia é 'merge' (pode ser nulo em 'delete').",
            "note": "Nota livre da Scryfall explicando o motivo da migração.",
            "metadata_id": "Id da carta associada a esta migração.",
            "metadata_lang": "Idioma da carta associada a esta migração.",
            "metadata_name": "Nome da carta associada a esta migração.",
            "metadata_set_code": "Código do set da carta associada a esta migração.",
            "metadata_oracle_id": "Oracle id da carta associada a esta migração.",
            "metadata_collector_number": "Número de colecionador da carta associada a esta migração.",
        },
    },
}


def obter_comentario_tabela(nome_tabela_bronze):
    return TABELAS_BRONZE.get(nome_tabela_bronze, {}).get("comment")


def obter_comentarios_colunas(nome_tabela_bronze):
    """COLUNAS_COMUNS + colunas específicas da tabela (específica vence em conflito de chave)."""
    colunas_tabela = TABELAS_BRONZE.get(nome_tabela_bronze, {}).get("columns", {})
    return {**COLUNAS_COMUNS, **colunas_tabela}


if __name__ == "__main__":
    for nome_tabela in TABELAS_BRONZE:
        assert obter_comentario_tabela(nome_tabela), f"{nome_tabela} sem comment de tabela"
    comentarios_esclarecimentos = obter_comentarios_colunas("rulings")
    assert comentarios_esclarecimentos["source"] != COLUNAS_COMUNS["source"], \
        "rulings.source deveria sobrescrever o COLUNAS_COMUNS genérico"
    comentarios_cartas = obter_comentarios_colunas("cards")
    assert comentarios_cartas["source"] == COLUNAS_COMUNS["source"], \
        "cards.source não deveria ter override - devia vir do COLUNAS_COMUNS"
    assert comentarios_cartas["name"] == TABELAS_BRONZE["cards"]["columns"]["name"]
    print("bronze_column_docs: OK")
