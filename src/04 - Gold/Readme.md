# Camada Gold

`TB_FATO_MERCADO_CARTAS` - visão de mercado de cartas de Magic: The Gathering pronta para consumo direto por analista, BI ou Genie, sem precisar conhecer Bronze/Silver. Tabela: preços da Silver x `TB_DIM_CARTAS` (atributos atuais da carta). A dimensão é regravada com overwrite; a fato é incremental (cotações novas + histórico das cartas que mudaram na dimensão), com `rebuild=true` para recalcular tudo. Ver [ADR-013](../../docs/ADR.md#adr-013--gold-incremental-com-propagação-da-dimensão).

- **Script:** `Dev/TB_FATO_MERCADO_CARTAS.py`
- **Utilitários:** `Dev/gold_utils.py` (config/extract/load/auditoria, mesmo padrão de `silver_utils.py`)
- **Comentários de negócio:** `Dev/gold_column_docs.py` (fonte única, aplicada via `COMMENT ON TABLE`/`ALTER COLUMN...COMMENT`)
- **Documentação:** [`Documentação/TB_FATO_MERCADO_CARTAS/Readme.md`](./Documentação/TB_FATO_MERCADO_CARTAS/Readme.md)

## Modelagem (Silver -> Gold)

`TB_FATO_MERCADO_CARTAS` usa as 5 tabelas Silver, via `TB_DIM_CARTAS`. A dimensão é regravada inteira (overwrite); a fato recebe MERGE só com as cotações novas e com o histórico das cartas que mudaram na dimensão. Carga completa na primeira run, se a dimensão mudar de colunas ou com `rebuild=true`.

```mermaid
graph TD
    subgraph SILVER["Camada Silver (5 tabelas)"]
        FATO_CARTAS["TB_FATO_CARTAS<br/>PK: ID_CARTA"]
        DIM_COLECOES["TB_DIM_COLECOES<br/>PK: COD_COLECAO"]
        FATO_PRECOS["TB_FATO_PRECOS_CARTAS<br/>PK: ID_CARTA + DT_INGESTAO"]
        FATO_ESCLARECIMENTOS["TB_FATO_ESCLARECIMENTOS_CARTAS<br/>PK: ID_ESCLARECIMENTO"]
        MOV_MIGRACOES["TB_MOV_MIGRACOES_CARTAS<br/>PK: ID_MIGRACAO"]
    end

    subgraph GOLD["Camada Gold"]
        GOLD_DIM["TB_DIM_CARTAS<br/>PK: ID_CARTA<br/>overwrite"]
        GOLD_MERCADO["TB_FATO_MERCADO_CARTAS<br/>PK: ID_CARTA + DT_COTACAO<br/>MERGE incremental"]
    end

    FATO_CARTAS -->|"driver"| GOLD_DIM
    DIM_COLECOES -->|"COD_COLECAO"| GOLD_DIM
    FATO_ESCLARECIMENTOS -->|"ID_ORACLE, qtd esclarecimentos"| GOLD_DIM
    MOV_MIGRACOES -->|"ID_CARTA_ANTIGO, resolve ID_CARTA_CANONICO"| GOLD_DIM
    GOLD_DIM -->|"ID_CARTA; cartas que mudaram reprocessam todo o histórico"| GOLD_MERCADO
    FATO_PRECOS -->|"ID_CARTA (INNER); só DT_INGESTAO > última DT_COTACAO"| GOLD_MERCADO

    classDef used fill:#2f6f4f,stroke:#1b4332,color:#ffffff,stroke-width:2px;
    classDef gold fill:#b8860b,stroke:#7a5c00,color:#ffffff,stroke-width:2px;

    class FATO_CARTAS,DIM_COLECOES,FATO_PRECOS,FATO_ESCLARECIMENTOS,MOV_MIGRACOES used;
    class GOLD_DIM,GOLD_MERCADO gold;
```

Verde = alimenta a Gold.
