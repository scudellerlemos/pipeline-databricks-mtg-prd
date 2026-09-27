# Camada Gold

Uma única tabela: `TB_FATO_MERCADO_CARTAS` - visão de mercado de cartas de Magic: The Gathering pronta para consumo direto por analista, BI ou Genie, sem precisar conhecer Bronze/Silver.

- **Script:** `Dev/TB_FATO_MERCADO_CARTAS.py`
- **Utilitários:** `Dev/gold_utils.py` (config/extract/load/auditoria, mesmo padrão de `silver_utils.py`)
- **Comentários de negócio:** `Dev/gold_column_docs.py` (fonte única, aplicada via `COMMENT ON TABLE`/`ALTER COLUMN...COMMENT`)
- **Documentação:** [`Documentação/TB_FATO_MERCADO_CARTAS/Readme.md`](./Documentação/TB_FATO_MERCADO_CARTAS/Readme.md)

As 3 tabelas Gold anteriores (schema pré-DAMA, colunas em inglês que não existem mais na Silver) foram removidas - sem valor de negócio, sem consumidor real.

## Modelagem (Silver -> Gold)

`TB_FATO_MERCADO_CARTAS` usa 5 das 7 tabelas Silver. `TB_DOM_SIMBOLOS` e `TB_PONTE_CARTA_SIMBOLOS` existem na Silver (análise por símbolo/cor de mana) mas têm grão incompatível com a Gold (`TB_DOM_SIMBOLOS`: 1 linha por símbolo; `TB_PONTE_CARTA_SIMBOLOS`: carta x símbolo - nenhum é carta x cotação) - não entram na junção.

```mermaid
graph TD
    subgraph SILVER["Camada Silver (7 tabelas)"]
        FATO_CARTAS["TB_FATO_CARTAS<br/>PK: ID_CARTA"]
        DIM_COLECOES["TB_DIM_COLECOES<br/>PK: COD_COLECAO"]
        FATO_PRECOS["TB_FATO_PRECOS_CARTAS<br/>PK: ID_CARTA + DT_INGESTAO"]
        FATO_ESCLARECIMENTOS["TB_FATO_ESCLARECIMENTOS_CARTAS<br/>PK: ID_ESCLARECIMENTO"]
        MOV_MIGRACOES["TB_MOV_MIGRACOES_CARTAS<br/>PK: ID_MIGRACAO"]
        DOM_SIMBOLOS["TB_DOM_SIMBOLOS<br/>PK: COD_SIMBOLO"]
        PONTE_SIMBOLOS["TB_PONTE_CARTA_SIMBOLOS<br/>PK: ID_CARTA + NUM_ORDEM_SIMBOLO<br/>Liga FATO_CARTAS a DOM_SIMBOLOS"]
    end

    subgraph GOLD["Camada Gold"]
        GOLD_MERCADO["TB_FATO_MERCADO_CARTAS<br/>PK: ID_CARTA + DT_COTACAO"]
    end

    FATO_CARTAS -->|"COD_COLECAO"| GOLD_MERCADO
    DIM_COLECOES -->|"COD_COLECAO"| GOLD_MERCADO
    FATO_PRECOS -->|"ID_CARTA, toda cotacao (INNER)"| GOLD_MERCADO
    FATO_ESCLARECIMENTOS -->|"ID_ORACLE, qtd esclarecimentos"| GOLD_MERCADO
    MOV_MIGRACOES -->|"ID_CARTA_ANTIGO, resolve ID_CARTA_CANONICO"| GOLD_MERCADO

    FATO_CARTAS -.->|"DESC_CUSTO_MANA explodido"| PONTE_SIMBOLOS
    DOM_SIMBOLOS -.->|"COD_SIMBOLO, FK"| PONTE_SIMBOLOS

    classDef used fill:#2f6f4f,stroke:#1b4332,color:#ffffff,stroke-width:2px;
    classDef gold fill:#b8860b,stroke:#7a5c00,color:#ffffff,stroke-width:2px;
    classDef unused fill:#555555,stroke:#999999,color:#ffffff,stroke-dasharray:4 4;

    class FATO_CARTAS,DIM_COLECOES,FATO_PRECOS,FATO_ESCLARECIMENTOS,MOV_MIGRACOES used;
    class GOLD_MERCADO gold;
    class DOM_SIMBOLOS,PONTE_SIMBOLOS unused;
```

Verde = alimenta a Gold. Cinza tracejado = existe na Silver mas não entra na junção (grão incompatível). `TB_PONTE_CARTA_SIMBOLOS` não tem seta pra Gold - por isso, não precisa de aresta "cruzada" pra dizer que não entra.
