# Camada Gold

Uma única tabela: `TB_FATO_MERCADO_CARTAS` - visão de mercado de cartas de Magic: The Gathering pronta para consumo direto por analista, BI ou Genie, sem precisar conhecer Bronze/Silver.

- **Script:** `Dev/TB_FATO_MERCADO_CARTAS.py`
- **Utilitários:** `Dev/gold_utils.py` (config/extract/load/auditoria, mesmo padrão de `silver_utils.py`)
- **Comentários de negócio:** `Dev/gold_column_docs.py` (fonte única, aplicada via `COMMENT ON TABLE`/`ALTER COLUMN...COMMENT`)
- **Documentação:** [`Documentação/TB_FATO_MERCADO_CARTAS/Readme.md`](./Documentação/TB_FATO_MERCADO_CARTAS/Readme.md)

As 3 tabelas Gold anteriores (schema pré-DAMA, colunas em inglês que não existem mais na Silver) foram removidas - sem valor de negócio, sem consumidor real.

## Modelagem (Silver -> Gold)

`TB_FATO_MERCADO_CARTAS` usa 5 das 7 tabelas Silver. `TB_DOM_SIMBOLOS` e `TB_PONTE_CARTA_SIMBOLOS` existem na Silver (análise por símbolo/cor de mana) mas têm grão incompatível com a Gold (carta x símbolo, não carta x cotação) - não entram na junção.

```mermaid
graph TD
    subgraph SILVER["Camada Silver (7 tabelas)"]
        FATO_CARTAS["TB_FATO_CARTAS<br/>PK: Id_carta"]
        DIM_COLECOES["TB_DIM_COLECOES<br/>PK: Cod_colecao"]
        FATO_PRECOS["TB_FATO_PRECOS_CARTAS<br/>PK: Nme_carta + Dt_ingestao"]
        FATO_ESCLARECIMENTOS["TB_FATO_ESCLARECIMENTOS_CARTAS<br/>PK: Id_esclarecimento"]
        MOV_MIGRACOES["TB_MOV_MIGRACOES_CARTAS<br/>PK: Id_migracao"]
        DOM_SIMBOLOS["TB_DOM_SIMBOLOS<br/>PK: Cod_simbolo"]
        PONTE_SIMBOLOS["TB_PONTE_CARTA_SIMBOLOS<br/>PK: Id_carta + Num_ordem_simbolo<br/>Junta FATO_CARTAS x DOM_SIMBOLOS"]
    end

    subgraph GOLD["Camada Gold"]
        GOLD_MERCADO["TB_FATO_MERCADO_CARTAS<br/>PK: Id_carta"]
    end

    FATO_CARTAS -->|"Cod_colecao"| GOLD_MERCADO
    DIM_COLECOES -->|"Cod_colecao"| GOLD_MERCADO
    FATO_PRECOS -->|"Nme_carta, ultima cotacao"| GOLD_MERCADO
    FATO_ESCLARECIMENTOS -->|"Id_oracle, qtd esclarecimentos"| GOLD_MERCADO
    MOV_MIGRACOES -->|"Id_carta_antigo, resolve Id_carta_canonico"| GOLD_MERCADO

    FATO_CARTAS -.->|"Desc_custo_mana explodido"| PONTE_SIMBOLOS
    DOM_SIMBOLOS -.->|"Cod_simbolo, FK"| PONTE_SIMBOLOS

    classDef used fill:#2f6f4f,stroke:#1b4332,color:#ffffff,stroke-width:2px;
    classDef gold fill:#b8860b,stroke:#7a5c00,color:#ffffff,stroke-width:2px;
    classDef unused fill:#555555,stroke:#999999,color:#ffffff,stroke-dasharray:4 4;

    class FATO_CARTAS,DIM_COLECOES,FATO_PRECOS,FATO_ESCLARECIMENTOS,MOV_MIGRACOES used;
    class GOLD_MERCADO gold;
    class DOM_SIMBOLOS,PONTE_SIMBOLOS unused;
```

Verde = alimenta a Gold. Cinza tracejado = existe na Silver mas não entra na junção (grão incompatível). `TB_PONTE_CARTA_SIMBOLOS` não tem seta pra Gold - por isso, não precisa de aresta "cruzada" pra dizer que não entra.
