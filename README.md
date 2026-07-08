# Case Técnico — Data Architect · iFood

Pipeline de dados NYC TLC (Yellow/Green Cab) em arquitetura Medalhão (Bronze/Silver/Gold),
implementado com PySpark e Delta Lake no Databricks.

## Objetivo

Ingerir os dados de corridas de táxi de NY (jan-mai/2023), disponibilizá-los para consumo via
SQL, e responder duas perguntas de negócio sobre a frota.

## Estrutura do repositório

```
ifood-case/
├─ src/                     # Pipeline de ingestao e transformacao
│  ├─ 01_bronze.ipynb       # Ingestao e auditoria dos dados brutos
│  ├─ 02_Silver.ipynb       # Padronizacao, qualidade e tratamento de dados
│  └─ 03_Gold.ipynb         # Modelagem da camada de consumo
├─ analysis/                # Respostas as perguntas de negocio do case
│  └─ 01_perguntas.ipynb
├─ docs/
│  └─ data_dictionary.md    # Dicionario de dados completo (schema Silver)
├─ README.md
└─ requirements.txt
```

## Arquitetura

```
Bronze (/Volumes/ifood_case/bronze/raw - Parquet brutos)
    |  Leitura individual por arquivo, validacao de contagem/schema,
    |  auditoria de divergencia de tipos entre meses
    v
Silver (ifood_case.silver.trips)
    |  Uniao Yellow + Green Cab, padronizacao de schema e nomenclatura
    |  Tratamento: nulos, timestamps invertidos, periodo invalido, duplicatas
    v
Gold
    +-- ifood_case.gold.trips          - grao de corrida individual (consumo)
    +-- ifood_case.gold.trip_metrics   - grao agregado (tipo, ano_mes, hora_do_dia)
```

**Bronze** preserva os arquivos originais e documenta divergências de schema entre meses
(detectadas na auditoria, tratadas na Silver).

**Silver** consolida Yellow e Green em um schema único e tipado, mantendo todas as colunas de
origem para suportar análises exploratórias adicionais além do escopo deste case. Schema
completo documentado em [`docs/data_dictionary.md`](docs/data_dictionary.md).

**Gold** tem duas tabelas com propósitos diferentes:
- `gold.trips`, no grão de corrida individual, com as colunas exigidas pelo case
  (`VendorID`, `passenger_count`, `total_amount`, `pickup_datetime`, `dropoff_datetime`),
  para consultas ad-hoc não previstas neste case;
- `gold.trip_metrics`, pré-agregada por tipo de táxi/mês/hora, armazenando **soma e contagem**
  (não médias prontas) para responder as perguntas de negócio sem reprocessar a Silver a cada
  consulta, e sem o problema de "média das médias" ao recombinar em granularidades diferentes.

## Decisões de negócio

- **Outliers de `total_amount`** (critério IQR): mantidos — justificados por corridas de maior
  duração, não por erro de dado.
- **`total_amount` negativo**: mantido — associado a uma regra de negócio específica do
  `VendorID = 2`, confirmada batendo o valor total com a soma dos componentes financeiros.
- **Nulos em `passenger_count`** (2,73% dos registros): imputados pela mediana.
- **Timestamps invertidos** (`dropoff_datetime` < `pickup_datetime`): corrigidos via
  `least`/`greatest`, preservados em `pickup_datetime_tratado`/`dropoff_datetime_tratado`.

Detalhes completos de cada decisão, com evidência (contagens, distribuições), estão nos outputs
já executados dentro de `src/02_Silver.ipynb`.

## Trade-offs de performance

- **`percentile_approx` em vez de `percentile`**: usado consistentemente para cálculos de
  quantis (mediana de `passenger_count`, quartis de `total_amount` para o IQR), evitando o
  custo de ordenação completa exigido pelo cálculo exato — irrelevante para as decisões de
  negócio tomadas a partir desses valores.
- **`repartition("ano_mes")` antes da escrita** (Silver e `gold.trips`): evita o problema de
  small files dentro de cada partição física, alinhando o número de arquivos de saída ao
  número real de partições lógicas (5 meses), em vez de depender do paralelismo default de
  processamento (200 partições de shuffle).
- **`coalesce(1)` na tabela de métricas**: como o volume agregado é pequeno (~240 linhas), evita
  o custo de shuffle do `repartition`, consolidando direto em um único arquivo por partição sem
  redistribuir dados pela rede.
- **`OPTIMIZE ... ZORDER`**: aplicado em `silver.trips` (por `data_corrida`) e `gold.trips`
  (por `pickup_datetime`) — colunas de filtro comum dentro de uma partição já selecionada por
  `ano_mes`. Não aplicado em `gold.trip_metrics` por ser pequena o suficiente para não ter
  ganho de otimização física.
- **Sem cache/checkpoint intermediário**: diversas ações de validação na Silver reprocessam a
  partir da leitura dos parquets originais. Para o volume deste case (5 meses de NYC TLC), o
  custo de reprocessar é aceitável frente à complexidade de um checkpoint intermediário; em um
  cenário de maior escala, essa seria a próxima otimização a avaliar.
- **Auditoria de schema no Bronze**: leitura individual por arquivo é necessária (schemas
  divergem entre meses), mas as estatísticas de cada arquivo são calculadas em uma única ação
  Spark por arquivo (`agg_exprs` combinado), em vez de uma ação por coluna.

## Perguntas de negócio respondidas

1. Qual a média de valor total (`total_amount`) recebido em um mês, considerando todos os
   yellow táxis da frota?
2. Qual a média de passageiros (`passenger_count`) por hora do dia no mês de maio,
   considerando todos os táxis da frota?

Consultas e resultados em `analysis/01_respostas.ipynb` (também presentes em `src/03_Gold.ipynb`,
como parte do fluxo de construção da camada Gold).

## Dicionário de dados

Schema completo da camada Silver, com descrição de cada coluna traduzida fielmente dos
dicionários oficiais do TLC, domínio de valores e linhagem:
[`docs/data_dictionary.md`](docs/data_dictionary.md).

As mesmas descrições estão registradas como metadado de catálogo (`COMMENT ON COLUMN`) nas
tabelas `silver.trips`, `gold.trips` e `gold.trip_metrics`, visíveis no Catalog Explorer do
Databricks.

## Ambiente e execução

Desenvolvido e executado no **Databricks** (Spark 4.1.0), com Delta Lake nativo do runtime.

```
pyspark==4.1.0
```

O `requirements.txt` cobre a dependência de PySpark para leitura do código fora do ambiente
gerenciado; não inclui `delta-spark` porque a escrita/leitura de tabelas Delta neste projeto
depende de configuração adicional do ambiente Databricks, fora do escopo de uma reprodução
local simples. Notebooks usam `spark` e `dbutils`, providos automaticamente pelo runtime —
não há setup adicional necessário dentro do Databricks.

**Como navegar este repositório:**
1. `src/01_bronze.ipynb` — ingestão e auditoria de divergência de schema
2. `src/02_Silver.ipynb` — tratamento de qualidade de dados, célula a célula, com evidência
3. `src/03_Gold.ipynb` — modelagem de consumo e respostas às perguntas de negócio
4. `analysis/01_respostas.ipynb` — respostas às perguntas de negócio, isoladas
5. `docs/data_dictionary.md` — referência de schema, quando necessário durante a leitura