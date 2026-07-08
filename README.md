<div align="center">

# 🚕 Case Técnico — Data Architect · iFood

**Pipeline de dados NYC TLC (Yellow/Green Cab) em arquitetura Medalhão (Bronze → Silver → Gold)**

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat)
![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=flat&logo=amazons3&logoColor=white)
![CloudFront](https://img.shields.io/badge/CloudFront-232F3E?style=flat&logo=amazonaws&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB--Wasm-FFF000?style=flat&logo=duckdb&logoColor=black)

</div>

<br>

Os dados deste case podem ser consultados de forma interativa, sem sair do navegador,
através do painel disponível no [site pessoal da autora](https://carlasampaio.com.br):
**carlasampaio.com.br** → **Projetos** → **Case iFood**.

---

## 📑 Sumário

- [Objetivo](#-objetivo)
- [Arquitetura](#-arquitetura)
- [Como o painel consulta os dados (S3 + CloudFront + DuckDB-Wasm)](#-como-o-painel-consulta-os-dados-s3--cloudfront--duckdb-wasm)
- [Estrutura do repositório](#-estrutura-do-repositório)
- [Decisões de negócio](#-decisões-de-negócio)
- [Trade-offs de performance](#-trade-offs-de-performance)
- [Perguntas de negócio respondidas](#-perguntas-de-negócio-respondidas)
- [Dicionário de dados](#-dicionário-de-dados)
- [Ambiente e execução](#-ambiente-e-execução)
- [Como navegar este repositório](#-como-navegar-este-repositório)

---

## 🎯 Objetivo

Ingerir os dados de corridas de táxi de NY (jan-mai/2023), disponibilizá-los para consumo via
SQL, e responder duas perguntas de negócio sobre a frota.

---

## 🏗️ Arquitetura

```
Bronze (/Volumes/ifood_case/bronze/raw - Parquet brutos)
    │  Leitura individual por arquivo, validação de contagem/schema,
    │  auditoria de divergência de tipos entre meses
    ▼
Silver (ifood_case.silver.trips)
    │  União Yellow + Green Cab, padronização de schema e nomenclatura
    │  Tratamento: nulos, timestamps invertidos, período inválido, duplicatas
    ▼
Gold
    ├── ifood_case.gold.trips          → grão de corrida individual (consumo)
    └── ifood_case.gold.trip_metrics   → grão agregado (tipo, ano_mes, hora_do_dia)
```

| Camada | O que faz |
|---|---|
| **Bronze** | Preserva os arquivos originais e documenta divergências de schema entre meses (detectadas na auditoria, tratadas na Silver). |
| **Silver** | Consolida Yellow e Green em um schema único e tipado, mantendo todas as colunas de origem para análises exploratórias além do escopo deste case. Schema completo em [`docs/data_dictionary.md`](docs/data_dictionary.md). |
| **Gold** | Duas tabelas: `gold.trips` (grão individual, colunas exigidas pelo case, para consultas ad-hoc) e `gold.trip_metrics` (pré-agregada por tipo/mês/hora, guardando **soma e contagem** — não médias prontas — para responder as perguntas sem reprocessar a Silver e sem o problema de "média das médias"). |

---

## 🌐 Como o painel consulta os dados (S3 + CloudFront + DuckDB-Wasm)

```
Databricks (gold.trips / gold.trip_metrics — tabelas Delta)
    │  Exportação manual para Parquet (ver detalhe abaixo)
    ▼
S3 — bucket ifood-case-data-715428148112 (PRIVADO, Block Public Access ativo)
    │  Origin Access Control (OAC): só a distribuição CloudFront pode ler,
    │  via s3:GetObject — sem ListBucket, sem escrita
    ▼
CloudFront — distribuição HTTPS pública
    │  Serve os arquivos Parquet via HTTP range requests
    ▼
Navegador do visitante — DuckDB-Wasm (WebAssembly)
    │  Lê os Parquet direto do CloudFront, executa SQL 100% no cliente,
    │  sem nenhum servidor/backend no meio
    ▼
docs/painel.html — gráficos (Chart.js) + campo de SQL livre
```

**Por que essa exportação é manual, e não automática dentro do pipeline:**

Foi avaliada uma conexão segura entre o **Databricks Community Edition** e o S3 via **STS**
(credenciais temporárias), com o objetivo de automatizar a leitura/escrita direta no bucket a
partir dos notebooks. As regras IAM foram validadas no console AWS e estavam consistentes; o
teste de conexão pelo próprio Databricks também funcionou. No entanto, **a leitura via PySpark
apresentava erro de forma consistente**. A investigação indicou que o **Databricks Community
Edition não oferece suporte a acesso direto ao S3** nesse cenário — por isso a exportação da
camada Gold para o S3 foi feita manualmente (célula a célula, fora do pipeline automatizado),
em vez de programática.

- **S3**: guarda os arquivos Parquet exportados manualmente da camada Gold — privado, sem
  acesso direto.
- **CloudFront**: única porta de entrada pública para esses arquivos, com HTTPS e cache.
- **DuckDB-Wasm**: motor SQL que roda inteiramente no navegador de quem acessa o painel —
  não existe backend nem servidor de consulta.

<details>
<summary>Detalhes técnicos completos (script de exportação, bucket, URLs diretas)</summary>
<br>

**Publicação da camada Gold no S3**: os dados de saída (tabelas Delta `gold.trips` e
`gold.trip_metrics`) foram extraídos manualmente em formato Parquet, a partir de um Volume
intermediário (`/Volumes/ifood_case/gold/export`), e carregados para o bucket S3:

```python
EXPORT_PATH = "/Volumes/ifood_case/gold/export"

(
    df_gold_metrics
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(f"{EXPORT_PATH}/trip_metrics")
)

(
    df_gold_trips
    .repartition(5, "ano_mes")
    .write
    .mode("overwrite")
    .parquet(f"{EXPORT_PATH}/trips")
)
```

`trip_metrics` sai como um único arquivo (`coalesce(1)`), sem partições físicas — por isso o
`docs/manifest.json` lista só 1 arquivo para essa tabela. `trips` usa
`repartition(5, "ano_mes")`, que faz hash da coluna em 5 baldes; com exatamente 5 valores
distintos de `ano_mes`, existe chance real de colisão de hash — foi o que ocorreu: dois meses
caíram no mesmo balde (arquivo maior) e um balde ficou vazio (sem gerar arquivo), resultando
em **4 arquivos publicados**, não 5. A tabela Delta original (`ifood_case.gold.trips`)
continua com 5 arquivos internamente (`repartition("ano_mes")` na escrita da Silver, sem
colisão nesse caso) — a divergência é específica deste passo de exportação manual, não da
modelagem da camada Gold em si.

**Bucket:** `ifood-case-data-715428148112` (região `us-east-2`)

**URLs diretas (HTTPS via CloudFront, sem login necessário):**

Tabela de métricas (`gold.trip_metrics`, agregada por tipo/mês/hora):
```
https://d2kktwauihnki.cloudfront.net/gold/trip_metrics/part-00000-tid-4160011368293954417-d573e3c2-b725-49ab-a216-7423ebde4919-227-1.c000.snappy.parquet
```

Tabela de corridas (`gold.trips`, grão individual, particionada por `ano_mes`, ~200 MB total,
prefixo `https://d2kktwauihnki.cloudfront.net/gold/trips/`):

| Arquivo | Tamanho |
|---|---|
| `part-00000-tid-2765581612056450840-...-233-1.c000.snappy.parquet` | 2,4 KB |
| `part-00001-tid-2765581612056450840-...-237-1.c000.snappy.parquet` | 119,6 MB |
| `part-00002-tid-2765581612056450840-...-234-1.c000.snappy.parquet` | 42,6 MB |
| `part-00004-tid-2765581612056450840-...-236-1.c000.snappy.parquet` | — |

Nomes completos e atualizados em [`docs/manifest.json`](docs/manifest.json).

**Console AWS** *(referência interna, requer login na conta da autora)*:
[Bucket](https://us-east-2.console.aws.amazon.com/s3/buckets/ifood-case-data-715428148112) ·
[gold/](https://us-east-2.console.aws.amazon.com/s3/buckets/ifood-case-data-715428148112?prefix=gold/)

</details>

---

## 🗂️ Estrutura do repositório

```
ifood-case/
├─ src/                        # Pipeline de ingestão e transformação
│  ├─ 01_bronze.ipynb          # Ingestão e auditoria dos dados brutos
│  ├─ 02_Silver.ipynb          # Padronização, qualidade e tratamento de dados
│  └─ 03_Gold.ipynb            # Modelagem da camada de consumo e exportação para S3
├─ analysis/                   # Respostas às perguntas de negócio do case
│  └─ 01_perguntas.ipynb
├─ docs/
│  ├─ data_dictionary.md       # Dicionário de dados completo (schema Silver)
│  ├─ manifest.json            # Índice dos arquivos Parquet publicados no S3/CloudFront
│  └─ painel.html              # Painel web (DuckDB-Wasm) para consulta interativa dos dados
├─ README.md
└─ requirements.txt
```

> O painel (`docs/painel.html`) está disponível através do
> [site pessoal da autora](https://carlasampaio.com.br), na seção "Projetos".

---

## 🧭 Decisões de negócio

As decisões de tratamento de dados (outliers, valores negativos, nulos, timestamps invertidos,
duplicatas, entre outras) estão completamente documentadas — com critério aplicado e evidência
(contagens, distribuições) — diretamente no código, célula a célula, em `src/02_Silver.ipynb`.

---

## ⚡ Trade-offs de performance

| Decisão | Motivo |
|---|---|
| `percentile_approx` em vez de `percentile` | Usado para todos os cálculos de quantis (mediana de `passenger_count`, quartis de `total_amount` para IQR), evitando o custo de ordenação completa exigido pelo cálculo exato — irrelevante para as decisões tomadas a partir desses valores. |
| `repartition("ano_mes")` antes da escrita | Evita small files dentro de cada partição física, alinhando o número de arquivos de saída ao número real de partições lógicas (5 meses), em vez do paralelismo default (200 partições de shuffle). |
| `coalesce(1)` na tabela de métricas | Volume agregado pequeno (~240 linhas): evita o custo de shuffle do `repartition`, consolidando direto em um único arquivo por partição. |
| `OPTIMIZE ... ZORDER` | Aplicado em `silver.trips` (por `data_corrida`) e `gold.trips` (por `pickup_datetime`) — colunas de filtro comum dentro de uma partição já selecionada por `ano_mes`. Não aplicado em `gold.trip_metrics` por já ser pequena o suficiente. |
| Sem checkpoint intermediário | Não usamos porque o volume de dados e a estrutura leve, simples e linear do pipeline dispensavam. |
| Auditoria de schema no Bronze | Leitura individual por arquivo é necessária (schemas divergem entre meses), mas as estatísticas de cada arquivo são calculadas em uma única ação Spark (`agg_exprs` combinado), em vez de uma ação por coluna. |

---

## ❓ Perguntas de negócio respondidas

1. Qual a média de valor total (`total_amount`) recebido em um mês, considerando todos os
   yellow táxis da frota?
2. Qual a média de passageiros (`passenger_count`) por hora do dia no mês de maio,
   considerando todos os táxis da frota?

**Onde encontrar as respostas:**

| Onde | O quê |
|---|---|
| 📊 Painel interativo | Disponível no [site pessoal da autora](https://carlasampaio.com.br) (Projetos → Case iFood) — os dois gráficos já respondem as perguntas acima, mais consulta SQL livre |
| 📓 `analysis/01_perguntas.ipynb` | Respostas isoladas |
| 📓 `src/03_Gold.ipynb` | Como parte do fluxo de construção da camada Gold |
| 📁 [`src/`](src/) | Pipeline completo que gera os dados por trás das respostas |

---

## 📖 Dicionário de dados

Schema completo da camada Silver, com descrição de cada coluna traduzida fielmente dos
dicionários oficiais do TLC, domínio de valores e linhagem:
[`docs/data_dictionary.md`](docs/data_dictionary.md).

A linhagem (origem de cada coluna, transformações aplicadas Bronze → Silver → Gold) está
documentada como metadado de catálogo (`COMMENT ON COLUMN`) diretamente nas tabelas
`silver.trips`, `gold.trips` e `gold.trip_metrics`, visível no Catalog Explorer do Databricks —
e reproduzida integralmente no arquivo Markdown acima, para consulta fora do ambiente Databricks.

---

## 🛠️ Ambiente e execução

Desenvolvido e executado no **Databricks** (Spark 4.1.0), com Delta Lake nativo do runtime.

```
pyspark==4.1.0
```

O `requirements.txt` cobre a dependência de PySpark para leitura do código fora do ambiente
gerenciado; não inclui `delta-spark` porque a escrita/leitura de tabelas Delta neste projeto
depende de configuração adicional do ambiente Databricks, fora do escopo de uma reprodução
local simples. Notebooks usam `spark` e `dbutils`, providos automaticamente pelo runtime —
não há setup adicional necessário dentro do Databricks.

<details>
<summary><strong>⚠️ Outras limitações de ambiente</strong></summary>
<br>

- **Ingestão dos dados brutos**: os arquivos originais (NYC TLC) foram carregados manualmente
  para um Volume criado na camada Bronze do Databricks (`/Volumes/ifood_case/bronze/raw`), em
  vez de uma ingestão automatizada a partir de uma origem externa.
- **Sem cache intermediário**: o **Databricks Community Edition não permite `cache()`/
  `persist()`** de DataFrames. Diversas ações de validação na Silver, portanto, reprocessam a
  partir da leitura dos parquets originais a cada execução — não por escolha de performance,
  mas por restrição da plataforma.

</details>

---

## 🧩 Como navegar este repositório

| Ordem | Arquivo | Conteúdo |
|---|---|---|
| 1 | `src/01_bronze.ipynb` | Ingestão e auditoria de divergência de schema |
| 2 | `src/02_Silver.ipynb` | Tratamento de qualidade de dados, célula a célula, com evidência |
| 3 | `src/03_Gold.ipynb` | Modelagem de consumo, respostas às perguntas de negócio e exportação para o S3 |
| 4 | `analysis/01_perguntas.ipynb` | Respostas às perguntas de negócio, isoladas |
| 5 | `docs/data_dictionary.md` | Referência de schema, quando necessário durante a leitura |
| 6 | `docs/manifest.json` | Índice dos arquivos Parquet publicados (usado pelo painel e disponível para consulta manual) |
| 7 | `docs/painel.html` | Painel interativo (DuckDB-Wasm), acessível pelo [site pessoal da autora](https://carlasampaio.com.br) |