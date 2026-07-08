# Dicionário de Dados — NYC TLC Trip Record Data

Fonte oficial: PDFs do TLC (18/03/2025) — *Data Dictionary – Yellow Taxi Trip Records* e
*Data Dictionary - LPEP Trip Records* — http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Tradução literal dos campos descritos nos dicionários oficiais, mapeados para os nomes de
coluna padronizados na camada Silver deste case (`ifood_case.silver.trips`). Nenhuma descrição
foi complementada ou reinterpretada além do texto original — onde a fonte não documenta um
campo ou um valor, isso é sinalizado explicitamente em vez de presumido.

---

## Colunas — Camada Silver

| Coluna (Silver) | Origem Yellow | Origem Green | Tipo (Silver) | Descrição (tradução literal do dicionário oficial) |
|---|---|---|---|---|
| `VendorID` | VendorID | VendorID | int | Código indicando o provedor TPEP (Yellow) / LPEP (Green) que forneceu o registro. Ver domínio de valores abaixo. |
| `pickup_datetime` | tpep_pickup_datetime | lpep_pickup_datetime | timestamp_ntz | Data e hora em que o taxímetro foi acionado. |
| `dropoff_datetime` | tpep_dropoff_datetime | lpep_dropoff_datetime | timestamp_ntz | Data e hora em que o taxímetro foi desligado. |
| `store_and_fwd_flag` | store_and_fwd_flag | store_and_fwd_flag | string | Indica se o registro da corrida foi mantido na memória do veículo antes de ser enviado ao provedor ("store and forward"), por o veículo não ter conexão com o servidor. **Domínio de valores (Y/N) documentado apenas no dicionário Yellow** — ver nota no domínio de valores abaixo. |
| `RatecodeID` | RatecodeID | RatecodeID | long | Código da tarifa final em vigor ao término da corrida. Ver domínio de valores abaixo. |
| `PULocationID` | PULocationID | PULocationID | long | Zona de táxi do TLC em que o taxímetro foi acionado. |
| `DOLocationID` | DOLocationID | DOLocationID | long | Zona de táxi do TLC em que o taxímetro foi desligado. |
| `passenger_count` | passenger_count | passenger_count | long | Número de passageiros no veículo. |
| `trip_distance` | trip_distance | trip_distance | double | Distância percorrida da corrida, em milhas, registrada pelo taxímetro. |
| `fare_amount` | fare_amount | fare_amount | double | Tarifa calculada pelo taxímetro em função do tempo e distância. Para informações adicionais, ver https://www.nyc.gov/site/tlc/passengers/taxi-fare.page |
| `extra` | extra | extra | double | Acréscimos e sobretaxas diversas. |
| `mta_tax` | mta_tax | mta_tax | double | Taxa acionada automaticamente com base na tarifa registrada no taxímetro. |
| `tip_amount` | tip_amount | tip_amount | double | Valor da gorjeta. Preenchido automaticamente para gorjetas em cartão de crédito. Gorjetas em dinheiro não são incluídas. |
| `tolls_amount` | tolls_amount | tolls_amount | double | Valor total de todos os pedágios pagos na corrida. |
| `ehail_fee` | *(campo não documentado no dicionário oficial vigente)* | *(campo não documentado no dicionário oficial vigente)* | double | **Não consta em nenhum dos dois dicionários oficiais (18/03/2025).** Campo presente no schema físico dos arquivos parquet processados, sem descrição oficial atual disponível. Preenchido com `0.0` na Silver para registros Yellow (campo ausente na origem). |
| `improvement_surcharge` | improvement_surcharge | improvement_surcharge | double | Sobretaxa de melhoria cobrada na bandeirada. A sobretaxa de melhoria começou a ser cobrada em 2015. |
| `total_amount` | total_amount | total_amount | double | Valor total cobrado dos passageiros. Não inclui gorjetas em dinheiro. |
| `payment_type` | payment_type | payment_type | long | Código numérico indicando como o passageiro pagou a corrida. Ver domínio de valores abaixo. |
| `trip_type` | *(campo não existe no dicionário Yellow)* | trip_type | long | Código indicando se a corrida foi uma parada na rua (street-hail) ou um despacho (dispatch), atribuído automaticamente com base na tarifa em uso, podendo ser alterado pelo motorista. Ausente no dicionário/origem Yellow — preenchido com `0` na Silver como marcador de "não aplicável". |
| `congestion_surcharge` | congestion_surcharge | congestion_surcharge | double | Valor total cobrado na corrida referente à sobretaxa de congestionamento do estado de NY (NYS). |
| `airport_fee` | airport_fee | *(campo não existe no dicionário Green)* | double | Aplicável somente para embarques nos aeroportos LaGuardia e John F. Kennedy. Ausente no dicionário/origem Green — preenchido com `0.0` na Silver. |
| `tipo` | *(derivada)* | *(derivada)* | string | Coluna adicionada no pipeline (não faz parte do dicionário oficial do TLC) para identificar o serviço de origem do registro: `yellow` ou `green`. |

> **Não incluído na camada Silver:** `cbd_congestion_fee` — "Per-trip charge for MTA's Congestion
> Relief Zone starting Jan. 5, 2025" (cobrança por corrida para a Zona de Alívio de Congestionamento
> da MTA, a partir de 5 de janeiro de 2025). Documentado em ambos os dicionários oficiais, mas fora
> do período de dados deste case (jan-mai/2023).

---

## Colunas derivadas / tratadas — exclusivas da Silver

Estas colunas não existem na origem TLC nem em seus dicionários oficiais; foram criadas durante
o tratamento de dados documentado no notebook `src/02_Silver.ipynb`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_corrida` | date | Data (sem hora) extraída de `pickup_datetime`, usada para agregações diárias e particionamento lógico das análises. |
| `ano_mes` | string | Ano e mês (`yyyy-MM`) extraído de `pickup_datetime`. Usado como chave de particionamento físico das tabelas Silver e Gold. |
| `pickup_datetime_tratado` | timestamp_ntz | Versão corrigida de `pickup_datetime`: para os registros em que `dropoff_datetime` era anterior a `pickup_datetime` (timestamps invertidos na origem), os dois valores foram trocados entre si. |
| `dropoff_datetime_tratado` | timestamp_ntz | Versão corrigida de `dropoff_datetime`, correspondente a `pickup_datetime_tratado`. |

---

## Domínio de valores

Valores exatamente como documentados nos dicionários oficiais (18/03/2025).

### VendorID (idêntico em ambos os dicionários)
| Código | Provedor |
|---|---|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix *(exclusivo do dicionário Yellow — não consta no dicionário Green)* |

### RatecodeID (idêntico em ambos os dicionários)
| Código | Tarifa |
|---|---|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |
| 99 | Null/unknown |

### payment_type (idêntico em ambos os dicionários)
| Código | Forma de pagamento |
|---|---|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

### trip_type (documentado apenas no dicionário Green)
| Código | Tipo |
|---|---|
| 1 | Street-hail |
| 2 | Dispatch |

> Yellow não possui este campo em sua origem; preenchido com `0` na Silver como
> marcador de "não aplicável" (decisão do pipeline, não do dicionário oficial).

### store_and_fwd_flag (domínio documentado apenas no dicionário Yellow)
| Valor | Significado |
|---|---|
| Y | store and forward trip |
| N | not a store and forward trip |

> O dicionário Green descreve o campo mas não lista esses valores explicitamente. Como é o
> mesmo campo conceitual em ambas as origens, presume-se o mesmo domínio — mas isso é uma
> inferência do pipeline, não uma afirmação documentada no dicionário Green.

---

## Linhagem

```
Bronze (/Volumes/ifood_case/bronze/raw, arquivos Parquet brutos)
    |
    v
Silver (ifood_case.silver.trips)
    |  - Uniao Yellow + Green Cab
    |  - Padronizacao de schema e nomenclatura
    |  - Tratamento de nulos, timestamps invertidos, periodo invalido
    |  - Deduplicacao
    |
    +--> Gold (ifood_case.gold.trips)
    |      Grao de corrida individual - camada de consumo
    |      (VendorID, passenger_count, total_amount, pickup_datetime, dropoff_datetime)
    |
    +--> Gold (ifood_case.gold.trip_metrics)
           Grao agregado (tipo, ano_mes, hora_do_dia) - perguntas de negocio do case
```

Detalhes de cada transformação: `src/02_Silver.ipynb` e `src/03_Gold.ipynb`.
Linhagem automática (gerada pelo motor) disponível em Catalog Explorer → tabela → aba *Lineage*.