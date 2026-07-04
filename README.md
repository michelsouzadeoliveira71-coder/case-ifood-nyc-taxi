# Case iFood — Pipeline de Dados NYC Taxi 🚕

Este projeto implementa um pipeline de dados utilizando PySpark e Delta Lake para ingestão, padronização e disponibilização dos dados de corridas de táxi de Nova York (NYC TLC). A solução segue a arquitetura Medallion (Bronze → Silver → Gold) e responde às análises propostas no desafio técnico do iFood.

---

## Índice

1. [Contexto](#contexto)
2. [Tecnologias](#tecnologias)
3. [Arquitetura](#arquitetura)
4. [Modelo de Dados](#modelo-de-dados)
5. [Estrutura do Repositório](#estrutura-do-repositório)
6. [Pré-requisitos](#pré-requisitos)
7. [Como Executar](#como-executar)
8. [Resultados Analíticos](#resultados-analíticos)
9. [Decisões Técnicas](#decisões-técnicas)
10. [Data Quality](#data-quality)
11. [Limitações Conhecidas](#limitações-conhecidas)
12. [Licença](#licença)

---

## Contexto

**Fonte de dados:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
**Período:** Janeiro a Maio de 2023  
**Tipos de táxi:** Yellow, Green, HVFHV (Uber/Lyft), FHV  
**Ambiente:** Databricks Community Edition + Delta Lake + Hive Metastore

---

## Tecnologias

- PySpark
- Delta Lake
- Databricks Community Edition
- Hive Metastore
- SQL

---

## Arquitetura

O pipeline segue a arquitetura Medallion com separação clara de responsabilidades:

```text
NYC TLC Parquet
        │
        ▼
┌─────────────────────────────────────────────────────┐
│ BRONZE                                              │
│ Ingestão fiel da fonte, sem transformações          │
│ 4 tabelas: yellow, green, hvfhv, fhv               │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│ SILVER                                              │
│ Padronização, qualidade e observabilidade           │
│ Schema canônico, ride_key, record_hash, flags       │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│ GOLD                                                │
│ Regras de negócio e modelo analítico                │
│ fact_trips + KPIs + vw_taxi_consumo                 │
└─────────────────────────────────────────────────────┘

Camada	Responsabilidade
Bronze	Preservar exatamente o que veio da fonte
Silver	Padronizar, validar, instrumentar
Gold	Regras de negócio, métricas e consumo

Modelo de Dados
Bronze
├── yellow_taxi
├── green_taxi
├── hvfhv_taxi
└── fhv_taxi

Silver
├── yellow_taxi
├── green_taxi
├── hvfhv_taxi
└── fhv_taxi

Gold
├── fact_trips                    ← UNION ALL das 4 fontes
├── kpi_monthly_total_amount      ← Pergunta 1
├── kpi_hourly_passenger_count    ← Pergunta 2
└── vw_taxi_consumo               ← View de consumo (contrato TLC)

Estrutura do Repositório
case-ifood-nyc-taxi/
│
├── src/
│   ├── 01_ingest_bronze.py
│   ├── 02_transform_silver.py
│   └── 03_gold.py
│
├── analysis/
│   ├── 03_data_quality_audit.py
│   ├── 04_gold_audit.py
│   └── queries_analiticas.sql
│
├── README.md
└── requirements.txt

Pré-requisitos
Databricks Community Edition
Arquivos Parquet da NYC TLC (jan–mai 2023) organizados na landing zone:
/Volumes/workspace/case_landing/raw_files/
├── yellow/   → yellow_tripdata_2023-0{1..5}.parquet
├── green/    → green_tripdata_2023-0{1..5}.parquet
├── hvfhv/    → fhvhv_tripdata_2023-0{1..5}.parquet
└── fhv/      → fhv_tripdata_2023-0{1..5}.parquet

Como Executar
Execute os notebooks na seguinte ordem:
1. src/01_ingest_bronze.py
2. src/02_transform_silver.py
3. src/03_gold.py
4. src/04_data_quality_audit.py   (opcional — EDA)
5. queries_analiticas.py           (opcional — validação semântica)

Ao final, os seguintes databases estarão disponíveis:

Database	Conteúdo
case_bronze	4 tabelas Delta com dados brutos
case_silver	4 tabelas padronizadas com qualidade
case_gold	fact_trips + 2 KPIs + view de consumo

View de consumo:
A modelagem interna utiliza nomenclatura canônica (provider_id, pickup_datetime, etc.). Para atender ao contrato solicitado pelo case, a view vw_taxi_consumo expõe os nomes originais da TLC (VendorID, tpep_pickup_datetime e tpep_dropoff_datetime) sem duplicação física dos dados.
SELECT * FROM case_gold.vw_taxi_consumo LIMIT 10;

Resultados Analíticos
Pergunta 1 — Média de total_amount por mês (Yellow taxis)
SELECT
    pickup_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM case_gold.fact_trips
WHERE taxi_type = 'yellow'
GROUP BY pickup_month
ORDER BY pickup_month;

Mês	Média total_amount	Corridas
Janeiro	$ 27,44	3.041.418
Fevereiro	$ 27,33	2.888.723
Março	$ 28,26	3.373.353
Abril	$ 28,76	3.257.912
Maio	$ 29,46	3.481.178

Conclusões:
O ticket médio apresentou crescimento gradual entre janeiro e maio de 2023.
Maio registrou o maior valor médio ($ 29,46), representando +7,4% em relação a janeiro.
O volume de corridas também cresceu, sugerindo aumento combinado de demanda e tarifa.

Pergunta 2 — Média de passenger_count por hora em maio (todos os táxis)
SELECT
    pickup_hour,
    ROUND(AVG(passenger_count), 4) AS avg_passenger_count
FROM case_gold.fact_trips
WHERE pickup_month = 5
  AND passenger_count > 0
GROUP BY pickup_hour
ORDER BY pickup_hour;
Hora	Média	Hora	Média
0h	1,4269	12h	1,3746
1h	1,4366	13h	1,3829
2h	1,4542	14h	1,3880
3h	1,4498	15h	1,3992
4h	1,4039	16h	1,3961
5h	1,2844	17h	1,3871
6h	1,2617	18h	1,3812
7h	1,2814	19h	1,3900
8h	1,2937	20h	1,3996
9h	1,3109	21h	1,4182
10h	1,3465	22h	1,4269
11h	1,3615	23h	1,4214
Conclusões:

A média de passageiros por corrida permaneceu próxima de 1,36 ao longo do dia.
O menor valor ocorre às 6h (1,2617), início do dia de trabalho com viagens predominantemente solo.
O maior valor ocorre às 2h (1,4542), compatível com grupos saindo de eventos noturnos.
Apenas Yellow e Green contribuem para esta métrica — FHV e HVFHV não coletam passenger_count por limitação regulatória.
Decisões Técnicas
1. Arquitetura Medallion
Separação clara de responsabilidades entre Bronze, Silver e Gold, evitando vazamento de regras de negócio para camadas de qualidade.

2. PySpark como tecnologia central
Utilizado em todas as camadas do pipeline, desde a ingestão até as transformações analíticas.

3. Delta Lake como formato de armazenamento
Transações ACID, schema evolution, time travel e integração nativa com Databricks e Hive Metastore.

4. passenger_count = 0 preservado na Silver
O valor 0 na fonte TLC frequentemente indica "não informado". O filtro > 0 é uma decisão analítica aplicada na Gold, não uma regra de qualidade da Silver.

5. total_amount do HVFHV reconstruído na Gold
O HVFHV não possui total_amount nativo. A soma dos componentes financeiros (base_passenger_fare + tips + tolls + bcf + sales_tax + congestion_surcharge + airport_fee) foi calculada na Gold como regra de negócio explícita.

6. ride_key separado do record_hash

ride_key → identidade lógica da corrida (quem, quando, onde) — usado para análises
record_hash → fingerprint do registro físico — usado para auditoria e lineage
7. View de consumo (vw_taxi_consumo)
Expõe as colunas com nomenclatura original da TLC sem alterar a modelagem canônica interna da Gold.

Data Quality
O pipeline inclui dois notebooks de auditoria completos (analysis/).

Regras aplicadas na Silver:

Remoção de corridas com total_amount < 0 (141.407 Yellow / 916 Green)
Remoção de corridas com base_passenger_fare < 0 (58.813 HVFHV)
Remoção de timelines invertidas — pickup > dropoff (795 Yellow)
Remoção de corridas com duração superior a 24h (1.914 FHV)
Remoção de distâncias fora do sanity bound de 200 milhas
Remoção de duplicatas físicas no FHV (10.115 linhas)
Preservação de passenger_count = 0 — filtro analítico pertence à Gold
Volumes do pipeline:

Camada	Yellow	Green	HVFHV	FHV	Total
Bronze	16.186.386	339.630	95.846.120	6.185.664	118.557.800
Silver	16.042.584	338.486	95.786.907	6.177.932	118.345.909
Removidos	143.802 (0,9%)	1.144 (0,3%)	59.213 (0,1%)	7.732 (0,1%)	—
Limitações Conhecidas
FHV sem dados financeiros: não reporta tarifas por obrigação regulatória — total_amount permanece NULL
HVFHV total_amount reconstruído: componentes NULL tratados como zero na soma — decisão analítica documentada
FHV sem localização: 78% dos registros sem pickup_location_id por limitação regulatória
HVFHV 19 pares com tarifas distintas: causa ambígua — preservados sem remoção na Silver
Licença
Projeto desenvolvido exclusivamente para fins de avaliação técnica.


