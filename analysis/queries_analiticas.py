# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- PERGUNTA 1
# MAGIC -- Qual a média de total_amount por mês considerando
# MAGIC -- todos os yellow taxis da frota? (jan–mai 2023)
# MAGIC --
# MAGIC -- Fonte : case_gold.fact_trips
# MAGIC -- Filtro: taxi_type = 'yellow'
# MAGIC -- Métrica: AVG(total_amount)
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     pickup_month,
# MAGIC     ROUND(AVG(total_amount), 2)  AS avg_total_amount,
# MAGIC     COUNT(*)                      AS n_corridas,
# MAGIC     ROUND(SUM(total_amount), 2)  AS total_receita
# MAGIC
# MAGIC FROM case_gold.fact_trips
# MAGIC
# MAGIC WHERE taxi_type = 'yellow'
# MAGIC
# MAGIC GROUP BY pickup_month
# MAGIC ORDER BY pickup_month;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- PERGUNTA 2
# MAGIC -- Qual a média de passenger_count por hora do dia no mês
# MAGIC -- de maio considerando todos os táxis da frota?
# MAGIC --
# MAGIC -- Fonte  : case_gold.fact_trips
# MAGIC -- Filtro : pickup_month = 5 (maio)
# MAGIC --          passenger_count > 0 (regra analítica: exclui
# MAGIC --          registros sem contagem informada)
# MAGIC -- Métrica: AVG(passenger_count) por hora do dia
# MAGIC --
# MAGIC -- Nota: FHV e HVFHV não disponibilizam passenger_count
# MAGIC -- por limitação regulatória. Apenas Yellow e Green
# MAGIC -- contribuem naturalmente para esta métrica.
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     pickup_hour,
# MAGIC     ROUND(AVG(passenger_count), 4) AS avg_passenger_count,
# MAGIC     COUNT(*)                        AS n_corridas
# MAGIC
# MAGIC FROM case_gold.fact_trips
# MAGIC
# MAGIC WHERE pickup_month    =  5
# MAGIC   AND passenger_count >  0
# MAGIC
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ALTERNATIVA: USANDO A VIEW DE CONSUMO
# MAGIC -- A view vw_taxi_consumo expõe os dados com os nomes
# MAGIC -- originais da fonte TLC para compatibilidade.
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Pergunta 1 via view:
# MAGIC SELECT
# MAGIC     MONTH(tpep_pickup_datetime)        AS pickup_month,
# MAGIC     ROUND(AVG(total_amount), 2)        AS avg_total_amount
# MAGIC FROM case_gold.vw_taxi_consumo
# MAGIC WHERE taxi_type = 'yellow'
# MAGIC GROUP BY MONTH(tpep_pickup_datetime)
# MAGIC ORDER BY pickup_month;
# MAGIC
# MAGIC -- Pergunta 2 via view:
# MAGIC SELECT
# MAGIC     HOUR(tpep_pickup_datetime)         AS pickup_hour,
# MAGIC     ROUND(AVG(passenger_count), 4)     AS avg_passenger_count
# MAGIC FROM case_gold.vw_taxi_consumo
# MAGIC WHERE MONTH(tpep_pickup_datetime) = 5
# MAGIC   AND passenger_count > 0
# MAGIC GROUP BY HOUR(tpep_pickup_datetime)
# MAGIC ORDER BY pickup_hour;
# MAGIC