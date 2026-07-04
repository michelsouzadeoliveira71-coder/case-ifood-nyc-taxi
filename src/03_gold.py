# Databricks notebook source
# ==============================================================================
# NOTEBOOK: 03_gold
# CAMADA:   Gold
# DEPENDE:  02_transform_silver (case_silver.*)
#
# OBJETIVO:
#   Produzir modelos analíticos prontos para consumo.
#   Aplica regras de negócio, reconstrói métricas derivadas e responde
#   às perguntas analíticas do case.
#
# RESPONSABILIDADES POR CAMADA:
#   Bronze : preservação da origem
#   Silver : padronização, qualidade e observabilidade
#   Gold   : regras de negócio, combinação de fontes e consumo analítico
#
#   Diferentemente da Silver, a Gold pode combinar fontes, reconstruir
#   métricas derivadas e aplicar filtros específicos de cada análise.
#
# LINHAGEM DOS ATIVOS:
#   fact_trips                → UNION ALL + reconstrução total_amount HVFHV
#   kpi_monthly_total_amount  → Média mensal de total_amount — Yellow
#   kpi_hourly_passenger_count→ Média horária de passenger_count — maio
#
# DECISÃO DE MODELAGEM — total_amount do HVFHV:
#   Na Silver, total_amount = NULL para HVFHV (sem equivalente direto).
#   Na Gold, é reconstruído pela soma dos componentes financeiros:
#     base_passenger_fare + tip_amount + tolls + bcf +
#     sales_tax + congestion_surcharge + airport_fee
#
#   Decisão de modelagem: componentes financeiros ausentes (NULL) representam
#   ausência de cobrança (valor zero), permitindo reconstruir um total
#   financeiro comparável entre tipos de táxi.
#   Essa é uma decisão analítica — não uma verdade sobre os dados da fonte.
#
#   O resultado padroniza total_amount na camada Gold para análises consistentes
#   entre fontes. Não altera o dado de origem — é uma reconstrução analítica
#   restrita à camada Gold.
#
# PERGUNTAS ANALÍTICAS DO CASE:
#   P1: Qual a média de total_amount por mês — Yellow taxis?
#       → kpi_monthly_total_amount
#   P2: Qual a média de passenger_count por hora do dia — maio, todos os táxis?
#       → kpi_hourly_passenger_count
#       Nota: apenas Yellow e Green contribuem porque FHV e HVFHV não
#       disponibilizam passenger_count na origem. O filtro passenger_count > 0
#       exclui naturalmente esses tipos, mantendo o modelo agnóstico à fonte.
# ==============================================================================

# COMMAND ----------

# ------------------------------------------------------------------------------
# IMPORTS — Gold
# ------------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from functools import reduce

# COMMAND ----------

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES CENTRALIZADAS
# ------------------------------------------------------------------------------

SRC_DATABASE = "case_silver"
DST_DATABASE = "case_gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DATABASE}")

print("✓ Configurações carregadas")
print(f"  Origem  : {SRC_DATABASE}")
print(f"  Destino : {DST_DATABASE}")

# COMMAND ----------

# ==============================================================================
# FACT_TRIPS — tabela base da Gold
#
# UNION ALL das quatro tabelas Silver em schema unificado.
# Campos ausentes por design regulatório permanecem NULL.
#
# total_amount:
#   Apenas HVFHV necessita reconstrução — Yellow, Green e FHV preservam
#   exatamente o total_amount validado na Silver.
#   Para HVFHV, é calculado como soma dos componentes financeiros disponíveis.
#   Decisão de modelagem: componentes NULL representam ausência de cobrança.
#   O resultado padroniza total_amount na camada Gold para análises consistentes
#   entre fontes. Não altera o dado de origem.
#
# Colunas derivadas:
#   pickup_date  : data da corrida — útil para análises de granularidade diária
#   pickup_year  : ano — mantido para compatibilidade com períodos futuros
#   pickup_month : mês (1–12) — filtro primário das análises e chave de partição
#   pickup_hour  : hora do dia (0–23) — granularidade da P2
#
# Particionamento por pickup_month:
#   Filtro de período é o mais frequente nas análises analíticas.
#   Cinco partições (jan–mai 2023) — simples e eficiente.
# ==============================================================================

def load_silver(taxi_type):
    return spark.table(f"{SRC_DATABASE}.{taxi_type}_taxi")


df_fact = (
    load_silver("yellow")
    .unionByName(load_silver("green"),  allowMissingColumns=True)
    .unionByName(load_silver("hvfhv"),  allowMissingColumns=True)
    .unionByName(load_silver("fhv"),    allowMissingColumns=True)

    # Reconstrução do total_amount — regra de negócio da Gold
    # Apenas HVFHV necessita reconstrução; os demais tipos preservam
    # o total_amount validado na Silver.
    # Decisão de modelagem: componentes NULL representam ausência de cobrança.
    .withColumn(
        "total_amount",
        F.when(
            F.col("taxi_type") == "hvfhv",
            F.coalesce(F.col("base_passenger_fare"), F.lit(0.0)) +
            F.coalesce(F.col("tip_amount"),          F.lit(0.0)) +
            F.coalesce(F.col("tolls"),               F.lit(0.0)) +
            F.coalesce(F.col("bcf"),                 F.lit(0.0)) +
            F.coalesce(F.col("sales_tax"),           F.lit(0.0)) +
            F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)) +
            F.coalesce(F.col("airport_fee"),          F.lit(0.0))
        ).otherwise(F.col("total_amount"))
    )

    # Colunas derivadas temporais
    .withColumn("pickup_date",  F.to_date( F.col("pickup_datetime")))
    .withColumn("pickup_year",  F.year(    F.col("pickup_datetime")))
    .withColumn("pickup_month", F.month(   F.col("pickup_datetime")))
    .withColumn("pickup_hour",  F.hour(    F.col("pickup_datetime")))
)

(
    df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_month")
    .saveAsTable(f"{DST_DATABASE}.fact_trips")
)

# Referência reutilizada nos KPIs e nas verificações finais
fact_trips = spark.table(f"{DST_DATABASE}.fact_trips")

print(f"✓ {DST_DATABASE}.fact_trips: {fact_trips.count():,} registros")

print("\n  Distribuição por tipo:")
fact_trips \
    .groupBy("taxi_type") \
    .agg(
        F.count("*").alias("n_corridas"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.sum(F.col("total_amount").isNotNull().cast("int")).alias("com_total_amount")
    ) \
    .orderBy("taxi_type") \
    .show()


# COMMAND ----------

# ==============================================================================
# KPI — MÉDIA DE TOTAL_AMOUNT POR MÊS (Yellow taxis)
#
# Pergunta 1: "Qual a média de total_amount por mês — Yellow taxis?"
#
# Filtro por taxi_type = 'yellow': a fact_trips contém os quatro tipos;
# o filtro restringe a análise ao escopo da pergunta.
#
# Colunas:
#   pickup_year      : ano (mantido para compatibilidade com múltiplos anos)
#   pickup_month     : mês (1=jan, 2=fev, ..., 5=mai)
#   avg_total_amount : média de total_amount das corridas do mês
#   n_corridas       : volume de corridas no mês (contexto)
#   total_receita    : soma total do mês (contexto)
# ==============================================================================

df_kpi_monthly = (
    fact_trips
    .filter(F.col("taxi_type") == "yellow")
    .groupBy("pickup_year", "pickup_month")
    .agg(
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.count("*").alias("n_corridas"),
        F.round(F.sum("total_amount"), 2).alias("total_receita")
    )
    .orderBy("pickup_year", "pickup_month")
)

(
    df_kpi_monthly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.kpi_monthly_total_amount")
)

kpi_monthly = spark.table(f"{DST_DATABASE}.kpi_monthly_total_amount")

print(f"✓ {DST_DATABASE}.kpi_monthly_total_amount")
print("\n  ── RESPOSTA — PERGUNTA 1 ──────────────────────────────")
print("  Média de total_amount por mês — Yellow taxis (jan–mai 2023)")
kpi_monthly \
    .select("pickup_month", "avg_total_amount", "n_corridas", "total_receita") \
    .show()


# COMMAND ----------

# ==============================================================================
# KPI — MÉDIA DE PASSENGER_COUNT POR HORA DO DIA (maio, todos os táxis)
#
# Pergunta 2: "Qual a média de passenger_count por hora — maio?"
#
# Filtro passenger_count > 0:
#   Regra analítica — exclui registros com contagem não informada ou inválida.
#   Na Silver o campo foi preservado sem filtragem; aqui aplica-se o critério
#   de qualidade específico desta análise.
#
#   A análise considera todas as fontes da fact_trips (UNION ALL).
#   FHV e HVFHV não disponibilizam passenger_count (NULL por design regulatório)
#   e são excluídos naturalmente pelo filtro > 0. Esse comportamento é
#   preferível a filtrar por taxi_type explicitamente: mantém o modelo
#   agnóstico à fonte — se uma categoria futuramente coletar esse campo,
#   entrará automaticamente.
#
# Colunas:
#   pickup_hour          : hora do dia (0–23)
#   avg_passenger_count  : média de passageiros por corrida nessa hora
#   n_corridas           : volume de corridas válidas (contexto)
# ==============================================================================

df_kpi_hourly = (
    fact_trips
    .filter(F.col("pickup_month") == 5)
    .filter(F.col("passenger_count") > 0)
    .groupBy("pickup_hour")
    .agg(
        F.round(F.avg("passenger_count"), 4).alias("avg_passenger_count"),
        F.count("*").alias("n_corridas")
    )
    .orderBy("pickup_hour")
)

(
    df_kpi_hourly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.kpi_hourly_passenger_count")
)

kpi_hourly = spark.table(f"{DST_DATABASE}.kpi_hourly_passenger_count")

print(f"✓ {DST_DATABASE}.kpi_hourly_passenger_count")
print("\n  ── RESPOSTA — PERGUNTA 2 ──────────────────────────────")
print("  Média de passenger_count por hora — todos os táxis, maio 2023")
kpi_hourly.show(24, truncate=False)


# COMMAND ----------

# ==============================================================================
# VERIFICAÇÃO FINAL
# ==============================================================================

print("\n" + "=" * 60)
print("  RESUMO GOLD")
print("=" * 60)

for tabela, df in [
    ("fact_trips",                 fact_trips),
    ("kpi_monthly_total_amount",   kpi_monthly),
    ("kpi_hourly_passenger_count", kpi_hourly),
]:
    n = df.count()
    print(f"  {tabela:<35}: {n:>12,} registros")

print("\n  Sanity checks:")

# total_amount não deve ser NULL em Yellow, Green e HVFHV
for tipo in ["yellow", "green", "hvfhv"]:
    n_null = fact_trips \
        .filter(F.col("taxi_type") == tipo) \
        .filter(F.col("total_amount").isNull()) \
        .count()
    status = "✓  0" if n_null == 0 else f"⚠️  {n_null:,}"
    print(f"  {tipo:<8} total_amount nulo              : {status}")

# kpi_monthly deve ter exatamente 5 meses (jan–mai 2023)
n_meses = kpi_monthly.count()
status = "✓  5" if n_meses == 5 else f"⚠️  {n_meses}"
print(f"  kpi_monthly — meses distintos          : {status}")

# kpi_hourly — verificação de horas presentes
# A presença de todas as 24 horas não é garantida para qualquer período.
# Exibimos o número real em vez de verificar igualdade estrita a 24.
horas_presentes = kpi_hourly.select("pickup_hour").distinct().count()
print(f"  kpi_hourly — horas presentes           : {horas_presentes} de 24")

# Quais tipos contribuíram para o passenger_count de maio
print("\n  Tipos que contribuíram para kpi_hourly (maio, passenger_count > 0):")
fact_trips \
    .filter(F.col("pickup_month") == 5) \
    .filter(F.col("passenger_count") > 0) \
    .groupBy("taxi_type") \
    .count() \
    .orderBy("taxi_type") \
    .show()

print()
print("Tabelas registradas:")
spark.sql(f"SHOW TABLES IN {DST_DATABASE}").show()


# COMMAND ----------

# ==============================================================================
# LIMPEZA DA CAMADA GOLD
#
# DRY_RUN = True  → simula sem executar (use para revisar antes)
# DRY_RUN = False → executa o DROP
#
# Categorias:
#   drop_direto   : duplicatas e versões obsoletas — sem valor analítico
#   drop_opcional : derivados exploratórios — remover apenas se confirmado
#                   que não são referenciados downstream
#
# Nota: em produção, drop_opcional deveria receber tag (exploratory |
# certified | deprecated) em vez de DROP direto.
# ==============================================================================

DRY_RUN = False  # altere para False para executar

def safe_drop(tabela):
    full = f"case_gold.{tabela}"
    if DRY_RUN:
        print(f"  [DRY RUN] DROP TABLE IF EXISTS {full}")
    else:
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        print(f"  ✓ Removida: {full}")


# Artefatos claramente redundantes
drop_direto = [
    "fact_trip",    # versão singular obsoleta — substituída por fact_trips
    "vw_fact_trip", # view duplicada — fact_trips é a tabela canônica
]

# Derivados exploratórios — confirmar ausência de dependência antes de remover
drop_opcional = [
    "avg_passengers_by_hour_may",
    "avg_total_amount_by_month",
    "market_share_may",
    "revenue_by_hour_may",
    "weekday_vs_weekend_may",
]

print("=" * 55)
print(f"  MODO: {'DRY RUN — nenhuma tabela será removida' if DRY_RUN else 'EXECUÇÃO REAL'}")
print("=" * 55)

print("\n  DROP DIRETO — artefatos obsoletos:")
for t in drop_direto:
    safe_drop(t)

print("\n  DROP OPCIONAL — derivados exploratórios:")
for t in drop_opcional:
    safe_drop(t)

print("\n  Estado atual da camada Gold:")
spark.sql("SHOW TABLES IN case_gold").show()


# COMMAND ----------

# ==============================================================================
# VIEW DE CONSUMO — contrato de dados para usuários externos
#
# Expõe as colunas exigidas pelo case com os nomes originais da fonte TLC,
# mantendo a Gold internamente com nomenclatura canônica.
#
# Separação de responsabilidades:
#   Gold interna  → nomes semânticos (provider_id, pickup_datetime, ...)
#   Contrato externo → nomes da fonte (VendorID, tpep_pickup_datetime, ...)
#
# A view não duplica dados — é uma projeção direta sobre fact_trips.
# Inclui todos os tipos de táxi para uso analítico geral.
# ==============================================================================

spark.sql("""
    CREATE OR REPLACE VIEW case_gold.vw_taxi_consumo AS
    SELECT
        taxi_type,
        provider_id          AS VendorID,
        passenger_count,
        total_amount,
        pickup_datetime      AS tpep_pickup_datetime,
        dropoff_datetime     AS tpep_dropoff_datetime
    FROM case_gold.fact_trips
""")

print("✓ case_gold.vw_taxi_consumo criada")

print("\n  Schema da view:")
spark.sql("DESCRIBE case_gold.vw_taxi_consumo").show()

print("  Amostra por tipo:")
spark.sql("""
    SELECT taxi_type, COUNT(*) AS n_corridas
    FROM case_gold.vw_taxi_consumo
    GROUP BY taxi_type
    ORDER BY taxi_type
""").show()

print("Tabelas e views registradas:")
spark.sql("SHOW TABLES IN case_gold").show()
