# Databricks notebook source
# ==============================================================================
# NOTEBOOK: 01_ingest_bronze
# CAMADA:   Bronze
# AMBIENTE: Databricks Community Edition (Hive Metastore)
#
# OBJETIVO:
#   Ingerir os arquivos Parquet brutos da NYC TLC (jan–mai 2023)
#   na camada Bronze do Data Lake, sem nenhuma transformação de negócio.
#
# PRÉ-REQUISITO — LANDING ZONE:
#   Arquivos Parquet organizados por tipo de táxi:
#   /Volumes/workspace/case_landing/raw_files/yellow/  ← 5 arquivos
#   /Volumes/workspace/case_landing/raw_files/green/   ← 5 arquivos
#   /Volumes/workspace/case_landing/raw_files/hvfhv/   ← 5 arquivos
#   /Volumes/workspace/case_landing/raw_files/fhv/     ← 5 arquivos
#
# PRINCÍPIO DO BRONZE:
#   Bronze preserva os dados exatamente como vieram da fonte.
#   Nenhuma coluna é renomeada, filtrada ou transformada.
#   Apenas metadados técnicos são adicionados:
#     - ingested_at      : momento do processamento (auditoria)
#     - source_file      : nome do arquivo de origem (rastreabilidade)
#     - _partition_year  : ano definido via lit() — sem dependência de parsing
#     - _partition_month : mês definido via lit() — sem dependência de parsing
#
# ESTRATÉGIA DE INGESTÃO:
#   Leitura arquivo por arquivo em loop (um por mês).
#   Primeiro mês: mode("overwrite") — cria/recria a tabela.
#   Meses seguintes: mode("append") — adiciona sem sobrescrever.
#   Isso garante que todos os meses sejam preservados na tabela final.
#
# SCHEMA DRIFT:
#   O Bronze prioriza ingestão resiliente. A padronização e governança
#   de tipos acontecem na camada Silver.
#
# IDEMPOTÊNCIA:
#   O Delta Lake garante atomicidade e consistência nativamente.
#   Reprocessar um tipo de táxi recria a tabela a partir do primeiro mês.
#
# NOTA SOBRE count():
#   df.count() força um job extra no Spark. Em produção seria
#   substituído por métricas de pipeline. Mantido aqui para
#   visibilidade durante o desenvolvimento do case.
# ==============================================================================

# COMMAND ----------

# ------------------------------------------------------------------------------
# IMPORTS — Bronze
#
# current_timestamp : registra o momento exato da ingestão
# lit               : define valores fixos (source_file, ano, mês)
#                     elimina dependência de input_file_name() ou _metadata
# ------------------------------------------------------------------------------

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# COMMAND ----------

# ------------------------------------------------------------------------------
# CONFIGURAÇÕES CENTRALIZADAS
#
# MONTHS       : meses disponíveis — controle explícito do pipeline
# FILE_PATTERNS: padrão de nome dos arquivos por tipo de táxi
# Centralizar aqui garante consistência e facilita manutenção.
# ------------------------------------------------------------------------------

BASE_LANDING = "/Volumes/workspace/case_landing/raw_files"

# Meses disponíveis no dataset
MONTHS = ["01", "02", "03", "04", "05"]

# Padrão de nome dos arquivos por tipo
FILE_PATTERNS = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "hvfhv":  "fhvhv_tripdata",
    "fhv":    "fhv_tripdata",
}

# Database no Hive Metastore
DATABASE = "case_bronze"

print("✓ Configurações carregadas")
print(f"  Landing Zone : {BASE_LANDING}")
print(f"  Database     : {DATABASE}")
print(f"  Meses        : {MONTHS}")

# COMMAND ----------

# ------------------------------------------------------------------------------
# DATABASE BRONZE
#
# IF NOT EXISTS garante idempotência — sem erro em execuções repetidas.
# ------------------------------------------------------------------------------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

print(f"✓ Database {DATABASE} criado (ou já existia)")

# COMMAND ----------

# ------------------------------------------------------------------------------
# FUNÇÃO DE INGESTÃO — Bronze
#
# Por que ler mês a mês e unir antes de escrever?
#   - Evita conflito de schema entre overwrite (mês 1) e append (meses 2-5)
#   - VendorID e outros campos podem ter tipos diferentes entre meses
#     (ex: int32 vs int64) — o Spark resolve isso ao unir os DataFrames
#   - unionByName(allowMissingColumns=True) garante que colunas ausentes
#     em algum mês sejam preenchidas com NULL, sem quebrar o schema
#   - Uma única escrita Delta é mais eficiente e atômica
#
# Por que lit() para _partition_year e _partition_month?
#   - Evita dependência de input_file_name() ou _metadata.file_path
#   - Determinístico e compatível com qualquer ambiente
# ------------------------------------------------------------------------------

from functools import reduce

def ingest_bronze(taxi_type):
    """
    Lê os arquivos Parquet mensais de um tipo de táxi, une todos em memória
    e salva como Delta Table na camada Bronze em uma única operação de escrita.
    """
    file_prefix = FILE_PATTERNS[taxi_type]
    table_name  = f"{DATABASE}.{taxi_type}_taxi"

    print(f"\n{'='*55}")
    print(f"  Bronze: {taxi_type.upper()}")
    print(f"{'='*55}")

    monthly_dfs = []
    total = 0

    for month in MONTHS:
        file_name = f"{file_prefix}_2023-{month}.parquet"
        file_path = f"{BASE_LANDING}/{taxi_type}/{file_name}"

        print(f"  [{month}] Lendo: {file_name}")

        df_month = (
            spark.read
            .option("mergeSchema", "true")  # Bronze prioriza ingestão resiliente;
                                            # padronização de schema acontece na Silver
            .parquet(file_path)
            .withColumn("ingested_at",      current_timestamp())
            .withColumn("source_file",      lit(file_name))
            .withColumn("_partition_year",  lit(2023))
            .withColumn("_partition_month", lit(int(month)))
        )

        n = df_month.count()
        total += n
        print(f"  [{month}] {n:,} registros")

        monthly_dfs.append(df_month)

    # Une todos os meses em um único DataFrame antes de escrever
    # allowMissingColumns=True: colunas ausentes em algum mês viram NULL
    # Isso resolve conflitos de schema entre meses (ex: VendorID int32 vs int64)
    df_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), monthly_dfs)

    # Escrita única e atômica — sem risco de conflito overwrite vs append
    (
        df_all.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_partition_year", "_partition_month")
        .saveAsTable(table_name)
    )

    print(f"\n  ✓ {table_name}: {total:,} registros totais")
    return total

# COMMAND ----------

# ------------------------------------------------------------------------------
# YELLOW TAXI — Ingestão Bronze
#
# Schema original relevante:
#   VendorID                    (int)
#   tpep_pickup_datetime        (timestamp) ← prefixo "tpep" exclusivo do Yellow
#   tpep_dropoff_datetime       (timestamp)
#   passenger_count             (double)
#   trip_distance               (double)
#   PULocationID / DOLocationID (int)
#   fare_amount, tip_amount, total_amount (double)
#
# O prefixo tpep_* diferencia o Yellow do Green (que usa lpep_*).
# Essa diferença será resolvida na Silver via mapeamento semântico
# para pickup_datetime / dropoff_datetime.
# ------------------------------------------------------------------------------

ingest_bronze("yellow")

# COMMAND ----------

# ------------------------------------------------------------------------------
# GREEN TAXI — Ingestão Bronze
#
# Schema original relevante:
#   VendorID                    (int)
#   lpep_pickup_datetime        (timestamp) ← prefixo "lpep" exclusivo do Green
#   lpep_dropoff_datetime       (timestamp)
#   passenger_count             (double)
#   trip_distance               (double)
#   PULocationID / DOLocationID (int)
#   fare_amount, tip_amount, total_amount (double)
#   ehail_fee                   (double/null) ← tipo varia entre meses em 2023
#
# O Green não é uma versão incompleta do Yellow — é uma categoria
# regulatória diferente com regras operacionais próprias.
# Opera principalmente em zonas fora de Manhattan.
# ------------------------------------------------------------------------------

ingest_bronze("green")

# COMMAND ----------

# ------------------------------------------------------------------------------
# HVFHV — High Volume For-Hire Vehicles (Uber, Lyft) — Ingestão Bronze
#
# Schema original relevante:
#   hvfhs_license_num           (string) ← HV0003=Uber, HV0005=Lyft
#   dispatching_base_num        (string) ← base que despachou a corrida
#   pickup_datetime / dropoff_datetime (timestamp)
#   PULocationID / DOLocationID (int)
#   trip_miles                  (double) ← equivalente semântico ao trip_distance
#   base_passenger_fare         (double) ← equivalente semântico ao fare_amount
#   tips                        (double) ← equivalente semântico ao tip_amount
#   tolls, bcf, sales_tax, congestion_surcharge, airport_fee (double)
#   shared_request_flag / shared_match_flag (string Y/N)
#
# O HVFHV é um modelo de negócio diferente, não uma versão incompleta
# dos táxis regulados. Não possui passenger_count nem total_amount direto.
# A composição do total_amount será calculada apenas na Gold.
# ------------------------------------------------------------------------------

ingest_bronze("hvfhv")

# COMMAND ----------

# ------------------------------------------------------------------------------
# FHV — For-Hire Vehicles (base tradicional) — Ingestão Bronze
#
# Schema original — apenas 7 colunas:
#   dispatching_base_num / Affiliated_base_number (string)
#   pickup_datetime             (timestamp)
#   dropOff_datetime            (timestamp) ← capitalização inconsistente na fonte
#   PUlocationID / DOlocationID (double)    ← capitalização inconsistente na fonte
#   SR_Flag                     (null)      ← 100% NULL nos dados de 2023
#
# O FHV é uma categoria regulatória distinta. A ausência de campos
# reflete obrigações regulatórias diferentes, não falha de coleta.
# SR_Flag preservado no Bronze por integridade do dado fonte.
# ------------------------------------------------------------------------------

ingest_bronze("fhv")

# COMMAND ----------

# ------------------------------------------------------------------------------
# VERIFICAÇÃO FINAL
#
# Confirma criação das tabelas e contagem total de registros.
# Execute esta célula após todas as ingestões.
#
# Contagens esperadas (jan–mai 2023):
#   yellow_taxi  : ~16,186,386 registros
#   green_taxi   :    ~339,630 registros
#   hvfhv_taxi   : ~95,846,120 registros
#   fhv_taxi     :  ~6,185,664 registros
# ------------------------------------------------------------------------------

print("=" * 58)
print("  RESUMO BRONZE")
print("=" * 58)

tabelas = ["yellow_taxi", "green_taxi", "hvfhv_taxi", "fhv_taxi"]

for tabela in tabelas:
    try:
        n = spark.table(f"{DATABASE}.{tabela}").count()
        print(f"  {DATABASE}.{tabela:<15}  {n:>12,} registros  ✓")
    except Exception:
        print(f"  {DATABASE}.{tabela:<15}  ainda não existe")

print()
print("Tabelas registradas:")
spark.sql(f"SHOW TABLES IN {DATABASE}").show()
