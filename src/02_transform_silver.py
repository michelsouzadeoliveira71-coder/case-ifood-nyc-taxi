# Databricks notebook source
# ==============================================================================
# NOTEBOOK: 02_transform_silver
# CAMADA:   Silver
# DEPENDE:  01_ingest_bronze (case_bronze.*)
#
# OBJETIVO:
#   Transformar os dados brutos do Bronze em dados padronizados, validados
#   e instrumentados — prontos para consumo na Gold.
#
# CONTRATO DA SILVER (quatro categorias de regras):
#
#   INTEGRIDADE ESTRUTURAL — dado impossível em qualquer contexto
#     · fare_amount >= 0 (Yellow/Green)
#     · base_passenger_fare >= 0 (HVFHV)
#
#   VALIDADE TEMPORAL — evento com timeline fisicamente impossível
#     · pickup_datetime <= dropoff_datetime
#
#   SANITY BOUNDS — limiares de plausibilidade operacional
#     · trip_distance BETWEEN 0 AND TRIP_DISTANCE_MAX
#     · duration <= MAX_DURATION_SECONDS
#     (Improbáveis em NYC, mas não fisicamente impossíveis.
#      São decisões de modelagem com thresholds parametrizados.)
#
#   OBSERVABILIDADE — instrumentação para auditoria e análise
#     · record_hash : fingerprint de conteúdo (auditoria e lineage)
#     · ride_key    : identidade lógica da corrida (uso analítico na Gold)
#     · financial_inconsistency_flag : inconsistência aritmética comprovada
#     · zero_fare_with_distance_flag : deslocamento sem valor financeiro
#
# POLÍTICA DE VERSÕES MÚLTIPLAS:
#   Silver preserva versões distintas quando há diferença de conteúdo.
#   Remove apenas registros indistinguíveis pelos atributos disponíveis.
#   No FHV, a auditoria confirmou que registros com mesmo record_hash
#   não apresentam qualquer atributo discriminante adicional
#   (campos_que_variam vazio). dropDuplicates(["record_hash"]) foi
#   adotado como aproximação operacional da remoção de indistinguíveis.
#
# EXCLUSÕES EXPLÍCITAS (com justificativa):
#   · passenger_count > 0 não filtrado: valor 0 pode significar
#     "não informado" na fonte TLC. Regra analítica pertence à Gold.
#   · fare=0, distance>0, total>0 não flagado: pode decorrer de
#     surcharge, congestion fee ou airport fee legítimos da TLC.
#     Não há evidência suficiente de inconsistência.
#   · Yellow/HVFHV: pares com campos financeiros distintos foram
#     preservados, pois representam registros com conteúdo diferente.
#     Não há evidência suficiente para determinar se correspondem a
#     versões do mesmo evento ou eventos distintos.
#
# LIMITAÇÕES DOCUMENTADAS:
#   · Yellow record_hash não inclui total_amount/tip_amount: registros
#     com mesma tarifa base mas totais distintos produzem o mesmo hash.
#     Volume negligível; causalidade indeterminada.
#   · HVFHV: 19 pares com base_passenger_fare distinto no Bronze.
#     Causa ambígua. Preservados sem remoção.
#   · FHV: ride_key estruturalmente fraco por 78% NULL em
#     pickup_location_id. Limitação da fonte TLC, não do pipeline.
# ==============================================================================

# COMMAND ----------

# ------------------------------------------------------------------------------
# IMPORTS — Silver
# ------------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

# ------------------------------------------------------------------------------
# PARÂMETROS
# ------------------------------------------------------------------------------

SRC_DATABASE = "case_bronze"
DST_DATABASE = "case_silver"

DATE_MIN = "2023-01-01"
DATE_MAX = "2023-06-01"  # exclusivo

DATE_MIN_TS = F.lit(DATE_MIN).cast(TimestampType())
DATE_MAX_TS = F.lit(DATE_MAX).cast(TimestampType())

# Sanity bound operacional — não representa limite físico absoluto.
# Escolhido para eliminar overflows observados na auditoria
# (342K milhas Yellow, 267K milhas Green) preservando ampla margem
# acima de qualquer corrida urbana plausível.
# Podem ser ajustados conforme requisitos do domínio sem alterar a lógica.
TRIP_DISTANCE_MAX    = 200    # milhas
MAX_DURATION_SECONDS = 86400  # 24h — P99 auditado: Yellow 64.8 min, HVFHV 67.5 min

print("✓ Configurações carregadas")
print(f"  Origem              : {SRC_DATABASE}")
print(f"  Destino             : {DST_DATABASE}")
print(f"  Período             : {DATE_MIN} até {DATE_MAX} (exclusivo)")
print(f"  TRIP_DISTANCE_MAX   : {TRIP_DISTANCE_MAX} milhas")
print(f"  MAX_DURATION_SECONDS: {MAX_DURATION_SECONDS}s ({MAX_DURATION_SECONDS // 3600}h)")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DST_DATABASE}")
print(f"✓ Database {DST_DATABASE} criado (ou já existia)")

# COMMAND ----------

# ------------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
# ------------------------------------------------------------------------------

def safe_col(df, col_name, data_type=None):
    """
    Retorna F.col(col_name) se existir, F.lit(None) se não.
    Aplica cast para data_type em ambos os casos quando especificado.

    Garante schema canônico Silver independente do tipo vindo do Bronze.
    Comportamento anterior: cast só era aplicado no caso de coluna ausente,
    propagando tipos originais da fonte (ex: PUlocationID como double no FHV).
    Agora o cast é sempre aplicado quando data_type é fornecido.
    """
    col = F.col(col_name) if col_name in df.columns else F.lit(None)
    if data_type is not None:
        col = col.cast(data_type)
    return col


def apply_time_filter(df):
    """
    Boundary temporal: o pickup pertence ao escopo do dataset.

    PRÉ-CONDIÇÃO: schema canônico Silver já aplicado —
    coluna 'pickup_datetime' do tipo TimestampType.
    Centralizado para garantir consistência de período entre todos os tipos.
    """
    return (
        df
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("pickup_datetime") >= DATE_MIN_TS)
        .filter(F.col("pickup_datetime") <  DATE_MAX_TS)
    )


def apply_temporal_validity(df):
    """
    Validade temporal — eventos com timeline fisicamente impossível.

    pickup_datetime <= dropoff_datetime: timeline invertida é impossível
    operacionalmente. Captura timestamps corrompidos (ex: dropoff em 1917).

    NULL explicitamente verificado antes da comparação por clareza.
    Na prática, pickup NULL já foi eliminado em apply_time_filter(),
    mas a verificação explícita melhora legibilidade e robustez.

    Distinto dos sanity bounds: aqui o evento é fisicamente impossível,
    não apenas improvável. Não requer threshold — é uma verdade lógica.
    """
    return df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("pickup_datetime") <= F.col("dropoff_datetime"))
    )


def apply_sanity_bounds(df, has_distance=True):
    """
    Sanity bounds — limiares de plausibilidade operacional.

    Diferença conceitual em relação à validade temporal:
      · pickup > dropoff      → fisicamente impossível → validade temporal
      · trip_distance > 200mi → improvável em NYC, não impossível → sanity bound
      · duration > 24h        → improvável operacionalmente → sanity bound

    São decisões de modelagem, não regras físicas absolutas.
    Thresholds parametrizados — ver TRIP_DISTANCE_MAX e MAX_DURATION_SECONDS.

    Nota sobre duração: registros com duração negativa já foram eliminados
    em apply_temporal_validity(). Aqui filtramos apenas durações positivas
    além do threshold operacional.

    has_distance=False para FHV, que não possui trip_distance disponível.
    """
    df = df.filter(
        (F.col("dropoff_datetime").cast("timestamp").cast("long") -
         F.col("pickup_datetime").cast("timestamp").cast("long"))
        <= MAX_DURATION_SECONDS
    )
    if has_distance:
        df = df.filter(F.col("trip_distance").between(0, TRIP_DISTANCE_MAX))
    return df


def add_record_hash(df):
    """
    record_hash (SHA-256): fingerprint de conteúdo do registro.

    Papel: auditoria e lineage — identifica o estado físico da linha.
    NÃO é chave de identidade da corrida (ver add_ride_key).

    Campos incluídos: descritores logísticos + tarifa base.
    Campos excluídos: ingested_at, source_file (variam entre execuções).

    Nota HVFHV: fare_amount é NULL por design arquitetural.
    Limitação conhecida: pares HVFHV com base_passenger_fare distinto
    produzem o mesmo hash — documentado na auditoria como ambiguidade
    sem evidência suficiente para determinar causalidade.
    """
    return df.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("|",
                F.col("taxi_type"),
                F.coalesce(F.col("pickup_datetime").cast("string"),        F.lit("null")),
                F.coalesce(F.col("dropoff_datetime").cast("string"),       F.lit("null")),
                F.coalesce(F.col("pickup_location_id").cast("string"),     F.lit("null")),
                F.coalesce(F.col("dropoff_location_id").cast("string"),    F.lit("null")),
                F.coalesce(F.col("provider_id"),                           F.lit("null")),
                F.coalesce(F.col("trip_distance").cast("string"),          F.lit("null")),
                F.coalesce(F.col("fare_amount").cast("string"),            F.lit("null")),
            ),
            256
        )
    )


def add_ride_key(df):
    """
    ride_key (SHA-256): identidade lógica da corrida (entidade de negócio).

    Representa a identidade lógica da corrida utilizando atributos
    considerados estáveis para identificação da corrida. Campos derivados
    ou sujeitos a variações de medição não fazem parte da chave para
    preservar estabilidade semântica.

    Campos incluídos: taxi_type, provider_id, pickup_datetime,
    dropoff_datetime, pickup_location_id, dropoff_location_id.
    Atributos estáveis do evento (quem, quando, onde).

    trip_distance excluído propositalmente:
      Medições são sujeitas a variação por GPS, arredondamento ou encoding.
      Dois registros da mesma corrida com medições diferentes gerariam
      ride_keys distintos — quebrando o propósito da chave.
      Princípio: chaves de identidade devem conter apenas atributos
      estáveis para identificação do evento.

    Trade-off documentado:
      Sem trip_distance → maior estabilidade semântica, menor seletividade.
      Com trip_distance → maior seletividade, menor estabilidade.
      Chaves de identidade priorizam estabilidade sobre seletividade.

    ride_key ≠ record_hash:
      ride_key    → identidade da corrida para uso analítico na Gold.
      record_hash → fingerprint de conteúdo para auditoria e lineage.
      A Silver NÃO utiliza ride_key como critério de deduplicação.

    Limitação FHV: ride_key estruturalmente fraco por 78% NULL em
    pickup_location_id. Limitação da fonte TLC, não do pipeline.
    """
    return df.withColumn(
        "ride_key",
        F.sha2(
            F.concat_ws("|",
                F.col("taxi_type"),
                F.coalesce(F.col("provider_id"),                           F.lit("null")),
                F.coalesce(F.col("pickup_datetime").cast("string"),        F.lit("null")),
                F.coalesce(F.col("dropoff_datetime").cast("string"),       F.lit("null")),
                F.coalesce(F.col("pickup_location_id").cast("string"),     F.lit("null")),
                F.coalesce(F.col("dropoff_location_id").cast("string"),    F.lit("null")),
            ),
            256
        )
    )


def add_financial_flags(df, taxi_type):
    """
    Duas flags de observabilidade financeira.
    Preservadas na Silver — Gold decide tratamento por análise.
    Nenhuma implica remoção de registro.

    financial_inconsistency_flag:
      Duas condições conceitualmente distintas, mantidas separadas porque
      representam anomalias de natureza diferente e podem divergir entre
      períodos e conjuntos de dados diferentes.
        · tip_amount < 0        → componente financeiro anômalo
        · total_amount < fare   → inconsistência aritmética

      A auditoria inicial identificou coincidência entre as duas condições
      no conjunto analisado. Após implementação completa da Silver confirmou-se
      que as condições podem divergir — reforçando a decisão de mantê-las
      independentes.

      A inconsistência aritmética pode decorrer de ajustes financeiros
      registrados pela fonte TLC, incluindo componentes negativos ou
      componentes não representados no schema canônico da Silver.
      A Silver apenas sinaliza o caso; não infere sua causa.

    zero_fare_with_distance_flag:
      Deslocamento registrado com distância real mas sem nenhum valor financeiro.
      Não implica erro — pode ser corrida subsidiada, erro de captura ou
      modelo tarifário específico. Preservado para avaliação na Gold.
        · fare=0 AND total=0 AND distance>0 (Yellow/Green)
        · base_fare=0 AND tip=0 AND distance>0 (HVFHV)

    Explicitamente NÃO incluído em nenhuma flag:
      fare=0, distance>0, total>0 (Yellow: 1.539 | Green: 23 | HVFHV com tip: 114)
      Pode decorrer de surcharge, congestion fee ou airport fee legítimos.
      Não há evidência suficiente para classificar como inconsistência.
    """
    if taxi_type in ["yellow", "green"]:
        return (
            df
            .withColumn(
                "financial_inconsistency_flag",
                (F.col("tip_amount") < 0) |
                (F.col("total_amount") < F.col("fare_amount"))
            )
            .withColumn(
                "zero_fare_with_distance_flag",
                (F.col("trip_distance") > 0) &
                (F.col("fare_amount")   == 0) &
                (F.col("total_amount")  == 0)
            )
        )
    elif taxi_type == "hvfhv":
        return (
            df
            .withColumn(
                "financial_inconsistency_flag",
                F.col("tip_amount") < 0
            )
            .withColumn(
                "zero_fare_with_distance_flag",
                (F.col("trip_distance")       > 0) &
                (F.col("base_passenger_fare") == 0) &
                (F.col("tip_amount")          == 0)
            )
        )
    else:  # fhv — sem campos financeiros por design regulatório
        return (
            df
            .withColumn("financial_inconsistency_flag",
                        F.lit(False).cast("boolean"))
            .withColumn("zero_fare_with_distance_flag",
                        F.lit(False).cast("boolean"))
        )


print("✓ Funções auxiliares carregadas")

# COMMAND ----------

# ==============================================================================
# YELLOW TAXI — Silver
#
# provider_id = VendorID (1=Creative Mobile, 2=VeriFone)
#   Representa o operador do equipamento de medição.
#
# dispatch_base_id = NULL
#   Táxi regulado não opera via base de despacho intermediária.
#
# passenger_count preservado com valor 0:
#   0 pode significar "não informado" na fonte TLC.
#   Regra analítica pertence à Gold conforme a métrica.
#
# Integridade estrutural: fare_amount >= 0
#   Tarifa base negativa não representa corrida real.
#   Filtro independente do total_amount — condições conceitualmente distintas.
#
# congestion_surcharge / airport_fee: existem no Yellow mas normalizados
#   como NULL — schema unificado prioriza consistência entre fontes.
# ==============================================================================

df_bronze_yellow = spark.table(f"{SRC_DATABASE}.yellow_taxi")

df_silver_yellow = (
    df_bronze_yellow

    .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    .withColumn("provider_id",         F.col("VendorID").cast(StringType()))
    .withColumn("dispatch_base_id",    F.lit(None).cast(StringType()))
    .withColumn("pickup_location_id",  F.col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id", F.col("DOLocationID").cast(IntegerType()))
    .withColumn("passenger_count",     F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",         F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount",          F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))

    .withColumn("is_shared_ride",         F.lit(None).cast("boolean"))
    .withColumn("base_passenger_fare",    F.lit(None).cast(DoubleType()))
    .withColumn("tolls",                  F.lit(None).cast(DoubleType()))
    .withColumn("bcf",                    F.lit(None).cast(DoubleType()))
    .withColumn("sales_tax",             F.lit(None).cast(DoubleType()))
    .withColumn("congestion_surcharge",   F.lit(None).cast(DoubleType()))
    .withColumn("airport_fee",            F.lit(None).cast(DoubleType()))

    .withColumn("ingested_at", F.coalesce(F.col("ingested_at"), F.current_timestamp()))
    .withColumn("source_file", F.coalesce(F.col("source_file"), F.lit("unknown")))
    .withColumn("taxi_type",   F.lit("yellow"))

    .select(
        "taxi_type", "pickup_datetime", "dropoff_datetime",
        "provider_id", "dispatch_base_id",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "is_shared_ride",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee",
        "ingested_at", "source_file"
    )
)

# Integridade estrutural
df_silver_yellow = df_silver_yellow.filter(F.col("fare_amount")  >= 0)
df_silver_yellow = df_silver_yellow.filter(F.col("total_amount") >= 0)

# Boundary temporal
df_silver_yellow = apply_time_filter(df_silver_yellow)

# Validade temporal
df_silver_yellow = apply_temporal_validity(df_silver_yellow)

# Sanity bounds
df_silver_yellow = apply_sanity_bounds(df_silver_yellow, has_distance=True)

# Observabilidade
df_silver_yellow = add_record_hash(df_silver_yellow)
df_silver_yellow = add_ride_key(df_silver_yellow)
df_silver_yellow = add_financial_flags(df_silver_yellow, "yellow")

(
    df_silver_yellow.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.yellow_taxi")
)

print(f"✓ {DST_DATABASE}.yellow_taxi: "
      f"{spark.table(f'{DST_DATABASE}.yellow_taxi').count():,} registros")


# COMMAND ----------

# ==============================================================================
# GREEN TAXI — Silver
#
# provider_id = VendorID — mesmo mapeamento do Yellow.
# dispatch_base_id = NULL — táxi regulado, sem base de despacho.
#
# ehail_fee: tipo inconsistente entre meses na fonte TLC (int/double).
#   Cast estabilizador aplicado antes de qualquer operação.
#   Descartado no select — sem equivalente nas demais fontes.
# ==============================================================================

df_bronze_green = spark.table(f"{SRC_DATABASE}.green_taxi")

if "ehail_fee" in df_bronze_green.columns:
    df_bronze_green = df_bronze_green.withColumn(
        "ehail_fee", F.col("ehail_fee").cast(DoubleType())
    )
else:
    df_bronze_green = df_bronze_green.withColumn(
        "ehail_fee", F.lit(None).cast(DoubleType())
    )

df_silver_green = (
    df_bronze_green

    .withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    .withColumn("provider_id",         F.col("VendorID").cast(StringType()))
    .withColumn("dispatch_base_id",    F.lit(None).cast(StringType()))
    .withColumn("pickup_location_id",  F.col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id", F.col("DOLocationID").cast(IntegerType()))
    .withColumn("passenger_count",     F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",         F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount",          F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))

    .withColumn("is_shared_ride",         F.lit(None).cast("boolean"))
    .withColumn("base_passenger_fare",    F.lit(None).cast(DoubleType()))
    .withColumn("tolls",                  F.lit(None).cast(DoubleType()))
    .withColumn("bcf",                    F.lit(None).cast(DoubleType()))
    .withColumn("sales_tax",             F.lit(None).cast(DoubleType()))
    .withColumn("congestion_surcharge",   F.lit(None).cast(DoubleType()))
    .withColumn("airport_fee",            F.lit(None).cast(DoubleType()))

    .withColumn("ingested_at", F.coalesce(F.col("ingested_at"), F.current_timestamp()))
    .withColumn("source_file", F.coalesce(F.col("source_file"), F.lit("unknown")))
    .withColumn("taxi_type",   F.lit("green"))

    .select(
        "taxi_type", "pickup_datetime", "dropoff_datetime",
        "provider_id", "dispatch_base_id",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "is_shared_ride",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee",
        "ingested_at", "source_file"
    )
)

# Integridade estrutural
df_silver_green = df_silver_green.filter(F.col("fare_amount")  >= 0)
df_silver_green = df_silver_green.filter(F.col("total_amount") >= 0)

# Boundary temporal
df_silver_green = apply_time_filter(df_silver_green)

# Validade temporal
df_silver_green = apply_temporal_validity(df_silver_green)

# Sanity bounds
df_silver_green = apply_sanity_bounds(df_silver_green, has_distance=True)

# Observabilidade
df_silver_green = add_record_hash(df_silver_green)
df_silver_green = add_ride_key(df_silver_green)
df_silver_green = add_financial_flags(df_silver_green, "green")

(
    df_silver_green.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.green_taxi")
)

print(f"✓ {DST_DATABASE}.green_taxi: "
      f"{spark.table(f'{DST_DATABASE}.green_taxi').count():,} registros")

# COMMAND ----------

# ==============================================================================
# HVFHV — Silver (Uber, Lyft)
#
# provider_id = hvfhs_license_num — empresa regulada pela TLC
#   HV0003 = Uber | HV0005 = Lyft
#   Distribuição auditada: 72.7% Uber, 27.3% Lyft (jan-mai 2023)
#
# dispatch_base_id = dispatching_base_num — base operacional
#   Nível operacional distinto do provider_id (nível de empresa).
#
# fare_amount = NULL por design arquitetural:
#   HVFHV não possui equivalente direto ao fare_amount do táxi regulado.
#   Componentes financeiros preservados para cálculo de total na Gold.
#
# trip_distance = trip_miles (renomeação semântica — mesma unidade, milhas)
#
# tip_amount ← tips: mesma semântica do Yellow/Green (gorjeta do passageiro).
#
# Pares com base_passenger_fare distinto (19 casos auditados):
#   Registros com conteúdo diferente preservados. Não há evidência
#   suficiente para determinar se correspondem a versões do mesmo
#   evento ou eventos distintos.
#
# is_shared_ride: F.lit(True/False) — Databricks não converte "Y"/"N"
#   diretamente para boolean via BIGINT. F.lit() retorna boolean nativo.
# ==============================================================================

df_bronze_hvfhv = spark.table(f"{SRC_DATABASE}.hvfhv_taxi")

df_silver_hvfhv = (
    df_bronze_hvfhv

    .withColumn("provider_id",         F.col("hvfhs_license_num").cast(StringType()))
    .withColumn("dispatch_base_id",    F.col("dispatching_base_num").cast(StringType()))
    .withColumn("pickup_location_id",  F.col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id", F.col("DOLocationID").cast(IntegerType()))
    .withColumn("trip_distance",       F.col("trip_miles").cast(DoubleType()))

    .withColumn("fare_amount",         F.lit(None).cast(DoubleType()))
    .withColumn("tip_amount",          F.col("tips").cast(DoubleType()))
    .withColumn("total_amount",        F.lit(None).cast(DoubleType()))
    .withColumn("passenger_count",     F.lit(None).cast(IntegerType()))

    .withColumn(
        "is_shared_ride",
        F.when(F.col("shared_match_flag") == "Y", F.lit(True))
         .when(F.col("shared_match_flag") == "N", F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )

    .withColumn("base_passenger_fare",  F.col("base_passenger_fare").cast(DoubleType()))
    .withColumn("tolls",                F.col("tolls").cast(DoubleType()))
    .withColumn("bcf",                  safe_col(df_bronze_hvfhv, "bcf", DoubleType()))
    .withColumn("sales_tax",            safe_col(df_bronze_hvfhv, "sales_tax", DoubleType()))
    .withColumn("congestion_surcharge", safe_col(df_bronze_hvfhv, "congestion_surcharge", DoubleType()))
    .withColumn("airport_fee",          safe_col(df_bronze_hvfhv, "airport_fee", DoubleType()))

    .withColumn("ingested_at", F.coalesce(F.col("ingested_at"), F.current_timestamp()))
    .withColumn("source_file", F.coalesce(F.col("source_file"), F.lit("unknown")))
    .withColumn("taxi_type",   F.lit("hvfhv"))

    .select(
        "taxi_type", "pickup_datetime", "dropoff_datetime",
        "provider_id", "dispatch_base_id",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "is_shared_ride",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee",
        "ingested_at", "source_file"
    )
)

# Integridade estrutural
df_silver_hvfhv = df_silver_hvfhv.filter(F.col("base_passenger_fare") >= 0)

# Boundary temporal
df_silver_hvfhv = apply_time_filter(df_silver_hvfhv)

# Validade temporal
df_silver_hvfhv = apply_temporal_validity(df_silver_hvfhv)

# Sanity bounds
df_silver_hvfhv = apply_sanity_bounds(df_silver_hvfhv, has_distance=True)

# Observabilidade
df_silver_hvfhv = add_record_hash(df_silver_hvfhv)
df_silver_hvfhv = add_ride_key(df_silver_hvfhv)
df_silver_hvfhv = add_financial_flags(df_silver_hvfhv, "hvfhv")

(
    df_silver_hvfhv.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.hvfhv_taxi")
)

print(f"✓ {DST_DATABASE}.hvfhv_taxi: "
      f"{spark.table(f'{DST_DATABASE}.hvfhv_taxi').count():,} registros")

# COMMAND ----------

# ==============================================================================
# FHV — Silver (For-Hire Vehicles tradicionais)
#
# provider_id = dispatching_base_num
#   Melhor identificador disponível. FHV não possui license_num
#   equivalente ao hvfhs_license_num do HVFHV.
#
# dispatch_base_id = NULL
#   FHV não separa entidade de provedor de base operacional.
#
# Campos ausentes por design regulatório (NULL, não falha de dados):
#   passenger_count, trip_distance, fare_amount, tip_amount, total_amount
#
# dropoff_datetime:
#   Parquet TLC usa "dropOff_datetime" — case inconsistente normalizado.
#   Cast explícito para timestamp_ntz alinha com as demais tabelas Silver,
#   evitando incompatibilidade em UNION ALL na Gold.
#
# pickup_location_id / dropoff_location_id:
#   FHV usa "PUlocationID" — normalizado para nome canônico Silver.
#   safe_col com cast explícito para IntegerType garante tipo canônico
#   independente do tipo da coluna no Bronze (double no FHV).
#   78.3% NULL por design regulatório — limitação da fonte TLC.
#
# SR_Flag: 100% NULL em 2023 — is_shared_ride será NULL para todo dataset.
#
# DEDUPLICAÇÃO — aplicada apenas no FHV:
#   Nesta base específica, a auditoria confirmou que registros FHV com
#   mesmo record_hash não apresentam qualquer atributo discriminante
#   adicional (campos_que_variam vazio na Etapa 3 do audit). Por esse
#   motivo, dropDuplicates(["record_hash"]) foi adotado como aproximação
#   operacional da remoção de registros indistinguíveis pelos atributos
#   disponíveis.
#   · Bronze confirmou 13.848 linhas duplicadas: origem na fonte TLC.
#   Esta equivalência é específica deste dataset — não é propriedade
#   geral do algoritmo dropDuplicates(["record_hash"]).
# ==============================================================================

df_bronze_fhv = spark.table(f"{SRC_DATABASE}.fhv_taxi")

df_silver_fhv = (
    df_bronze_fhv

    .withColumn("provider_id",      F.col("dispatching_base_num").cast(StringType()))
    .withColumn("dispatch_base_id", F.lit(None).cast(StringType()))

    # dropoff_datetime: cast explícito para timestamp_ntz
    # alinha com pickup_datetime e com as demais tabelas Silver
    .withColumn(
        "dropoff_datetime",
        F.coalesce(
            safe_col(df_bronze_fhv, "dropOff_datetime", "timestamp"),
            safe_col(df_bronze_fhv, "dropoff_datetime", "timestamp")
        ).cast("timestamp_ntz")
    )

    # safe_col com cast explícito: garante IntegerType mesmo que
    # a coluna exista no Bronze como double
    .withColumn("pickup_location_id",
                safe_col(df_bronze_fhv, "PUlocationID", IntegerType()))
    .withColumn("dropoff_location_id",
                safe_col(df_bronze_fhv, "DOlocationID", IntegerType()))

    .withColumn("passenger_count", F.lit(None).cast(IntegerType()))
    .withColumn("trip_distance",   F.lit(None).cast(DoubleType()))
    .withColumn("fare_amount",     F.lit(None).cast(DoubleType()))
    .withColumn("tip_amount",      F.lit(None).cast(DoubleType()))
    .withColumn("total_amount",    F.lit(None).cast(DoubleType()))

    .withColumn(
        "is_shared_ride",
        F.when(F.col("SR_Flag").cast("string").isin("1", "Y", "True"), True)
         .when(F.col("SR_Flag").cast("string").isin("0", "N", "False"), False)
         .otherwise(None).cast("boolean")
    )

    .withColumn("base_passenger_fare",  F.lit(None).cast(DoubleType()))
    .withColumn("tolls",                F.lit(None).cast(DoubleType()))
    .withColumn("bcf",                  F.lit(None).cast(DoubleType()))
    .withColumn("sales_tax",            F.lit(None).cast(DoubleType()))
    .withColumn("congestion_surcharge", F.lit(None).cast(DoubleType()))
    .withColumn("airport_fee",          F.lit(None).cast(DoubleType()))

    .withColumn("ingested_at", F.coalesce(F.col("ingested_at"), F.current_timestamp()))
    .withColumn("source_file", F.coalesce(F.col("source_file"), F.lit("unknown")))
    .withColumn("taxi_type",   F.lit("fhv"))

    .select(
        "taxi_type", "pickup_datetime", "dropoff_datetime",
        "provider_id", "dispatch_base_id",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "is_shared_ride",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee",
        "ingested_at", "source_file"
    )
)

# Boundary temporal
df_silver_fhv = apply_time_filter(df_silver_fhv)

# Validade temporal
df_silver_fhv = apply_temporal_validity(df_silver_fhv)

# Sanity bound — apenas duração (FHV não possui trip_distance)
df_silver_fhv = apply_sanity_bounds(df_silver_fhv, has_distance=False)

# Observabilidade
df_silver_fhv = add_record_hash(df_silver_fhv)
df_silver_fhv = add_ride_key(df_silver_fhv)
df_silver_fhv = add_financial_flags(df_silver_fhv, "fhv")

# Deduplicação — ver justificativa no cabeçalho desta seção
df_silver_fhv = df_silver_fhv.dropDuplicates(["record_hash"])

(
    df_silver_fhv.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{DST_DATABASE}.fhv_taxi")
)

print(f"✓ {DST_DATABASE}.fhv_taxi: "
      f"{spark.table(f'{DST_DATABASE}.fhv_taxi').count():,} registros")


# COMMAND ----------

# ==============================================================================
# VERIFICAÇÃO FINAL
# ==============================================================================

print("\n" + "=" * 72)
print("  RESUMO SILVER")
print("=" * 72)

tabelas = ["yellow_taxi", "green_taxi", "hvfhv_taxi", "fhv_taxi"]

for tabela in tabelas:
    try:
        n_bronze = spark.table(f"{SRC_DATABASE}.{tabela}").count()
        n_silver = spark.table(f"{DST_DATABASE}.{tabela}").count()
        removidos = n_bronze - n_silver
        pct = removidos / n_bronze * 100
        print(f"  {tabela:<15}  Bronze: {n_bronze:>12,}  "
              f"Silver: {n_silver:>12,}  removidos: {removidos:>8,} ({pct:.1f}%)")
    except Exception as e:
        print(f"  {tabela:<15}  erro: {e}")

print("\n  Validação pós-reprocessamento (alertas críticos do audit):")

validacoes = [
    ("Yellow fare_amount < 0",
     "yellow_taxi",
     F.col("fare_amount") < 0),
    ("Yellow pickup > dropoff",
     "yellow_taxi",
     F.col("pickup_datetime") > F.col("dropoff_datetime")),
    ("Yellow duration > 24h",
     "yellow_taxi",
     (F.col("dropoff_datetime").cast("timestamp").cast("long") -
      F.col("pickup_datetime").cast("timestamp").cast("long")) > MAX_DURATION_SECONDS),
    ("Yellow trip_distance > MAX",
     "yellow_taxi",
     F.col("trip_distance") > TRIP_DISTANCE_MAX),
    ("Green fare_amount < 0",
     "green_taxi",
     F.col("fare_amount") < 0),
    ("Green trip_distance > MAX",
     "green_taxi",
     F.col("trip_distance") > TRIP_DISTANCE_MAX),
    ("HVFHV trip_distance > MAX",
     "hvfhv_taxi",
     F.col("trip_distance") > TRIP_DISTANCE_MAX),
    ("FHV duration > 24h",
     "fhv_taxi",
     (F.col("dropoff_datetime").cast("timestamp").cast("long") -
      F.col("pickup_datetime").cast("timestamp").cast("long")) > MAX_DURATION_SECONDS),
    ("FHV hashes repetidos",
     "fhv_taxi",
     None),
]

for label, tabela, cond in validacoes:
    df = spark.table(f"{DST_DATABASE}.{tabela}")
    if cond is not None:
        n = df.filter(cond).count()
    else:
        n = (df.groupBy("record_hash").count()
               .filter(F.col("count") > 1).count())
    status = "✓  0" if n == 0 else f"⚠️  {n:,}"
    print(f"  {label:<35}: {status}")

print("\n  Schema FHV — verificação de tipos canônicos:")
fhv_schema = {f.name: str(f.dataType)
              for f in spark.table(f"{DST_DATABASE}.fhv_taxi").schema}

checks_schema = [
    ("pickup_datetime",      "TimestampNTZType"),
    ("dropoff_datetime",     "TimestampNTZType"),
    ("pickup_location_id",   "IntegerType"),
    ("dropoff_location_id",  "IntegerType"),
]
for col, expected in checks_schema:
    actual = fhv_schema.get(col, "não encontrado")
    status = "✓" if expected in actual else f"⚠️  {actual}"
    print(f"  fhv.{col:<25}: {status}")

print()
print("Tabelas registradas:")
spark.sql(f"SHOW TABLES IN {DST_DATABASE}").show()
