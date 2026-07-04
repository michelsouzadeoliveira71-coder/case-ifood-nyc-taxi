-- ============================================================
-- CASE — Perguntas Analíticas
-- Fonte: case_gold.fact_trips
-- Ambiente: Databricks Community Edition + Delta Lake
-- ============================================================


-- ============================================================
-- PERGUNTA 1
-- Qual a média de total_amount por mês considerando
-- todos os yellow taxis da frota? (jan–mai 2023)
--
-- Decisões técnicas:
--   - Fonte: case_gold.fact_trips (camada Gold)
--   - Filtro: taxi_type = 'yellow'
--   - total_amount já validado na Silver (>= 0)
--   - pickup_month derivado de pickup_datetime na Gold
-- ============================================================

SELECT
    pickup_month,
    ROUND(AVG(total_amount), 2)  AS avg_total_amount,
    COUNT(*)                      AS n_corridas,
    ROUND(SUM(total_amount), 2)  AS total_receita
FROM case_gold.fact_trips
WHERE taxi_type = 'yellow'
GROUP BY pickup_month
ORDER BY pickup_month;

-- RESULTADO:
-- +-------------+-----------------+----------+------------------+
-- | pickup_month | avg_total_amount | n_corridas | total_receita  |
-- +-------------+-----------------+----------+------------------+
-- |      1       |      27.44      | 3.041.418  | 83.464.515,05  |
-- |      2       |      27.33      | 2.888.723  | 78.960.576,46  |
-- |      3       |      28.26      | 3.373.353  | 95.345.868,21  |
-- |      4       |      28.76      | 3.257.912  | 93.701.346,10  |
-- |      5       |      29.46      | 3.481.178  | 102.561.322,71 |
-- +-------------+-----------------+----------+------------------+

-- INTERPRETAÇÃO:
-- O ticket médio dos Yellow Taxis apresentou tendência de crescimento
-- gradual ao longo do período, variando de $27,44 (janeiro) a $29,46
-- (maio), representando aumento de +7,4%.
-- Maio registrou simultaneamente o maior ticket médio e o maior volume
-- de corridas, sugerindo aumento combinado de demanda e tarifa.
-- A variação mês a mês foi suave (máximo 3,4%), sem spikes anômalos.


-- ============================================================
-- PERGUNTA 2
-- Qual a média de passenger_count por hora do dia no mês
-- de maio considerando todos os táxis da frota?
--
-- Decisões técnicas:
--   - Fonte: case_gold.fact_trips (UNION ALL das 4 tabelas Silver)
--   - Filtro de mês: pickup_month = 5 (maio 2023)
--   - Filtro passenger_count > 0: valor 0 indica dado não informado
--     na fonte TLC — não representa corrida sem passageiro.
--     Essa é uma regra analítica da Gold, não um filtro de qualidade
--     da Silver, que preservou os zeros para permitir análises alternativas.
--   - pickup_hour derivado de pickup_datetime na Gold
--
-- Cobertura por tipo de táxi:
--   - Yellow e Green: contribuem com dados (campo disponível na fonte)
--   - HVFHV e FHV: excluídos naturalmente pelo filtro > 0, pois não
--     coletam passenger_count por limitação regulatória da TLC.
--     Isso é preferível a filtrar por taxi_type explicitamente:
--     mantém o modelo agnóstico à fonte.
-- ============================================================

SELECT
    pickup_hour,
    ROUND(AVG(passenger_count), 4) AS avg_passenger_count,
    COUNT(*)                        AS n_corridas
FROM case_gold.fact_trips
WHERE pickup_month    = 5
  AND passenger_count > 0
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- RESULTADO (maio 2023 — Yellow + Green):
-- +-----------+--------------------+----------+
-- |pickup_hour| avg_passenger_count|n_corridas|
-- +-----------+--------------------+----------+
-- |     0     |       1.4269       |  89.635  |
-- |     1     |       1.4366       |  58.270  |
-- |     2     |       1.4542       |  37.575  |
-- |     3     |       1.4498       |  24.553  |
-- |     4     |       1.4039       |  16.078  |
-- |     5     |       1.2844       |  18.576  |
-- |     6     |       1.2617       |  46.498  | ← mínimo
-- |     7     |       1.2814       |  94.162  |
-- |     8     |       1.2937       | 128.384  |
-- |     9     |       1.3109       | 144.095  |
-- |    10     |       1.3465       | 156.699  |
-- |    11     |       1.3615       | 170.737  |
-- |    12     |       1.3746       | 183.934  |
-- |    13     |       1.3829       | 187.845  |
-- |    14     |       1.3880       | 204.649  |
-- |    15     |       1.3992       | 209.291  |
-- |    16     |       1.3961       | 209.773  |
-- |    17     |       1.3871       | 228.900  | ← maior volume
-- |    18     |       1.3812       | 243.064  | ← maior volume
-- |    19     |       1.3900       | 217.787  |
-- |    20     |       1.3996       | 193.151  |
-- |    21     |       1.4182       | 196.941  |
-- |    22     |       1.4269       | 181.689  |
-- |    23     |       1.4214       | 141.655  |
-- +-----------+--------------------+----------+

-- INTERPRETAÇÃO:
-- A média de passageiros por corrida permaneceu entre 1,26 e 1,46
-- durante todo o dia, padrão típico de NYC com predominância de
-- viagens individuais.
--
-- Padrões identificados:
--   - Mínimo às 6h (1,2617): início do dia de trabalho, viagens
--     predominantemente solo (commuters).
--   - Máximo às 2h (1,4542): madrugada, grupos saindo de eventos
--     noturnos compartilham corridas.
--   - Maior volume de corridas: 17h–18h (rush hour), com média
--     próxima ao valor central (~1,38).
--   - Curva suave ao longo do dia, sem anomalias ou spikes isolados.
