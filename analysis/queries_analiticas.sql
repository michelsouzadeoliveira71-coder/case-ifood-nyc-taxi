-- ============================================================
-- CASE iFOOD — Perguntas Analíticas
-- Fonte: case_gold.fact_trips
-- ============================================================

-- PERGUNTA 1
-- Qual a média de total_amount por mês — Yellow taxis?

SELECT
    pickup_month,
    ROUND(AVG(total_amount), 2)  AS avg_total_amount,
    COUNT(*)                      AS n_corridas,
    ROUND(SUM(total_amount), 2)  AS total_receita
FROM case_gold.fact_trips
WHERE taxi_type = 'yellow'
GROUP BY pickup_month
ORDER BY pickup_month;

-- Resultado:
-- Mês 1: $ 27,44 | Mês 2: $ 27,33 | Mês 3: $ 28,26
-- Mês 4: $ 28,76 | Mês 5: $ 29,46


-- ============================================================
-- PERGUNTA 2
-- Qual a média de passenger_count por hora — maio, todos os táxis?
--
-- Nota: FHV e HVFHV não coletam passenger_count por limitação
-- regulatória. O filtro > 0 os exclui naturalmente.
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

-- Resultado: curva entre 1,26 (6h) e 1,45 (2h)
-- Menor demanda: madrugada cedo (4h-6h)
-- Maior média: madrugada tardia (1h-3h, grupos saindo de eventos)
