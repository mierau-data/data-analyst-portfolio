/*
 * Проект «Секреты Темнолесья» — SQL-анализ внутриигровой экономики
 * Учебный проект курса «Аналитик данных» Яндекс Практикум
 *
 * Цель: оценить, как характеристики игроков и игровых персонажей влияют
 * на покупку внутриигровой валюты «райские лепестки», и проанализировать
 * активность игроков при совершении внутриигровых покупок.
 *
 * Источник данных: схема fantasy в PostgreSQL (DBeaver)
 *   - users  — игроки (id, payer, race_id, ...)
 *   - race   — справочник рас
 *   - events — внутриигровые покупки (transaction_id, id, item_code, amount)
 *   - items  — справочник предметов (item_code, game_items)
 *
 * Структура запросов:
 *   Часть 1. Исследовательский анализ
 *     1.1 Доля платящих игроков по всем данным
 *     1.2 Доля платящих игроков в разрезе расы
 *     2.1 Статистика по полю amount
 *     2.2 Аномальные нулевые покупки
 *     2.3 Популярность эпических предметов
 *   Часть 2. Ad hoc
 *     Зависимость активности игроков от расы
 */


-- ============================================================
-- Часть 1. Исследовательский анализ данных
-- ============================================================

-- 1.1. Доля платящих игроков по всем данным
SELECT
    COUNT(id)                            AS total_players,   -- всего зарегистрированных игроков
    SUM(payer)                           AS paying_players,  -- платящих игроков
    ROUND(AVG(payer)::NUMERIC, 2)        AS payer_share      -- доля платящих
FROM fantasy.users;


-- 1.2. Доля платящих игроков в разрезе расы персонажа
SELECT
    r.race_id,
    r.race,
    SUM(u.payer)                                          AS paying_players,
    COUNT(u.id)                                           AS total_players,
    ROUND(SUM(u.payer)::NUMERIC / COUNT(u.id), 2)         AS payer_share
FROM fantasy.users AS u
JOIN fantasy.race  AS r ON u.race_id = r.race_id
GROUP BY r.race_id, r.race
ORDER BY payer_share DESC;


-- 2.1. Базовая статистика по стоимости покупки (amount)
SELECT
    COUNT(amount)                                                  AS total_purchases,
    SUM(amount)                                                    AS total_amount,
    MIN(amount)                                                    AS min_amount,
    MAX(amount)                                                    AS max_amount,
    ROUND(AVG(amount)::NUMERIC, 2)                                 AS avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount::numeric)   AS median_amount,
    ROUND(STDDEV(amount)::NUMERIC, 2)                              AS stddev_amount
FROM fantasy.events;


-- 2.2. Покупки с нулевой стоимостью (аномалии)
SELECT
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END)                                 AS zero_amount_purchases,
    COUNT(*)                                                                    AS total_purchases,
    ROUND(SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 5)   AS zero_amount_share
FROM fantasy.events;


-- 2.3. Популярность эпических предметов
-- При расчёте долей исключаем нулевые покупки.
WITH total_buyers AS (
    SELECT COUNT(DISTINCT id) AS total_buyers
    FROM fantasy.events
    WHERE amount > 0
)
SELECT
    i.game_items                                                                      AS epic_item,
    COUNT(e.transaction_id)                                                           AS total_sales,
    ROUND(COUNT(e.transaction_id)::NUMERIC
          / SUM(COUNT(e.transaction_id)) OVER(), 2)                                   AS sales_share,
    COUNT(DISTINCT e.id)                                                              AS buyers_count,
    ROUND(COUNT(DISTINCT e.id)::NUMERIC / t.total_buyers, 2)                          AS buyer_share
FROM fantasy.events    AS e
JOIN fantasy.items     AS i ON e.item_code = i.item_code
CROSS JOIN total_buyers AS t
WHERE e.amount > 0
GROUP BY i.game_items, t.total_buyers
ORDER BY buyer_share DESC;


-- ============================================================
-- Часть 2. Ad hoc-задача: активность игроков по расам
-- ============================================================
-- Для каждой расы считаем:
--   - всего игроков и сделавших покупки (с долей),
--   - долю платящих среди покупателей,
--   - средние: число покупок на покупателя, средний чек, суммарные траты на покупателя.
-- Нулевые покупки исключены.

WITH all_players AS (
    SELECT
        r.race          AS race_name,
        COUNT(u.id)     AS total_players
    FROM fantasy.users AS u
    JOIN fantasy.race  AS r ON u.race_id = r.race_id
    GROUP BY r.race
),
purchasing_players AS (
    SELECT
        r.race                                                       AS race_name,
        COUNT(DISTINCT e.id)                                         AS purchasing_players,
        COUNT(DISTINCT CASE WHEN u.payer = 1 THEN e.id END)          AS paying_players
    FROM fantasy.events AS e
    JOIN fantasy.users  AS u ON e.id = u.id
    JOIN fantasy.race   AS r ON u.race_id = r.race_id
    WHERE e.amount > 0
    GROUP BY r.race
),
activity AS (
    SELECT
        r.race                          AS race_name,
        COUNT(e.transaction_id)         AS total_purchases,
        SUM(e.amount)                   AS total_amount,
        COUNT(DISTINCT e.id)            AS active_players
    FROM fantasy.events AS e
    JOIN fantasy.users  AS u ON e.id = u.id
    JOIN fantasy.race   AS r ON u.race_id = r.race_id
    WHERE e.amount > 0
    GROUP BY r.race
)
SELECT
    ap.race_name,
    ap.total_players,
    pp.purchasing_players,
    ROUND(pp.purchasing_players::NUMERIC / ap.total_players, 2)        AS purchase_share,
    ROUND(pp.paying_players::NUMERIC / pp.purchasing_players, 2)       AS paying_share,
    ROUND(act.total_purchases::NUMERIC / act.active_players, 2)        AS avg_purchases_per_player,
    ROUND(act.total_amount::NUMERIC / act.total_purchases, 2)          AS avg_purchase_amount,
    ROUND(act.total_amount::NUMERIC / act.active_players, 2)           AS avg_total_per_player
FROM all_players          AS ap
LEFT JOIN purchasing_players AS pp  ON ap.race_name = pp.race_name
LEFT JOIN activity           AS act ON ap.race_name = act.race_name
ORDER BY avg_total_per_player DESC;
