/*
 * Анализ ключевых метрик сервиса доставки еды «Всё.из.кафе»
 * Учебный проект курса «Аналитик данных» Яндекс Практикум
 *
 * Источник данных: схема rest_analytics в PostgreSQL (Yandex Cloud)
 *   - analytics_events — события пользователей (просмотры, заказы)
 *   - cities           — справочник городов
 *   - partners         — рестораны-партнёры
 *   - dishes           — блюда ресторанов
 *
 * Город: Саранск
 * Период: 1 мая – 30 июня 2021 года
 *
 * Запросы под 7 чартов дашборда:
 *   1. DAU                 — активные пользователи (заказы) по дням
 *   2. Conversion Rate     — конверсия посетителей в заказы по дням
 *   3. Средний чек         — по месяцам, через комиссию (revenue * commission)
 *   4. LTV ресторанов      — топ-3 по сумме комиссии
 *   5. LTV блюд            — топ-5 блюд из топ-2 ресторанов по LTV
 *   6. Retention Rate      — по дням после первого визита (общий)
 *   7. Retention Rate      — то же, в разрезе месячных когорт
 */


-- ============================================================
-- 1. DAU — Daily Active Users (по событию order)
-- ============================================================
SELECT
    log_date,
    COUNT(DISTINCT user_id) AS DAU
FROM rest_analytics.analytics_events AS events
JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
WHERE log_date BETWEEN '2021-05-01' AND '2021-06-30'
  AND city_name = 'Саранск'
  AND event = 'order'
GROUP BY log_date
ORDER BY log_date;


-- ============================================================
-- 2. Conversion Rate — доля пользователей, оформивших заказ, от всех активных
-- ============================================================
SELECT
    log_date,
    ROUND(
        (COUNT(DISTINCT user_id) FILTER (WHERE event = 'order'))
        / COUNT(DISTINCT user_id)::numeric,
        2
    ) AS CR
FROM rest_analytics.analytics_events AS events
JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
WHERE log_date BETWEEN '2021-05-01' AND '2021-06-30'
  AND city_name = 'Саранск'
GROUP BY log_date
ORDER BY log_date;


-- ============================================================
-- 3. Средний чек по месяцам
-- ============================================================
-- Сначала считаем комиссионную выручку по каждому заказу,
-- затем агрегируем по месяцу.
WITH orders AS (
    SELECT
        events.*,
        revenue * commission AS commission_revenue
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE revenue IS NOT NULL
      AND log_date BETWEEN '2021-05-01' AND '2021-06-30'
      AND city_name = 'Саранск'
)
SELECT
    CAST(DATE_TRUNC('month', log_date) AS date)         AS "Месяц",
    COUNT(DISTINCT order_id)                             AS "Количество заказов",
    ROUND(SUM(commission_revenue)::numeric, 2)           AS "Сумма комиссии",
    ROUND(
        (SUM(commission_revenue) / COUNT(DISTINCT order_id))::numeric,
        2
    ) AS "Средний чек"
FROM orders
GROUP BY "Месяц"
ORDER BY "Месяц";


-- ============================================================
-- 4. LTV ресторанов — топ-3
-- ============================================================
WITH orders AS (
    SELECT
        events.rest_id,
        events.city_id,
        revenue * commission AS commission_revenue
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE revenue IS NOT NULL
      AND log_date BETWEEN '2021-05-01' AND '2021-06-30'
      AND city_name = 'Саранск'
)
SELECT
    orders.rest_id,
    chain AS "Название сети",
    type  AS "Тип кухни",
    ROUND(SUM(commission_revenue)::numeric, 2) AS LTV
FROM orders
JOIN rest_analytics.partners
    ON orders.rest_id  = partners.rest_id
   AND orders.city_id  = partners.city_id
GROUP BY 1, 2, 3
ORDER BY LTV DESC
LIMIT 3;


-- ============================================================
-- 5. LTV блюд — топ-5 в двух самых прибыльных ресторанах
-- ============================================================
WITH orders AS (
    SELECT
        events.rest_id,
        events.city_id,
        events.object_id,
        revenue * commission AS commission_revenue
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE revenue IS NOT NULL
      AND log_date BETWEEN '2021-05-01' AND '2021-06-30'
      AND city_name = 'Саранск'
),
-- Два ресторана с максимальным LTV — далее ограничиваем выборку блюд только ими
top_ltv_restaurants AS (
    SELECT
        orders.rest_id,
        chain,
        type,
        ROUND(SUM(commission_revenue)::numeric, 2) AS LTV
    FROM orders
    JOIN rest_analytics.partners partners
        ON orders.rest_id = partners.rest_id
       AND orders.city_id = partners.city_id
    GROUP BY 1, 2, 3
    ORDER BY LTV DESC
    LIMIT 2
)
SELECT
    chain          AS "Название сети",
    dishes.name    AS "Название блюда",
    spicy,
    fish,
    meat,
    ROUND(SUM(orders.commission_revenue)::numeric, 2) AS LTV
FROM orders
JOIN top_ltv_restaurants
    ON orders.rest_id = top_ltv_restaurants.rest_id
JOIN rest_analytics.dishes dishes
    ON orders.object_id = dishes.object_id
   AND top_ltv_restaurants.rest_id = dishes.rest_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY LTV DESC
LIMIT 5;


-- ============================================================
-- 6. Retention Rate — общий, по дням после первого визита
-- ============================================================
WITH new_users AS (
    -- новые пользователи: дата первого посещения попадает в окно
    SELECT DISTINCT
        first_date,
        user_id
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE first_date BETWEEN '2021-05-01' AND '2021-06-24'
      AND city_name = 'Саранск'
),
active_users AS (
    -- активные пользователи: любая активность в окне анализа
    SELECT DISTINCT
        log_date,
        user_id
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE log_date BETWEEN '2021-05-01' AND '2021-06-30'
      AND city_name = 'Саранск'
),
daily_retention AS (
    SELECT
        n.user_id,
        first_date,
        log_date::date - first_date::date AS day_since_install
    FROM new_users  AS n
    JOIN active_users AS a
        ON n.user_id = a.user_id
       AND log_date >= first_date
)
SELECT
    day_since_install,
    COUNT(DISTINCT user_id) AS retained_users,
    -- база — день 0 (MAX по окну сверху вниз)
    ROUND(
        (1.0 * COUNT(DISTINCT user_id) /
              MAX(COUNT(DISTINCT user_id)) OVER (ORDER BY day_since_install))::numeric,
        2
    ) AS retention_rate
FROM daily_retention
WHERE day_since_install < 8
GROUP BY day_since_install
ORDER BY day_since_install;


-- ============================================================
-- 7. Retention Rate в разрезе месячных когорт (май vs июнь)
-- ============================================================
WITH new_users AS (
    SELECT DISTINCT
        first_date,
        user_id
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE first_date BETWEEN '2021-05-01' AND '2021-06-24'
      AND city_name = 'Саранск'
),
active_users AS (
    SELECT DISTINCT
        log_date,
        user_id
    FROM rest_analytics.analytics_events AS events
    JOIN rest_analytics.cities cities ON events.city_id = cities.city_id
    WHERE log_date BETWEEN '2021-05-01' AND '2021-06-30'
      AND city_name = 'Саранск'
),
daily_retention AS (
    SELECT
        n.user_id,
        first_date,
        log_date::date - first_date::date AS day_since_install
    FROM new_users  AS n
    JOIN active_users AS a
        ON n.user_id = a.user_id
       AND log_date >= first_date
)
SELECT DISTINCT
    CAST(DATE_TRUNC('month', first_date) AS date) AS "Месяц",
    day_since_install,
    COUNT(DISTINCT user_id) AS retained_users,
    -- база — день 0 внутри своей когорты (PARTITION BY месяц)
    ROUND(
        (1.0 * COUNT(DISTINCT user_id) /
              MAX(COUNT(DISTINCT user_id)) OVER (
                  PARTITION BY CAST(DATE_TRUNC('month', first_date) AS date)
                  ORDER BY day_since_install
              ))::numeric,
        2
    ) AS retention_rate
FROM daily_retention
WHERE day_since_install < 8
GROUP BY "Месяц", day_since_install
ORDER BY "Месяц", day_since_install;
