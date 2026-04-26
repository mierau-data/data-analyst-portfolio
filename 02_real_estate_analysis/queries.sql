/*
 * Анализ рынка недвижимости Санкт-Петербурга и Ленинградской области
 * Учебный проект курса «Аналитик данных» Яндекс Практикум
 *
 * Источник данных: real_estate (схема в PostgreSQL)
 * Период: 2015–2018 годы, только города
 *
 * Содержит два запроса:
 *   1. Время активности объявлений по сегментам (1 мес / 1–3 / 3–6 / 6+ / не снято)
 *   2. Сезонность публикаций и снятий объявлений по месяцам
 *
 * Перед расчётами в обоих запросах:
 *   - отфильтрованы выбросы по 99 перцентилю (площадь, комнаты, балконы, потолки)
 *     и 1 перцентилю (нижний предел потолков)
 *   - оставлены строки с NULL в неключевых полях
 */


-- ============================================================
-- Задача 1. Время активности объявлений
-- ============================================================
-- Сравнение Санкт-Петербурга и Ленинградской области по сегментам срока продажи.
-- Для каждой пары (регион, сегмент) считаем количество объявлений, долю,
-- среднюю цену за м², среднюю площадь, медианы по комнатам/балконам/этажности
-- и дополнительные метрики: высота потолков, доля студий/апартаментов/свободной
-- планировки, расстояние до аэропорта, парки и водоёмы в радиусе 3 км.

WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area)     AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms)          AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony)        AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- id объявлений без выбросов; пропуски сохраняем
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
              AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits))
             OR ceiling_height IS NULL)
),
-- Базовый набор: только города, 2015–2018 годы, без выбросов
base AS (
    SELECT
        CASE
            WHEN c.city = 'Санкт-Петербург' THEN 'Saint Petersburg'
            ELSE 'Leningrad Oblast'
        END AS region,
        CASE
            WHEN a.days_exposition IS NULL              THEN 'non_category'
            WHEN a.days_exposition BETWEEN 1   AND 30   THEN 'up_to_1_month'
            WHEN a.days_exposition BETWEEN 31  AND 90   THEN 'up_to_3_months'
            WHEN a.days_exposition BETWEEN 91  AND 180  THEN 'up_to_6_months'
            WHEN a.days_exposition >= 181               THEN 'more_than_6_months'
        END AS activity_segment,
        a.last_price / NULLIF(f.total_area, 0) AS price_per_sqm,
        f.total_area,
        f.rooms,
        f.balcony,
        f.floors_total,
        f.ceiling_height,
        f.airports_nearest,
        f.parks_around3000,
        f.ponds_around3000,
        CASE WHEN f.rooms = 0 THEN 1 ELSE 0 END                AS is_studio,
        CASE WHEN f.is_apartment = 1 THEN 1 ELSE 0 END         AS is_apartment_flag,
        CASE WHEN f.open_plan = 1 THEN 1 ELSE 0 END            AS is_open_plan_flag
    FROM real_estate.advertisement a
    JOIN real_estate.flats f ON a.id      = f.id
    JOIN real_estate.city  c ON f.city_id = c.city_id
    JOIN real_estate.type  t ON f.type_id = t.type_id
    WHERE
        a.id IN (SELECT id FROM filtered_id)
        AND EXTRACT(YEAR FROM a.first_day_exposition) BETWEEN 2015 AND 2018
        AND t.type = 'город'
)
SELECT
    region,
    activity_segment,
    COUNT(*) AS ad_count,
    ROUND((COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER (PARTITION BY region)) * 100, 2) AS adshare_pct,
    ROUND(AVG(price_per_sqm)::numeric, 2) AS avg_price_per_sqm,
    ROUND(AVG(total_area)::numeric,    2) AS avg_total_area,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rooms)        AS median_rooms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY balcony)      AS median_balconies,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY floors_total) AS median_floor_count,
    ROUND(AVG(ceiling_height)::numeric, 2)                    AS avg_ceiling_height,
    ROUND((SUM(is_studio)::DECIMAL        / COUNT(*)) * 100, 2) AS studio_pct,
    ROUND((SUM(is_apartment_flag)::DECIMAL / COUNT(*)) * 100, 2) AS apartment_pct,
    ROUND((SUM(is_open_plan_flag)::DECIMAL / COUNT(*)) * 100, 2) AS open_plan_pct,
    ROUND(AVG(airports_nearest)::numeric, 2)                  AS avg_distance_to_airport_km,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY parks_around3000) AS median_parks_3km,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ponds_around3000) AS median_water_bodies_3km
FROM base
GROUP BY region, activity_segment
ORDER BY
    CASE region
        WHEN 'Saint Petersburg' THEN 1
        ELSE 2
    END,
    CASE activity_segment
        WHEN 'non_category'        THEN 1
        WHEN 'up_to_1_month'       THEN 2
        WHEN 'up_to_3_months'      THEN 3
        WHEN 'up_to_6_months'      THEN 4
        WHEN 'more_than_6_months'  THEN 5
    END;


-- ============================================================
-- Задача 2. Сезонность объявлений
-- ============================================================
-- Поведение рынка по месяцам: сравнение публикаций и снятий объявлений.
-- Дата снятия рассчитывается как first_day_exposition + days_exposition.
-- Считаем количество публикаций/снятий, доли, ранги, среднюю цену за м²
-- и среднюю площадь по каждому месяцу за все годы 2015–2018.

SET lc_time = 'ru_RU';

WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area)     AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms)          AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony)        AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
filtered_id AS (
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
              AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits))
             OR ceiling_height IS NULL)
),
base AS (
    SELECT
        a.id,
        a.first_day_exposition,
        EXTRACT(MONTH FROM a.first_day_exposition)::int AS pub_month_num,
        a.days_exposition,
        (a.first_day_exposition + a.days_exposition * INTERVAL '1 day') AS close_date,
        EXTRACT(MONTH FROM (a.first_day_exposition + a.days_exposition * INTERVAL '1 day'))::int AS close_month_num,
        a.last_price,
        f.total_area,
        CASE
            WHEN f.total_area > 0 THEN a.last_price / f.total_area
            ELSE NULL
        END AS price_per_m2
    FROM real_estate.advertisement a
    JOIN real_estate.flats f ON a.id      = f.id
    JOIN real_estate.city  c ON f.city_id = c.city_id
    JOIN real_estate.type  t ON f.type_id = t.type_id
    WHERE
        a.id IN (SELECT id FROM filtered_id)
        AND EXTRACT(YEAR FROM a.first_day_exposition) BETWEEN 2015 AND 2018
        AND t.type = 'город'
),
total_ads AS (
    SELECT COUNT(*) AS total_count
    FROM base
),
publish_stats AS (
    SELECT
        pub_month_num,
        TO_CHAR(MAKE_DATE(2015, pub_month_num, 1), 'TMMonth') AS month_name,
        COUNT(*) AS publish_cnt,
        ROUND(AVG(price_per_m2)::numeric, 2) AS avg_price_per_m2_pub,
        ROUND(AVG(total_area)::numeric,   2) AS avg_area_pub
    FROM base
    GROUP BY pub_month_num
),
close_stats AS (
    SELECT
        close_month_num,
        TO_CHAR(MAKE_DATE(2015, close_month_num, 1), 'TMMonth') AS month_name,
        COUNT(*) AS close_cnt,
        ROUND(AVG(price_per_m2)::numeric, 2) AS avg_price_per_m2_close,
        ROUND(AVG(total_area)::numeric,   2) AS avg_area_close
    FROM base
    WHERE days_exposition IS NOT NULL
    GROUP BY close_month_num
)
SELECT
    COALESCE(p.month_name, c.month_name) AS month,
    p.publish_cnt,
    ROUND((p.publish_cnt::DECIMAL / (SELECT total_count FROM total_ads)) * 100, 2) AS publish_pct,
    RANK() OVER (ORDER BY p.publish_cnt DESC NULLS LAST) AS publish_rank,
    c.close_cnt,
    ROUND((c.close_cnt::DECIMAL / (SELECT total_count FROM total_ads)) * 100, 2) AS close_pct,
    RANK() OVER (ORDER BY c.close_cnt DESC NULLS LAST) AS close_rank,
    p.avg_price_per_m2_pub,
    c.avg_price_per_m2_close,
    p.avg_area_pub,
    c.avg_area_close
FROM publish_stats p
FULL JOIN close_stats c ON p.pub_month_num = c.close_month_num
ORDER BY COALESCE(p.pub_month_num, c.close_month_num);
