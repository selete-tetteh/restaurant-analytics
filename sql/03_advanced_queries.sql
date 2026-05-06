-- =============================================================================
-- File:    03_advanced_queries.sql
-- Project: Restaurant Analytics Engine
-- Purpose: Advanced analytical queries covering order composition and seasonal
--          decomposition base data.
--          Results from these queries feed forecasting and experimental
--          design work in:
--            notebooks/03_forecasting.ipynb
--            notebooks/04_experiments.ipynb
--
-- All queries use the platos_pizza star schema:
--   fact_orders, dim_date, dim_time, dim_pizza, dim_pizza_type
--
-- Note on basket analysis (co-occurrence and cannibalisation):
--   Lift scores, pairwise co-occurrence, and cannibalisation screening are
--   computed in R inside notebooks/03_forecasting.ipynb rather than here.
--   MySQL cannot reopen the same temporary table twice within a single query,
--   which makes the pairwise self-join required for lift calculation either
--   time out or fail entirely on this dataset. R handles this cleanly using
--   in-memory data frames. The analytical logic is unchanged — only the
--   execution environment differs.
--
-- Execution order matters. Run sections in the order they appear.
-- =============================================================================

USE platos_pizza;


-- =============================================================================
-- SECTION 0: ORDER COMPOSITION BASELINE
-- Purpose: Quantify how many orders contain multiple pizza types. This is
--          the foundation for basket analysis. If most orders were single
--          items, basket analysis would be a waste of time. Confirming the
--          mix up front justifies the analytical approach.
-- =============================================================================

-- 0a. Distribution of order sizes by distinct pizza types
--     One row per (number of distinct pizza types in an order).
--     Confirmed that multi-item orders account for 61% of all orders,
--     which provides sufficient signal for basket analysis.
SELECT
    total_items,
    COUNT(*)                                              AS order_count,
    ROUND(COUNT(*) /
          (SELECT COUNT(DISTINCT order_id) FROM fact_orders) * 100, 1) AS share_pct
FROM (
    SELECT
        order_id,
        COUNT(DISTINCT pizza_type_id)                     AS total_items
    FROM fact_orders
    GROUP BY order_id
) AS order_sizes
GROUP BY total_items
ORDER BY total_items;


-- 0b. Headline split: single-item vs multi-item orders
--     Used in the notebook narrative to motivate basket analysis.
SELECT
    CASE
        WHEN total_items = 1              THEN 'Single item'
        WHEN total_items BETWEEN 2 AND 4  THEN 'Standard multi-item (2-4)'
        WHEN total_items >= 5             THEN 'Group/catering (5+)'
    END                                                   AS order_type,
    COUNT(*)                                              AS order_count,
    ROUND(COUNT(*) /
          (SELECT COUNT(DISTINCT order_id) FROM fact_orders) * 100, 1) AS share_pct
FROM (
    SELECT
        order_id,
        COUNT(DISTINCT pizza_type_id)                     AS total_items
    FROM fact_orders
    GROUP BY order_id
) AS order_sizes
GROUP BY order_type
ORDER BY FIELD(order_type, 'Single item', 'Standard multi-item (2-4)', 'Group/catering (5+)');


-- =============================================================================
-- SECTION 1: BASKET ANALYSIS — COMPUTED IN R
-- See: notebooks/03_forecasting.ipynb
--
-- Why R rather than SQL:
--   Lift scores require a pairwise self-join across all order-item combinations.
--   MySQL cannot reopen a temporary table within the same query that created it,
--   and a direct self-join on fact_orders (48,620 rows) times out before
--   completing. R loads the order-item data into memory as a data frame and
--   performs the join without these constraints.
--
-- What the R analysis produces:
--   - Pairwise co-occurrence counts for all pizza type pairs
--   - Lift scores: lift(A,B) = P(A and B together) / (P(A) * P(B))
--   - Top complementary pairs (lift > 1, ordered by score)
--   - Within-category lift to identify cannibalisation candidates
--   - Daily sales correlation between same-category items as a
--     second cannibalisation screening signal
-- =============================================================================


-- =============================================================================
-- SECTION 2: SEASONAL DECOMPOSITION BASE DATA
-- Purpose: Provide clean time series at daily, weekly, and monthly granularity
--          for downstream forecasting (Prophet) and seasonal decomposition
--          (R's stl() or decompose() functions).
--
-- Why three granularities:
--   Daily   : finest grain, used by Prophet and to detect day-of-week effects
--   Weekly  : smooths daily noise, useful for inspecting the trend component
--   Monthly : top-level view for the management report and high-level chart
--
-- Gap handling:
--   These queries return only days/weeks/months that exist in fact_orders.
--   If the dataset has missing days they will not appear here. Gap filling
--   is handled in the notebook using a calendar reference — that is an
--   analysis-layer decision, not a warehouse decision.
-- =============================================================================

-- 2a. Daily order time series
--     One row per trading day. Contains everything Prophet and stl() need:
--     date, total orders, total pizzas, total revenue.
SELECT
    fo.date_id,
    dd.day_of_week,
    dd.day_name,
    dd.is_weekend,
    COUNT(DISTINCT fo.order_id)                           AS total_orders,
    SUM(fo.quantity)                                      AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                         AS total_revenue,
    ROUND(SUM(fo.total_price) /
          COUNT(DISTINCT fo.order_id), 2)                 AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY fo.date_id, dd.day_of_week, dd.day_name, dd.is_weekend
ORDER BY fo.date_id;


-- 2b. Weekly order time series
--     ISO week numbering used here so weeks align with R's lubridate default
--     and Prophet's weekly seasonality detection.
SELECT
    dd.year,
    dd.week,
    MIN(fo.date_id)                                       AS week_start_date,
    COUNT(DISTINCT fo.order_id)                           AS total_orders,
    SUM(fo.quantity)                                      AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                         AS total_revenue,
    ROUND(SUM(fo.total_price) /
          COUNT(DISTINCT fo.order_id), 2)                 AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY dd.year, dd.week
ORDER BY dd.year, dd.week;


-- 2c. Monthly order time series
--     Used for the management report and the seasonal pattern chart in
--     the forecasting notebook.
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id)                           AS total_orders,
    SUM(fo.quantity)                                      AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                         AS total_revenue,
    ROUND(SUM(fo.total_price) /
          COUNT(DISTINCT fo.order_id), 2)                 AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;


-- 2d. Daily orders by hour
--     Prophet can produce hourly forecasts if given hourly input. This is
--     the source data structured for that purpose. The notebook will decide
--     whether daily or hourly forecasting better serves the staffing question.
SELECT
    fo.date_id,
    dt.hour,
    dt.meal_period,
    COUNT(DISTINCT fo.order_id)                           AS total_orders,
    SUM(fo.quantity)                                      AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                         AS total_revenue
FROM fact_orders fo
JOIN dim_time dt ON fo.time_id = dt.time_id
GROUP BY fo.date_id, dt.hour, dt.meal_period
ORDER BY fo.date_id, dt.hour;


-- =============================================================================
-- END OF FILE
-- =============================================================================
