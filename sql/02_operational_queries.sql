-- =============================================================================
-- File:    02_operational_queries.sql
-- Project: Restaurant Analytics Engine
-- Purpose: Operational analysis queries covering peak trading patterns,
--          menu performance, and size performance.
--          Results from these queries feed directly into
--          notebooks/02_operational_analysis.ipynb for statistical testing
--          and visualisation in R.
--
-- All queries use the platos_pizza star schema:
--   fact_orders, dim_date, dim_time, dim_pizza, dim_pizza_type
--
-- Execution order matters. Run sections in the order they appear.
-- =============================================================================

USE platos_pizza;

-- =============================================================================
-- SECTION 0: DATA INTEGRITY CHECK
-- Purpose: Confirm Sunday is genuinely absent from the trading data, not
--          missing due to a load error. The dim_date distinct values show
--          day_of_week running 2-8, which means day_of_week = 1 (Sunday in
--          MySQL convention) either never traded or was excluded.
--          This must be confirmed before any day-of-week analysis is run.
-- =============================================================================

SELECT
    dd.day_name,
    dd.day_of_week,
    COUNT(fo.order_id) AS total_orders
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY
    dd.day_name,
    dd.day_of_week
ORDER BY
    dd.day_of_week;


-- =============================================================================
-- SECTION 1: PEAK ANALYSIS
-- Purpose: Identify when the restaurant is busiest by hour, day, and
--          meal period. This drives staffing recommendations and is the
--          foundation for the revenue leakage analysis in Notebook 02.
--
-- Why orders AND revenue separately:
--   A high order count hour is not always the highest revenue hour.
--   If order volume peaks at lunch but average order value peaks at dinner,
--   those are different operational problems requiring different responses.
-- =============================================================================

-- 1a. Orders and revenue by hour of day
--     This is the most granular view of trading patterns.
--     Use this to identify peak hours and shoulder hours.
SELECT
    dt.hour,
    COUNT(DISTINCT fo.order_id)  AS total_orders,
    SUM(fo.quantity)             AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue,
    ROUND(AVG(fo.total_price), 2) AS avg_line_item_value
FROM fact_orders fo
JOIN dim_time dt ON fo.time_id = dt.time_id
GROUP BY
    dt.hour
ORDER BY
    dt.hour;


-- 1b. Orders and revenue by meal period
--     Meal periods aggregate the hourly data into operationally meaningful
--     buckets. Morning is included for completeness but was flagged in the
--     audit as negligible (<0.05% of volume).
SELECT
    dt.meal_period,
    COUNT(DISTINCT fo.order_id)   AS total_orders,
    SUM(fo.quantity)              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue,
    ROUND(
        SUM(fo.total_price) / COUNT(DISTINCT fo.order_id),
    2)                            AS avg_order_value
FROM fact_orders fo
JOIN dim_time dt ON fo.time_id = dt.time_id
GROUP BY
    dt.meal_period
ORDER BY
    FIELD(dt.meal_period, 'Morning', 'Lunch', 'Afternoon', 'Dinner', 'Late Night');

-- Why FIELD() for ORDER BY:
--   Alphabetical ordering would put Afternoon before Dinner before Lunch,
--   which is not chronological. FIELD() lets us define an explicit sort order
--   that matches the actual sequence of the trading day.


-- 1c. Orders and revenue by day of week
--     Use day_of_week for sort order, day_name for readability.
SELECT
    dd.day_name,
    dd.day_of_week,
    COUNT(DISTINCT fo.order_id)   AS total_orders,
    SUM(fo.quantity)              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue,
    ROUND(
        SUM(fo.total_price) / COUNT(DISTINCT fo.order_id),
    2)                            AS avg_order_value
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY
    dd.day_name,
    dd.day_of_week
ORDER BY
    dd.day_of_week;


-- 1d. Orders and revenue by month
--     Provides the seasonal view needed for the decomposition analysis
--     in Notebook 03. Also surfaces whether any months are outliers.
SELECT
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fo.order_id)   AS total_orders,
    SUM(fo.quantity)              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
GROUP BY
    dd.month,
    dd.month_name
ORDER BY
    dd.month;


-- 1e. Heatmap base: orders by day of week and hour
--     This produces the data for an order volume heatmap in R.
--     Shows which day-hour combinations are genuinely peak versus those
--     that merely look busy on a single-dimension view.
SELECT
    dd.day_name,
    dd.day_of_week,
    dt.hour,
    COUNT(DISTINCT fo.order_id)   AS total_orders,
    SUM(fo.quantity)              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
JOIN dim_time dt ON fo.time_id = dt.time_id
GROUP BY
    dd.day_name,
    dd.day_of_week,
    dt.hour
ORDER BY
    dd.day_of_week,
    dt.hour;


-- =============================================================================
-- SECTION 2: MENU PERFORMANCE
-- Purpose: Rank menu items by volume and by revenue separately.
--          These produce different rankings. A pizza that sells frequently
--          but at a low price looks strong on volume but weak on revenue.
--          Both rankings together reveal which items are carrying the menu
--          and which are occupying space without justification.
-- =============================================================================

-- 2a. All menu items ranked by volume and revenue
--     Includes category for grouping in R visualisations.
SELECT
    pt.name                           AS pizza_name,
    pt.category,
    SUM(fo.quantity)                  AS total_quantity,
    ROUND(SUM(fo.total_price), 2)     AS total_revenue,
    ROUND(AVG(fo.unit_price), 2)      AS avg_unit_price,
    RANK() OVER (ORDER BY SUM(fo.quantity) DESC)      AS volume_rank,
    RANK() OVER (ORDER BY SUM(fo.total_price) DESC)   AS revenue_rank
FROM fact_orders fo
JOIN dim_pizza_type pt ON fo.pizza_type_id = pt.pizza_type_id
GROUP BY
    pt.name,
    pt.category
ORDER BY
    total_revenue DESC;

-- Why window functions here:
--   RANK() lets R immediately identify rank divergence — items where
--   volume_rank and revenue_rank differ significantly. That gap is itself
--   a finding worth surfacing to the business.


-- 2b. Bottom 20% of menu items by revenue
--     Identifies candidates for rationalisation. These are the items
--     generating the least revenue while still carrying menu complexity cost.
--     The 20% threshold follows the Pareto principle as a starting point,
--     but the R analysis will test whether there is a natural breakpoint
--     in the distribution rather than applying the 20% cutoff rigidly.
SELECT
    pt.name                       AS pizza_name,
    pt.category,
    SUM(fo.quantity)              AS total_quantity,
    ROUND(SUM(fo.total_price), 2) AS total_revenue
FROM fact_orders fo
JOIN dim_pizza_type pt ON fo.pizza_type_id = pt.pizza_type_id
GROUP BY
    pt.name,
    pt.category
ORDER BY
    total_revenue ASC
LIMIT 7;
-- 7 = 20% of 32 menu items (32 * 0.20 = 6.4, rounded up to 7)


-- 2c. Menu performance by category
--     Aggregates to the four categories: Chicken, Classic, Supreme, Veggie.
--     Shows whether any category is underperforming relative to its share
--     of menu space.
SELECT
    pt.category,
    COUNT(DISTINCT pt.pizza_type_id)  AS menu_items,
    SUM(fo.quantity)                  AS total_quantity,
    ROUND(SUM(fo.total_price), 2)     AS total_revenue,
    ROUND(
        SUM(fo.total_price) / SUM(fo.quantity),
    2)                                AS avg_revenue_per_pizza,
    ROUND(
        100.0 * SUM(fo.total_price) / SUM(SUM(fo.total_price)) OVER (),
    1)                                AS revenue_share_pct
FROM fact_orders fo
JOIN dim_pizza_type pt ON fo.pizza_type_id = pt.pizza_type_id
GROUP BY
    pt.category
ORDER BY
    total_revenue DESC;

-- Why revenue_share_pct:
--   Raw revenue numbers alone don't tell you whether a category punches
--   above or below its weight. Dividing by total revenue puts each
--   category in context. SUM() OVER () is a window function that computes
--   the grand total without collapsing the GROUP BY.


-- 2d. Zero and near-zero performers
--     Retrieves all menu items from dim_pizza_type and left joins to
--     fact_orders to surface any items with zero or very low sales.
--     The audit in Notebook 01 identified five zero-order items.
--     This query confirms and extends that finding.
SELECT
    pt.pizza_type_id,
    pt.name                           AS pizza_name,
    pt.category,
    COALESCE(SUM(fo.quantity), 0)     AS total_quantity,
    COALESCE(
        ROUND(SUM(fo.total_price), 2),
    0)                                AS total_revenue
FROM dim_pizza_type pt
LEFT JOIN fact_orders fo ON pt.pizza_type_id = fo.pizza_type_id
GROUP BY
    pt.pizza_type_id,
    pt.name,
    pt.category
HAVING
    total_quantity < 500
ORDER BY
    total_quantity ASC;

-- Why LEFT JOIN:
--   An INNER JOIN would silently exclude items with zero sales because
--   they have no matching rows in fact_orders. LEFT JOIN keeps all menu
--   items in the result set, with NULLs for items that never sold.
--   COALESCE converts those NULLs to 0 for clean output.
-- The HAVING threshold of 500 is a starting point — adjust in R after
-- reviewing the distribution.


-- =============================================================================
-- SECTION 3: SIZE PERFORMANCE
-- Purpose: Understand how pizza size drives volume and revenue.
--          The audit flagged XL (1.1% of volume) and XXL (0.06%, $1,007
--          annual revenue) as candidates for rationalisation.
--          This section quantifies that fully.
-- =============================================================================

-- 3a. Volume and revenue by size
SELECT
    p.size,
    SUM(fo.quantity)                  AS total_quantity,
    ROUND(SUM(fo.total_price), 2)     AS total_revenue,
    ROUND(AVG(fo.unit_price), 2)      AS avg_unit_price,
    ROUND(
        100.0 * SUM(fo.quantity) / SUM(SUM(fo.quantity)) OVER (),
    1)                                AS volume_share_pct,
    ROUND(
        100.0 * SUM(fo.total_price) / SUM(SUM(fo.total_price)) OVER (),
    1)                                AS revenue_share_pct
FROM fact_orders fo
JOIN dim_pizza p ON fo.pizza_id = p.pizza_id
GROUP BY
    p.size
ORDER BY
    FIELD(p.size, 'S', 'M', 'L', 'XL', 'XXL');


-- 3b. Size performance by category
--     Tests whether size preferences differ across pizza categories.
--     If Veggie customers skew toward smaller sizes, that has different
--     pricing and upsell implications than if they mirror the overall pattern.
SELECT
    pt.category,
    p.size,
    SUM(fo.quantity)              AS total_quantity,
    ROUND(SUM(fo.total_price), 2) AS total_revenue,
    ROUND(
        100.0 * SUM(fo.quantity) /
        SUM(SUM(fo.quantity)) OVER (PARTITION BY pt.category),
    1)                            AS volume_share_within_category
FROM fact_orders fo
JOIN dim_pizza p         ON fo.pizza_id      = p.pizza_id
JOIN dim_pizza_type pt   ON fo.pizza_type_id = pt.pizza_type_id
GROUP BY
    pt.category,
    p.size
ORDER BY
    pt.category,
    FIELD(p.size, 'S', 'M', 'L', 'XL', 'XXL');

-- Why PARTITION BY category in the window function:
--   Without PARTITION BY, the percentage would be each row's share of
--   ALL sales across all categories. With PARTITION BY, it calculates
--   each size's share WITHIN its own category. This lets you compare
--   size distribution patterns across categories on equal footing.


-- 3c. Upsell opportunity: medium to large conversion
--     Quantifies the revenue gain if a defined percentage of medium
--     orders converted to large. This feeds directly into the revenue
--     leakage section of Notebook 02.
--
--     The query calculates the price gap between M and L for each pizza
--     type, then applies a 10% conversion assumption as a baseline.
--     The conversion rate is a parameter — R will run this across a range
--     of scenarios (5%, 10%, 15%, 20%) to show the sensitivity.
SELECT
    pt.name                               AS pizza_name,
    pt.category,
    pm.price                              AS medium_price,
    pl.price                              AS large_price,
    ROUND(pl.price - pm.price, 2)         AS price_gap,
    SUM(CASE WHEN fo.pizza_id = pm.pizza_id
             THEN fo.quantity ELSE 0 END) AS medium_quantity_sold,
    ROUND(
        SUM(CASE WHEN fo.pizza_id = pm.pizza_id
                 THEN fo.quantity ELSE 0 END)
        * 0.10
        * (pl.price - pm.price),
    2)                                    AS upsell_revenue_at_10pct
FROM dim_pizza_type pt
JOIN dim_pizza pm  ON pt.pizza_type_id = pm.pizza_type_id AND pm.size = 'M'
JOIN dim_pizza pl  ON pt.pizza_type_id = pl.pizza_type_id AND pl.size = 'L'
JOIN fact_orders fo ON pt.pizza_type_id = fo.pizza_type_id
GROUP BY
    pt.name,
    pt.category,
    pm.price,
    pl.price
ORDER BY
    upsell_revenue_at_10pct DESC;
