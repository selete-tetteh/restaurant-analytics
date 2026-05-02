-- =============================================================================
-- 01_schema.sql
-- Project:  Restaurant Analytics Engine
-- Purpose:  Create the platos_pizza database and star schema warehouse.
--
-- Architecture: star schema with one fact table and four dimension tables.
--   - fact_orders    : one row per pizza line item (the measurable events)
--   - dim_pizza      : pizza size and price (slowly changing attributes)
--   - dim_pizza_type : pizza name, category, and ingredients (descriptive)
--   - dim_date       : pre-computed date attributes for fast filtering
--   - dim_time       : pre-computed time attributes for fast filtering
--
-- Why pre-compute date and time dimensions?
--   Calling HOUR(), DAYOFWEEK(), or MONTH() inside every analytical query
--   forces MySQL to compute those values row-by-row at query time. Joining
--   to a dimension table instead means the computation happens once, here,
--   at load time. Queries stay clean and performance scales better.
--
-- Run order: this file must be run before load_data.py.

CREATE DATABASE IF NOT EXISTS platos_pizza
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE platos_pizza;

-- -----------------------------------------------------------------------------
-- Dimension: dim_pizza
-- One row per unique pizza_id (combination of pizza type and size).
-- Stores the price at the time of the original dataset — treated as static
-- for this analysis since no price change history exists in the source data.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_pizza (
    pizza_id      VARCHAR(50)    NOT NULL,
    pizza_type_id VARCHAR(50)    NOT NULL,
    size          VARCHAR(5)     NOT NULL,   -- S, M, L, XL, XXL
    price         DECIMAL(6, 2)  NOT NULL,
    CONSTRAINT pk_dim_pizza PRIMARY KEY (pizza_id)
);

-- -----------------------------------------------------------------------------
-- Dimension: dim_pizza_type
-- One row per unique pizza type (recipe).
-- Ingredients stored as a comma-separated string matching the source format.
-- This is acceptable here because ingredient-level analysis is not a project
-- objective; if it were, ingredients would be normalised into a separate table.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_pizza_type (
    pizza_type_id VARCHAR(50)   NOT NULL,
    name          VARCHAR(100)  NOT NULL,
    category      VARCHAR(50)   NOT NULL,   -- Classic, Supreme, Veggie, Chicken
    ingredients   TEXT          NOT NULL,
    CONSTRAINT pk_dim_pizza_type PRIMARY KEY (pizza_type_id)
);

-- -----------------------------------------------------------------------------
-- Dimension: dim_date
-- One row per unique date present in the orders data.
-- Attributes pre-computed at load time so analytical queries never need to
-- call date functions inline.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id       DATE         NOT NULL,
    year          SMALLINT     NOT NULL,
    quarter       TINYINT      NOT NULL,   -- 1 to 4
    month         TINYINT      NOT NULL,   -- 1 to 12
    month_name    VARCHAR(10)  NOT NULL,   -- January ... December
    week          TINYINT      NOT NULL,   -- ISO week number 1 to 53
    day_of_month  TINYINT      NOT NULL,   -- 1 to 31
    day_of_week   TINYINT      NOT NULL,   -- 1 = Sunday, 7 = Saturday (MySQL default)
    day_name      VARCHAR(10)  NOT NULL,   -- Sunday ... Saturday
    is_weekend    TINYINT(1)   NOT NULL,   -- 1 if Saturday or Sunday, else 0
    CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
);

-- -----------------------------------------------------------------------------
-- Dimension: dim_time
-- One row per unique time value present in the orders data.
-- meal_period groups hours into operationally meaningful shifts, which maps
-- directly to how a restaurant manager thinks about staffing and peak demand.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time (
    time_id      TIME         NOT NULL,
    hour         TINYINT      NOT NULL,   -- 0 to 23
    minute       TINYINT      NOT NULL,   -- 0 to 59
    meal_period  VARCHAR(15)  NOT NULL,   -- Lunch, Afternoon, Dinner, Late Night
    CONSTRAINT pk_dim_time PRIMARY KEY (time_id)
);

-- -----------------------------------------------------------------------------
-- Fact: fact_orders
-- One row per pizza line item (one order can contain multiple line items).
-- Foreign keys reference all four dimension tables.
-- quantity and unit_price are stored here because they are the measurable
-- facts — they can change per transaction even for the same pizza.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_details_id  INT            NOT NULL,
    order_id          INT            NOT NULL,
    date_id           DATE           NOT NULL,
    time_id           TIME           NOT NULL,
    pizza_id          VARCHAR(50)    NOT NULL,
    pizza_type_id     VARCHAR(50)    NOT NULL,
    quantity          TINYINT        NOT NULL,
    unit_price        DECIMAL(6, 2)  NOT NULL,
    total_price       DECIMAL(8, 2)  NOT NULL,   -- quantity * unit_price, pre-computed
    CONSTRAINT pk_fact_orders     PRIMARY KEY (order_details_id),
    CONSTRAINT fk_fact_date       FOREIGN KEY (date_id)        REFERENCES dim_date(date_id),
    CONSTRAINT fk_fact_time       FOREIGN KEY (time_id)        REFERENCES dim_time(time_id),
    CONSTRAINT fk_fact_pizza      FOREIGN KEY (pizza_id)       REFERENCES dim_pizza(pizza_id),
    CONSTRAINT fk_fact_pizza_type FOREIGN KEY (pizza_type_id)  REFERENCES dim_pizza_type(pizza_type_id)
);