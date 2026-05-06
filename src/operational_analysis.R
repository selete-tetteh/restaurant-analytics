# =============================================================================
# File:    operational_analysis.R
# Project: Restaurant Analytics Engine
# Purpose: Production script for operational analysis. Reproduces all
#          visualisations and statistical tests from
#          notebooks/02_operational_analysis.ipynb.
#
#          Run this script end-to-end to regenerate all nine charts and
#          print all statistical test results. Output is written to
#          reports/charts/. The directory is created automatically if absent.
#
#          This script queries the platos_pizza warehouse directly.
#          The warehouse must be running and populated before execution.
#          Run sql/01_schema.sql and src/load_data.py first if rebuilding.
#
# Usage:
#   Rscript src/operational_analysis.R
#
# Dependencies: DBI, RMariaDB, dplyr, ggplot2, scales, tidyr, dotenv
#               All listed in environment.yml.
# =============================================================================


# -----------------------------------------------------------------------------
# 1. Setup
# -----------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(RMariaDB)
  library(dplyr)
  library(ggplot2)
  library(scales)
  library(tidyr)
  library(dotenv)
})

# Load credentials from .env at project root.
# The .env file is never committed to Git. See .env.example for the required
# variable names.
load_dot_env(file = here::here(".env"))

# Resolve the output directory relative to project root.
# Using here::here() means this script produces consistent output paths
# regardless of the working directory it is called from.
OUTPUT_DIR <- file.path(here::here(), "reports", "charts")
if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
  message("Created output directory: ", OUTPUT_DIR)
}


# -----------------------------------------------------------------------------
# 2. Database connection
# -----------------------------------------------------------------------------

# bigint = "integer" forces MySQL COUNT() results from integer64 to standard
# R integers. integer64 is incompatible with scales::comma() and other
# formatting functions. Applying this at the connection level fixes it
# globally rather than requiring per-column casting in every query.
con <- dbConnect(
  RMariaDB::MariaDB(),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  dbname   = Sys.getenv("DB_NAME"),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  bigint   = "integer"
)

on.exit(dbDisconnect(con), add = TRUE)
message("Connected to platos_pizza warehouse.")


# -----------------------------------------------------------------------------
# 3. Shared theme and palette
# -----------------------------------------------------------------------------

# A single theme applied to every chart in this script.
# Defined once here so any style change propagates to all nine charts
# without hunting through the file.
theme_platos <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold", size = 14),
      plot.subtitle    = element_text(size = 10, colour = "#555555"),
      plot.caption     = element_text(size = 8, colour = "#888888"),
      panel.grid.minor = element_blank(),
      axis.title       = element_text(size = 10),
      legend.position  = "bottom"
    )
}

# Meal period palette — consistent colour assignment across all meal period
# charts. Named vector so ggplot can match colours to factor levels
# regardless of order.
meal_period_palette <- c(
  "Morning"    = "#AAAAAA",
  "Lunch"      = "#E6A817",
  "Afternoon"  = "#5BB8F5",
  "Dinner"     = "#2EAF7D",
  "Late Night" = "#F0E442"
)

# Day of week palette — seven distinct colours, one per day.
day_palette <- c(
  "Monday"    = "#E6A817",
  "Tuesday"   = "#5BB8F5",
  "Wednesday" = "#2EAF7D",
  "Thursday"  = "#F0E442",
  "Friday"    = "#1F77B4",
  "Saturday"  = "#D95F02",
  "Sunday"    = "#CC79A7"
)

# Category palette for menu and bubble charts.
category_palette <- c(
  "Chicken" = "#E6A817",
  "Classic" = "#5BB8F5",
  "Supreme" = "#2EAF7D",
  "Veggie"  = "#F0E442"
)


# -----------------------------------------------------------------------------
# 4. Data queries
# -----------------------------------------------------------------------------

message("Querying warehouse...")

# Hourly order volume (hours 9-10 excluded from charts, <0.05% of volume).
hourly <- dbGetQuery(con, "
  SELECT
    dt.hour,
    COUNT(DISTINCT fo.order_id)         AS total_orders,
    SUM(fo.quantity)                    AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)       AS total_revenue,
    ROUND(SUM(fo.total_price) /
          SUM(fo.quantity), 2)          AS avg_line_item_value
  FROM fact_orders fo
  JOIN dim_time dt ON fo.time_id = dt.time_id
  GROUP BY dt.hour
  ORDER BY dt.hour
")

# Meal period summary.
meal_period <- dbGetQuery(con, "
  SELECT
    dt.meal_period,
    COUNT(DISTINCT fo.order_id)                                   AS total_orders,
    SUM(fo.quantity)                                              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                                 AS total_revenue,
    ROUND(SUM(fo.total_price) / COUNT(DISTINCT fo.order_id), 2)   AS avg_order_value
  FROM fact_orders fo
  JOIN dim_time dt ON fo.time_id = dt.time_id
  GROUP BY dt.meal_period
  ORDER BY FIELD(dt.meal_period, 'Morning', 'Lunch', 'Afternoon', 'Dinner', 'Late Night')
")

# Day of week summary.
day_of_week <- dbGetQuery(con, "
  SELECT
    dd.day_name,
    dd.day_of_week,
    COUNT(DISTINCT fo.order_id)                                   AS total_orders,
    SUM(fo.quantity)                                              AS total_pizzas,
    ROUND(SUM(fo.total_price), 2)                                 AS total_revenue,
    ROUND(SUM(fo.total_price) / COUNT(DISTINCT fo.order_id), 2)   AS avg_order_value
  FROM fact_orders fo
  JOIN dim_date dd ON fo.date_id = dd.date_id
  GROUP BY dd.day_name, dd.day_of_week
  ORDER BY dd.day_of_week
")

# Day-hour heatmap grid.
heatmap_grid <- dbGetQuery(con, "
  SELECT
    dd.day_name,
    dd.day_of_week,
    dt.hour,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.quantity)            AS total_pizzas,
    ROUND(SUM(fo.total_price), 2) AS total_revenue
  FROM fact_orders fo
  JOIN dim_date dd ON fo.date_id = dd.date_id
  JOIN dim_time dt ON fo.time_id = dt.time_id
  GROUP BY dd.day_name, dd.day_of_week, dt.hour
  ORDER BY dd.day_of_week, dt.hour
")

# Menu performance — all items with volume and revenue ranks.
menu_items <- dbGetQuery(con, "
  SELECT
    dpt.name                              AS pizza_name,
    dpt.category,
    SUM(fo.quantity)                      AS total_quantity,
    ROUND(SUM(fo.total_price), 2)         AS total_revenue,
    ROUND(AVG(fo.unit_price), 2)          AS avg_unit_price,
    RANK() OVER (ORDER BY SUM(fo.quantity) DESC) AS volume_rank,
    RANK() OVER (ORDER BY SUM(fo.total_price) DESC) AS revenue_rank
  FROM fact_orders fo
  JOIN dim_pizza_type dpt ON fo.pizza_type_id = dpt.pizza_type_id
  GROUP BY dpt.name, dpt.category
  ORDER BY revenue_rank
")

# Category efficiency summary.
category_summary <- dbGetQuery(con, "
  SELECT
    dpt.category,
    COUNT(DISTINCT dpt.pizza_type_id)             AS menu_items,
    SUM(fo.quantity)                              AS total_quantity,
    ROUND(SUM(fo.total_price), 2)                 AS total_revenue,
    ROUND(SUM(fo.total_price) / SUM(fo.quantity), 2) AS avg_revenue_per_pizza,
    ROUND(SUM(fo.total_price) /
          (SELECT SUM(total_price) FROM fact_orders) * 100, 1) AS revenue_share_pct
  FROM fact_orders fo
  JOIN dim_pizza_type dpt ON fo.pizza_type_id = dpt.pizza_type_id
  GROUP BY dpt.category
  ORDER BY total_revenue DESC
")

# Size performance.
size_summary <- dbGetQuery(con, "
  SELECT
    dp.size,
    SUM(fo.quantity)                      AS total_quantity,
    ROUND(SUM(fo.total_price), 2)         AS total_revenue,
    ROUND(AVG(fo.unit_price), 2)          AS avg_unit_price,
    ROUND(SUM(fo.quantity) /
          (SELECT SUM(quantity) FROM fact_orders) * 100, 1) AS volume_share_pct,
    ROUND(SUM(fo.total_price) /
          (SELECT SUM(total_price) FROM fact_orders) * 100, 1) AS revenue_share_pct
  FROM fact_orders fo
  JOIN dim_pizza dp ON fo.pizza_id = dp.pizza_id
  GROUP BY dp.size
  ORDER BY FIELD(dp.size, 'S', 'M', 'L', 'XL', 'XXL')
")

# Upsell sensitivity — revenue gain at four conversion scenarios.
# Medium-to-Large price gap per pizza type, applied across four conversion rates.
upsell_base <- dbGetQuery(con, "
  SELECT
    dpt.name                                AS pizza_name,
    dpt.category,
    MAX(CASE WHEN dp.size = 'M' THEN dp.price END) AS medium_price,
    MAX(CASE WHEN dp.size = 'L' THEN dp.price END) AS large_price,
    SUM(CASE WHEN dp.size = 'M' THEN fo.quantity ELSE 0 END) AS medium_quantity_sold
  FROM fact_orders fo
  JOIN dim_pizza dp ON fo.pizza_id = dp.pizza_id
  JOIN dim_pizza_type dpt ON fo.pizza_type_id = dpt.pizza_type_id
  GROUP BY dpt.name, dpt.category
  HAVING medium_price IS NOT NULL AND large_price IS NOT NULL
")

message("All queries complete.")


# -----------------------------------------------------------------------------
# 5. Statistical significance tests
# -----------------------------------------------------------------------------

# Daily order counts are right-skewed. ANOVA assumes a normal distribution
# and is inappropriate here. Kruskal-Wallis is the correct non-parametric
# alternative — it tests whether group medians differ without assuming
# any particular distribution.

# Build a daily order count table for the tests.
daily_counts <- dbGetQuery(con, "
  SELECT
    fo.date_id,
    dd.day_name,
    dt.meal_period,
    COUNT(DISTINCT fo.order_id) AS daily_orders
  FROM fact_orders fo
  JOIN dim_date dd ON fo.date_id = dd.date_id
  JOIN dim_time dt ON fo.time_id = dt.time_id
  WHERE dt.meal_period != 'Morning'
  GROUP BY fo.date_id, dd.day_name, dt.meal_period
")

kw_meal   <- kruskal.test(daily_orders ~ meal_period, data = daily_counts)
kw_day    <- kruskal.test(daily_orders ~ day_name,    data = daily_counts)

message("\nKruskal-Wallis test — meal period vs daily orders:")
print(kw_meal)
cat(sprintf(
  "Interpretation:\np = %.2g %s 0.05.\n%s\n\n",
  kw_meal$p.value,
  ifelse(kw_meal$p.value < 0.05, "<", ">="),
  ifelse(
    kw_meal$p.value < 0.05,
    "The differences in order volume between meal periods are statistically significant.\nThese are genuine trading patterns, not random variation.",
    "No statistically significant difference detected between meal periods."
  )
))

message("Kruskal-Wallis test — day of week vs daily orders:")
print(kw_day)
cat(sprintf(
  "Interpretation:\np = %.2g %s 0.05.\n%s\n\n",
  kw_day$p.value,
  ifelse(kw_day$p.value < 0.05, "<", ">="),
  ifelse(
    kw_day$p.value < 0.05,
    "Day of week has a statistically significant effect on order volume.",
    "No statistically significant difference detected between days of week."
  )
))


# -----------------------------------------------------------------------------
# 6. Chart helpers
# -----------------------------------------------------------------------------

save_chart <- function(plot, filename, width = 8, height = 5) {
  path <- file.path(OUTPUT_DIR, filename)
  ggsave(path, plot = plot, width = width, height = height, dpi = 150)
  message("Saved: ", path)
}


# -----------------------------------------------------------------------------
# 7. Charts — Section 2: Peak analysis
# -----------------------------------------------------------------------------

# Chart 1: Order volume by hour of day.
# Hours 9-10 excluded — fewer than 10 orders each across the full year
# (<0.05% of annual volume). Including them compresses the y-axis and
# visually misrepresents the operating day.
hourly_plot <- hourly |>
  filter(hour >= 11) |>
  mutate(hour_label = sprintf("%02d:00", hour)) |>
  ggplot(aes(x = factor(hour_label), y = total_orders)) +
  geom_col(fill = "#5BB8F5", width = 0.7) +
  geom_text(
    aes(label = scales::comma(total_orders)),
    vjust = -0.4, size = 3
  ) +
  scale_y_continuous(labels = scales::comma, expand = expansion(mult = c(0, 0.1))) +
  labs(
    title    = "Order Volume by Hour of Day",
    subtitle = "Full year 2015. Hours 9-10 excluded (<0.05% of annual volume).",
    x        = "Hour",
    y        = "Total Orders",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(hourly_plot, "01_hourly_volume.png")


# Chart 2: Order volume by meal period.
# Morning excluded from this chart. It represents 9 orders across the full
# year and would compress the y-axis without adding analytical value.
meal_period_levels <- c("Lunch", "Afternoon", "Dinner", "Late Night")

meal_vol_plot <- meal_period |>
  filter(meal_period != "Morning") |>
  mutate(meal_period = factor(meal_period, levels = meal_period_levels)) |>
  ggplot(aes(x = meal_period, y = total_orders, fill = meal_period)) +
  geom_col(width = 0.65) +
  geom_text(aes(label = scales::comma(total_orders)), vjust = -0.4, size = 3.5) +
  scale_fill_manual(values = meal_period_palette, guide = "none") +
  scale_y_continuous(labels = scales::comma, expand = expansion(mult = c(0, 0.1))) +
  labs(
    title    = "Order Volume by Meal Period",
    subtitle = sprintf(
      "Kruskal-Wallis p = %.2g \u2014 differences are statistically significant",
      kw_meal$p.value
    ),
    x        = NULL,
    y        = "Total Orders",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(meal_vol_plot, "02_meal_period_volume.png")


# Chart 3: Average order value by meal period.
# Separating volume and average order value into two charts is intentional.
# A period with high volume is not automatically the highest-value period.
# Lunch leads on both here, but that is a finding — not an assumption.
meal_aov_plot <- meal_period |>
  filter(meal_period != "Morning") |>
  mutate(meal_period = factor(meal_period, levels = meal_period_levels)) |>
  ggplot(aes(x = meal_period, y = avg_order_value, fill = meal_period)) +
  geom_col(width = 0.65) +
  geom_text(
    aes(label = scales::dollar(avg_order_value)),
    vjust = -0.4, size = 3.5
  ) +
  scale_fill_manual(values = meal_period_palette, guide = "none") +
  scale_y_continuous(labels = scales::dollar, expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Average Order Value by Meal Period",
    subtitle = "Lunch generates the highest average order value, not Dinner.",
    x        = NULL,
    y        = "Average Order Value (USD)",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(meal_aov_plot, "03_meal_period_avg_order_value.png")


# Chart 4: Order volume by day of week.
day_vol_plot <- day_of_week |>
  mutate(day_name = factor(day_name, levels = c(
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
  ))) |>
  ggplot(aes(x = day_name, y = total_orders, fill = day_name)) +
  geom_col(width = 0.7) +
  geom_text(aes(label = scales::comma(total_orders)), vjust = -0.4, size = 3.5) +
  scale_fill_manual(values = day_palette, guide = "none") +
  scale_y_continuous(labels = scales::comma, expand = expansion(mult = c(0, 0.1))) +
  labs(
    title    = "Order Volume by Day of Week",
    subtitle = sprintf(
      "Kruskal-Wallis p = %.2g \u2014 differences are statistically significant",
      kw_day$p.value
    ),
    x        = NULL,
    y        = "Total Orders",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(day_vol_plot, "04_day_of_week_volume.png")


# Chart 5: Day-hour heatmap.
# This chart reveals interaction effects that the individual day and hour
# charts cannot show. Friday evening sustaining high volume to hour 22
# is only visible here.
heatmap_plot <- heatmap_grid |>
  filter(hour >= 11) |>
  mutate(
    day_name   = factor(day_name, levels = rev(c(
      "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ))),
    hour_label = sprintf("%02d:00", hour)
  ) |>
  ggplot(aes(x = hour_label, y = day_name, fill = total_orders)) +
  geom_tile(colour = "white", linewidth = 0.4) +
  geom_text(aes(label = total_orders), size = 2.8, colour = "white") +
  scale_fill_gradient(low = "#DEEBF7", high = "#08306B", name = "Orders") +
  labs(
    title    = "Order Volume Heatmap \u2014 Day of Week by Hour",
    subtitle = "Darker cells indicate higher order concentration.\nFriday evening sustains peak volume later than any other day.",
    x        = "Hour",
    y        = NULL,
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos() +
  theme(
    axis.text.x    = element_text(size = 8),
    panel.grid     = element_blank(),
    legend.key.width = unit(1.5, "cm")
  )

save_chart(heatmap_plot, "05_day_hour_heatmap.png", width = 10, height = 5)


# -----------------------------------------------------------------------------
# 8. Charts — Section 3: Menu performance
# -----------------------------------------------------------------------------

# Chart 6: Volume rank vs revenue rank divergence.
# Positive divergence (orange): earns more revenue than its sales volume
#   would suggest — premium positioning or higher unit price.
# Negative divergence (blue): sells in higher volume than it earns revenue —
#   underpriced relative to demand, or a low-margin high-traffic item.
# The divergence value is (volume_rank - revenue_rank). A positive number
# means volume_rank is higher (worse) than revenue_rank, so the item earns
# above its volume weight.
divergence_plot <- menu_items |>
  mutate(
    divergence  = volume_rank - revenue_rank,
    direction   = ifelse(divergence >= 0, "earns above volume weight", "sells above revenue weight"),
    pizza_short = gsub("^The ", "", pizza_name),
    pizza_short = factor(pizza_short, levels = pizza_short[order(divergence)])
  ) |>
  ggplot(aes(x = divergence, y = pizza_short, fill = direction)) +
  geom_col(width = 0.7) +
  geom_vline(xintercept = 0, linewidth = 0.6, colour = "#333333") +
  scale_fill_manual(
    values = c(
      "earns above volume weight"  = "#D95F02",
      "sells above revenue weight" = "#1F77B4"
    ),
    labels = c(
      "earns above volume weight"  = "Orange bars: sells more than it earns",
      "sells above revenue weight" = "Blue bars: earns more than it sells"
    ),
    name = NULL
  ) +
  labs(
    title    = "Volume Rank vs Revenue Rank Divergence",
    subtitle = "Orange bars: sells more than it earns. Blue bars: earns more than volume suggests.",
    x        = "Volume Rank minus Revenue Rank",
    y        = NULL,
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos() +
  theme(
    axis.text.y    = element_text(size = 8),
    legend.position = "bottom"
  )

save_chart(divergence_plot, "06_rank_divergence.png", width = 9, height = 8)


# Chart 7: Category efficiency bubble chart.
# x-axis: number of menu items (menu space consumed)
# y-axis: average revenue per pizza sold (efficiency)
# bubble size: total annual revenue (scale of contribution)
# Upper-left quadrant is most desirable: high efficiency, low menu complexity.
# Manual label positions used because with four points, geom_text_repel
# produces unpredictable placement. Manual nudge is more reliable.
cat_nudge <- data.frame(
  category = c("Chicken", "Classic", "Supreme", "Veggie"),
  nudge_x  = c(-0.25,      0,         0.3,       0.3),
  nudge_y  = c( 0.05,      0.05,      0.05,     -0.05)
)

category_plot_data <- category_summary |>
  left_join(cat_nudge, by = "category")

bubble_plot <- category_plot_data |>
  ggplot(aes(
    x    = menu_items,
    y    = avg_revenue_per_pizza,
    size = total_revenue,
    fill = category
  )) +
  geom_point(shape = 21, alpha = 0.85) +
  geom_text(
    aes(
      label = category,
      x     = menu_items + nudge_x,
      y     = avg_revenue_per_pizza + nudge_y
    ),
    size   = 3.5,
    colour = "#333333",
    inherit.aes = FALSE
  ) +
  scale_fill_manual(values = category_palette, guide = "none") +
  scale_size_continuous(range = c(8, 22), guide = "none") +
  scale_y_continuous(labels = scales::dollar) +
  labs(
    title    = "Category Efficiency: Revenue per Pizza vs Menu Space",
    subtitle = "Bubble size = total annual revenue. Upper-left is most efficient.",
    x        = "Number of Menu Items",
    y        = "Average Revenue per Pizza Sold (USD)",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(bubble_plot, "07_category_efficiency.png")


# -----------------------------------------------------------------------------
# 9. Charts — Section 4: Revenue leakage
# -----------------------------------------------------------------------------

# Chart 8: Volume share vs revenue share by pizza size.
# Large generates 45.9% of revenue from 38.2% of volume — it punches above
# its weight. Small does the opposite. This is the core upsell justification.
size_levels <- c("S", "M", "L", "XL", "XXL")

size_share_plot <- size_summary |>
  mutate(size = factor(size, levels = size_levels)) |>
  select(size, revenue_share_pct, volume_share_pct) |>
  pivot_longer(
    cols      = c(revenue_share_pct, volume_share_pct),
    names_to  = "metric",
    values_to = "share_pct"
  ) |>
  mutate(metric = recode(metric,
    "revenue_share_pct" = "Revenue Share",
    "volume_share_pct"  = "Volume Share"
  )) |>
  ggplot(aes(x = size, y = share_pct / 100, fill = metric)) +
  geom_col(position = position_dodge(width = 0.7), width = 0.6) +
  geom_text(
    aes(label = scales::percent(share_pct / 100, accuracy = 0.1)),
    position = position_dodge(width = 0.7),
    vjust = -0.4, size = 3
  ) +
  scale_fill_manual(
    values = c("Revenue Share" = "#5BB8F5", "Volume Share" = "#E6A817"),
    name   = NULL
  ) +
  scale_y_continuous(labels = scales::percent, expand = expansion(mult = c(0, 0.12))) +
  labs(
    title    = "Volume Share vs Revenue Share by Pizza Size",
    subtitle = "Large converts 38% of volume into 46% of revenue. Small does the opposite.",
    x        = "Size",
    y        = "Share of Annual Total (%)",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(size_share_plot, "08_size_volume_revenue_share.png")


# Chart 9: Upsell revenue sensitivity.
# Shows the financial value of Medium-to-Large upsell at four conversion
# scenarios (5%, 10%, 15%, 20%). The point is not to predict which scenario
# will occur — it is to show the revenue range so management can set a
# realistic target. This is a sensitivity analysis, not a forecast.
upsell_scenarios <- upsell_base |>
  mutate(price_gap = large_price - medium_price) |>
  summarise(
    base_upsell_revenue = sum(price_gap * medium_quantity_sold, na.rm = TRUE)
  ) |>
  mutate(
    `5%`  = base_upsell_revenue * 0.05,
    `10%` = base_upsell_revenue * 0.10,
    `15%` = base_upsell_revenue * 0.15,
    `20%` = base_upsell_revenue * 0.20
  ) |>
  pivot_longer(
    cols      = c(`5%`, `10%`, `15%`, `20%`),
    names_to  = "conversion_rate",
    values_to = "total_upsell_revenue"
  ) |>
  mutate(conversion_rate = factor(conversion_rate, levels = c("5%", "10%", "15%", "20%")))

upsell_colours <- c(
  "5%"  = "#E6A817",
  "10%" = "#5BB8F5",
  "15%" = "#2EAF7D",
  "20%" = "#F0E442"
)

upsell_plot <- upsell_scenarios |>
  ggplot(aes(x = conversion_rate, y = total_upsell_revenue, fill = conversion_rate)) +
  geom_col(width = 0.6) +
  geom_text(
    aes(label = scales::dollar(round(total_upsell_revenue))),
    vjust = -0.4, size = 3.5
  ) +
  scale_fill_manual(values = upsell_colours, guide = "none") +
  scale_y_continuous(labels = scales::dollar, expand = expansion(mult = c(0, 0.12))) +
  labs(
    title    = "Medium-to-Large Upsell Revenue Sensitivity",
    subtitle = "Annual revenue gain if X% of Medium orders convert to Large.",
    x        = "Conversion Rate",
    y        = "Additional Annual Revenue (USD)",
    caption  = "Source: platos_pizza warehouse"
  ) +
  theme_platos()

save_chart(upsell_plot, "09_upsell_sensitivity.png")


# -----------------------------------------------------------------------------
# 10. Completion summary
# -----------------------------------------------------------------------------

message("\nAll charts saved to: ", OUTPUT_DIR)
message("operational_analysis.R complete.")
