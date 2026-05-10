# Restaurant Analytics Engine

A full-stack analytics solution built on Plato's Pizza — a publicly available restaurant dataset from Maven Analytics. The project covers a production-style MySQL data warehouse, operational analysis, demand forecasting, an experimental A/B test framework, and a non-technical management report.

Built as part of a data analytics portfolio. Every decision in the code, SQL, and documentation includes a reason, not just a description.

---

## Project narrative

The goal was to answer three questions a restaurant owner actually cares about:

1. When are customers coming, and is the business staffed correctly for it?
2. Which menu items are driving revenue, and which are not earning their place?
3. If we changed a price or launched a promotion, would we be able to detect whether it worked?

The analysis moves from raw CSV files through a structured data warehouse, operational findings, a forward-looking demand forecast, and a retrospective pricing experiment — all the way to a formatted management report a non-technical stakeholder can act on.

---

## Stack

| Layer | Tool |
|---|---|
| Warehouse | MySQL 8.0 |
| Analytical queries | SQL |
| Statistical analysis and visualisation | R (dplyr, ggplot2, pwr, effsize) |
| Demand forecasting | Python (Prophet) |
| Report generation | Python (openpyxl) |
| Testing | Python (pytest) |
| Environment | conda |

---

## Warehouse architecture

Raw CSVs are loaded once into a MySQL star schema. From that point forward, nothing touches the raw files. All analysis queries the warehouse.

```
fact_orders          — one row per pizza ordered (48,620 rows)
dim_pizza            — pizza SKUs with size and price
dim_pizza_type       — pizza names, categories, and ingredients
dim_date             — date dimension with week, day, and weekend flag
dim_time             — time dimension with hour, minute, and meal period
```

The warehouse is built and populated by `src/load_data.py`. The schema is defined in `sql/01_schema.sql`.

---

## Repository structure

```
platos-pizza/
├── data/
│   ├── raw/              # Original CSVs — never modified after load
│   └── processed/        # Intermediate outputs where applicable
├── notebooks/            # Development and verification notebooks
├── sql/                  # Schema and analytical queries
├── src/                  # Production scripts
│   └── dashboard/        # Streamlit dashboard (planned)
├── reports/
│   ├── charts/           # All chart outputs from R and Python
│   └── staffing_forecast.csv
│   └── ab_test_summary.csv
│   └── platos_pizza_management_report.xlsx
├── tests/                # Unit tests
├── environment.yml
└── README.md
```

---

## Setup

**Requirements:** conda, MySQL 8.0

**1. Clone the repository**

```bash
git clone https://github.com/selete-tetteh/restaurant-analytics.git
cd restaurant-analytics
```

**2. Create the environment**

```bash
conda env create -f environment.yml
conda activate platos-pizza
```

**3. Configure credentials**

Copy `.env.example` to `.env` and fill in your MySQL connection details:

```bash
cp .env.example .env
```

```
DB_HOST=localhost
DB_PORT=3306
DB_NAME=platos_pizza
DB_USER=your_username
DB_PASSWORD=your_password
```

**4. Build the warehouse**

Create the database in MySQL first:

```sql
CREATE DATABASE platos_pizza;
```

Then run the schema and load script:

```bash
mysql -u your_username -p platos_pizza < sql/01_schema.sql
python src/load_data.py
```

**5. macOS note — RMariaDB**

R's database connector requires a symlink to the MySQL client library. If RMariaDB fails to connect, run:

```bash
ln -s /usr/local/mysql/lib/libmysqlclient.21.dylib \
      ~/miniconda3/envs/platos-pizza/lib/libmysqlclient.21.dylib
```

---

## Running the analysis

**Notebooks** — open in VS Code with the R and Python extensions installed. Run cells in order. Clear all outputs before committing (`Restart Kernel then Clear All Outputs`).

| Notebook | Purpose |
|---|---|
| `01_warehouse_and_audit.ipynb` | Schema verification and data quality checks |
| `02_operational_analysis.ipynb` | Peak analysis, menu performance, revenue leakage |
| `03_forecasting.ipynb` | Basket analysis, STL decomposition, Prophet forecast |
| `04_experiments.ipynb` | A/B test framework, power analysis, retrospective pricing test |

**Production scripts** — run from the project root.

```bash
# Regenerate all operational charts
Rscript src/operational_analysis.R

# Regenerate the demand forecast and staffing CSV
python src/forecasting.py

# Regenerate the management report
python src/generate_report.py
```

**SQL files**

| File | Purpose |
|---|---|
| `sql/01_schema.sql` | Star schema definition and raw data load |
| `sql/02_operational_queries.sql` | Peak, menu, and revenue leakage queries |
| `sql/03_advanced_queries.sql` | Basket analysis architecture note and seasonal decomposition base data |

---

## Key findings

**Operations**

Friday is the highest-volume day (8,242 orders across the year). Lunch is the busiest meal period by both volume (7,678 orders) and average order value ($41.95). Sunday is the lowest-volume day, 16% below the weekly average.

**Menu performance**

Thai Chicken Pizza is the highest revenue item but only fifth by volume — it is under-promoted relative to its commercial value. Pepperoni is fourth by volume but eleventh by revenue, indicating low price or low attachment to extras. Large pizzas represent 38% of volume but 46% of revenue. A 10% conversion of medium orders to large would generate an additional $6,276 per year.

**Basket analysis**

The strongest complementary pair is Italian Vegetables and Pepperoni Mushroom Peppers, with a lift score of 1.500 — customers who order one are 50% more likely than average to also order the other. All top 15 pairs have lift above 1.0.

**Forecasting**

The Prophet model captures weekly seasonality reliably — Friday peak, Sunday trough, approximately ±10 orders around the daily baseline. The trend component is unreliable on single-year data and is explicitly noted as such in both the notebook and the forecast chart. Weekly seasonality is the operationally useful output.

**Pricing experiment**

A retrospective A/B test comparing medium pizza volume in H1 versus H2 of 2015 found no statistically significant difference (p = 0.931, Cohen's d = 0.009). Medium pizza demand is stable across the year, which is consistent with a degree of price inelasticity. A 5–10% price increase on medium pizzas is unlikely to produce a meaningful volume reduction based on the available evidence. The test was underpowered (achieved power approximately 50%) — a properly designed live test would require 394 days per group.

---

## Testing

```bash
pytest tests/ -v
```

18 tests covering the forecasting pipeline: staffing calculation correctness, holiday definitions, training cutoff filtering, and a report generation smoke test.

---

## Limitations

The dataset covers a single calendar year (2015) with one location. This constrains what is statistically detectable — particularly for forecasting (trend is unreliable) and for the A/B test (insufficient sample size for small effects). All limitations are documented explicitly in the relevant notebooks rather than obscured.

---

## Author

Selete Akpotosu-Nartey — [github.com/selete-tetteh](https://github.com/selete-tetteh)
