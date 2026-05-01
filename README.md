# Restaurant Analytics Engine

A full-stack analytics project built on the Plato's Pizza dataset from Maven Analytics. The project covers data warehouse design, operational analysis, demand forecasting, and an experimental A/B testing framework.

## Project Narrative

The goal is not to describe what the data contains — it is to answer questions the business should be asking. Where is revenue being lost? Which menu items are underperforming relative to the space they occupy on the menu? What does demand look like next week, and how should that shape staffing decisions?

Every finding in this project includes a financial implication and a specific recommendation.

## Tools

| Layer | Tool |
|---|---|
| Warehouse | MySQL |
| Statistical analysis and visualisation | R |
| Forecasting | Python (Prophet) |
| Dashboard | Python (Streamlit) |
| Notebooks | Jupyter (.ipynb), R Markdown (.Rmd) |

## Architecture

Raw CSV files are loaded once into a MySQL star schema. From that point forward, nothing touches the raw files. All analysis queries the warehouse. This enforces a clean separation between source data and analytical work.

**Star schema:**
- Fact table: `fact_orders` — one row per pizza ordered
- Dimension tables: `dim_pizza`, `dim_pizza_type`, `dim_date`, `dim_time`

## Repository Structure
```
platos-pizza/ 
│── data/ 
│ ├── raw/ # Original source files. Never modified after download. 
│ └── processed/ # Outputs from cleaning and transformation steps. 
│── sql/ # Schema setup and all analytical queries. 
│── notebooks/ # Development and verification notebooks. 
│── src/ # Production scripts. 
│ └── dashboard/ # Streamlit app. 
│── reports/ # Excel management report and generated outputs. 
│── tests/ # Unit tests. 
│── environment.yml # Full environment specification for reproducibility. └── README.md
```

## Setup

**Requirements:** conda, MySQL 8.0+

```bash
# 1. Clone the repository
git clone https://github.com/selete-tetteh/platos-pizza.git
cd platos-pizza

# 2. Create and activate the environment
conda env create -f environment.yml
conda activate platos-pizza

# 3. Add your database credentials
cp .env.example .env
# Edit .env with your MySQL username and password

# 4. Place the raw data files in data/raw/
# Download from: https://www.mavenanalytics.io/data-playground

# 5. Run the warehouse setup script
mysql -u root -p < sql/01_schema.sql
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `01_warehouse_and_audit.ipynb` | Schema verification and data quality checks |
| `02_operational_analysis.ipynb` | Peak analysis, menu performance, revenue leakage |
| `03_forecasting_and_experiments.ipynb` | Prophet forecast, A/B framework, power analysis |

## Status

In development.
