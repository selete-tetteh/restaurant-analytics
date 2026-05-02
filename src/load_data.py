"""
load_data.py
Project:  Restaurant Analytics Engine
Purpose:  Load raw CSV data into the platos_pizza star schema warehouse.

This script is the single source of truth for how raw data moves into the
warehouse. It runs once after 01_schema.sql has been executed. If the
warehouse needs to be rebuilt from scratch, truncate all tables and re-run
this script.

Why Python for this step rather than MySQL LOAD DATA INFILE?
  LOAD DATA INFILE loads flat rows directly — it cannot compute derived
  columns (total_price), split source tables into dimension/fact structure,
  or generate the date and time dimension rows. Python gives us that control
  cleanly before anything touches the database.

Dependencies: sqlalchemy, pymysql, pandas (all in environment.yml)
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Load database credentials from .env file.
# The .env file is never committed to Git — see .gitignore and .env.example.
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "platos_pizza")

# Resolve paths relative to the project root, not the working directory.
# This means the script runs correctly regardless of where it is called from.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = PROJECT_ROOT / "data" / "raw"


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_engine():
    """
    Create and return a SQLAlchemy engine connected to the platos_pizza database.

    quote_plus is used to percent-encode the password before embedding it in
    the connection string. This handles special characters such as @, $, and #
    that would otherwise be misinterpreted as URL syntax delimiters.

    Why SQLAlchemy instead of a raw pymysql connection?
    SQLAlchemy's engine manages connection pooling and lets us use pandas
    .to_sql() for bulk inserts, which is significantly faster than inserting
    row by row in a loop.
    """
    password = quote_plus(os.getenv("DB_PASSWORD"))

    connection_string = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{password}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(connection_string, echo=False)


# ---------------------------------------------------------------------------
# Load functions — one per dimension, fact last
# ---------------------------------------------------------------------------

def load_dim_pizza_type(engine: object) -> pd.DataFrame:
    """
    Load dim_pizza_type from pizza_types.csv.
    Returns the DataFrame so load_dim_pizza can reference pizza_type_id.
    """
    df = pd.read_csv(RAW_DATA / "pizza_types.csv", encoding="latin-1")

    df = df.rename(columns={
        "pizza_type_id": "pizza_type_id",
        "name":          "name",
        "category":      "category",
        "ingredients":   "ingredients"
    })

    df.to_sql("dim_pizza_type", con=engine, if_exists="append", index=False)
    print(f"  dim_pizza_type: {len(df)} rows loaded.")
    return df


def load_dim_pizza(engine: object) -> pd.DataFrame:
    """
    Load dim_pizza from pizzas.csv.
    Returns the DataFrame so fact_orders can reference pizza_id.
    """
    df = pd.read_csv(RAW_DATA / "pizzas.csv")

    df = df.rename(columns={
        "pizza_id":      "pizza_id",
        "pizza_type_id": "pizza_type_id",
        "size":          "size",
        "price":         "price"
    })

    df.to_sql("dim_pizza", con=engine, if_exists="append", index=False)
    print(f"  dim_pizza:      {len(df)} rows loaded.")
    return df


def load_dim_date(engine: object, dates: pd.Series) -> None:
    """
    Build and load dim_date from the unique dates present in orders.csv.

    The date dimension is derived from the data, not read from a separate file.
    We extract every unique order date and compute all attributes from it.
    This guarantees the dimension contains exactly the dates that exist in
    the fact table — no more, no less.
    """
    unique_dates = pd.DataFrame({"date_id": pd.to_datetime(dates.unique())})
    unique_dates = unique_dates.sort_values("date_id").reset_index(drop=True)

    unique_dates["year"]         = unique_dates["date_id"].dt.year
    unique_dates["quarter"]      = unique_dates["date_id"].dt.quarter
    unique_dates["month"]        = unique_dates["date_id"].dt.month
    unique_dates["month_name"]   = unique_dates["date_id"].dt.strftime("%B")
    unique_dates["week"]         = unique_dates["date_id"].dt.isocalendar().week.astype(int)
    unique_dates["day_of_month"] = unique_dates["date_id"].dt.day
    unique_dates["day_of_week"]  = unique_dates["date_id"].dt.dayofweek + 2  # align to MySQL: 1=Sun
    unique_dates["day_name"]     = unique_dates["date_id"].dt.strftime("%A")
    unique_dates["is_weekend"]   = unique_dates["day_name"].isin(["Saturday", "Sunday"]).astype(int)

    # Store as plain date, not datetime, to match the DATE column type in MySQL.
    unique_dates["date_id"] = unique_dates["date_id"].dt.date

    unique_dates.to_sql("dim_date", con=engine, if_exists="append", index=False)
    print(f"  dim_date:       {len(unique_dates)} rows loaded.")


def load_dim_time(engine: object, times: pd.Series) -> None:
    """
    Build and load dim_time from the unique times present in orders.csv.

    meal_period boundaries reflect standard restaurant shift definitions:
      Lunch       : 11:00 to 14:59
      Afternoon   : 15:00 to 17:59
      Dinner      : 18:00 to 21:59
      Late Night  : 22:00 onwards and before 11:00

    These boundaries are a deliberate business decision, not arbitrary cuts.
    They will be stated explicitly in Notebook 01 and can be revised if the
    operational analysis suggests different natural breakpoints in the data.
    """
    unique_times = pd.DataFrame({"time_id": pd.to_datetime(times.unique(), format="%H:%M:%S")})
    unique_times = unique_times.sort_values("time_id").reset_index(drop=True)

    unique_times["hour"]   = unique_times["time_id"].dt.hour
    unique_times["minute"] = unique_times["time_id"].dt.minute

        
    def assign_meal_period(hour: int) -> str:
        if 9 <= hour <= 10:
            return "Morning"
        elif 11 <= hour <= 14:
            return "Lunch"
        elif 15 <= hour <= 17:
            return "Afternoon"
        elif 18 <= hour <= 21:
            return "Dinner"
        else:
            return "Late Night"

    unique_times["meal_period"] = unique_times["hour"].apply(assign_meal_period)

    # Store as plain time string to match the TIME column type in MySQL.
    unique_times["time_id"] = unique_times["time_id"].dt.strftime("%H:%M:%S")

    unique_times.to_sql("dim_time", con=engine, if_exists="append", index=False)
    print(f"  dim_time:       {len(unique_times)} rows loaded.")


def load_fact_orders(engine: object, pizzas_df: pd.DataFrame) -> None:
    """
    Build and load fact_orders by joining orders.csv and order_details.csv.

    total_price is computed here (quantity * unit_price) rather than in SQL
    queries later. Pre-computing it means every downstream query that needs
    revenue can read a column directly rather than recalculating every time.

    The unit_price in the fact table comes from dim_pizza (the price list),
    not from the source order_details.csv — because order_details.csv does
    not contain price. This is a join we make here at load time.
    """
    orders         = pd.read_csv(RAW_DATA / "orders.csv")
    order_details  = pd.read_csv(RAW_DATA / "order_details.csv")

    # Merge order date and time onto each line item.
    fact = order_details.merge(orders, on="order_id", how="left")

    # Bring in unit_price from the pizza dimension.
    fact = fact.merge(
        pizzas_df[["pizza_id", "pizza_type_id", "price"]],
        on="pizza_id",
        how="left"
    )

    # Compute total_price before loading.
    fact["unit_price"]  = fact["price"]
    fact["total_price"] = fact["quantity"] * fact["unit_price"]

    # Normalise date and time formats to match the dimension tables.
    fact["date"] = pd.to_datetime(fact["date"]).dt.date
    fact["time"] = pd.to_datetime(fact["time"], format="%H:%M:%S").dt.strftime("%H:%M:%S")

    fact = fact.rename(columns={
        "order_details_id": "order_details_id",
        "order_id":         "order_id",
        "date":             "date_id",
        "time":             "time_id",
    })

    fact = fact[[
        "order_details_id", "order_id", "date_id", "time_id",
        "pizza_id", "pizza_type_id", "quantity", "unit_price", "total_price"
    ]]

    fact.to_sql("fact_orders", con=engine, if_exists="append", index=False)
    print(f"  fact_orders:    {len(fact)} rows loaded.")


# ---------------------------------------------------------------------------
# Row count verification
# ---------------------------------------------------------------------------

def verify_row_counts(engine: object) -> None:
    """
    Query each table after loading and print row counts.

    This is a basic data integrity check. If a count is 0 or unexpectedly low,
    something went wrong in the load step and must be investigated before
    any analysis begins. Catching this here is far cheaper than discovering
    it mid-analysis.
    """
    tables = ["dim_pizza_type", "dim_pizza", "dim_date", "dim_time", "fact_orders"]
    print("\nPost-load row counts:")
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table:<20} {count:>6} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Starting data load...\n")

    engine = get_engine()

    # Dimensions must be loaded before the fact table because the fact table
    # has foreign keys referencing all four dimensions. MySQL will reject any
    # fact row whose keys do not exist in the dimension tables.
    print("Loading dimensions:")
    pizza_type_df = load_dim_pizza_type(engine)
    pizzas_df     = load_dim_pizza(engine)

    # Date and time dimensions are built from orders.csv directly.
    orders_raw = pd.read_csv(RAW_DATA / "orders.csv")
    load_dim_date(engine, orders_raw["date"])
    load_dim_time(engine, orders_raw["time"])

    print("\nLoading fact table:")
    load_fact_orders(engine, pizzas_df)

    verify_row_counts(engine)
    print("\nData load complete.")