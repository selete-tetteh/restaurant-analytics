"""
forecasting.py
--------------
Production script for demand forecasting and staffing recommendations.

Graduates the verified Prophet forecast from notebooks/03_forecasting.ipynb.

What this script does:
    1. Pulls daily order volume from the warehouse for the full dataset period.
    2. Trains a Prophet model, excluding the final 14 days of December to avoid
       the boundary artefact where sparse year-end data causes a false downward trend.
    3. Forecasts order volume for the next 30 days.
    4. Translates the forecast into staffing recommendations using peak-hour ratio.
    5. Saves the forecast chart to reports/charts/13_prophet_forecast.png.
    6. Saves the staffing CSV to reports/staffing_forecast.csv.

Run from the project root:
    python src/forecasting.py

Dependencies: prophet, sqlalchemy, pymysql, pandas, matplotlib, python-dotenv
All are listed in environment.yml.
"""

import os
import logging
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend — safe for scripts with no display
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
from sqlalchemy import create_engine
from prophet import Prophet
from prophet.plot import plot_plotly  # noqa: F401 — imported for completeness, not used here

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# INFO level gives enough visibility to confirm each stage completed without
# flooding the terminal with debug output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# Suppress Prophet's internal Stan output — it is verbose and not useful in
# production runs where we only care about the final forecast.
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
logging.getLogger("prophet.forecaster").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# __file__ is the absolute path to this script.
# .parent      = src/
# .parent.parent = project root
# This is reliable regardless of the working directory the script is called from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR  = PROJECT_ROOT / "reports"
CHARTS_DIR   = REPORTS_DIR / "charts"

CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FORECAST_DAYS       = 30
PEAK_HOUR_RATIO     = 0.095   # 9.5% of daily volume falls in the peak hour
ORDERS_PER_STAFF    = 15      # Orders one staff member can handle per peak hour
TRAINING_CUTOFF     = "2015-12-17"  # Excludes final 14 days of December

# ---------------------------------------------------------------------------
# Environment and database connection
# ---------------------------------------------------------------------------
def get_engine():
    """
    Load credentials from .env and return a SQLAlchemy engine.

    quote_plus is used on the password to handle special characters safely.
    Without it, characters like @ or # in a password break the connection string.
    """
    load_dotenv(PROJECT_ROOT / ".env")

    user     = os.getenv("DB_USER")
    password = quote_plus(os.getenv("DB_PASSWORD"))
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT", "3306")
    database = os.getenv("DB_NAME")

    if not all([user, password, host, database]):
        raise EnvironmentError(
            "Missing one or more required environment variables: "
            "DB_USER, DB_PASSWORD, DB_HOST, DB_NAME"
        )

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
def load_daily_orders(engine) -> pd.DataFrame:
    """
    Pull daily total order volume from the warehouse.

    Groups by date_id rather than individual order lines so Prophet receives
    one row per day — the correct input format (ds, y).
    """
    query = """
        SELECT
            date_id          AS ds,
            COUNT(DISTINCT order_id) AS y
        FROM fact_orders
        GROUP BY date_id
        ORDER BY date_id;
    """
    df = pd.read_sql(query, engine)
    df["ds"] = pd.to_datetime(df["ds"])
    log.info("Loaded %d days of order data. Range: %s to %s",
             len(df), df["ds"].min().date(), df["ds"].max().date())
    return df


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
def build_holidays() -> pd.DataFrame:
    """
    Define holidays for the Prophet model.

    Christmas uses lower_window = -1 to capture Christmas Eve demand, which
    the notebook verified is a genuine peak. Boxing Day is included as a
    separate holiday because post-Christmas ordering behaviour differs
    from Christmas itself.
    """
    holidays = pd.DataFrame({
        "holiday": [
            "new_years_day",
            "independence_day",
            "thanksgiving",
            "christmas",
            "boxing_day",
        ],
        "ds": pd.to_datetime([
            "2015-01-01",
            "2015-07-04",
            "2015-11-26",
            "2015-12-25",
            "2015-12-26",
        ]),
        "lower_window": [-1, -1, -1, -1,  0],
        "upper_window": [ 0,  0,  0,  0,  0],
    })
    return holidays


def train_model(df: pd.DataFrame) -> Prophet:
    """
    Train a Prophet model on data up to TRAINING_CUTOFF.

    The cutoff excludes the final 14 days of December. Without this, the sharp
    drop in orders over the holiday closure period causes Prophet to fit a
    downward trend at the boundary and project it into the forecast — an
    artefact of data sparsity, not a genuine business trend.
    """
    df_train = df[df["ds"] <= TRAINING_CUTOFF].copy()
    log.info("Training on %d days (cutoff: %s)", len(df_train), TRAINING_CUTOFF)

    model = Prophet(
        holidays              = build_holidays(),
        yearly_seasonality    = True,
        weekly_seasonality    = True,
        daily_seasonality     = False,
        seasonality_mode      = "additive",
        changepoint_prior_scale = 0.05,
    )
    model.fit(df_train)
    return model


def generate_forecast(model: Prophet) -> pd.DataFrame:
    """
    Generate a 30-day forward forecast and return the full forecast dataframe.
    """
    future   = model.make_future_dataframe(periods=FORECAST_DAYS)
    forecast = model.predict(future)
    log.info("Forecast generated for %d days ahead", FORECAST_DAYS)
    return forecast


# ---------------------------------------------------------------------------
# Staffing
# ---------------------------------------------------------------------------
def build_staffing(forecast: pd.DataFrame) -> pd.DataFrame:
    """
    Translate the demand forecast into staffing recommendations.

    Peak-hour volume is estimated as PEAK_HOUR_RATIO of the daily forecast.
    Staff needed is then daily_peak_volume / ORDERS_PER_STAFF, rounded up.

    Note: with ~60 daily orders, peak hourly volume is approximately 6 orders,
    which yields staff_needed = 1 throughout. This is correct given the dataset
    scale — the framework is valid and will produce meaningful variation with a
    higher-volume dataset.
    """
    future_only = forecast[forecast["ds"] > forecast["ds"].max() - pd.Timedelta(days=FORECAST_DAYS)].copy()

    future_only["peak_hour_volume"] = future_only["yhat"] * PEAK_HOUR_RATIO
    future_only["staff_needed"]     = (future_only["peak_hour_volume"] / ORDERS_PER_STAFF).apply(
        lambda x: max(1, int(x) + (1 if x % 1 > 0 else 0))
    )

    staffing = future_only[["ds", "yhat", "yhat_lower", "yhat_upper", "peak_hour_volume", "staff_needed"]].copy()
    staffing.columns = ["date", "forecast_orders", "forecast_lower", "forecast_upper", "peak_hour_volume", "staff_needed"]
    staffing["date"] = staffing["date"].dt.date

    return staffing


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
def save_forecast_chart(df: pd.DataFrame, forecast: pd.DataFrame) -> None:
    """
    Save the forecast chart showing actuals, predicted values, and uncertainty band.

    The uncertainty band (yhat_lower to yhat_upper) is Prophet's 80% credible
    interval — it represents the range within which 80% of future observations
    are expected to fall given the model's uncertainty.
    """
    fig, ax = plt.subplots(figsize=(12, 5))

    # Actuals
    ax.plot(df["ds"], df["y"], color="#aaaaaa", linewidth=0.8, alpha=0.8, label="Actual orders")

    # Forecast line
    ax.plot(forecast["ds"], forecast["yhat"], color="#2c7bb6", linewidth=1.5, label="Forecast")

    # Uncertainty band
    ax.fill_between(
        forecast["ds"],
        forecast["yhat_lower"],
        forecast["yhat_upper"],
        alpha=0.2,
        color="#2c7bb6",
        label="80% credible interval"
    )

    # Period boundary
    last_actual = df["ds"].max()
    ax.axvline(last_actual, color="#555555", linestyle="--", linewidth=0.8)
    ax.annotate(
        "Forecast start",
        xy=(last_actual, ax.get_ylim()[1]),
        xytext=(10, -15),
        textcoords="offset points",
        fontsize=8,
        color="#555555"
    )

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=30, ha="right")

    ax.set_title("Daily Order Volume — Prophet Forecast", fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("")
    ax.set_ylabel("Orders per day", fontsize=10)
    ax.legend(fontsize=9)

    note = (
        "Prophet trend component is unreliable on single-year data — "
        "weekly seasonality is the reliable signal."
    )
    fig.text(0.01, 0.01, note, fontsize=8, color="#888888", ha="left")

    plt.tight_layout()

    output_path = CHARTS_DIR / "13_prophet_forecast.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("Forecast chart saved to %s", output_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    log.info("Starting forecasting pipeline")

    engine   = get_engine()
    df       = load_daily_orders(engine)
    model    = train_model(df)
    forecast = generate_forecast(model)
    staffing = build_staffing(forecast)

    # Save outputs
    save_forecast_chart(df, forecast)

    staffing_path = REPORTS_DIR / "staffing_forecast.csv"
    staffing.to_csv(staffing_path, index=False)
    log.info("Staffing forecast saved to %s", staffing_path)

    log.info("Forecasting pipeline complete")
    log.info("  Chart:   reports/charts/13_prophet_forecast.png")
    log.info("  Staffing: reports/staffing_forecast.csv")


if __name__ == "__main__":
    main()
