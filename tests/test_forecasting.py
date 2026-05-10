"""
test_forecasting.py
-------------------
Unit tests for src/forecasting.py.

Tests cover the logic that can fail silently without a live database:
    - Staffing calculation correctness
    - Holiday definitions (dates and window values)
    - Training cutoff filtering
    - Report generation smoke test

Run from the project root:
    pytest tests/ -v
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# Add src/ to path so we can import forecasting directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import forecasting


# ---------------------------------------------------------------------------
# Staffing calculation
# ---------------------------------------------------------------------------

class TestBuildStaffing:
    """
    The staffing function converts a forecast dataframe into peak-hour
    volume and staff headcount. These tests verify the arithmetic is correct
    and that edge cases are handled without silent failure.
    """

    def _make_forecast(self, yhat_values: list, start_date="2016-01-01") -> pd.DataFrame:
        """Helper: build a minimal forecast dataframe matching Prophet's output shape."""
        dates = pd.date_range(start=start_date, periods=len(yhat_values), freq="D")
        return pd.DataFrame({
            "ds":         dates,
            "yhat":       yhat_values,
            "yhat_lower": [v * 0.85 for v in yhat_values],
            "yhat_upper": [v * 1.15 for v in yhat_values],
        })

    def test_peak_hour_volume_calculation(self):
        """Peak hour volume must equal yhat multiplied by PEAK_HOUR_RATIO."""
        forecast = self._make_forecast([100.0])
        result   = forecasting.build_staffing(forecast)
        expected = round(100.0 * forecasting.PEAK_HOUR_RATIO, 1)
        assert round(result["peak_hour_volume"].iloc[0], 1) == expected

    def test_staff_needed_rounds_up(self):
        """
        Staff count must always round up, never down.
        A fractional staff requirement means you need the next whole person.
        """
        # At PEAK_HOUR_RATIO=0.095 and ORDERS_PER_STAFF=15:
        # peak_volume = 100 * 0.095 = 9.5
        # staff = 9.5 / 15 = 0.633 -> rounds up to 1
        forecast = self._make_forecast([100.0])
        result   = forecasting.build_staffing(forecast)
        assert result["staff_needed"].iloc[0] >= 1

    def test_staff_needed_minimum_is_one(self):
        """
        Even with very low forecast volume, staff_needed must be at least 1.
        A restaurant cannot operate with zero staff.
        """
        forecast = self._make_forecast([1.0])
        result   = forecasting.build_staffing(forecast)
        assert result["staff_needed"].iloc[0] >= 1

    def test_output_columns_present(self):
        """Output dataframe must contain exactly the expected columns."""
        forecast = self._make_forecast([60.0, 55.0, 70.0])
        result   = forecasting.build_staffing(forecast)
        expected_cols = {
            "date", "forecast_orders", "forecast_lower",
            "forecast_upper", "peak_hour_volume", "staff_needed"
        }
        assert expected_cols.issubset(set(result.columns))

    def test_output_row_count_matches_forecast_period(self):
        """Output must have exactly FORECAST_DAYS rows."""
        n = forecasting.FORECAST_DAYS
        forecast = self._make_forecast([60.0] * n)
        result   = forecasting.build_staffing(forecast)
        assert len(result) == n

    def test_higher_volume_produces_more_staff(self):
        """
        A forecast with double the daily volume should require at least as many
        staff as the original. Monotonicity check on the staffing function.
        """
        low_forecast  = self._make_forecast([60.0]  * forecasting.FORECAST_DAYS)
        high_forecast = self._make_forecast([600.0] * forecasting.FORECAST_DAYS)

        low_staff  = forecasting.build_staffing(low_forecast)["staff_needed"].mean()
        high_staff = forecasting.build_staffing(high_forecast)["staff_needed"].mean()

        assert high_staff >= low_staff


# ---------------------------------------------------------------------------
# Holiday definitions
# ---------------------------------------------------------------------------

class TestBuildHolidays:
    """
    Holidays affect the forecast materially. Wrong dates or window values
    produce silent errors — the model trains without complaint but forecasts
    incorrectly around key trading periods.
    """

    def setup_method(self):
        self.holidays = forecasting.build_holidays()

    def test_returns_dataframe(self):
        assert isinstance(self.holidays, pd.DataFrame)

    def test_required_columns_present(self):
        assert set(self.holidays.columns) >= {"holiday", "ds", "lower_window", "upper_window"}

    def test_christmas_date_correct(self):
        christmas = self.holidays[self.holidays["holiday"] == "christmas"]
        assert not christmas.empty, "Christmas holiday not defined"
        assert pd.Timestamp("2015-12-25") in christmas["ds"].values

    def test_christmas_lower_window_captures_eve(self):
        """
        Christmas Eve is a genuine demand peak for this restaurant.
        lower_window = -1 means the holiday effect starts one day before Christmas.
        """
        christmas = self.holidays[self.holidays["holiday"] == "christmas"]
        assert christmas["lower_window"].values[0] == -1, (
            "Christmas lower_window should be -1 to capture Christmas Eve demand"
        )

    def test_boxing_day_defined(self):
        boxing = self.holidays[self.holidays["holiday"] == "boxing_day"]
        assert not boxing.empty, "Boxing Day holiday not defined"
        assert pd.Timestamp("2015-12-26") in boxing["ds"].values

    def test_no_negative_upper_windows(self):
        """Upper windows must be >= 0. A negative upper window is never valid."""
        assert (self.holidays["upper_window"] >= 0).all()

    def test_holiday_dates_are_timestamps(self):
        assert pd.api.types.is_datetime64_any_dtype(self.holidays["ds"])


# ---------------------------------------------------------------------------
# Training cutoff
# ---------------------------------------------------------------------------

class TestTrainingCutoff:
    """
    The training cutoff excludes the final 14 days of December to prevent
    the boundary artefact where sparse year-end data causes Prophet to fit
    a false downward trend. These tests verify the cutoff is applied correctly.
    """

    def _make_full_year(self) -> pd.DataFrame:
        dates = pd.date_range(start="2015-01-01", end="2015-12-31", freq="D")
        return pd.DataFrame({"ds": dates, "y": [60] * len(dates)})

    def test_cutoff_excludes_dates_after_december_17(self):
        df       = self._make_full_year()
        df_train = df[df["ds"] <= forecasting.TRAINING_CUTOFF]
        assert df_train["ds"].max() <= pd.Timestamp(forecasting.TRAINING_CUTOFF)

    def test_cutoff_retains_data_before_december_17(self):
        df       = self._make_full_year()
        df_train = df[df["ds"] <= forecasting.TRAINING_CUTOFF]
        assert len(df_train) > 0

    def test_cutoff_removes_expected_number_of_days(self):
        """
        December 17 cutoff should remove the last 14 days of December
        (Dec 18 through Dec 31 inclusive).
        """
        df       = self._make_full_year()
        df_train = df[df["ds"] <= forecasting.TRAINING_CUTOFF]
        full_year_days   = len(df)
        training_days    = len(df_train)
        excluded_days    = full_year_days - training_days
        assert excluded_days == 14, (
            f"Expected 14 excluded days, got {excluded_days}. "
            "Check TRAINING_CUTOFF constant in forecasting.py."
        )

    def test_cutoff_constant_is_correct_date_string(self):
        """TRAINING_CUTOFF must be parseable as a date and be December 17."""
        cutoff = pd.Timestamp(forecasting.TRAINING_CUTOFF)
        assert cutoff.month == 12
        assert cutoff.day   == 17


# ---------------------------------------------------------------------------
# Report generation smoke test
# ---------------------------------------------------------------------------

class TestGenerateReport:
    """
    Smoke test — verifies the report script runs without error and writes a file.
    Does not validate content or formatting, only that the output exists and is
    a non-empty file.
    """

    def test_report_file_exists_after_run(self, tmp_path, monkeypatch):
        """
        Patch the output path to a temp directory so the test does not write
        to the real reports/ folder, then confirm the file is created.
        """
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
        import generate_report

        # Redirect output to temp path
        test_output = tmp_path / "test_report.xlsx"
        monkeypatch.setattr(generate_report, "OUTPUT_PATH", test_output)

        # Staffing CSV must exist for the forecasting sheet — use the real one
        real_staffing = (
            Path(__file__).resolve().parent.parent / "reports" / "staffing_forecast.csv"
        )
        if not real_staffing.exists():
            pytest.skip("staffing_forecast.csv not found — run src/forecasting.py first")

        monkeypatch.setattr(generate_report, "STAFFING_CSV", real_staffing)

        generate_report.main()

        assert test_output.exists(), "Report file was not created"
        assert test_output.stat().st_size > 0, "Report file is empty"
