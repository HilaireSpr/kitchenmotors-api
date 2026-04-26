from datetime import datetime
import pandas as pd


def format_date_eu(value) -> str:
    """
    Zet een losse datumwaarde om naar dd/mm/yyyy.
    Ondersteunt o.a.:
    - '2026-03-31'
    - datetime
    - pandas Timestamp
    """
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return ""


def format_series_eu(series):
    """
    Zet een pandas Series met datums om naar dd/mm/yyyy.
    """
    return pd.to_datetime(series, errors="coerce").dt.strftime("%d/%m/%Y")


def parse_iso_date(value):
    """
    Zet een ISO datumstring om naar een Python date/datetime-achtig object via pandas.
    Handig voor filteren en sorteren.
    """
    return pd.to_datetime(value, errors="coerce")


def get_today_date():
    """
    Vandaag als Python date.
    """
    return datetime.today().date()