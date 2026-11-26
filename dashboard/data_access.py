"""Load pipeline output: Postgres if DATABASE_URL is set, else the bundled sample run."""
import os
from pathlib import Path

import pandas as pd
import streamlit as st

SAMPLE_DIR = Path(__file__).parent / "sample_data"
TABLES = ["sensors", "sensor_averages", "alerts", "pipeline_metrics"]
TIME_COLS = {
    "sensor_averages": ["window_start", "window_end"],
    "alerts": ["event_ts", "detected_ts"],
    "pipeline_metrics": ["ts"],
}


def _database_url():
    try:
        if "DATABASE_URL" in st.secrets:
            return st.secrets["DATABASE_URL"]
    except Exception:
        pass
    return os.environ.get("DATABASE_URL")


@st.cache_data(ttl=60, show_spinner="Loading pipeline output...")
def load_all():
    url = _database_url()
    if url:
        from sqlalchemy import create_engine
        engine = create_engine(url)
        data = {t: pd.read_sql(f"SELECT * FROM {t}", engine) for t in TABLES}
        source = "postgres"
    else:
        data = {t: pd.read_csv(SAMPLE_DIR / f"{t}.csv") for t in TABLES}
        source = "sample"
    for table, cols in TIME_COLS.items():
        for c in cols:  # naive UTC timestamps are easier to chart and compare
            data[table][c] = pd.to_datetime(data[table][c], utc=True).dt.tz_localize(None)
    return data, source