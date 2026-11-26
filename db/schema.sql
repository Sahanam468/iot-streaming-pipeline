-- Output tables: written by the Flink job, read by the Streamlit dashboard.

-- Static lookup: where each sensor sits in the lab (metres)
CREATE TABLE IF NOT EXISTS sensors (
    device_id TEXT PRIMARY KEY,
    x REAL NOT NULL,
    y REAL NOT NULL
);

-- 1-minute (and later 5-minute) averages per sensor
CREATE TABLE IF NOT EXISTS sensor_averages (
    device_id       TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    window_size_sec INT         NOT NULL,
    avg_temp        REAL,
    avg_humidity    REAL,
    avg_light       REAL,
    avg_voltage     REAL,
    reading_count   INT         NOT NULL,
    PRIMARY KEY (device_id, window_start, window_size_sec)
);
CREATE INDEX IF NOT EXISTS idx_avg_window_end ON sensor_averages (window_end);

-- Faults detected by Flink
-- event_ts    = when the sensor reading happened (event time)
-- detected_ts = when Flink raised the alert
CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT        NOT NULL,
    alert_type  TEXT        NOT NULL
                CHECK (alert_type IN ('temp_high','temp_jump','low_battery','dead_sensor')),
    value       REAL,
    threshold   REAL,
    event_ts    TIMESTAMPTZ NOT NULL,
    detected_ts TIMESTAMPTZ NOT NULL,
    message     TEXT        NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_detected ON alerts (detected_ts);

-- One row per minute of pipeline health
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    ts                 TIMESTAMPTZ PRIMARY KEY,
    messages_processed INT   NOT NULL,   -- readings processed in that minute
    avg_latency_ms     REAL  NOT NULL,   -- reading -> result
    late_events        INT   NOT NULL,   -- out-of-order events handled
    active_sensors     INT   NOT NULL
);