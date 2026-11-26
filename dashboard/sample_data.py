"""Generate a 2-hour sample run (synthetic, modelled on the Intel Berkeley Lab data).

Creates CSVs in dashboard/sample_data/ with the same columns as db/schema.sql,
so the dashboard works before the Flink job exists.  Alerts are produced by
applying simple rules to the generated averages, so the data is self-consistent.
"""
from pathlib import Path

import numpy as np
import pandas as pd

OUT = Path(__file__).parent / "sample_data"
START = pd.Timestamp("2004-02-28 12:00:00")
MINUTES, N = 120, 54
TEMP_LIMIT, JUMP_LIMIT, BATT_LIMIT, SILENCE_SEC = 35.0, 8.0, 2.3, 120


def build():
    rng = np.random.default_rng(42)
    ids = [f"mote{i}" for i in range(1, N + 1)]

    sensors = pd.DataFrame({
        "device_id": ids,
        "x": [round((i % 9) * 4.5 + 2 + rng.uniform(-0.8, 0.8), 2) for i in range(N)],
        "y": [round((i // 9) * 5 + 2.5 + rng.uniform(-0.8, 0.8), 2) for i in range(N)],
    })

    base_t = {d: rng.uniform(19, 25) for d in ids}
    base_h = {d: rng.uniform(35, 48) for d in ids}
    base_l = {d: rng.uniform(50, 400) for d in ids}

    rows = []
    for m in range(MINUTES):
        ws = START + pd.Timedelta(minutes=m)
        for d in ids:
            if d == "mote7" and 50 <= m < 60:
                continue  # sensor goes silent for 10 minutes
            t = base_t[d] + 1.5 * np.sin(2 * np.pi * m / MINUTES) + rng.normal(0, 0.15)
            if d == "mote12" and 35 <= m < 38:
                t = rng.uniform(55, 62)          # overheating
            if d == "mote44" and 100 <= m < 102:
                t = rng.uniform(47, 50)          # second hot spot
            if d == "mote33" and m >= 95:
                t += 9                           # sudden jump, stays below the limit
            v = 2.65 - 0.0006 * m + rng.normal(0, 0.005)
            if d == "mote21":
                v = 2.62 - 0.004 * m + rng.normal(0, 0.004)  # battery draining
            rows.append({
                "device_id": d, "window_start": ws,
                "window_end": ws + pd.Timedelta(minutes=1), "window_size_sec": 60,
                "avg_temp": round(t, 2),
                "avg_humidity": round(base_h[d] - 0.4 * (t - base_t[d]) + rng.normal(0, 0.3), 2),
                "avg_light": round(max(0, base_l[d] + rng.normal(0, 8)), 1),
                "avg_voltage": round(v, 3),
                "reading_count": int(1 + rng.binomial(2, 0.5)),
            })
    avg = pd.DataFrame(rows)

    # Apply the detection rules to produce alerts
    alerts = []

    def add(d, kind, value, threshold, evt, det, msg):
        alerts.append({"device_id": d, "alert_type": kind, "value": round(float(value), 2),
                       "threshold": threshold, "event_ts": evt, "detected_ts": det, "message": msg})

    for d, g in avg.groupby("device_id"):
        g = g.sort_values("window_start").reset_index(drop=True)
        n = d.replace("mote", "")
        hot_prev, batt_done = False, False
        for i, r in g.iterrows():
            evt = r.window_start + pd.Timedelta(seconds=float(rng.uniform(5, 55)))
            det = evt + pd.Timedelta(seconds=float(rng.uniform(0.4, 1.8)))
            hot = r.avg_temp > TEMP_LIMIT
            if hot and not hot_prev:
                add(d, "temp_high", r.avg_temp, TEMP_LIMIT, evt, det,
                    f"Sensor {n} read {r.avg_temp:.1f}°C (limit {TEMP_LIMIT:.0f}°C)")
            hot_prev = hot
            if i > 0 and r.avg_temp - g.avg_temp[i - 1] > JUMP_LIMIT:
                delta = r.avg_temp - g.avg_temp[i - 1]
                add(d, "temp_jump", delta, JUMP_LIMIT, evt, det,
                    f"Sensor {n} temperature jumped +{delta:.1f}°C in one minute")
            if r.avg_voltage < BATT_LIMIT and not batt_done:
                batt_done = True
                add(d, "low_battery", r.avg_voltage, BATT_LIMIT, evt, det,
                    f"Sensor {n} battery at {r.avg_voltage:.2f} V (limit {BATT_LIMIT} V), replace soon")
            if i + 1 < len(g):
                gap = (g.window_start[i + 1] - r.window_end).total_seconds()
                if gap >= SILENCE_SEC:
                    last = r.window_end - pd.Timedelta(seconds=8)
                    add(d, "dead_sensor", SILENCE_SEC, SILENCE_SEC, last,
                        last + pd.Timedelta(seconds=SILENCE_SEC + 1),
                        f"Sensor {n} stopped reporting for 2 minutes")

    alerts = (pd.DataFrame(alerts).sort_values("detected_ts").reset_index(drop=True))
    alerts.insert(0, "id", alerts.index + 1)

    per_min = avg.groupby("window_end").agg(
        messages_processed=("reading_count", "sum"),
        active_sensors=("device_id", "nunique")).reset_index().rename(columns={"window_end": "ts"})
    per_min["avg_latency_ms"] = np.clip(rng.normal(850, 80, len(per_min)), 300, None).round(0)
    per_min["late_events"] = rng.poisson(1.5, len(per_min))
    metrics = per_min[["ts", "messages_processed", "avg_latency_ms", "late_events", "active_sensors"]]

    OUT.mkdir(exist_ok=True)
    sensors.to_csv(OUT / "sensors.csv", index=False)
    avg.to_csv(OUT / "sensor_averages.csv", index=False)
    alerts.to_csv(OUT / "alerts.csv", index=False)
    metrics.to_csv(OUT / "pipeline_metrics.csv", index=False)
    print(f"{len(avg)} averages, {len(alerts)} alerts, {len(metrics)} metric rows -> {OUT}")


if __name__ == "__main__":
    build()