"""Dashboard for the MQTT -> Kafka -> Flink sensor pipeline.

Run:  streamlit run dashboard/app.py
Data: Postgres if DATABASE_URL is set (env var or Streamlit secret), else the bundled sample run.
"""
import datetime as dt

import altair as alt
import pandas as pd
import streamlit as st

from data_access import load_all

st.set_page_config(page_title="Sensor Pipeline Demo", page_icon="📡", layout="wide")

TEMP_LIMIT, BATT_LIMIT = 35.0, 2.3
STATUS_COLORS = {"Normal": "#2ca02c", "Warning": "#ff7f0e", "Silent": "#d62728"}
ALERT_META = {
    "temp_high": ("🔥", "High temperature"),
    "temp_jump": ("📈", "Sudden temperature jump"),
    "low_battery": ("🔋", "Low battery"),
    "dead_sensor": ("📵", "Sensor went silent"),
}

data, source = load_all()
sensors = data["sensors"]
avg = data["sensor_averages"]
avg = avg[avg["window_size_sec"] == 60]
alerts = data["alerts"]
metrics = data["pipeline_metrics"]

LO = avg["window_end"].min().to_pydatetime()
HI = avg["window_end"].max().to_pydatetime()
DEVICES = sorted(sensors["device_id"], key=lambda s: int(s.replace("mote", "")))


def xenc(field, title=None):
    return alt.X(f"{field}:T", title=title,
                 scale=alt.Scale(domain=[LO.isoformat(), HI.isoformat()]),
                 axis=alt.Axis(format="%H:%M"))


def now_rule(now):
    return (alt.Chart(pd.DataFrame({"t": [now]}))
            .mark_rule(color="gray", strokeDash=[2, 2]).encode(x=xenc("t")))


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.header("Replay controls")
    st.toggle("▶ Auto-play the run", key="play",
              help="Moves the pipeline clock forward automatically.")
    speed = st.select_slider("Auto-play speed", ["Slow", "Normal", "Fast"], value="Normal")
    step_seconds = {"Slow": 2.0, "Normal": 1.0, "Fast": 0.4}[speed]
    st.divider()
    st.subheader("How to read this page")
    st.markdown(
        "- **Green** sensor = healthy\n"
        "- **Orange** = warning (hot or low battery)\n"
        "- **Red** = stopped reporting\n"
        "- Drag the **pipeline clock** to see what the system knew at any moment."
    )
    st.divider()
    st.caption("Data source: " + ("Postgres (live Flink output)" if source == "postgres"
                                  else "bundled sample run"))


# ---------------------------------------------------------------- header
st.title("📡 Real-Time Sensor Monitoring Pipeline")
st.markdown(
    "**The problem:** a building has 54 wireless sensors. Overheating rooms, dying batteries "
    "and dead sensors usually get noticed hours later. **This pipeline spots them within "
    "seconds** as the data streams in. Use the clock below to replay a 2-hour run."
)
if source == "sample":
    st.info("You are viewing a **sample run**: synthetic data modelled on the Intel Berkeley "
            "Lab sensor dataset. The pipeline writes the same tables when run for real.", icon="ℹ️")


# ---------------------------------------------------------------- replay body
def body():
    st.session_state.setdefault("clock", LO + dt.timedelta(minutes=45))
    st.session_state.setdefault("sensor_sel", "mote12")
    if st.session_state.get("play"):
        nxt = st.session_state["clock"] + dt.timedelta(minutes=1)
        st.session_state["clock"] = LO if nxt > HI else nxt

    now = st.slider("⏱️ Pipeline clock: drag to replay what the pipeline saw at any moment",
                    min_value=LO, max_value=HI, step=dt.timedelta(minutes=1),
                    format="HH:mm", key="clock")
    now = pd.Timestamp(now)

    a_now = alerts[alerts["detected_ts"] <= now].sort_values("detected_ts", ascending=False)
    avg_now = avg[avg["window_end"] <= now]
    m_now = metrics[metrics["ts"] <= now]

    # ---- sensor status
    latest = (avg_now.sort_values("window_end").groupby("device_id").tail(1)
              .set_index("device_id"))
    status = sensors.copy()
    status["last_seen"] = status["device_id"].map(latest["window_end"])
    status["Temp °C"] = status["device_id"].map(latest["avg_temp"])
    status["Battery V"] = status["device_id"].map(latest["avg_voltage"])
    silent_min = (now - status["last_seen"]).dt.total_seconds() / 60
    warn = set(a_now[(a_now["detected_ts"] >= now - pd.Timedelta(minutes=10))
                     & a_now["alert_type"].isin(["temp_high", "temp_jump"])]["device_id"])
    warn |= set(a_now[a_now["alert_type"] == "low_battery"]["device_id"])
    status["status"] = "Normal"
    status.loc[status["device_id"].isin(warn), "status"] = "Warning"
    status.loc[silent_min.isna() | (silent_min >= 2), "status"] = "Silent"
    status["num"] = status["device_id"].str.replace("mote", "")

    # ---- 1. how the data flows
    st.subheader("1 · How the data flows")
    readings = int(avg_now["reading_count"].sum())
    stages = [
        ("🌡️", "Sensors", f"{len(sensors)} wireless sensors measure temperature, humidity, "
                          "light and battery.", f"{len(sensors)} devices"),
        ("📨", "MQTT", "A lightweight messaging protocol that small devices use to send "
                       "readings.", f"{readings:,} readings"),
        ("🧵", "Kafka", "A durable queue that holds every reading so none are lost.",
         f"{readings:,} stored"),
        ("⚙️", "Flink", "Processes the stream: per-minute averages and fault detection "
                        "in seconds.", f"{len(a_now)} alerts raised"),
        ("📊", "This dashboard", "Reads Flink's results from a database and displays them.",
         f"as of {now:%H:%M}"),
    ]
    for col, (icon, title, text, live) in zip(st.columns(5), stages):
        with col.container(border=True):
            st.markdown(f"**{icon} {title}**")
            st.caption(text)
            st.markdown(f"`{live}`")

    # ---- 2. KPIs
    st.subheader("2 · What the pipeline sees right now")
    n_silent = int((status["status"] == "Silent").sum())
    recent_alerts = int((a_now["detected_ts"] >= now - pd.Timedelta(hours=1)).sum())
    fast = a_now[a_now["alert_type"] != "dead_sensor"]
    typical = (fast["detected_ts"] - fast["event_ts"]).dt.total_seconds().median()
    k = st.columns(4)
    k[0].metric("Sensors reporting", f"{len(sensors) - n_silent} / {len(sensors)}")
    k[1].metric("Silent sensors", n_silent)
    k[2].metric("Alerts (last hour)", recent_alerts)
    k[3].metric("Typical detection time", "n/a" if pd.isna(typical) else f"{typical:.1f} s",
                help="Time from a sensor reading to Flink raising the alert "
                     "(silent-sensor alerts wait 2 minutes by design).")

    left, right = st.columns([3, 2])
    with left:
        st.markdown("**Sensor map** (lab floor plan, each dot is a sensor)")
        base = alt.Chart(status)
        dots = base.mark_circle(size=420, opacity=0.9).encode(
            x=alt.X("x:Q", axis=None, scale=alt.Scale(domain=[0, 41])),
            y=alt.Y("y:Q", axis=None, scale=alt.Scale(domain=[0, 30], reverse=True)),
            color=alt.Color("status:N", legend=alt.Legend(title=None, orient="bottom"),
                            scale=alt.Scale(domain=list(STATUS_COLORS),
                                            range=list(STATUS_COLORS.values()))),
            tooltip=["device_id", "status",
                     alt.Tooltip("Temp °C:Q", format=".1f"),
                     alt.Tooltip("Battery V:Q", format=".2f")])
        labels = base.mark_text(color="white", fontSize=10, fontWeight="bold").encode(
            x="x:Q", y="y:Q", text="num:N")
        st.altair_chart((dots + labels).properties(height=340), width="stretch")
    with right:
        st.markdown("**Alert feed** (newest first)")
        with st.container(height=340, border=True):
            if a_now.empty:
                st.caption("No alerts yet. Everything looks normal.")
            for _, r in a_now.head(15).iterrows():
                icon, label = ALERT_META[r["alert_type"]]
                st.markdown(f"{icon} **{label}** · {r['detected_ts']:%H:%M:%S}  \n{r['message']}")

    # ---- 3. follow one alert
    st.subheader("3 · Follow one alert through the pipeline")
    if a_now.empty:
        st.caption("Move the clock forward until the first alert appears.")
    else:
        r = a_now.iloc[0]
        icon, label = ALERT_META[r["alert_type"]]
        st.markdown(f"Latest alert: {icon} **{label}**, {r['message']}")
        if r["alert_type"] == "dead_sensor":
            steps = [
                ("📡 Last reading", f"{r['event_ts']:%H:%M:%S}", "The sensor sent its final reading."),
                ("⏳ Silence", "2 min", "Flink waits using event time and watermarks."),
                ("⚙️ Flink alert", f"{r['detected_ts']:%H:%M:%S}", "No data arrived, so a timer fired."),
                ("🔔 Shown here", f"{now:%H:%M}", "The dashboard reads it from the database."),
            ]
        else:
            lat = (r["detected_ts"] - r["event_ts"]).total_seconds()
            steps = [
                ("📡 Reading taken", f"{r['event_ts']:%H:%M:%S}", "A sensor measured something unusual."),
                ("📨 MQTT → Kafka", "in transit", "Published, queued and picked up (not timed separately)."),
                ("⚙️ Flink detected", f"{r['detected_ts']:%H:%M:%S}", f"Rule matched {lat:.1f} s after the reading."),
                ("🔔 Shown here", f"{now:%H:%M}", "The dashboard reads it from the database."),
            ]
        for col, (title, big, cap) in zip(st.columns(4), steps):
            with col.container(border=True):
                st.markdown(f"**{title}**")
                st.markdown(f"### {big}")
                st.caption(cap)

    # ---- 4. zoom into one sensor
    st.subheader("4 · Zoom in on one sensor")
    dev = st.selectbox("Sensor", DEVICES, key="sensor_sel")
    d_avg = avg_now[avg_now["device_id"] == dev]
    d_alerts = a_now[a_now["device_id"] == dev]
    alert_rules = (alt.Chart(d_alerts).mark_rule(color="#d62728", strokeDash=[4, 3])
                   .encode(x=xenc("detected_ts"), tooltip=["alert_type", "message"]))

    def sensor_chart(field, title, domain, limit):
        line = alt.Chart(d_avg).mark_line().encode(
            x=xenc("window_start"),
            y=alt.Y(f"{field}:Q", title=title, scale=alt.Scale(domain=domain)))
        limit_rule = (alt.Chart(pd.DataFrame({"y": [limit]}))
                      .mark_rule(color="#d62728", opacity=0.5).encode(y="y:Q"))
        return (line + limit_rule + alert_rules + now_rule(now)).properties(height=220)

    c1, c2 = st.columns(2)
    c1.markdown(f"**Temperature** (1-minute averages; red line = {TEMP_LIMIT:.0f}°C limit)")
    c1.altair_chart(sensor_chart("avg_temp", "°C", [15, 65], TEMP_LIMIT), width="stretch")
    c2.markdown(f"**Battery voltage** (red line = {BATT_LIMIT} V limit)")
    c2.altair_chart(sensor_chart("avg_voltage", "Volts", [2.0, 2.8], BATT_LIMIT), width="stretch")
    st.caption("Dashed red vertical lines mark alerts for this sensor. "
               "A gap in the line means the sensor stopped reporting.")

    # ---- 5. pipeline health
    st.subheader("5 · Is the pipeline itself healthy?")
    h1, h2 = st.columns(2)
    h1.markdown("**Readings processed per minute**")
    h1.altair_chart(
        (alt.Chart(m_now).mark_bar().encode(
            x=xenc("ts"), y=alt.Y("messages_processed:Q", title="Readings"))
         + now_rule(now)).properties(height=200), width="stretch")
    h2.markdown("**Reading-to-result latency** (lower is better)")
    h2.altair_chart(
        (alt.Chart(m_now).mark_line().encode(
            x=xenc("ts"),
            y=alt.Y("avg_latency_ms:Q", title="Milliseconds", scale=alt.Scale(domain=[0, 1500])))
         + now_rule(now)).properties(height=200), width="stretch")
    st.caption(f"Late or out-of-order events handled correctly so far: "
               f"**{int(m_now['late_events'].sum())}**. Flink uses event time and watermarks, "
               "so a reading that arrives late still lands in the right minute.")


if st.session_state.get("play"):
    st.fragment(run_every=step_seconds)(body)()
else:
    st.fragment(body)()

st.divider()
st.caption("Built with MQTT · Kafka · Flink · PostgreSQL · Streamlit")