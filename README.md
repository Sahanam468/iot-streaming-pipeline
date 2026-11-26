# Real-Time Environmental Monitoring and Fault Detection

Sensors publish readings over MQTT, Kafka buffers them, Flink detects faults in seconds (high temperature, sudden jumps, low battery, silent sensors), and a Streamlit dashboard shows the results. Dataset: Intel Berkeley Research Lab.

Sensors/Simulator -> MQTT (Mosquitto) -> Kafka -> Flink -> PostgreSQL -> Streamlit dashboard
Status
Step	State
Mosquitto + Kafka (Docker Compose)	done
Dataset replay simulator	done
Output schema (db/schema.sql)	done
Dashboard with sample data	done
MQTT -> Kafka bridge	next
Flink job writing to Postgres	planned
Try the dashboard (no pipeline needed)
bash
cd dashboard
pip install -r requirements.txt
python sample_data.py          # optional: regenerate the bundled sample run
streamlit run app.py

Drag the pipeline clock to replay a 2-hour run, or turn on Auto-play in the sidebar.

To show real Flink output instead, set DATABASE_URL (env var or Streamlit secret) to a Postgres database created with db/schema.sql.

Run the ingestion side
bash
docker compose up -d
pip install -r simulator/requirements.txt

#put data.txt in ./data (from the dataset page above)
python simulator/replay.py --speed 500 --anomalies
docker exec -it mosquitto mosquitto_sub -t 'sensors/#' -v
Deploy the dashboard (free)

Push the repo to GitHub, then on Streamlit Community Cloud create an app with main file dashboard/app.py. Add DATABASE_URL under Secrets if using a hosted Postgres.
