# """Replay the Intel Berkeley Lab sensor dataset to MQTT.

# Dataset: http://db.csail.mit.edu/labdata/labdata.html  (data.txt.gz)
# Row format: date time epoch moteid temperature humidity light voltage
# Topic:      sensors/<moteid>/telemetry
# """
# import argparse
# import json
# import random
# import time
# from datetime import datetime, timezone

# import paho.mqtt.client as mqtt


# def parse_rows(path):
#     with open(path) as f:
#         for line in f:
#             p = line.split()
#             if len(p) != 8:
#                 continue  # skip malformed / incomplete rows
#             try:
#                 ts = datetime.fromisoformat(f"{p[0]} {p[1]}").replace(tzinfo=timezone.utc)
#                 yield ts.timestamp(), {
#                     "deviceId": f"mote{int(p[3])}",
#                     "temp": float(p[4]),
#                     "humidity": float(p[5]),
#                     "light": float(p[6]),
#                     "voltage": float(p[7]),
#                 }
#             except ValueError:
#                 continue


# def main():
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--file", default="data/data.txt")
#     ap.add_argument("--host", default="localhost")
#     ap.add_argument("--port", type=int, default=1883)
#     ap.add_argument("--speed", type=float, default=500, help="replay speed multiplier")
#     ap.add_argument("--anomalies", action="store_true", help="inject spikes and sensor dropouts")
#     ap.add_argument("--limit", type=int, default=0, help="stop after N messages (0 = all)")
#     args = ap.parse_args()

#     client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#     client.connect(args.host, args.port)
#     client.loop_start()

#     offset = None          # shifts old timestamps so data looks live
#     prev_ts = None
#     dropout_until = {}     # deviceId -> data-time when it comes back
#     sent = 0

#     for ts, msg in parse_rows(args.file):
#         if offset is None:
#             offset = time.time() - ts
#         if prev_ts is not None:
#             time.sleep(max(0.0, (ts - prev_ts) / args.speed))
#         prev_ts = ts

#         dev = msg["deviceId"]
#         if args.anomalies:
#             if ts < dropout_until.get(dev, 0):
#                 continue                                   # sensor is "dead"
#             if random.random() < 0.0002:
#                 dropout_until[dev] = ts + 180              # 3 min of silence
#                 continue
#             if random.random() < 0.001:
#                 msg["temp"] += 40                          # temperature spike
#             if random.random() < 0.0005:
#                 msg["voltage"] = 1.8                       # low battery

#         msg["ts"] = int((ts + offset) * 1000)              # event time, ms
#         client.publish(f"sensors/{dev}/telemetry", json.dumps(msg), qos=1)

#         sent += 1
#         if sent % 1000 == 0:
#             print(f"sent {sent} messages")
#         if args.limit and sent >= args.limit:
#             break

#     client.loop_stop()
#     client.disconnect()
#     print(f"done, {sent} messages")


# if __name__ == "__main__":
#     main()

"""Replay the Intel Berkeley Lab sensor dataset to MQTT.

Dataset: http://db.csail.mit.edu/labdata/labdata.html  (data.txt.gz)
Row format: date time epoch moteid temperature humidity light voltage
Topic:      sensors/<moteid>/telemetry
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# The dataset covers 2004-02-28 to 2004-04-05 and contains some corrupted rows
# (wrong timestamps, impossible temperatures or humidity). Skip those.
LO_TS = datetime(2004, 2, 27, tzinfo=timezone.utc).timestamp()
HI_TS = datetime(2004, 4, 7, tzinfo=timezone.utc).timestamp()


def parse_rows(path):
    """Yield (unix_ts, reading) for valid rows only."""
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) != 8:
                continue  # incomplete row
            try:
                ts = datetime.fromisoformat(f"{p[0]} {p[1]}").replace(
                    tzinfo=timezone.utc).timestamp()
                mote = int(p[3])
                temp, hum, light, volt = float(p[4]), float(p[5]), float(p[6]), float(p[7])
            except ValueError:
                continue
            if not (LO_TS <= ts <= HI_TS and 1 <= mote <= 54 and -10 <= temp <= 50
                    and 0 <= hum <= 100 and 2.0 <= volt <= 3.0):
                continue  # corrupted timestamp or sensor values
            yield ts, {"deviceId": f"mote{mote}", "temp": temp, "humidity": hum,
                       "light": light, "voltage": volt}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/data.txt")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--speed", type=float, default=500, help="replay speed multiplier")
    ap.add_argument("--anomalies", action="store_true", help="inject spikes and sensor dropouts")
    ap.add_argument("--limit", type=int, default=0, help="stop after N messages (0 = all)")
    args = ap.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(args.host, args.port)
    client.loop_start()

    offset = None          # shifts old timestamps so data looks live
    prev_ts = None
    dropout_until = {}     # deviceId -> data-time when it comes back
    sent = 0

    for ts, msg in parse_rows(args.file):
        if offset is None:
            offset = time.time() - ts
        if prev_ts is not None:
            time.sleep(max(0.0, (ts - prev_ts) / args.speed))
        prev_ts = max(prev_ts or ts, ts)  # out-of-order rows are sent without waiting

        dev = msg["deviceId"]
        if args.anomalies:
            if ts < dropout_until.get(dev, 0):
                continue                                   # sensor is "dead"
            if random.random() < 0.0002:
                dropout_until[dev] = ts + 180              # 3 min of silence
                continue
            if random.random() < 0.001:
                msg["temp"] += 40                          # temperature spike
            if random.random() < 0.0005:
                msg["voltage"] = 1.8                       # low battery

        msg["ts"] = int((ts + offset) * 1000)              # event time, ms
        client.publish(f"sensors/{dev}/telemetry", json.dumps(msg), qos=1)

        sent += 1
        if sent % 1000 == 0:
            print(f"sent {sent} messages")
        if args.limit and sent >= args.limit:
            break

    client.loop_stop()
    client.disconnect()
    print(f"done, {sent} messages")


if __name__ == "__main__":
    main()