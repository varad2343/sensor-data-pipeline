"""Sensor data pipeline: raw CSV -> validation -> SQLite -> hourly report with anomaly flags."""
import argparse
import csv
import math
import random
import sqlite3
from datetime import datetime, timedelta

LIMITS = {"temperature": (-20.0, 80.0), "humidity": (0.0, 100.0), "vibration": (0.0, 16.0)}

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    device TEXT NOT NULL, ts TEXT NOT NULL,
    temperature REAL NOT NULL, humidity REAL NOT NULL, vibration REAL NOT NULL,
    PRIMARY KEY (device, ts));
CREATE TABLE IF NOT EXISTS rejected (line INTEGER, raw TEXT, reason TEXT);
"""

HOURLY = """
SELECT device, substr(ts, 1, 13) AS hour, COUNT(*) AS samples,
       ROUND(AVG(temperature), 2) AS avg_temp, ROUND(MAX(temperature), 2) AS max_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity, ROUND(MAX(vibration), 2) AS max_vibration
FROM readings GROUP BY device, hour ORDER BY device, hour
"""

# robust z-score within each device-hour, so the daily temperature cycle does not hide spikes
ANOMALY = """
WITH r AS (SELECT device, ts, temperature, substr(ts, 1, 13) AS hour FROM readings),
ranked AS (
  SELECT device, hour, temperature,
         ROW_NUMBER() OVER (PARTITION BY device, hour ORDER BY temperature) AS n,
         COUNT(*) OVER (PARTITION BY device, hour) AS total FROM r),
median AS (
  SELECT device, hour, AVG(temperature) AS m FROM ranked
  WHERE n IN ((total + 1) / 2, (total + 2) / 2) GROUP BY device, hour),
deviation AS (
  SELECT r.device, r.hour, ABS(r.temperature - median.m) AS d,
         ROW_NUMBER() OVER (PARTITION BY r.device, r.hour ORDER BY ABS(r.temperature - median.m)) AS n,
         COUNT(*) OVER (PARTITION BY r.device, r.hour) AS total
  FROM r JOIN median USING (device, hour)),
mad AS (
  SELECT device, hour, AVG(d) AS mad FROM deviation
  WHERE n IN ((total + 1) / 2, (total + 2) / 2) GROUP BY device, hour)
SELECT r.device, r.ts, r.temperature
FROM r JOIN median USING (device, hour) JOIN mad USING (device, hour)
WHERE mad.mad > 0 AND ABS(r.temperature - median.m) > :k * 1.4826 * mad.mad
ORDER BY r.device, r.ts
"""


def generate(path, devices=3, minutes=1440, seed=0):
    """Write a synthetic day of data containing known faults. Returns the injected fault counts."""
    rng = random.Random(seed)
    start = datetime(2026, 1, 1)
    rows, faults = [], {"spike": 0, "invalid": 0, "duplicate": 0}
    for d in range(devices):
        for m in range(minutes):
            ts = (start + timedelta(minutes=m)).isoformat()
            temp = 25 + 3 * math.sin(2 * math.pi * m / 1440) + rng.gauss(0, 0.3)
            row = [f"node-{d + 1}", ts, f"{temp:.2f}", f"{50 + rng.gauss(0, 2):.2f}", f"{abs(rng.gauss(0.5, 0.1)):.3f}"]
            if m % 397 == 100:
                row[2] = f"{temp + 20:.2f}"
                faults["spike"] += 1
            elif m % 251 == 50:
                row[3] = rng.choice(["", "abc", "140.0", "nan"])
                faults["invalid"] += 1
            rows.append(row)
            if m % 503 == 7:
                rows.append(list(row))
                faults["duplicate"] += 1
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["device", "ts", *LIMITS])
        writer.writerows(rows)
    return faults


def validate(row):
    """Return a clean tuple, or raise ValueError saying why the row is bad."""
    if not row.get("device"):
        raise ValueError("missing device")
    ts = datetime.fromisoformat(row.get("ts") or "").isoformat()
    values = []
    for name, (low, high) in LIMITS.items():
        value = float(row.get(name) or "")
        if not low <= value <= high:          # also rejects NaN
            raise ValueError(f"{name} out of range: {value}")
        values.append(value)
    return (row["device"], ts, *values)


def load(csv_path, db):
    """Load a CSV in one transaction. Safe to run twice: duplicates are counted, not inserted."""
    db.executescript(SCHEMA)
    stats = {"inserted": 0, "rejected": 0, "duplicate": 0}
    with db, open(csv_path, newline="") as f:
        for line, row in enumerate(csv.DictReader(f), start=2):
            try:
                clean = validate(row)
            except (ValueError, TypeError) as error:
                db.execute("INSERT INTO rejected VALUES (?, ?, ?)", (line, str(row), str(error)))
                stats["rejected"] += 1
                continue
            added = db.execute("INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?)", clean).rowcount
            stats["inserted" if added else "duplicate"] += 1
    return stats


def hourly(db):
    return db.execute(HOURLY).fetchall()


def anomalies(db, k=6.0):
    return db.execute(ANOMALY, {"k": k}).fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", help="input file (a synthetic one is generated when omitted)")
    parser.add_argument("--db", default="sensors.db")
    args = parser.parse_args()
    if not args.csv:
        args.csv = "sample.csv"
        print("generated:", generate(args.csv))
    db = sqlite3.connect(args.db)
    print("load:", load(args.csv, db))
    with open("hourly_report.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["device", "hour", "samples", "avg_temp", "max_temp", "avg_humidity", "max_vibration"])
        writer.writerows(hourly(db))
    found = anomalies(db)
    print(f"hourly rows: {len(hourly(db))} -> hourly_report.csv")
    print(f"anomalies: {len(found)}")
    for row in found[:10]:
        print("  ", row)


if __name__ == "__main__":
    main()
