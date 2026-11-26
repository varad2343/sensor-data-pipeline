# IoT Sensor Data Pipeline

**Job profile: data engineer / analyst**

`CSV -> validation -> SQLite -> hourly report -> anomaly detection`, using only the Python standard library.

- Rejects missing, non-numeric, NaN and out-of-range values and records the reason
- Idempotent loading inside one transaction (primary key on device and time)
- SQL window functions compute a median/MAD robust z-score for each device and hour, so the daily temperature cycle does not hide spikes
- Synthetic data generator with a known number of injected faults; tests require every fault to be accounted for

```bash
pip install pytest
python -m pytest
python pipeline.py              # generates sample data and writes hourly_report.csv
python pipeline.py --csv my.csv
```
Status: tested on synthetic data.
