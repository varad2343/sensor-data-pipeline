import sqlite3
from statistics import mean

import pytest

from pipeline import anomalies, generate, hourly, load, validate


@pytest.fixture
def data(tmp_path):
    path = tmp_path / "day.csv"
    faults = generate(path)
    db = sqlite3.connect(":memory:")
    return path, faults, db, load(path, db)


def test_every_injected_fault_is_accounted_for(data):
    _, faults, _, stats = data
    assert stats["rejected"] == faults["invalid"]
    assert stats["duplicate"] == faults["duplicate"]
    assert stats["inserted"] == 3 * 1440 - faults["invalid"]


def test_loading_twice_changes_nothing(data):
    path, _, db, first = data
    second = load(path, db)
    assert second["inserted"] == 0
    assert db.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == first["inserted"]


def test_exactly_the_spikes_are_flagged(data):
    _, faults, db, _ = data
    found = anomalies(db)
    assert len(found) == faults["spike"]
    assert all(temp > 40 for _, _, temp in found)


def test_hourly_average_matches_python(data):
    _, _, db, _ = data
    rows = hourly(db)
    assert len(rows) == 3 * 24
    temps = [t for (t,) in db.execute(
        "SELECT temperature FROM readings WHERE device='node-2' AND ts LIKE '2026-01-01T05%'")]
    row = next(r for r in rows if r[:2] == ("node-2", "2026-01-01T05"))
    assert row[2] == len(temps) and row[3] == pytest.approx(mean(temps), abs=0.006)


@pytest.mark.parametrize("field,value", [("humidity", "140"), ("temperature", "nan"),
                                         ("vibration", ""), ("ts", "yesterday"), ("device", "")])
def test_bad_values_are_rejected(field, value):
    row = {"device": "n", "ts": "2026-01-01T00:00:00", "temperature": "20", "humidity": "50", "vibration": "1"}
    row[field] = value
    with pytest.raises(ValueError):
        validate(row)
