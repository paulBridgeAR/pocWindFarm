"""Source simulator - stands in for the SCADA system.
 
Reads the pristine month from source_data and lands it into the raw landing zone
one day at a time, injecting faults on the way.
 
Not part of the pipeline. Not deployed, not scheduled. In production the source
system writes these files itself.
 
Every injected fault is recorded in a manifest, which is the expected result the
tests assert against. Seeded, so the same day always produces the same files.
 
    python source_simulator.py                    # land the next unlanded day
    python source_simulator.py --day 2022-03-12   # land a specific day
    python source_simulator.py --all              # land every remaining day
    python source_simulator.py --reset            # clear the landing zone first
    python source_simulator.py --clean            # no faults
 
From a Databricks notebook:
 
    %run ./source_simulator
    land_next()
"""
 
import argparse
import csv
import os
import random
import shutil
 
# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
 
SEED = 79   # Rolling Stones fan. Always this one.
 
SOURCE   = "/Volumes/turbine_poc/wind_farm/landing/source_data"
RAW      = "/Volumes/turbine_poc/wind_farm/landing/raw"
MANIFEST = "/Volumes/turbine_poc/wind_farm/landing/manifest"   # outside RAW - never ingested
 
# Background faults on every day. Low, so the data still resembles the original.
BACKGROUND = {
    "drop_rate": 0.005,     # ~2 readings per group per day
    "null_rate": 0.005,
    "corrupt_n": 1,         # per group per day
}
 
# Deliberate faults on specific days. This is the scenario the tests assert against.
# Turbines 1-5 are in group 1, 6-10 in group 2, 11-15 in group 3.
SCENARIO = {
    "2022-03-12": [
        # Sensor offline for 6 hours. Readings never arrive, so completeness drops
        # to 75%. Verified: turbine 3 reading_count 18, no anomaly raised.
        ("outage",       {"turbine_id": 3, "start_hour": 8, "hours": 6}),
 
        # Turbine underperforms for 12 hours. Every value stays legal - in range,
        # not null, correctly typed - so the cleaning rules must NOT reject these
        # rows. Only gold, scoring the daily average, should catch it.
        # Verified: daily avg 2.91 -> 2.01, z-score -0.54 -> -3.15.
        ("underperform", {"turbine_id": 7, "start_hour": 6, "hours": 12, "factor": 0.35}),
    ],
}
 
MANIFEST_FIELDS = ["fault", "day", "turbine_id", "timestamp", "field", "original", "new"]
 
 
# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
 
def rng_for(name):
    """Independent, reproducible generator per fault per day.
 
    A single shared generator would leak between faults: adding one earlier in the
    chain changes which rows every later fault touches, and the manifest stops
    matching the files. String seeds go through sha512, so this is stable across
    processes.
    """
    return random.Random(f"{SEED}::{name}")
 
 
def entry(fault, day, row, field=None, original=None, new=None):
    """One manifest record."""
    return {
        "fault": fault,
        "day": day,
        "turbine_id": row["turbine_id"],
        "timestamp": row["timestamp"],
        "field": field,
        "original": original,
        "new": new,
    }
 
 
def hour_of(row):
    return int(row["timestamp"][11:13])
 
 
# --------------------------------------------------------------------------
# Faults - all the same shape: (rows, day, ...) -> (rows, manifest_entries)
# --------------------------------------------------------------------------
 
def drop_readings(rows, day, rate):
    """Requirement 1 - missing entries. Scattered transmission losses."""
    rng = rng_for(f"drop::{day}")
    kept, manifest = [], []
 
    for row in rows:
        if rng.random() < rate:
            manifest.append(entry("DROPPED_READING", day, row,
                                  original=row["power_output"]))
        else:
            kept.append(row)
 
    return kept, manifest
 
 
def sensor_outage(rows, day, turbine_id, start_hour, hours):
    """Requirement 1 - missing entries, contiguous.
 
    A real sensor failure is a block of hours, not scattered singles. This is what
    makes completeness_pct visibly move.
    """
    kept, manifest = [], []
    window = range(start_hour, start_hour + hours)
 
    for row in rows:
        if int(row["turbine_id"]) == turbine_id and hour_of(row) in window:
            manifest.append(entry("SENSOR_OUTAGE", day, row,
                                  original=row["power_output"]))
        else:
            kept.append(row)
 
    return kept, manifest
 
 
def null_values(rows, day, rate, field="power_output"):
    """Requirement 1 - missing values. The reading arrives, the value is empty."""
    rng = rng_for(f"null::{day}::{field}")
    out, manifest = [], []
 
    for row in rows:
        if rng.random() < rate and row[field] != "":
            row = dict(row)
            manifest.append(entry("NULL_VALUE", day, row, field=field,
                                  original=row[field], new=""))
            row[field] = ""
        out.append(row)
 
    return out, manifest
 
 
def corrupt_values(rows, day, n):
    """Requirement 1 - outliers. Physically impossible readings."""
    rng = rng_for(f"corrupt::{day}")
    out = [dict(r) for r in rows]
    manifest = []
 
    if not out:
        return out, manifest
 
    for row in rng.sample(out, min(n, len(out))):
        original = row["power_output"]
        row["power_output"] = rng.choice(["-2.5", "47.0", "999.9"])
        manifest.append(entry("OUT_OF_RANGE", day, row, field="power_output",
                              original=original, new=row["power_output"]))
 
    return out, manifest
 
 
def underperform(rows, day, turbine_id, start_hour, hours, factor):
    """Requirement 3 - the anomaly.
 
    A turbine running well below normal. Values stay in range, not null, correctly
    typed, so silver must pass them through. If the cleaning rules quarantine these,
    they are conflating 'malformed' with 'anomalous'. Only gold should catch it.
    """
    out, manifest = [], []
    window = range(start_hour, start_hour + hours)
 
    for row in rows:
        row = dict(row)
        if int(row["turbine_id"]) == turbine_id and hour_of(row) in window:
            original = row["power_output"]
            row["power_output"] = f"{round(float(original) * factor, 1)}"
            manifest.append(entry("UNDERPERFORMANCE", day, row, field="power_output",
                                  original=original, new=row["power_output"]))
        out.append(row)
 
    return out, manifest
 
 
TARGETED = {
    "outage":       sensor_outage,
    "underperform": underperform,
}
 
 
def apply_faults(rows, day, background=BACKGROUND, scenario=SCENARIO):
    """Targeted faults first, then background noise.
 
    Order matters: corrupt and underperform run before the drops, so a row cannot
    be recorded as corrupted and then silently removed, leaving a manifest entry
    with no matching row.
    """
    manifest = []
 
    for name, params in scenario.get(day, []):
        rows, entries = TARGETED[name](rows, day, **params)
        manifest.extend(entries)
 
    rows, entries = corrupt_values(rows, day, background["corrupt_n"]); manifest.extend(entries)
    rows, entries = null_values(rows, day, background["null_rate"]);    manifest.extend(entries)
    rows, entries = drop_readings(rows, day, background["drop_rate"]);  manifest.extend(entries)
 
    return rows, manifest
 
 
# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------
 
def write_manifest(entries, day, manifest_dir=MANIFEST):
    """One file per day, so regenerating a day replaces its manifest rather than
    appending a second copy."""
    os.makedirs(manifest_dir, exist_ok=True)
    with open(f"{manifest_dir}/faults_{day}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        w.writeheader()
        w.writerows(entries)
 
 
def read_manifest(manifest_dir=MANIFEST):
    """Every fault injected so far - the expected result for the tests."""
    if not os.path.exists(manifest_dir):
        return []
    rows = []
    for name in sorted(os.listdir(manifest_dir)):
        if name.endswith(".csv"):
            with open(f"{manifest_dir}/{name}") as f:
                rows.extend(csv.DictReader(f))
    return rows
 
 
# --------------------------------------------------------------------------
# Which days exist, which have landed
# --------------------------------------------------------------------------
 
def days_in_source(source=SOURCE):
    """Every day present in the source files."""
    days = set()
    for name in sorted(os.listdir(source)):
        if name.endswith(".csv"):
            with open(f"{source}/{name}") as f:
                days |= {r["timestamp"][:10] for r in csv.DictReader(f)}
    return sorted(days)
 
 
def days_landed(raw=RAW):
    """Every day already in the landing zone.
 
    Derived from the directory listing rather than stored in a control file, so it
    cannot drift out of step with reality.
    """
    if not os.path.exists(raw):
        return set()
    return {d.split("=", 1)[1] for d in os.listdir(raw) if d.startswith("ingest_date=")}
 
 
def next_day(source=SOURCE, raw=RAW):
    """Earliest source day not yet landed. None when the month is finished."""
    remaining = sorted(set(days_in_source(source)) - days_landed(raw))
    return remaining[0] if remaining else None
 
 
# --------------------------------------------------------------------------
# Landing
# --------------------------------------------------------------------------
 
def generate_day(day, noise=True, source=SOURCE, raw=RAW, manifest_dir=MANIFEST):
    """Land one day's files into raw/ingest_date=<day>/.
 
    Idempotent: same day, same seed, same files. Re-running overwrites in place.
    """
    out_dir = f"{raw}/ingest_date={day}"
    os.makedirs(out_dir, exist_ok=True)
    manifest = []
    rows_written = 0
 
    for name in sorted(os.listdir(source)):
        if not name.endswith(".csv"):
            continue
 
        with open(f"{source}/{name}") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            rows = [r for r in reader if r["timestamp"].startswith(day)]
 
        if noise:
            rows, entries = apply_faults(rows, day)
            manifest.extend(entries)
 
        with open(f"{out_dir}/{name}", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerows(rows)
 
        rows_written += len(rows)
 
    write_manifest(manifest, day, manifest_dir)
    return day, rows_written, manifest
 
 
def land_next(noise=True, source=SOURCE, raw=RAW, manifest_dir=MANIFEST):
    """Land the next unlanded day. Returns None when the month is finished."""
    day = next_day(source, raw)
    if day is None:
        return None
    return generate_day(day, noise, source, raw, manifest_dir)
 
 
def reset_landing(raw=RAW, manifest_dir=MANIFEST):
    """Clear the landing zone and manifests.
 
    Always follow with a FULL REFRESH of the pipeline: bronze is append-only, so
    deleting files does not remove rows that were already ingested.
    """
    shutil.rmtree(raw, ignore_errors=True)
    shutil.rmtree(manifest_dir, ignore_errors=True)
    os.makedirs(raw, exist_ok=True)
 
 
def summarise(source=SOURCE, raw=RAW, manifest_dir=MANIFEST):
    """What actually landed."""
    landed = sorted(days_landed(raw))
    total = 0
    for day in landed:
        d = f"{raw}/ingest_date={day}"
        files = sorted(os.listdir(d))
        rows = sum(sum(1 for _ in open(f"{d}/{f}")) - 1 for f in files)
        total += rows
        print(f"  {day}  files={len(files)}  rows={rows:>4}")
 
    remaining = len(days_in_source(source)) - len(landed)
    print(f"\n  days landed: {len(landed)}   remaining: {remaining}   rows: {total}")
 
    counts = {}
    for f in read_manifest(manifest_dir):
        counts[f["fault"]] = counts.get(f["fault"], 0) + 1
    if counts:
        print("\n  injected faults:")
        for name, n in sorted(counts.items()):
            print(f"    {name:<20} {n}")
 
 
# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
 
def main():
    p = argparse.ArgumentParser(description="Land turbine data one day at a time.")
    p.add_argument("--day", help="Land this specific day (YYYY-MM-DD)")
    p.add_argument("--all", action="store_true", help="Land every remaining day")
    p.add_argument("--reset", action="store_true", help="Clear the landing zone first")
    p.add_argument("--clean", action="store_true", help="No faults")
    p.add_argument("--summary", action="store_true", help="Show what has landed and stop")
    args = p.parse_args()
 
    if args.summary:
        summarise()
        return
 
    if args.reset:
        reset_landing()
        print("landing zone cleared - remember to FULL REFRESH the pipeline\n")
 
    noise = not args.clean
 
    if args.day:
        day, rows, manifest = generate_day(args.day, noise)
        print(f"landed {day}  rows={rows}  faults={len(manifest)}")
    elif args.all:
        while True:
            result = land_next(noise)
            if result is None:
                break
            day, rows, manifest = result
            print(f"landed {day}  rows={rows}  faults={len(manifest)}")
    else:
        result = land_next(noise)
        if result is None:
            print("nothing left to land - every source day is already in the landing zone")
        else:
            day, rows, manifest = result
            print(f"landed {day}  rows={rows}  faults={len(manifest)}")
            nxt = next_day()
            print(f"next: {nxt}" if nxt else "that was the last day")
 
 
if __name__ == "__main__":
    main()