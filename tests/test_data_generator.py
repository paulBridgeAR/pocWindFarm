"""Tests for the generator's random number handling.

The generator injects faults into the source data and writes down what it broke
in a manifest. The manifest is the answer key the pipeline is checked against, so
it has to describe the files exactly. That only holds if the randomness is
reproducible and if each fault draws independently of every other fault.

These tests pin both properties down. No Spark needed - plain Python, fast.
"""

import subprocess
import sys

import pytest

import data_generator as gen


DAY = "2022-03-02"


def fake_rows(n=200):
    """A day of readings, shaped like the real source rows."""
    return [
        {
            "turbine_id": str(i % 15 + 1),
            "timestamp": f"{DAY} {i % 24:02d}:00:00",
            "wind_speed": "12.0",
            "wind_direction": "180",
            "power_output": "3.0",
        }
        for i in range(n)
    ]


def test_same_name_gives_the_same_numbers():
    """Rerunning the generator must reproduce the files byte for byte."""
    a = gen.rng_for(f"drop::{DAY}").sample(range(1000), 20)
    b = gen.rng_for(f"drop::{DAY}").sample(range(1000), 20)

    assert a == b


def test_different_faults_do_not_share_a_stream():
    """Each fault gets its own dice, so they can't pick the same rows in lockstep."""
    drop = gen.rng_for(f"drop::{DAY}").sample(range(1000), 20)
    corrupt = gen.rng_for(f"corrupt::{DAY}").sample(range(1000), 20)

    assert drop != corrupt


def test_different_days_do_not_lose_the_same_rows():
    """The day is part of the seed. Without it every day would drop the same
    positions, which is a pattern, not noise - and it would show up in the stats."""
    day_one = gen.rng_for("drop::2022-03-01").sample(range(1000), 20)
    day_two = gen.rng_for("drop::2022-03-02").sample(range(1000), 20)

    assert day_one != day_two


def test_an_earlier_fault_does_not_move_a_later_one():
    """This is the reason rng_for exists.

    With one shared generator the faults draw from the same stream in order, so
    what drop_readings picks depends on how many numbers the faults before it
    already consumed. Add a corruption fault and the dropped rows silently move -
    the manifest then describes rows that are still in the file.
    """
    rows = fake_rows()

    dropped_alone = gen.drop_readings(rows, DAY, rate=0.05)[1]

    # Simulate an earlier fault running first and pulling from its own generator.
    gen.rng_for(f"corrupt::{DAY}").sample(range(1000), 50)
    dropped_after = gen.drop_readings(rows, DAY, rate=0.05)[1]

    assert [e["timestamp"] for e in dropped_alone] == [e["timestamp"] for e in dropped_after]


def test_manifest_accounts_for_every_removed_row():
    """The count in the manifest has to equal the rows actually missing from the
    file. 99_verify.sql leans on this: it expects 150 rejected rows because the
    manifest says 150 were broken."""
    rows = fake_rows()
    kept, manifest = gen.drop_readings(rows, DAY, rate=0.05)

    assert len(kept) + len(manifest) == len(rows)
    assert 0 < len(manifest) < len(rows)   # the fault actually fired


def test_seeding_is_stable_across_processes():
    """Python's built-in hash() is randomised per process, so a string-keyed dict
    order can change between runs. random.Random(str) does not use it - it hashes
    the string with sha512 - so a file generated today reproduces tomorrow, on
    another machine, inside Databricks.
    """
    code = (
        "import random;"
        "print(random.Random('79::drop::2022-03-02').sample(range(1000), 20))"
    )
    other_process = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()

    assert str(gen.rng_for(f"drop::{DAY}").sample(range(1000), 20)) == other_process
