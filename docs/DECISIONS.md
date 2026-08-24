# Decisions

Every decision I made and why, with what I would do differently behind a real
production system. The running narrative of how I got here — including the parts I got
wrong first — is in [plan_decisions.md](plan_decisions.md).

---

## The data

**1. I kept the supplied data instead of finding a better source.**
The correlation between wind speed and power output is -0.0025, so there is no power
curve in this data. My first instinct was to find a real source. I decided against it:
the brief says I can add or remove rows to test the requirements, which is permission to
change the data rather than replace it — and the 2σ rule only looks at power output, so
the pipeline behaves identically either way.
*In production* I would build the physical checks I could not build here: power against
the wind curve, ramp rate limits, a turbine against its neighbours in the same wind.

**2. I generated the faults instead of editing files by hand.**
Hand-editing is not repeatable and gives me nothing to check against. The generator is
seeded, so the same day always produces the same files, and it writes a manifest of
everything it breaks. That manifest is what turns "my pipeline handles bad data" into
something measurable: it injected 150 bad values and the pipeline rejected exactly 150.

**3. The generator is not part of the pipeline.**
It sits in `tools/`, is not deployed and is not scheduled. It stands in for the SCADA
system I don't have. A pipeline that modifies its own input reads like a
misunderstanding — the pipeline starts at the landing zone, and everything before that
belongs to somebody else's system.

---

## Ingestion

**4. The landing zone holds immutable daily files.**
The brief describes a CSV updated each day, which means a file that grows. A file that
changes underneath you is a bad interface, and Auto Loader tracks files by path so it
would never notice. The first thing that happens is that the growing file becomes one
immutable file per day.
*In production* I would ask the source for immutable daily extracts with unique names,
so a correction arrives as a new file. Failing that, `cloudFiles.allowOverwrites` plus
the deduplication in silver, accepting the reprocessing cost.

**5. No watermark table and no control file.**
Auto Loader's checkpoint is already a watermark. Writing my own would give me two
sources of truth about what has been processed, and eventually they disagree. The
generator works the same way — which days have landed comes from listing the directory,
not from a file. State that can be derived should not be stored.

**6. Bronze stores every column as a string.**
If a sensor sends `"N/A"` where a number should be, that has to reach the table. Typing
on the way in either fails the load or silently nulls the value, and both lose
information. Bronze captures what arrived; silver decides what it means.

**7. No validation rules in bronze.**
Filtering there destroys the raw record, and if a rule turns out to be wrong I cannot
recover what it removed. Bronze should be a faithful copy of what the source sent,
including the parts I don't like.

---

## Cleaning

**8. Validation is SQL expectations, not Python filters.**
I started with the cleaning in a Python function so I could unit test it, then changed
my mind — I don't need to test that `BETWEEN` works. That's testing Databricks. It also
gives me something I'd otherwise build: the pipeline records pass and fail counts per
rule per run in the event log. What I do test is my own logic, which is the aggregation
and the anomaly detection.

**9. Bad rows are dropped and counted, not quarantined.**
`ON VIOLATION DROP ROW` discards the row and records the count. Quarantine is better —
keeping bad rows means you can investigate and replay them — but it needs a temporary
view and two output tables, and for a POC the event log tells me what I need.
*In production* I would quarantine. The first time someone asks why Tuesday is short,
you want the actual rows.

**10. Missing power readings are removed, not imputed.**
The brief allows either. Power output is the number the business reports on, and
imputing it invents generation that never happened — those numbers end up in reporting
and, in a real energy company, in settlement. I leave the gap and publish
`completeness_pct` beside every statistic instead. I would impute wind speed and
direction, since they are context rather than the measure — but only if the data had
time structure, and I checked: it doesn't, so interpolating would be no better than
inserting the mean.

**11. The lower bound on power stays at 0.**
The data never goes below 1.5 MW so I could have set the bound there. A turbine
legitimately produces zero when the wind is calm, when it is curtailed, or when it is in
maintenance — exactly the reading I least want to throw away. Tightening a rule to the
range you happen to have observed is how you end up rejecting the interesting data.

**12. "Missing" and "out of range" are separate rules.**
`power_present` and `power_in_range` could be one expression. Keeping them apart means
the event log tells me how many readings were missing and how many were implausible —
two different upstream problems, and two different conversations with whoever owns the
sensors.

**13. Deduplication on `(turbine_id, event_ts)`, latest ingest wins.**
Bronze is append-only, so a re-delivered file gives me the same reading twice. This also
handles a corrected value arriving later. It is record-level idempotency, and it matters
alongside the file-level idempotency Auto Loader gives me — the first stops me reading a
file twice, the second keeps the data correct if a file is re-delivered anyway.
*In production* an AUTO CDC flow rather than `QUALIFY` — same idea, declarative, and it
handles late arrivals more cleanly.

---

## Statistics and anomalies

**14. Anomalies are scored on the daily average, not on individual readings.**
This is the decision I would defend hardest, and I got it wrong first. The brief asks for
turbines that deviated over a time period, and the grain decides whether the whole thing
works. A single reading has a spread of about 0.87 MW; a daily average of 24 readings has
about 0.18 — five times tighter, because averaging cancels the noise.

I tested it with an injected fault - a turbine at 35% output for 12 hours. Scored on
individual readings the fault is indistinguishable from ordinary variation, because a
single reading is drawn from a spread five times wider than the daily average. Scored on
the daily average it comes out at **z = -2.577, 0.822 MW below the fleet** - the largest
deviation of any of the 17 flags, where the next largest is 0.523. The noise is in the
readings and the signal is in the aggregate.

Worth being precise about what that flag is worth. By z-score alone the fault ranks
fourth of 17, behind pure sampling noise on other days, because the z-score divides by
that day's fleet spread. By deviation in MW it ranks first by a clear margin. That is an
argument for reporting both and ranking by severity, not for trusting a 2-sigma
boolean.

**15. Each turbine is compared against the fleet on the same day.**
I checked whether the turbines share weather, because that is normally the argument for
comparing them. They don't — the hour explains 0% of the variation in wind speed,
randomly shuffled timestamps score the same, and no pair of turbines correlates beyond
chance. What is still true is that all 15 draw from the same distribution, so the fleet
is a valid reference. But it's a statistical test, not a physical one, and I'd rather say
that than claim an argument the data doesn't support.
*In production* the physical argument holds too, and I'd also compare each turbine
against its own recent history to catch slow drift.

**16. I kept the 2σ rule even though it is fragile.**
With 24 readings, one extreme value inflates the standard deviation enough to partly hide
itself. A median-based method such as MAD would be more robust. I implemented what the
brief asked for — following the requirement matters, and I can say why it's fragile
without quietly substituting something else.

Worth stating: on clean data the rule flags 17 of 465 turbine-days, which is 3.7%. That
is the false positive rate of the threshold, not 17 real faults.

**17. Guards on the degenerate cases.**
A standard deviation of zero returns null rather than dividing by zero. Days with fewer
than 5 turbines reporting are not scored, because a fleet mean built from a handful of
turbines is not worth comparing anything against. These never show up in the sample data
and always show up eventually.

---

## Structure and platform

**18. Gold logic lives outside the pipeline files.**
Declarative pipeline code only runs inside a pipeline, so anything written inside the
decorated functions cannot be unit tested. `transforms.py` imports nothing from the
pipeline API, so the same functions run on Databricks and under pytest with a local Spark
session. The gold notebook is two three-line wrappers.

**19. Silver in SQL, gold in Python.**
Silver is casting, deduplication and constraints — all declarative, and SQL says it more
clearly than a chain of DataFrame calls. Gold contains decisions worth testing. SQL where
the work is declarative, Python where there's logic worth testing.

**20. Delta tables in Unity Catalog, no external database.**
The brief says store it in a database. A Delta table in Unity Catalog is one: catalogued,
transactional, schema-enforced, queryable in SQL. I didn't add Postgres — the workload is
analytical rather than transactional, JDBC writes from Spark bottleneck long before Delta
does, and a second system needs securing, syncing and backing up for no gain.
*In production*, if an operations dashboard needed low-latency point lookups I'd add a
serving layer — but that's a serving decision, not a storage one. The system of record
stays in Delta.

**21. All three layers in one pipeline.**
Free Edition allows one active pipeline per type. This is a platform constraint, not a
preference.
*In production* I'd split ingestion from transformation so they can be scheduled and
scaled separately — ingestion is small and frequent, transformation is heavier and can
lag.

**22. Triggered mode, started by hand.**
Auto Loader decides *what* gets processed; the trigger decides *when*. Starting it myself
also makes the incremental behaviour easy to demonstrate — land a day, run, land another,
run, then run again with nothing new and watch it process zero rows.
*In production* a daily schedule, or a file-arrival trigger so it fires when the source
delivers.

**23. The pipeline is defined in an asset bundle.**
It started in the UI, which meant its configuration — catalog, schema, source files,
compute — lived only in the workspace and not in the repository. `databricks.yml` fixes
that: versioned next to the code, deploys with one command.

**24. What I deliberately did not build.**
I considered a turbine dimension with point-in-time status (SCD Type 2) so a turbine in
scheduled maintenance would be excluded from the fleet baseline rather than flagged. It's
a good idea and I'd build it for real. I left it out — the brief says the emphasis is on
the functionality and not on the overall design of the application, and it's a couple of
hours for something nobody asked for.

Same reasoning for a metadata-driven ingestion framework. Reference data about turbines
would be worth modelling; configuration driving the pipeline is not, at three files and
fifteen turbines. Knowing where to stop is part of the job.
