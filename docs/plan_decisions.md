# Build Plan & Decisions

The working plan. I co-planned this using Claude (AI) with my own ideas and the instructions received. The idea is that this plan gets implemented by me manually without AI as required. So here I will narrate the steps performed and why, and any decisions or change done.

# Starting
Got up my databricks free edition again, created a repo "poc_wind_turbine_farm", and know thinking of what to do next, probably create a catalog so I can add a volume and drop the given files there. So next steps catalog, schema and volume. Opened a sql query and ran the following:

CREATE CATALOG IF NOT EXISTS turbine_poc;
CREATE SCHEMA  IF NOT EXISTS turbine_poc.wind_farm;
CREATE VOLUME  IF NOT EXISTS turbine_poc.wind_farm.landing;

stored this code as seed_0_foundations.sql inside the repo.

<DECISION>
I need to think the folder structure inside the volume, for example if the original source files and the new files ingested will be separated or not
--> Decided to create a folder inside the volume called source_data and dropped the files there, will use a different folder for the generated data
</DECISION>

Now I will just manually add the file to the created volume and start checking and doing some analysis on the data I got, and start thinking of how I am going to generate data to feed and test the pipeline.

I am planning to create a separate notebook for this nb_data_analysis.ipynb.

# Data Analysis

The first step here is to consolidate the 3 files into one dataframe and then start working there.

Not going to lie, using Genie here is quite convenient, but looking at the data on a quick glance there are things that are easy to decide.

the dimensions are:
* timestamp: 1 per hour, ordered by time first and turbine id second apparently, automatically ingested as timestamp tipe.
* turbineid = integer, as the notes said 15 turbines, but need to be scalable, so need to think about this later.
* wind_speed, obviously cannot be negative but anything more than 0 is possible, thinking of checking against a real england location, and historical records to try to get to a max value (maybe with a warning), need to research on this, will assume its the wind speed measure from/by the turbine (anemometer?)
* wind direction, the limits are also easy from 0 to 359, anything different shoudl be treated as an error, decimal allowed??
* power output too, check the values range, is there a capped max, I remember reading that after certain speed the turbines can catch on fire so they have a safe mnechanism... should be a max value here, will try to determine from available data if not will do a bit of reading

### Checks

* duplicates: there are no duplicates present but we need to prepare for that to happen, so as part of the generator we need to introduce duplicates and see how that is handled. At the moment there are 11160 records without duplicates

* bad values wrong types? what is the expected type for each column, what happens if the value comes in an unexpected format or with errors. Also an scenario for the generator.

* To check for anomalies outliers, drafting a powercurve and expected values could be something useful, to use to statistically decide what is an outlier or not with a proper method, need to refresh myself on those bits, and get the power curve done.  After checking the correlation and the low results, and seeing that to my surprise there is no correlation at all between wind speed and power, I am a lilttle bit confused, after checking it seems that:

The power output (against everything I believed) is statistically independent of the wind_speed. I verified it at all, and per turbine and find the same results. The scatter fills all the rectangle, and there is no correlation whatsoever.

Correlation analysis:
wind_speed vs power_output: -0.0025

Test with corr method from pyspark.sql.functions

So I have two options here.
A) I can get an alternate source (that matches my expecations and where the speed correlates with the power generated) or 
B)I can stick to this source, amplify the data adding some noise in a way I can pick up anomalies even when there is no correlation generating artificial data similar to this one and adding noise with an generator. 

**DECISION**  Asked Alex if I can augment the data with a real source, or not, and will go from there.

Either way I will start building the basics as is with what I have

The current data shows that for any speed band from 9 to 15 we can get all the possible power outputs from 1.5 to 4.5 the std moves between 0.85 and 0.91, and for the 15 wind speed band the number of observations in cosiderable lower, it looks like sintetic data created with something like uniform(1.5,4.5) with a defined std around 0.87

My next question would be if at the same time all the turbines face the same weather conditions, and generate a similar power output or not, which could be another rule to define

**CONCLUSION**
I check the variance of wind speed at total and per turbine, and analyzed the total variance, to see if there was any correlation, and found again 0.
There is no agreement between the turbines at any specific point in time, so this is also random, and not something I can build a rule on as I originally expected.

With all this findings I need to start working on the Generator NExt

# Generator & Ingestion

So first step I created another directory "raw" inside the landing Volume as the placeholder for the generated data. THe initial idea is that the source simulator pipeline will read from the files in source data, add some noise, and write the updated files into raw to be picked up by the ingestion pipeline.

Actually might be better only 1 pipeline and the source simulator to run before the ingestion, everything in the same pipeline, The work will be carried in the nb_data_generator.ipynb


## Step 0:
The first step is that the generates just move the file as is and I build up the ingestion pipeline all the way to the end. so next lets render bronze, silver, gold

Bronze:
* inferColumnTypes=false so everything lands as a string. Deliberate — a "N/A" in a numeric column has to reach the table rather than kill the load. Bronze captures, silver conforms.

Silver
Originally decided to all transformations to be in the notebook as python and imported so I can test everything increasing coverage, but its simpler to try to stick to everything native databericks under the assumption (I always choice optimistic routes :D) that I dont need to test databricks default functionality :D.


Decided to keep the constraints in the SQL itself as expectations. I do lose the
ability to unit test them, but I am not testing that BETWEEN works, and I get
something back for it: the pipeline counts passes and failures per rule per run in
the event log, which I would otherwise have had to build myself.

<DECISION>
Cleaning rules go in silver as SQL expectations. The Python transforms stay for gold
only, where there is actual logic worth testing.
</DECISION>

I also had to pick what the rules reject. The obvious trap is tightening them to the
range I can see in the data - power never goes below 1.5 MW here, so why not set the
bound there? Because a turbine legitimately produces zero when there is no wind or
when it is curtailed, and that is the reading I would least want to lose. The rules
reject what is impossible, not what is unusual.

I split "missing" from "out of range" into two rules on purpose. They are two
different problems upstream and I want the event log to tell them apart.

# Gold

Started building min/max/avg per turbine per day, which is straightforward, and then
got to the anomalies and got it wrong first time.

My first version scored individual readings against the daily mean. I injected a
turbine running at 40% for six hours to test it and the pipeline found nothing. Worst
z-score was -1.87, under the threshold.

Took me a while to see why. The spread of a single reading is about 0.87 MW, which is
enormous compared to a mean of 3.0. A 60% drop for a few hours just disappears into
that noise.

Then I re-read the requirement: "identify any turbines that have significantly
deviated from their expected power output over the same time period". Turbines, over
a period. Not readings. So I should aggregate first and score the aggregate.

<DECISION>
Anomalies are scored on the daily average per turbine, not on individual readings.
</DECISION>

The daily average of 24 readings has a spread of about 0.18 MW, five times tighter,
because the noise cancels when you average. Same fault, same data, same rule - the
daily average caught what the readings had missed. The signal was always there, I was
just looking at the wrong grain.

That also made gold simpler. Requirement 2 produces the daily statistics and
requirement 3 scores them, so they chain instead of both windowing over raw readings.

Added two guards while I was in there. A standard deviation of zero would divide by
zero, so it returns null. And I skip days where fewer than 5 turbines reported,
because a fleet mean built from three turbines is not worth comparing against.

One thing I want to be honest about in the write-up: comparing a turbine to the fleet
normally rests on them all being in the same weather. I measured that earlier and it
is not true here. What is still true is that all 15 draw from the same distribution,
so the comparison works as a statistical test. I would rather say what it actually is
than claim an argument the data does not support.

# Generator

Went back and finished the generator properly once the pipeline was working end to
end. Five faults, each one mapped to something the brief asks for:

* dropped readings and a contiguous sensor outage - the "system sometimes misses
  entries" the brief mentions
* nulls - missing values
* out of range values - outliers
* sustained underperformance - the anomaly

The last one is the interesting one. Those values are all legal: in range, not null,
correctly typed. So silver has to let them straight through and only gold should catch
them. If my cleaning rules quarantined that turbine I would have confused "malformed"
with "abnormal", which are different things.

Everything is seeded so the same day always produces the same files, and it writes a
manifest of what it broke. That manifest is what lets me check the pipeline rather
than trust it.

Two deliberate faults on 2 March, chosen so they show up in different places:
turbine 3 loses 6 hours to an outage, turbine 7 runs at 35% for 12 hours. Turbine 3
ends up at 75% completeness and raises no anomaly, because its average is fine.
Turbine 7 has complete data and z = -2.577, 0.822 MW below the fleet. Missing data and abnormal data,
reported separately.

# Results

Ran the full month:

* bronze 11,112 rows (48 removed by the simulated sensor faults)
* silver 10,962
* 150 rejected by the expectations - and the generator injected exactly 57 nulls and
  93 out-of-range values, so it caught all of them and nothing else
* gold_turbine_daily_stats 465, which is 15 turbines x 31 days
* gold_turbine_anomalies 17

The 17 is worth a note. On clean data the 2 sigma rule flags about 3.7% of
turbine-days by construction, and 17 of 465 is exactly that. They are not 17 real
faults, they are the false positive rate of the threshold. Better to say so.

# Repo and deployment

Moved everything into a proper repo structure and hit a problem worth recording: I
regenerated day 1 with faults, re-ran the pipeline, and bronze processed nothing.

Auto Loader tracks files by path, not by content. Same path, already ingested, so it
skipped it. Full refresh fixed it.

Annoying at the time but it is actually the behaviour I want - it is the same
mechanism that makes re-running the pipeline free. The fix in a real system is that
the source writes immutable files with unique names, so a correction arrives as a new
file rather than an overwrite. My generator overwrites because it is replaying a
month I already have, which no real source would do.

<DECISION>
Defined the pipeline in a Databricks asset bundle instead of leaving it in the UI, so
the configuration is versioned with the code. Deploys with one command.
</DECISION>

Deployed it from VS Code, ran it, got the same numbers back. Everything survived the
restructure.
