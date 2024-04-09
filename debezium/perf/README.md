# Debezium - Kafka Perf

The docker compose file in this directory is similar to
the one in the ../demo directory, with additional
images for running Materialize and Deephaven side-by-side
with the same Debezium and Kafka input stream being
fed to both.

Please see `../demo/README.md` for a general
reference that also applies to this setup;
in particular, the 'How to run' section should
apply verbatim, substituting `debezium/demo`
for this directory, `debezium/perf`.

Please see the "Memory and CPU requirements"
section below; as this compose is performance
analysis oriented, it has considerably
larger requirements than our other
feature-oriented demos.

## Additional building steps

On top of what is required for `../demo` (see
`../demo/README.md`), the automated testing
requires building the Deephaven Java client examples.
At the top level directory of your git clone (`../..`), run:

```
./gradlew java-client-session-examples:installDist
```

## Automated testing

The script `run_experiment.sh` in this directory performs a
full test for one engine (either Deephaven or Materialize).
It will:

- Start the containers required for a particular run (and only those).
- Ensure container logs are preserved for the run.
- Load the demo code in the respective engine and sample update delays to a log file.
- Set the given pageviews per second rate, and wait a fixed amount of time thereafter for processing to settle.
- Take multiple samples for CPU and memory utilization over a defined period.
  Output from top in batch mode is sent to a log file and later post-processed.
  Aside from individual samples, the following statistics are computed:
  - median
  - sandard deviation (`sd` in the output).
  - standard error of the mean (`sem` in the output).
- Stop and "reset" the containers.

The example

```
cd debezium/perf
./run_experiment.sh dh 50000 20 30 1.0
```

will run an experiment for Deephaven (tag `dh`; use tag `mz` for Materialize) with a target rate of 50,000 pageviews per second.
It will wait 20 seconds after setting the target rate to begin sampling CPU and memory utilization using `top` in batch mode.
30 samples will be obtained, with a delay between samples of 1.0 seconds.

Example output from a run:

```
About to run an experiment for dh with 50000 pageviews/s.

Actions that will be performed in this run:
1. Start compose services required for for dh and initialize simulation.
2. Execute demo in dh and setup update delay logging.
3. Set 50000 pageviews per second rate.
4. Wait 20 seconds.
5. Take 30 samples for mem and CPU utilization, 1.0 seconds between samples.
6. Stop and 'reset' (down) compose.

Running experiment.

1. Starting compose and initializing simulation.
PERF_TAG=2022.03.24.01.59.34_UTC_dh_50000

Logs are being saved to logs/2022.03.24.01.59.34_UTC_dh_50000.

2. Running demo in dh and sampling delays.
1 compiler directives added
Table users = <new>
Table items = <new>
Table purchases = <new>
Table pageviews = <new>
Table pageviews_stg = <new>
Table purchases_by_item = <new>
Table pageviews_by_item = <new>
Table item_summary = <new>
Table top_viewed_items = <new>
Table top_converting_items = <new>
Table profile_views_per_minute_last_10 = <new>
Table profile_views = <new>
Table profile_views_enriched = <new>
Table dd_flagged_profiles = <new>
Table dd_flagged_profile_view = <new>
Table high_value_users = <new>
Table hvu_test = <new>
Table pageviews_summary = <new>

1 compiler directives added
No displayable variables updated


3. Setting pageviews per second
LOADGEN Connected.
Setting pageviews_per_second: old value was 50, new value is 50000.
Goodbye.

4. Waiting for 20 seconds.

5. Sampling top for 30 * 1.0 seconds.
name=deephaven, tag=CPU_PCT, mean=86.48, sd=29.84, sem=5.45, samples=[120.0, 64.4, 64.0, 66.0, 108.0, 59.0, 83.0, 68.0, 102.0, 71.0, 83.0, 169.3, 75.0, 63.0, 73.0, 147.0, 66.0, 104.0, 58.0, 70.3, 62.0, 86.0, 88.0, 138.0, 90.0, 78.0, 67.0, 56.4, 72.0, 143.0]
name=deephaven, tag=RES_GiB, mean=3.10, sd=0.00, sem=0.00, samples=[3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1, 3.1]
name=redpanda, tag=CPU_PCT, mean=89.08, sd=2.46, sem=0.45, samples=[80.0, 86.1, 88.0, 88.0, 85.0, 87.0, 86.0, 89.0, 90.0, 89.0, 89.0, 89.1, 90.0, 90.0, 88.0, 90.0, 91.0, 91.0, 91.0, 89.1, 90.0, 91.0, 90.0, 89.0, 92.0, 92.0, 89.0, 90.1, 92.0, 91.0]
name=redpanda, tag=RES_GiB, mean=0.95, sd=0.05, sem=0.01, samples=[0.8659, 0.8678, 0.8783, 0.8844, 0.8903, 0.8942, 0.9001, 0.9059, 0.9098, 0.9176, 0.9235, 0.9294, 0.9333, 0.9391, 0.945, 0.9489, 0.9548, 0.9606, 0.9665, 0.9704, 0.9763, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

6. Stopping and 'reset' (down) compose.
Stopping core-debezium-perf_envoy_1      ... done
Stopping core-debezium-perf_grpc-proxy_1 ... done
Stopping core-debezium-perf_loadgen_1    ... done
Stopping core-debezium-perf_debezium_1   ... done
Stopping core-debezium-perf_redpanda_1   ... done
Stopping core-debezium-perf_server_1     ... done
Stopping core-debezium-perf_mysql_1      ... done
Stopping core-debezium-perf_web_1        ... done
Removing core-debezium-perf_envoy_1      ... done
Removing core-debezium-perf_grpc-proxy_1 ... done
Removing core-debezium-perf_loadgen_1    ... done
Removing core-debezium-perf_debezium_1   ... done
Removing core-debezium-perf_redpanda_1   ... done
Removing core-debezium-perf_server_1     ... done
Removing core-debezium-perf_mysql_1      ... done
Removing core-debezium-perf_web_1        ... done
Removing network core-debezium-perf_default

Experiment finished.
```

The CPU and memory utilization samples are shown on stdout and also saved to a file in the
new directory under `logs/`, in this case `logs/2022.03.24.01.59.34_UTC_dh_50000`.

## Manual testing

### Once the compose is running

Both Materialize and Deephaven are running.  We now
can make them execute their respective demo scripts.

* To load the DH script, run a web console
  connecting to http://localhost:10000 and type:
  `exec(open('/scripts/demo.py').read())`
  That will load the contents of the `demo.py` script
  in the DH python session as if you had typed it.
  The script defines a number of refreshing tables
  which will show up in the tables display
  in the UI, and as events arrive, you should
  see the tables refreshing.
* To load the Materialize script, run the
  materialized command line interface (cli) via:
  `docker compose run mzcli`
  (this command needs to be executed from the `perf`
   directory to work).
  Once in the materialize cli, run:
  `\i /scripts/demo.sql`
  You should see a number of `CREATE SOURCE`
  and `CREATE VIEW` lines of output.

After running the commands above, both Materialize and
Deephaven should be running semantically-equivalent
demo scripts fed from Debezium and Kafka events
(triggered by the loadgen script).

You can increase or decrease the rate of events using
a command socket interface for loadgen; see `../demo/README.md`
for instructions.


### Tracking the last processed pageview timestamp

* In DH, the `pageviews_summary` table can help track
  the last pageview seen.
* In Materialize, if the host has `psql` installed, we can
  use the shell watch command to run a select statement
  for similar effect:
  ```
  watch -n1 "psql -c '
  SELECT
      total,
      to_timestamp(max_received_at) max_received_ts,
      mz_logical_timestamp - 1000*max_received_at AS dt_ms
  FROM pageviews_summary;'  -U materialize -h localhost -p 6875
  ```

## Memory and CPU requirements

The parameters used for images in the docker compose file in this
directory are geared towards high message throughput.  While Deephaven
itself is running with the same default configuration used for general
demos (as of this writing, 4 CPU threads and an update cycle of 1
second), the configurations for redpanda, MySQL, and Debezium are
tweaked to reduce their impact in end-to-end latency and throughput
measurements; we make extensive use of RAM disks (tmpfs) and increase
some parameters to ones closer to production (e.g., redpanda's number
of cpus and memory per core).  To get a full picture of the
configuration used, consult the files:

* `../docker-compose-debezium-common.yml`
* `../.env`
* `docker-compose.yml`
* `.env`

Once started the compose will take around 3 GiB of memory from the
host; as events arrive and specially if event rates are increased, it
will increase.  To test rates of the order of 100k msg/sec, either of
Deephaven or Materialize will need on the order of 12 GiB.

For the mild initial rate (same as default demo in `../demo`), once
the demo code is loaded in both engines the compose will consume
around 2 full CPUs (tested in a Xeon E5 2698 v4 CPU).  At high even
rates, deephaven will take up to 3 CPU threads.  Materialize will take
as many full cores as configured in their `-w` argument.
