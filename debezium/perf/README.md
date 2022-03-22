# Debezium - Kafka Perf

The docker compose file in this directory is similar to
the one in the ../demo directory, with additional
images for running Materialize and Deephaven side-by-side
with the same debezium and Kafka input stream being
fed to both.

Please see `../demo/README.md` for a general
reference that also applies to this setup;
in particular, the 'How to run' section should
apply verbatim, substituing `debezium/demo`
for this directory, `debezium/perf`.

Please see the "Memmory and CPU requirements"
section below; as this compose is performance
analysis oriented, it has considerably
larger requirements than our other
feature-oriented demos.

## Additional building steps

On top of what is required for `../demo` (see
`../demo/README.md`), the automated testing
requires building the Deephaven Java client examples.
At the toplevel directory of your git clone (`../..`), run:

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
- Stop and "reset" the containers.

The example

```
cd debezium/perf
./run_experiment.sh dh 5000 20 10 1.0
```

will run an experiment for Deephaven (tag `dh`; use tag `mz` for Materialize) with a target rate of 5,000 pageviews per second.
It will wait 20 seconds after setting the target rate to begin sampling CPU and memory utilization using `top` in batch mode.
10 samples will be obtained, with a delay between samples of 1.0 seconds.

Example output from a run:

```
cfs@erke 12:18:20 ~/dh/oss3/deephaven-core/debezium/perf
$ ./run_experiment.sh dh 5000 20 10 1.0
About to run an experiment for dh with 5000 pageviews/s.

Actions that will be performed in this run:
1. Start compose services required for for dh.
2. Execute demo in dh and setup update delay logging.
3. Set 5000 pageviews per second rate.
4. Wait 20 seconds.
5. Take 10 samples for mem and CPU utilization, 1.0 seconds between samples.
6. Stop and 'reset' (down) compose.

Running experiment.

1. Starting compose.
PERF_TAG=2022.03.22.16.18.41_UTC_dh_5000

Logs are being saved to logs/2022.03.22.16.18.41_UTC_dh_5000.

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
Setting pageviews_per_second: old value was 50, new value is 5000.
Goodbye.

4. Waiting for 20 seconds.

5. Sampling top.
name=redpanda, tag=CPU_PCT, mean=84.14, samples=80.0, 84.2, 85.0, 87.0, 85.0, 82.0, 85.0, 84.0, 84.2, 85.0
name=redpanda, tag=RES_GiB, mean=0.77, samples=0.7678, 0.7698, 0.7698, 0.7698, 0.7718, 0.7718, 0.7718, 0.7718, 0.7718, 0.7776
name=deephaven, tag=CPU_PCT, mean=35.21, samples=66.7, 31.7, 28.0, 31.0, 27.0, 23.0, 46.0, 47.0, 25.7, 26.0
name=deephaven, tag=RES_GiB, mean=2.40, samples=2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4, 2.4

6. Stopping and 'reset' (down) compose.
Stopping core-debezium-perf_envoy_1      ... done
Stopping core-debezium-perf_grpc-proxy_1 ... done
Stopping core-debezium-perf_loadgen_1    ... done
Stopping core-debezium-perf_debezium_1   ... done
Stopping core-debezium-perf_server_1     ... done
Stopping core-debezium-perf_redpanda_1   ... done
Stopping core-debezium-perf_mysql_1      ... done
Stopping core-debezium-perf_web_1        ... done
Removing core-debezium-perf_envoy_1      ... done
Removing core-debezium-perf_grpc-proxy_1 ... done
Removing core-debezium-perf_loadgen_1    ... done
Removing core-debezium-perf_debezium_1   ... done
Removing core-debezium-perf_server_1     ... done
Removing core-debezium-perf_redpanda_1   ... done
Removing core-debezium-perf_mysql_1      ... done
Removing core-debezium-perf_web_1        ... done
Removing network core-debezium-perf_default

Experiment finished.
```

The CPU and memory utilization samples are shown on stdout and also saved to a file in the
new directory under `logs/`, in this case `logs/2022.03.22.16.18.41_UTC_dh_5000.`

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
  `docker-compose run mzcli`
  (this command needs to be executed from the `perf`
   directory to work).
  Once in the materialize cli, run:
  `\i /scripts/demo.sql`
  You should see a number of `CREATE SOURCE`
  and `CREATE VIEW` lines of output.

After running the commands above, both Materialize and
Deephaven should be running semantically-equivalent
demo scripts fed from debezium and kafka events
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
      mz_logical_timestamp()/1000.0 AS logical_ts_ms,
      mz_logical_timestamp()/1000.0 - max_received_at AS dt_ms
  FROM pageviews_summary;'  -U materialize -h localhost -p 6875
  ```

## Memory and CPU requirements

The parameters used for images in the docker compose file in this
directory are geared towards high message throughput.  While Deephaven
itself is running with the same default configuration used for
general demos (as of this writing, 4 cpus and 4 Gb of memory), the
configurations for redpanda, mysql, and debezium are tweaked to reduce
their impact in end-to-end latency and throughput measurements;
we make extensive use of RAM disks (tmpfs) and increase some
parameters to ones closer to production (e.g., redpanda's number
of cpus and memory per core).  To get a full picture of the
configuration used, consult the files:

* `../docker-compose-debezium-common.yml`
* `../.env`
* `docker-compose.yml`
* `.env`

Once started the compose will take around 6 Gb
of memory from the host; as events arrive and
specially if event rates are increased, it
will increase to 10-16 Gb or more.

For the mild initial rate (same as default demo in `../demo`),
the compose will consume around 2 CPU threads
(tested in a Xeon E5 2698 v4 CPU).
For increased event rates (eg, 50,000 pageviews per second),
CPU utilization will spike to 14 CPU threads or more.
