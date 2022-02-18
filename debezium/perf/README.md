Debezium - Kafka Perf
=====================

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

Once the compose is running
===========================

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


Tracking the last processed pageview timestamp
==============================================

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

Memory and CPU requirements
===========================

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
