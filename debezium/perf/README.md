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