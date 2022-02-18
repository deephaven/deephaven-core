Debezium - Kafka Demo
=====================

The docker compose file in this directory starts a compose with
images for mysql, Debezium, Redpanda (kafka implementation) and Deephaven,
plus an additional image to generate an initial mysql schema and
then generate updates to the tables over time for a simple e-commerce demo.

The demo follows closely the one defined for Materialize here:
https://github.com/MaterializeInc/ecommerce-demo/blob/main/README_RPM.md

The load generation script is in `loadgen/generate_load.py`.

It is possible to configure the update rate for both purchase
(mysql updates) and pageviews (kafka pageview events) via
environment definitions set for the loadgen image in extended
`../docker-compose-debezium/common.yml` file, loadgen image
definition, environment section.  The variables are called
PAGEVIEWS_PER_SECOND_START and PURCHASES_PER_SECOND_START.

Once the compose is running, the load generation script listens on
port 8090 (exported to the host) for a simple command interface;
you can change the number of actions generated dynamically
with the commands `set purchases_per_second n` and
`set pageviews_per_second n`, where n is the desired rate
target number.

How to run
==========

First, if you are building from sources, follow the instructions
in https://deephaven.io/core/docs/how-to-guides/launch-build,
and ensure you can launch a regular version of Deephaven
as per the instructions there.  Once that works, stop that
Deephaven instance, and continue below.

Start docker-compose with the compose file in this
directory:

```
cd debezium/demo
docker-compose up --build
```

Then start a Deephaven web console (will be in python mode
by default per the command above) by navigating to

```
http://localhost:10000/ide
```

and cut & paste to it from `debezium/scripts/demo.py`.  If Deephaven is running
on a different host from your browser, you can replace `localhost`
with the right hostname.

Suggesting that you cut&paste instead of automatically setting
the script to run is intentional, so that you can see tables
as they are created and populated and watch them update
before you execute the next command.

If you want to load everything in one command, however,
you can do it as the `demo.py` file is available inside
the DH server container under `/scripts/demo.py`.
You can load that in its entirety on the DH console with
`exec(open('/scripts/demo.py').read())`

The file `debezium/scripts/demo.sql` contains the original
Materialize script; see
https://github.com/MaterializeInc/ecommerce-demo/blob/main/README_RPM.md

Attributions
============

Files in this directory are based on demo code by
Debezium, Redpanda, and Materialize

* Debezium -- https://github.com/debezium/debezium
* Redpanda -- https://github.com/vectorizedio/redpanda
* Materialize -- https://github.com/MaterializeInc/materialize
* Materialize e-commerce demo -- https://github.com/MaterializeInc/ecommerce-demo/blob/main/README_RPM.md