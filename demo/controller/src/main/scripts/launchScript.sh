#!/bin/bash
test -f /certs/deephaven-svc-act.json && gcloud auth activate-service-account --key-file=/certs/deephaven-svc-act.json
gcloud config set core/disable_usage_reporting true
gcloud config set component_manager/disable_update_check true
gcloud config set project deephaven-oss
export JAVA_OPTIONS="-Dquarkus.log.level=DEBUG -Dquarkus.http.host=0.0.0.0 -Dquarkus.http.port=7117 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
cd /deployments
./run-java.sh --debug