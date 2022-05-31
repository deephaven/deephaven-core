#!/bin/sh
#
# Simple sh command line to load an example avro schema into our test compose including redpanda and apicurio.
#

if [ $# -ne 2 ]; then
    echo "Usage: $0 avro-as-json-file registry-name" 1>&2
    exit 1
fi

AVRO_AS_JSON_FILE="$1"
REGISTRY_NAME="$2"

# redpanda schema service does not support newlines, and needs the schema embedded in a json object as a
# value for a "schema" key, which means the keys and values in the schema itself need to have their
# quotes escaped... what a royal pain.
PAYLOAD=$(echo -n '{ "schema" : "'; cat "$AVRO_AS_JSON_FILE" | sed 's/"/\\"/g' | tr '\n' ' '; echo -n '" }')

exec curl -X POST \
     -H "Content-type: application/vnd.schemaregistry.v1+json; artifactType=AVRO" \
     --data-binary "$PAYLOAD" \
     "http://localhost:8081/subjects/$REGISTRY_NAME/versions"
