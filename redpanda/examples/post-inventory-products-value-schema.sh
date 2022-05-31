#
# Simple sh command line to load an example avro schema into our test compose including redpanda and apicurio.
#

# redpanda schema service does not support newlines, and needs the schema embedded in a json object as a
# value for a "schema" key, which means the keys and values in the schema itself need to have their
# quotes escaped... what a royal pain.
PAYLOAD=$(echo -n '{ "schema" : "'; cat avro/mysql.inventory.products-value.avro | sed 's/"/\\"/g' | tr '\n' ' '; echo -n '" }')

curl -X POST \
     -H "Content-type: application/vnd.schemaregistry.v1+json; artifactType=AVRO" \
     --data-binary "$PAYLOAD" \
     http://localhost:8081/subjects/inventory_products_value_record/versions
