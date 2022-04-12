#
# Simple sh command line to load an example avro schema into our test compose including redpanda and apicurio.
#
curl -X POST -H "Content-type: application/json; artifactType=AVRO" -H "X-Registry-ArtifactId: share_price_record" -H "X-Registry-Version: 1" --data-binary "@avro/share_price.json" http://localhost:8083/api/artifacts
