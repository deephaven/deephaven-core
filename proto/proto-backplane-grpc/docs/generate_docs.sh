#!/bin/bash

# This script generates documentation for all .proto files in the /protos directory

# Find all .proto files in the /protos directory
for proto_file in `find /protos/ -name \*.proto`; do
  # Extract the base name of the .proto file (e.g., myfile.proto -> myfile)
  base_name=$(basename "$proto_file" .proto)

# Generate documentation for each .proto file
/opt/protoc/bin/protoc \
  --plugin=protoc-gen-doc=/usr/local/bin/protoc-gen-doc \
  -I/usr/include -I/protos \
  --doc_out=/docs/out --doc_opt=markdown,"${base_name}.md" \
  "$proto_file"
done