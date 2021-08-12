Currently we are generting flatbuffer files manually. See deephaven-core/#1052 to track the work to automate this.

${FLATC}  --ts --no-fb-import --no-ts-reexport -o src/arrow/flight/flatbuf/ flight_format/\*.fbs
