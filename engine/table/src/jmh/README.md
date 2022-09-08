# engine-table JMH

```
./gradlew engine-table:jmhJar
java -jar engine/table/build/libs/deephaven-engine-table-<version>-jmh.jar <benchmark>
```

### Incremental Sort Cycles Benchmark

```
java -jar engine/table/build/libs/deephaven-engine-table-<version>-jmh.jar \
    io.deephaven.engine.bench.IncrementalSortCyclesBenchmark \
    -p params=REVERSE_START_0_CYCLE_1m 
```
