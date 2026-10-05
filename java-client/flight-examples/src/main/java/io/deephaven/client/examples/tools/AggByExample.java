//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import io.deephaven.client.examples.AuthenticationOptions;
import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.LabeledValue;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders8;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Load tool: creates a key-backed input table, publishes one table per named aggregation of it (countBy, sumBy, and so
 * on), then feeds random updates into the input table so the aggregations tick. Compare {@link AggregateAllExample},
 * which does the same through {@code aggAllBy} with aggregation specs.
 */
@Command(name = "agg-by", mixinStandardHelpOptions = true,
        description = "Agg by examples", version = "0.1.0")
class AggByExample implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"--input-table-size"}, description = "The input table size, default ${DEFAULT-VALUE}",
            defaultValue = "1000")
    int inputTableSize;

    @Option(names = {"--num-groups"}, description = "The number of groups, default ${DEFAULT-VALUE}",
            defaultValue = "10")
    int numGroups;

    @Option(names = {"--update-percentage"},
            description = "The update percentage per input table row per cycle, default ${DEFAULT-VALUE}",
            defaultValue = "0.01")
    double updatePercentage;

    @Option(names = {"--sleep-millis"}, description = "The sleep milliseconds between cycles, default ${DEFAULT-VALUE}",
            defaultValue = "100")
    long sleepMillis;

    @Option(names = {"--cycles"}, description = "The number of update cycles to run before exiting, unlimited if unset")
    Long cycles;

    @Override
    public Void call() throws Exception {
        final BufferAllocator allocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        try (final FlightSession flight = factory.newFlightSession()) {
            run(flight, allocator);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private void run(FlightSession flight, BufferAllocator allocator) throws Exception {
        // The input table: keyed by InputKey, grouped by GroupKey, with one numeric column of each width
        final ColumnHeader<Integer> inputKey = ColumnHeader.ofInt("InputKey");
        final ColumnHeader<Integer> groupKey = ColumnHeader.ofInt("GroupKey");
        final ColumnHeader<Byte> u = ColumnHeader.ofByte("U");
        final ColumnHeader<Short> v = ColumnHeader.ofShort("V");
        final ColumnHeader<Integer> w = ColumnHeader.ofInt("W");
        final ColumnHeader<Long> x = ColumnHeader.ofLong("X");
        final ColumnHeader<Float> y = ColumnHeader.ofFloat("Y");
        final ColumnHeader<Double> z = ColumnHeader.ofDouble("Z");
        final TableSpec base = InMemoryKeyBackedInputTable.of(TableHeader.of(inputKey, groupKey, u, v, w, x, y, z),
                Collections.singletonList(inputKey.name()));

        // One aggregation per convenience method, each published under its name
        final String by = groupKey.name();
        final LabeledTables tables = LabeledTables.builder()
                .putMap("base", base)
                .putMap("countBy", base.countBy("Count", by).sort(by))
                .putMap("selectDistinct", base.selectDistinct(by).sort(by))
                .putMap("firstBy", base.firstBy(by).sort(by))
                .putMap("lastBy", base.lastBy(by).sort(by))
                .putMap("minBy", base.minBy(by).sort(by))
                .putMap("maxBy", base.maxBy(by).sort(by))
                .putMap("sumBy", base.sumBy(by).sort(by))
                .putMap("avgBy", base.avgBy(by).sort(by))
                .putMap("medianBy", base.medianBy(by).sort(by))
                .putMap("stdBy", base.stdBy(by).sort(by))
                .putMap("varBy", base.varBy(by).sort(by))
                .putMap("absSumBy", base.absSumBy(by).sort(by))
                .build();

        // Execute them all in one batch and publish each result
        final LabeledValues<TableHandle> results = flight.session().batch().execute(tables);
        try {
            for (LabeledValue<TableHandle> result : results) {
                flight.session().publish(result.name(), result.value()).get(5, TimeUnit.SECONDS);
            }

            // Then update a random sample of the input rows each cycle
            final ColumnHeaders8<Integer, Integer, Byte, Short, Integer, Long, Float, Double> headers =
                    inputKey.header(groupKey).header(u).header(v).header(w).header(x).header(y).header(z);
            final TableHandle baseHandle = results.get("base");
            final Random random = new Random();
            final int sizeGuess = (int) Math.round(inputTableSize * updatePercentage);
            final long numCycles = cycles == null ? Long.MAX_VALUE : cycles;
            for (long cycle = 0; cycle < numCycles; ++cycle) {
                final ColumnHeaders8<Integer, Integer, Byte, Short, Integer, Long, Float, Double>.Rows rows =
                        headers.start(sizeGuess);
                for (int i = 0; i < inputTableSize; ++i) {
                    if (random.nextDouble() > updatePercentage) {
                        continue;
                    }
                    rows.row(i, random.nextInt(numGroups), (byte) random.nextInt(), (short) random.nextInt(),
                            random.nextInt(), random.nextLong(), random.nextFloat(), random.nextDouble());
                }
                flight.addToInputTable(baseHandle, rows.newTable(), allocator);
                Thread.sleep(sleepMillis);
            }
        } finally {
            for (LabeledValue<TableHandle> result : results) {
                result.value().close();
            }
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new AggByExample()).execute(args));
    }
}
