package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.LabeledValue;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders8;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.LabeledTables.Builder;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

abstract class AggInputBase extends FlightExampleBase {

    public static final ColumnHeader<Integer> INPUT_KEY = ColumnHeader.ofInt("InputKey");
    public static final ColumnHeader<Integer> GROUP_KEY = ColumnHeader.ofInt("GroupKey");

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

    public abstract LabeledTables labeledTables(TableSpec base);

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final ColumnHeader<Integer> inputKey = INPUT_KEY;
        final ColumnHeader<Integer> groupKey = GROUP_KEY;
        final ColumnHeader<Byte> u = ColumnHeader.ofByte("U");
        final ColumnHeader<Short> v = ColumnHeader.ofShort("V");
        final ColumnHeader<Integer> w = ColumnHeader.ofInt("W");
        final ColumnHeader<Long> x = ColumnHeader.ofLong("X");
        final ColumnHeader<Float> y = ColumnHeader.ofFloat("Y");
        final ColumnHeader<Double> z = ColumnHeader.ofDouble("Z");

        final Builder builder = LabeledTables.builder();

        final TableSpec spec = InMemoryKeyBackedInputTable.of(TableHeader.of(inputKey, groupKey, u, v, w, x, y, z),
                Collections.singletonList(inputKey.name()));
        builder.putMap("base", spec);
        for (LabeledTable labeledTable : labeledTables(spec)) {
            builder.addTables(labeledTable);
        }
        final LabeledTables tables = builder.build();
        final LabeledValues<TableHandle> results = flight.session().batch().execute(tables);
        try {
            for (LabeledValue<TableHandle> result : results) {
                flight.session().publish(result.name(), result.value()).get(5, TimeUnit.SECONDS);
            }

            final ColumnHeaders8<Integer, Integer, Byte, Short, Integer, Long, Float, Double> headers =
                    inputKey.header(groupKey).header(u).header(v).header(w).header(x).header(y).header(z);

            final TableHandle base = results.get("base");

            final Random random = new Random();

            final int sizeGuess = (int) Math.round(inputTableSize * updatePercentage);

            while (true) {
                final ColumnHeaders8<Integer, Integer, Byte, Short, Integer, Long, Float, Double>.Rows rows =
                        headers.start(sizeGuess);
                for (int i = 0; i < inputTableSize; ++i) {
                    if (random.nextDouble() > updatePercentage) {
                        continue;
                    }
                    final int group = random.nextInt(numGroups);
                    final byte u_ = (byte) random.nextInt();
                    final short v_ = (short) random.nextInt();
                    final int w_ = random.nextInt();
                    final long x_ = random.nextLong();
                    final float y_ = random.nextFloat();
                    final double z_ = random.nextDouble();
                    rows.row(i, group, u_, v_, w_, x_, y_, z_);
                }
                flight.addToInputTable(base, rows.newTable(), bufferAllocator);
                Thread.sleep(sleepMillis);
            }
        } finally {
            for (LabeledValue<TableHandle> result : results) {
                result.value().close();
            }
        }
    }
}
