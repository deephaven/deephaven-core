/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Command(name = "add-to-input-table", mixinStandardHelpOptions = true,
        description = "Add to Input Table", version = "0.1.0")
class AddToInputTable extends FlightExampleBase {

    @Override
    protected void execute(FlightSession flight) throws Exception {
        final var header = ColumnHeader.of(
                ColumnHeader.ofBoolean("Boolean"),
                ColumnHeader.ofByte("Byte"),
                ColumnHeader.ofChar("Char"),
                ColumnHeader.ofShort("Short"),
                ColumnHeader.ofInt("Int"),
                ColumnHeader.ofLong("Long"),
                ColumnHeader.ofFloat("Float"),
                ColumnHeader.ofDouble("Double"),
                ColumnHeader.ofString("String"),
                ColumnHeader.ofInstant("Instant"),
                ColumnHeader.of("ByteVector", byte[].class));
        final TableSpec timestamp = InMemoryAppendOnlyInputTable.of(TableHeader.of(header));
        final TableSpec timestampLastBy =
                timestamp.aggBy(Collections.singletonList(Aggregation.AggLast("Instant")));

        final List<TableHandle> handles = flight.session().batch().execute(Arrays.asList(timestamp, timestampLastBy));
        try (
                final TableHandle timestampHandle = handles.get(0);
                final TableHandle timestampLastByHandle = handles.get(1)) {

            // publish so we can see in a UI
            flight.session().publish("timestamp", timestampHandle).get(5, TimeUnit.SECONDS);
            flight.session().publish("timestampLastBy", timestampLastByHandle).get(5, TimeUnit.SECONDS);

            while (true) {
                // Add a new row, at least once every second
                final NewTable newRow = header.row(true, (byte) 42, 'a', (short) 32_000, 1234567, 1234567890123L, 3.14f,
                        3.14d, "Hello, World", Instant.now(), "abc".getBytes()).newTable();
                flight.addToInputTable(timestampHandle, newRow, bufferAllocator).get(5, TimeUnit.SECONDS);
                Thread.sleep(ThreadLocalRandom.current().nextLong(1000));
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AddToInputTable()).execute(args);
        System.exit(execute);
    }
}
