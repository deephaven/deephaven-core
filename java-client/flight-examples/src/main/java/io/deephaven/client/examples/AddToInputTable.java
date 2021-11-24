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
        final ColumnHeader<Instant> header = ColumnHeader.ofInstant("Timestamp");

        final TableSpec timestamp = InMemoryAppendOnlyInputTable.of(TableHeader.of(header));
        final TableSpec timestampLastBy =
                timestamp.aggBy(Collections.singletonList(Aggregation.AggLast("Timestamp")));

        final List<TableHandle> handles = flight.session().batch().execute(Arrays.asList(timestamp, timestampLastBy));
        try (
                final TableHandle timestampHandle = handles.get(0);
                final TableHandle timestampLastByHandle = handles.get(1)) {

            // publish so we can see in a UI
            flight.session().publish("timestamp", timestampHandle).get(5, TimeUnit.SECONDS);
            flight.session().publish("timestampLastBy", timestampLastByHandle).get(5, TimeUnit.SECONDS);

            while (true) {
                // Add a new row, at least once every second
                final NewTable newRow = header.row(Instant.now()).newTable();
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
