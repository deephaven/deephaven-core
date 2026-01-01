//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.impl.*;
import io.deephaven.proto.backplane.grpc.InputTableValidationErrorList;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.*;
import io.grpc.protobuf.StatusProto;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

            flight.session().console("groovy").get().executeCode(
                    "tsv = io.deephaven.server.table.inputtables.RangeValidatingInputTable.make(timestamp, \"Int\", 0, 30)");

            final TableHandle tsv = flight.session().ticket(TicketTable.fromQueryScopeField("tsv").ticket());

            int rowCount = 0;

            while (true) {
                // Add a new row, at least once every second
                final NewTable newRow =
                        header.row(true, (byte) 42, 'a', (short) 32_000, rowCount++, 1234567890123L, 3.14f,
                                3.14d, "Hello, World", Instant.now(), "abc".getBytes()).newTable();
                try {
                    flight.addToInputTable(tsv, newRow, bufferAllocator).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    Status status = StatusProto.fromThrowable(e);
                    System.out.println(Code.forNumber(status.getCode()) + ": " + status.getMessage());
                    final String expected =
                            "type.googleapis.com/" + InputTableValidationErrorList.getDescriptor().getFullName();
                    int detailCount = status.getDetailsCount();
                    for (int di = 0; di < detailCount; ++di) {
                        Any x = status.getDetails(di);
                        if (x.getTypeUrl().equals(expected)) {
                            final InputTableValidationErrorList errorList =
                                    x.unpack(InputTableValidationErrorList.class);
                            System.out.println(errorList);
                        } else {
                            System.out.println("Unknown type: " + x);
                        }
                    }

                }
                Thread.sleep(ThreadLocalRandom.current().nextLong(1000));
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AddToInputTable()).execute(args);
        System.exit(execute);
    }
}
