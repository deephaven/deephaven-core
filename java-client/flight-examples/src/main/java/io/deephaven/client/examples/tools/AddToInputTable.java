//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.examples.AuthenticationOptions;
import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.proto.backplane.grpc.DeephavenTableMetadata;
import io.deephaven.proto.backplane.grpc.InputTableColumnInfo;
import io.deephaven.proto.backplane.grpc.InputTableMetadata;
import io.deephaven.proto.backplane.grpc.InputTableValidationErrorList;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.grpc.protobuf.StatusProto;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Validation walkthrough: creates an append-only input table with one column of every type, wraps it server-side in a
 * validator that only accepts {@code Int} values from 0 to 30, reads the validation rules back from the table's schema
 * metadata, and then adds rows until the validator starts rejecting them, printing the structured error each time. The
 * validator is a server test utility, reached through jpy from the Python console.
 */
@Command(name = "add-to-input-table", mixinStandardHelpOptions = true,
        description = "Add to Input Table", version = "0.1.0")
class AddToInputTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"--rows"}, description = "The number of rows to add before exiting, unlimited if unset")
    Long rows;

    @Option(names = {"--sleep-millis"},
            description = "The sleep milliseconds between rows, defaults to a random duration under one second")
    Long sleepMillis;

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
            addRows(flight, allocator);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private void addRows(FlightSession flight, BufferAllocator allocator) throws Exception {
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

            // Wrap the input table in a server-side validator. RangeValidatingInputTable is a test utility that
            // ships with the server, so it is reached through jpy from the Python console.
            flight.session().console("python").get().executeCode(String.join("\n",
                    "import jpy",
                    "from deephaven.table import Table",
                    "_RangeValidatingInputTable = jpy.get_type(",
                    "    'io.deephaven.server.table.inputtables.RangeValidatingInputTable')",
                    "tsv = Table(_RangeValidatingInputTable.make(timestamp.j_table, 'Int', 0, 30))"));

            final TableHandle tsv = flight.session().ticket(TicketTable.fromQueryScopeField("tsv").ticket());

            // The schema's custom metadata describes each column's role and restrictions
            final Schema schema = SchemaHelper.flatbufSchema(tsv.response());
            final KeyValue.Vector mdv = schema.customMetadataVector();
            for (int ii = 0; ii < mdv.length(); ++ii) {
                final KeyValue keyValue = mdv.get(ii);
                if (keyValue.key().equals("deephaven:tableMetadata")) {
                    final String encoded = keyValue.value();
                    final byte[] decoded = Base64.getDecoder().decode(encoded);
                    final DeephavenTableMetadata metadata = DeephavenTableMetadata.parseFrom(decoded);
                    if (!metadata.hasInputTableMetadata()) {
                        throw new IllegalStateException("No input table metadata found");
                    }
                    final InputTableMetadata inputTableMetadata = metadata.getInputTableMetadata();
                    for (Map.Entry<String, InputTableColumnInfo> columnInfoEntry : inputTableMetadata.getColumnInfoMap()
                            .entrySet()) {
                        final StringBuilder infoString = new StringBuilder();
                        switch (columnInfoEntry.getValue().getKind()) {
                            case KIND_UNKNOWN:
                                throw new IllegalArgumentException(
                                        "Unknown column kind encountered: " + columnInfoEntry.getValue().getKind());
                            case KIND_KEY:
                                infoString.append("Key");
                                break;
                            case KIND_VALUE:
                                infoString.append("Value");
                                break;
                            case UNRECOGNIZED:
                                throw new IllegalArgumentException("Unrecognized column kind encountered: "
                                        + columnInfoEntry.getValue().getKind().getNumber());
                        }
                        if (columnInfoEntry.getValue().getRestrictionsCount() > 0) {
                            final List<Any> restrictionsList = columnInfoEntry.getValue().getRestrictionsList();
                            infoString.append(
                                    restrictionsList.stream().map(Any::getTypeUrl)
                                            .collect(Collectors.joining(", ", "; (", ")")));
                        }
                        System.out.println(columnInfoEntry.getKey() + " -> " + infoString);
                    }
                }
            }

            final long numRows = rows == null ? Long.MAX_VALUE : rows;
            int rowCount = 0;

            while (rowCount < numRows) {
                // Add a new row, at least once every second
                final NewTable newRow =
                        header.row(true, (byte) 42, 'a', (short) 32_000, rowCount++, 1234567890123L, 3.14f,
                                3.14d, "Hello, World", Instant.now(), "abc".getBytes()).newTable();
                try {
                    flight.addToInputTable(tsv, newRow, allocator).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // Validation failures arrive as a gRPC status with a structured error list in the details
                    final Status status = StatusProto.fromThrowable(e);
                    System.out.println(Code.forNumber(status.getCode()) + ": " + status.getMessage());
                    final String expected =
                            "type.googleapis.com/" + InputTableValidationErrorList.getDescriptor().getFullName();
                    for (Any detail : status.getDetailsList()) {
                        if (detail.getTypeUrl().equals(expected)) {
                            System.out.println(detail.unpack(InputTableValidationErrorList.class));
                        } else {
                            System.out.println("Unknown type: " + detail);
                        }
                    }
                }
                Thread.sleep(sleepMillis == null ? ThreadLocalRandom.current().nextLong(1000) : sleepMillis);
            }
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new AddToInputTable()).execute(args));
    }
}
