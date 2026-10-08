//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.BlinkInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * For each supported column type, creates a one-column blink input table, adds a few rows including null, and publishes
 * a tail of it as {@code <Type>_Table}.
 */
@Command(name = "add-to-blink-table", mixinStandardHelpOptions = true,
        description = "Add to Blink Table", version = "0.1.0")
class AddToBlinkTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read and written over Flight
        final BufferAllocator allocator = new RootAllocator();
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data)
        try (final FlightSession flight = factory.newFlightSession()) {
            addAndPublish(flight, allocator, "Boolean", Type.booleanType(), null, true, false);
            addAndPublish(flight, allocator, "Byte", Type.byteType(), null, (byte) 42);
            addAndPublish(flight, allocator, "Char", Type.charType(), null, 'a');
            addAndPublish(flight, allocator, "Short", Type.shortType(), null, (short) 42);
            addAndPublish(flight, allocator, "Int", Type.intType(), null, 42);
            addAndPublish(flight, allocator, "Long", Type.longType(), null, 42L);
            addAndPublish(flight, allocator, "Float", Type.floatType(), null, 42.24f);
            addAndPublish(flight, allocator, "Double", Type.doubleType(), null, 42.24);

            addAndPublish(flight, allocator, "BoxedBoolean", Type.booleanType().boxedType(), null, true, false);
            addAndPublish(flight, allocator, "BoxedByte", Type.byteType().boxedType(), null, (byte) 42);
            addAndPublish(flight, allocator, "BoxedChar", Type.charType().boxedType(), null, 'a');
            addAndPublish(flight, allocator, "BoxedShort", Type.shortType().boxedType(), null, (short) 42);
            addAndPublish(flight, allocator, "BoxedInt", Type.intType().boxedType(), null, 42);
            addAndPublish(flight, allocator, "BoxedLong", Type.longType().boxedType(), null, 42L);
            addAndPublish(flight, allocator, "BoxedFloat", Type.floatType().boxedType(), null, 42.24f);
            addAndPublish(flight, allocator, "BoxedDouble", Type.doubleType().boxedType(), null, 42.24);

            addAndPublish(flight, allocator, "String", Type.stringType(), null, "", "Hello");
            addAndPublish(flight, allocator, "Instant", Type.instantType(), null, Instant.now());

            addAndPublish(flight, allocator, "BooleanArray", Type.booleanType().arrayType(),
                    null,
                    new boolean[] {true},
                    new boolean[] {true, false});
            addAndPublish(flight, allocator, "ByteArray", Type.byteType().arrayType(),
                    null,
                    new byte[] {},
                    new byte[] {(byte) 42, (byte) 43});
            addAndPublish(flight, allocator, "CharArray", Type.charType().arrayType(),
                    null,
                    new char[] {},
                    new char[] {'a', 'b'});
            addAndPublish(flight, allocator, "ShortArray", Type.shortType().arrayType(),
                    null,
                    new short[] {},
                    new short[] {(short) 42, (short) 43});
            addAndPublish(flight, allocator, "IntArray", Type.intType().arrayType(),
                    null,
                    new int[] {},
                    new int[] {42, 43});
            addAndPublish(flight, allocator, "LongArray", Type.longType().arrayType(),
                    null,
                    new long[] {},
                    new long[] {42L, 43L});
            addAndPublish(flight, allocator, "FloatArray", Type.floatType().arrayType(),
                    null,
                    new float[] {},
                    new float[] {42.42f, 43.43f});
            addAndPublish(flight, allocator, "DoubleArray", Type.doubleType().arrayType(),
                    null,
                    new double[] {},
                    new double[] {42.42, 43.43});

            addAndPublish(flight, allocator, "StringArray", Type.stringType().arrayType(),
                    null,
                    new String[] {},
                    new String[] {null, "", "Hello World"});
            addAndPublish(flight, allocator, "InstantArray", Type.instantType().arrayType(),
                    null,
                    new Instant[] {},
                    new Instant[] {null, Instant.now()});
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    @SafeVarargs
    private static <T> void addAndPublish(FlightSession flight, BufferAllocator allocator, String name, Type<T> type,
            T... data) throws TableHandleException, InterruptedException, ExecutionException, TimeoutException {
        final ColumnHeader<T> header = ColumnHeader.of(name, type);
        final ColumnHeader<T>.Rows rows = header.start(data.length);
        for (T datum : data) {
            rows.row(datum);
        }
        final NewTable newTable = rows.newTable();
        // A blink table holds only the rows added in the current update cycle, so a tail of it is kept to look at
        final BlinkInputTable blinkTable = BlinkInputTable.of(TableHeader.of(header));
        final TableSpec tail = blinkTable.tail(32);
        final List<TableHandle> handles = flight.session().execute(List.of(blinkTable, tail));
        try (
                final TableHandle blinkHandle = handles.get(0);
                final TableHandle output = handles.get(1)) {
            flight.addToInputTable(blinkHandle, newTable, allocator).get(5, TimeUnit.SECONDS);
            flight.session().publish(name + "_Table", output).get(5, TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new AddToBlinkTable()).execute(args));
    }
}
