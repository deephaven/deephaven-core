//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.BlinkInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.type.Type;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Command(name = "add-to-blink-table", mixinStandardHelpOptions = true,
        description = "Add to Blink Table", version = "0.1.0")
class AddToBlinkTable extends FlightExampleBase {

    public <T> void addAndPublish(FlightSession flight, String name, Type<T> type, T... data)
            throws TableHandleException, InterruptedException, ExecutionException, TimeoutException {
        final ColumnHeader<T> header = ColumnHeader.of(name, type);
        final ColumnHeader<T>.Rows rows = header.start(data.length);
        for (T datum : data) {
            rows.row(datum);
        }
        final NewTable newTable = rows.newTable();
        final BlinkInputTable blinkTable = BlinkInputTable.of(TableHeader.of(header));
        final TableSpec tail = blinkTable.tail(32);
        final List<TableHandle> handles = flight.session().execute(List.of(blinkTable, tail));
        try (
                final TableHandle blinkHandle = handles.get(0);
                final TableHandle output = handles.get(1)) {
            flight.addToInputTable(blinkHandle, newTable, bufferAllocator).get(5, TimeUnit.SECONDS);
            flight.session().publish(name + "_Table", output).get(5, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void execute(FlightSession flight) throws Exception {
        addAndPublish(flight, "Boolean", Type.booleanType(), null, true, false);
        addAndPublish(flight, "Byte", Type.byteType(), null, (byte) 42);
        addAndPublish(flight, "Char", Type.charType(), null, 'a');
        addAndPublish(flight, "Short", Type.shortType(), null, (short) 42);
        addAndPublish(flight, "Int", Type.intType(), null, 42);
        addAndPublish(flight, "Long", Type.longType(), null, 42L);
        addAndPublish(flight, "Float", Type.floatType(), null, 42.24f);
        addAndPublish(flight, "Double", Type.doubleType(), null, 42.24);

        addAndPublish(flight, "BoxedBoolean", Type.booleanType().boxedType(), null, true, false);
        addAndPublish(flight, "BoxedByte", Type.byteType().boxedType(), null, (byte) 42);
        addAndPublish(flight, "BoxedChar", Type.charType().boxedType(), null, 'a');
        addAndPublish(flight, "BoxedShort", Type.shortType().boxedType(), null, (short) 42);
        addAndPublish(flight, "BoxedInt", Type.intType().boxedType(), null, 42);
        addAndPublish(flight, "BoxedLong", Type.longType().boxedType(), null, 42L);
        addAndPublish(flight, "BoxedFloat", Type.floatType().boxedType(), null, 42.24f);
        addAndPublish(flight, "BoxedDouble", Type.doubleType().boxedType(), null, 42.24);

        addAndPublish(flight, "String", Type.stringType(), null, "", "Hello");
        addAndPublish(flight, "Instant", Type.instantType(), null, Instant.now());

        addAndPublish(flight, "BooleanArray", Type.booleanType().arrayType(),
                null,
                new boolean[] {true},
                new boolean[] {true, false});
        addAndPublish(flight, "ByteArray", Type.byteType().arrayType(),
                null,
                new byte[] {},
                new byte[] {(byte) 42, (byte) 43});
        addAndPublish(flight, "CharArray", Type.charType().arrayType(),
                null,
                new char[] {},
                new char[] {'a', 'b'});
        addAndPublish(flight, "ShortArray", Type.shortType().arrayType(),
                null,
                new short[] {},
                new short[] {(short) 42, (short) 43});
        addAndPublish(flight, "IntArray", Type.intType().arrayType(),
                null,
                new int[] {},
                new int[] {42, 43});
        addAndPublish(flight, "LongArray", Type.longType().arrayType(),
                null,
                new long[] {},
                new long[] {42L, 43L});
        addAndPublish(flight, "FloatArray", Type.floatType().arrayType(),
                null,
                new float[] {},
                new float[] {42.42f, 43.43f});
        addAndPublish(flight, "DoubleArray", Type.doubleType().arrayType(),
                null,
                new double[] {},
                new double[] {42.42, 43.43});

        addAndPublish(flight, "StringArray", Type.stringType().arrayType(),
                null,
                new String[] {},
                new String[] {null, "", "Hello World"});
        addAndPublish(flight, "InstantArray", Type.instantType().arrayType(),
                null,
                new Instant[] {},
                new Instant[] {null, Instant.now()});
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AddToBlinkTable()).execute(args);
        System.exit(execute);
    }
}
