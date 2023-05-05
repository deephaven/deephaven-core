/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableCreatorImpl;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class DeephavenFlightSessionTest extends DeephavenFlightSessionTestBase {
    public static <T extends TableOperations<T, T>> T i32768(TableCreator<T> c) {
        return c.emptyTable(32768).view("I=i");
    }

    @Test
    public void getSchema() throws Exception {
        final TableSpec table = i32768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table)) {
            final Schema schema = flightSession.schema(handle.export());
            final Schema expected = new Schema(Collections.singletonList(
                    new Field("I", new FieldType(true, MinorType.INT.getType(), null, null), Collections.emptyList())));
            assertThat(metadataLess(schema)).isEqualTo(expected);
        }
    }

    @Test
    public void getStream() throws Exception {
        final TableSpec table = i32768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table);
                final FlightStream stream = flightSession.stream(handle)) {
            int numRows = 0;
            while (stream.next()) {
                numRows += stream.getRoot().getRowCount();
            }
            Assert.assertEquals(32768, numRows);
        }
    }

    @Test
    public void updateBy() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .updateBy(UpdateByOperation.CumSum("I"));
        try (
                final TableHandle handle = flightSession.session().batch().execute(spec);
                final FlightStream stream = flightSession.stream(handle)) {
            int i = 0;
            long sum = 0;
            while (stream.next()) {
                final VectorSchemaRoot root = stream.getRoot();
                final BigIntVector longVector = (BigIntVector) root.getVector("I");
                final int rowCount = root.getRowCount();
                for (int r = 0; r < rowCount; ++r, ++i) {
                    sum += i;
                    final long actual = longVector.get(r);
                    assertThat(actual).isEqualTo(sum);
                }
            }
            assertThat(i).isEqualTo(size);
        }
    }

    // TODO (deephaven-core#1373): Hook up doPut integration unit testing
    // @Test
    // public void doPutStream() throws Exception {
    // try (
    // final TableHandle ten = flightSession.session().execute(TableSpec.empty(10).view("I=i"));
    // // DoGet
    // final FlightStream tenStream = flightSession.stream(ten);
    // // DoPut
    // final TableHandle tenAgain = flightSession.put(tenStream)) {
    // assertThat(tenAgain.response().getSchemaHeader()).isEqualTo(ten.response().getSchemaHeader());
    // }
    // }
    //
    // @Test
    // public void doPutNewTable() throws TableHandleException, InterruptedException {
    // try (final TableHandle newTableHandle = flightSession.put(newTable(), bufferAllocator)) {
    // // ignore
    // }
    // }

    private static Schema metadataLess(Schema schema) {
        return new Schema(
                schema.getFields().stream().map(DeephavenFlightSessionTest::metadataLess).collect(Collectors.toList()),
                null);
    }

    private static Field metadataLess(Field field) {
        return new Field(field.getName(), metadataLess(field.getFieldType()), field.getChildren());
    }

    private static FieldType metadataLess(FieldType fieldType) {
        return new FieldType(fieldType.isNullable(), fieldType.getType(), fieldType.getDictionary(), null);
    }

    private NewTable newTable() {
        return ColumnHeader.of(
                ColumnHeader.ofBoolean("Boolean"),
                ColumnHeader.ofByte("Byte"),
                ColumnHeader.ofChar("Char"),
                ColumnHeader.ofShort("Short"),
                ColumnHeader.ofInt("Int"),
                ColumnHeader.ofLong("Long"),
                ColumnHeader.ofFloat("Float"),
                ColumnHeader.ofDouble("Double"),
                ColumnHeader.ofString("String"),
                ColumnHeader.ofInstant("Instant"))
                .start(3)
                .row(true, (byte) 42, 'a', (short) 32_000, 1234567, 1234567890123L, 3.14f, 3.14d, "Hello, World",
                        Instant.now())
                .row(null, null, null, null, null, null, null, null, null, (Instant) null)
                .row(false, (byte) -42, 'b', (short) -32_000, -1234567, -1234567890123L, -3.14f, -3.14d, "Goodbye.",
                        Instant.ofEpochMilli(0))
                .newTable();
    }
}
