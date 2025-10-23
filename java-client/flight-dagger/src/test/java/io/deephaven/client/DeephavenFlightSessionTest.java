//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableCreatorImpl;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.server.runner.RecordingErrorTransformer;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class DeephavenFlightSessionTest extends DeephavenFlightSessionTestBase {
    public static <T extends TableOperations<T, T>> T i132768(TableCreator<T> c) {
        return c.emptyTable(132768).view("I=i");
    }

    @Test
    public void getSchema() throws Exception {
        final TableSpec table = i132768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table)) {
            final Schema schema = flightSession.schema(handle.export());
            final Schema expected = new Schema(Collections.singletonList(
                    new Field("I", new FieldType(true, MinorType.INT.getType(), null, null), Collections.emptyList())));
            assertThat(metadataLess(schema)).isEqualTo(expected);
        }
    }

    @Test
    public void getStream() throws Exception {
        final TableSpec table = i132768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table);
                final FlightStream stream = flightSession.stream(handle)) {
            int numRows = 0;
            int flightCount = 0;
            while (stream.next()) {
                ++flightCount;
                numRows += stream.getRoot().getRowCount();
            }
            Assert.assertEquals(1, flightCount);
            Assert.assertEquals(132768, numRows);
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

    @Test
    public void updateByCountWhere() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .updateBy(List.of(UpdateByOperation.CumCountWhere("IS", "I%2==0"),
                        UpdateByOperation.RollingCountWhere(2, "RC", "I%2==0")));
        try (
                final TableHandle handle = flightSession.session().batch().execute(spec);
                final FlightStream stream = flightSession.stream(handle)) {
            int i = 0;
            long sum = 0;
            while (stream.next()) {
                final VectorSchemaRoot root = stream.getRoot();
                final BigIntVector cumVector = (BigIntVector) root.getVector("IS");
                final BigIntVector rollingVector = (BigIntVector) root.getVector("RC");
                final int rowCount = root.getRowCount();
                for (int r = 0; r < rowCount; ++r, ++i) {
                    if (r % 2 == 0) {
                        sum++;
                    }
                    assertThat(cumVector.get(r)).isEqualTo(sum);
                    assertThat(rollingVector.get(r)).isEqualTo(1);
                }
            }
            assertThat(i).isEqualTo(size);
        }
    }

    @Test
    public void updateByCountWhereNotPermitted() throws Exception {
        ((RecordingErrorTransformer) errorTransformer).clear();

        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .updateBy(UpdateByOperation.CumCountWhere("IS",
                        getClass().getCanonicalName() + ".disallowedFunction(I)"));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use static method disallowedFunction(int) on class io.deephaven.client.DeephavenFlightSessionTest");
        }

        ((RecordingErrorTransformer) errorTransformer).clear();
        final TableSpec spec2 = TableSpec.empty(size)
                .view("I=i")
                .updateBy(UpdateByOperation.RollingCountWhere(2, "RC",
                        getClass().getCanonicalName() + ".disallowedFunction(I)"));
        try {
            flightSession.session().batch().execute(spec2);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use static method disallowedFunction(int) on class io.deephaven.client.DeephavenFlightSessionTest");
        }
    }

    @Test
    public void updateByFormula() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i", "I2=100 + i")
                .updateBy(List.of(UpdateByOperation.RollingFormula(2, "sum(each)", "each"),
                        UpdateByOperation.RollingFormula(2, "RC=sum(I) + sum(I2)")));
        try (
                final TableHandle handle = flightSession.session().batch().execute(spec);
                final FlightStream stream = flightSession.stream(handle)) {
            int i = 0;
            while (stream.next()) {
                final VectorSchemaRoot root = stream.getRoot();
                final BigIntVector iVector = (BigIntVector) root.getVector("I");
                final BigIntVector i2Vector = (BigIntVector) root.getVector("I2");
                final BigIntVector rcVector = (BigIntVector) root.getVector("RC");
                final int rowCount = root.getRowCount();
                assertThat(iVector.get(0)).isEqualTo(0);
                assertThat(i2Vector.get(0)).isEqualTo(100);
                assertThat(rcVector.get(0)).isEqualTo(100);
                for (int r = 1; r < rowCount; ++r) {
                    int isum = (i + r) + (i + r) - 1;
                    assertThat(iVector.get(r)).isEqualTo(isum);
                    int i2sum = 200 + (i + r) + (i + r) - 1;
                    assertThat(i2Vector.get(r)).isEqualTo(i2sum);
                    assertThat(rcVector.get(r)).isEqualTo(isum + i2sum);
                }
                i += rowCount;
            }
            assertThat(i).isEqualTo(size);
        }
    }

    @Test
    public void updateByFormulaEachNotPermitted() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i", "I2=100 + i")
                .updateBy(List.of(UpdateByOperation.RollingFormula(2, "each.toArray()", "each")));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use method toArray() on interface io.deephaven.vector.IntVector");
        }
    }

    @Test
    public void updateByFormulaNoParamNotPermitted() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i", "I2=100 + i")
                .updateBy(List.of(UpdateByOperation.RollingFormula(2, "RC=I.toArray()")));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use method toArray() on interface io.deephaven.vector.IntVector");
        }
    }

    @Test
    public void updateByFormulaNotPermitted() throws Exception {
        final int size = 100;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i", "I2=100 + i")
                .updateBy(List.of(UpdateByOperation.RollingFormula(2, "RC=I2.toArray()")));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use method toArray() on interface io.deephaven.vector.IntVector");
        }
    }

    @Test
    public void aggFormula() throws Exception {
        final int size = 10;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .aggBy(Aggregation.AggFormula("Result", "sum(I)"));
        final long expected = IntStream.range(0, 10).sum();
        try (
                final TableHandle handle = flightSession.session().batch().execute(spec);
                final FlightStream stream = flightSession.stream(handle)) {
            assertThat(stream.next()).isTrue();
            final VectorSchemaRoot root = stream.getRoot();
            final BigIntVector longVector = (BigIntVector) root.getVector("Result");
            final int rowCount = root.getRowCount();
            assertThat(rowCount).isEqualTo(1);
            final long actual = longVector.get(0);
            assertThat(actual).isEqualTo(expected);
            assertThat(stream.next()).isFalse();
        }
    }

    @Test
    public void aggFormulaNotPermitted() throws Exception {
        ((RecordingErrorTransformer) errorTransformer).clear();

        final int size = 10;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .aggBy(Aggregation.AggFormula("Result", "I.toArray()"));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use method toArray() on interface io.deephaven.vector.IntVector");
        }
    }

    @Test
    public void aggCountWhere() throws Exception {
        final int size = 10;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .aggBy(Aggregation.AggCountWhere("Result", "I % 2 == 0"));
        final long expected = IntStream.range(0, 10).filter(x -> x % 2 == 0).count();
        try (
                final TableHandle handle = flightSession.session().batch().execute(spec);
                final FlightStream stream = flightSession.stream(handle)) {
            assertThat(stream.next()).isTrue();
            final VectorSchemaRoot root = stream.getRoot();
            final BigIntVector longVector = (BigIntVector) root.getVector("Result");
            final int rowCount = root.getRowCount();
            assertThat(rowCount).isEqualTo(1);
            final long actual = longVector.get(0);
            assertThat(actual).isEqualTo(expected);
            assertThat(stream.next()).isFalse();
        }
    }

    /**
     * Test function for not permitted tests.
     */
    public static boolean disallowedFunction(int i) {
        return true;
    }

    @Test
    public void aggCountWhereFormulaNotPermitted() throws Exception {
        ((RecordingErrorTransformer) errorTransformer).clear();

        final int size = 10;
        final TableSpec spec = TableSpec.empty(size)
                .view("I=i")
                .aggBy(Aggregation.AggCountWhere("Result", getClass().getCanonicalName() + ".disallowedFunction(I)"));
        try {
            flightSession.session().batch().execute(spec);
            Assert.fail("Expected exception");
        } catch (TableHandle.TableHandleException e) {
            assertThat(e.getMessage()).contains("INVALID_ARGUMENT");
            final List<Throwable> errors = ((RecordingErrorTransformer) errorTransformer).getErrors();
            assertThat(errors.size()).isEqualTo(1);
            assertThat(errors.get(0).getMessage()).contains(
                    "User expressions are not permitted to use static method disallowedFunction(int) on class io.deephaven.client.DeephavenFlightSessionTest");
        }
    }

    @Test
    public void doPutStream() throws Exception {
        try (final TableHandle ten = flightSession.session().execute(TableSpec.empty(10).view("I=i"));
                // DoGet
                final FlightStream tenStream = flightSession.stream(ten);
                // DoPut
                final TableHandle tenAgain = flightSession.putExport(tenStream)) {
            BarrageUtil.ConvertedArrowSchema tenSchema = BarrageUtil.convertArrowSchema(ten.response());
            BarrageUtil.ConvertedArrowSchema tenAgainSchema = BarrageUtil.convertArrowSchema(tenAgain.response());
            assertThat(tenSchema.tableDef).isEqualTo(tenAgainSchema.tableDef);
        }
    }

    @Test
    public void doPutNewTable() throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle newTableHandle = flightSession.putExport(newTable(), bufferAllocator)) {
            // ignore
        }
    }

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
                ColumnHeader.ofInstant("Instant"),
                ColumnHeader.of("ByteVector", byte[].class))
                .start(3)
                .row(true, (byte) 42, 'a', (short) 32_000, 1234567, 1234567890123L, 3.14f, 3.14d, "Hello, World",
                        Instant.now(), "abc".getBytes())
                .row(null, null, null, null, null, null, null, null, null, (Instant) null, (byte[]) null)
                .row(false, (byte) -42, 'b', (short) -32_000, -1234567, -1234567890123L, -3.14f, -3.14d, "Goodbye.",
                        Instant.ofEpochMilli(0), new byte[] {0x32, 0x02, 0x17, 0x42})
                .newTable();
    }
}
