//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.VectorSchemaRootAdapter;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableCreatorImpl;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.server.runner.RecordingErrorTransformer;
import io.deephaven.server.util.TestDataUtil;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.Validator;
import org.junit.Assert;
import org.junit.Test;

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
            assertThat(stripMetadata(schema)).isEqualTo(expected);
        }
    }

    @Test
    public void getStream() throws Exception {
        final TableSpec table = i132768(TableCreatorImpl.INSTANCE);
        try (final TableHandle handle = flightSession.session().execute(table);
                final FlightStream stream = flightSession.stream(handle)) {
            int numRows = 0;
            while (stream.next()) {
                numRows += stream.getRoot().getRowCount();
            }
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

    /**
     * Send a table to the server, pull it back to the client, and verify that the data is the same.
     */
    @Test
    public void doPutNewTable() throws Exception {
        final NewTable newTable = TestDataUtil.getTestDataNewTable();

        try (final TableHandle newTableHandle = flightSession.putExport(newTable, bufferAllocator);
                final FlightStream stream = flightSession.stream(newTableHandle);
                final VectorSchemaRoot expectedRoot = VectorSchemaRootAdapter.of(newTable, bufferAllocator)) {

            assertThat(stream.next()).isTrue();
            final VectorSchemaRoot retrievedRoot = stream.getRoot();

            try (final VectorSchemaRoot expectedRootNoMetadata =
                    transferStrippingMetadata(expectedRoot, bufferAllocator);
                    final VectorSchemaRoot retrievedRootNoMetadata =
                            transferStrippingMetadata(retrievedRoot, bufferAllocator)) {
                // Server-side metadata can differ while values are equivalent.
                Validator.compareVectorSchemaRoot(retrievedRootNoMetadata, expectedRootNoMetadata);

                // Validator.compareVectorSchemaRoot provides helpful error messages but performs approximate
                // comparisons for some field types (e.g. floating point numbers). Validate again with
                // VectorSchemaRoot.equals() to ensure the data matches exactly.
                assertThat(retrievedRootNoMetadata)
                        // Specify comparison method explicitly; VectorSchemaRoot does not implement equals(Object)
                        .usingEquals(VectorSchemaRoot::equals)
                        .isEqualTo(expectedRootNoMetadata);
            }

            assertThat(stream.next()).isFalse();
        }
    }

    /**
     * Creates a new {@link VectorSchemaRoot} backed by the same data buffers as {@code root}, but with all Arrow
     * metadata removed from the schema and fields. Ownership of the underlying buffers is <em>transferred</em> from
     * {@code root} to the returned root (zero-copy move via {@link TransferPair#transfer()}); {@code root} should not
     * be used after this call.
     */
    private static VectorSchemaRoot transferStrippingMetadata(VectorSchemaRoot root, BufferAllocator allocator) {
        final VectorSchemaRoot dest = VectorSchemaRoot.create(stripMetadata(root.getSchema()), allocator);
        final List<FieldVector> sourceVectors = root.getFieldVectors();
        final List<FieldVector> destVectors = dest.getFieldVectors();
        for (int i = 0; i < sourceVectors.size(); i++) {
            final TransferPair tp = sourceVectors.get(i).makeTransferPair(destVectors.get(i));
            tp.transfer();
        }
        dest.setRowCount(root.getRowCount());
        return dest;
    }

    /**
     * Returns a copy of {@code schema} with all key-value metadata removed from the schema itself and every field
     * (recursively).
     */
    private static Schema stripMetadata(Schema schema) {
        return new Schema(
                schema.getFields().stream().map(DeephavenFlightSessionTest::stripMetadata).collect(Collectors.toList()),
                null);
    }

    /**
     * Returns a copy of {@code field} with key-value metadata removed, applied recursively to child fields.
     */
    private static Field stripMetadata(Field field) {
        return new Field(field.getName(), stripMetadata(field.getFieldType()),
                field.getChildren().stream().map(DeephavenFlightSessionTest::stripMetadata)
                        .collect(Collectors.toList()));
    }

    /**
     * Returns a copy of {@code fieldType} with key-value metadata removed, preserving nullability, Arrow type, and
     * dictionary encoding.
     */
    private static FieldType stripMetadata(FieldType fieldType) {
        return new FieldType(fieldType.isNullable(), fieldType.getType(), fieldType.getDictionary(), null);
    }

}
