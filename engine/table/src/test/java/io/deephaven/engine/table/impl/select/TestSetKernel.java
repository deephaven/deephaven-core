//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static org.junit.Assert.assertEquals;

/**
 * Tests which floating point keys a {@link SetKernel} holds as one key, and which keys it matches, for single column
 * and compound keys alike.
 */
public class TestSetKernel {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testFloatKeys() {
        final float[] setValues = {NULL_FLOAT, Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f, 1.0f,
                Float.POSITIVE_INFINITY, Float.NaN};
        // NaN matches NaN, and -0.0 and 0.0 are one key, as they are for ==.
        final float[] presentValues = {NULL_FLOAT, Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f, 1.0f,
                Float.POSITIVE_INFINITY, Float.NaN, Float.intBitsToFloat(0x7fc00001)};
        final float[] absentValues = {-2.0f, 2.0f, Float.MIN_VALUE, -Float.MIN_VALUE, Float.MAX_VALUE,
                Math.nextUp(NULL_FLOAT)};

        final Table setTable = newTable(floatCol("X", setValues));
        final Table probeTable = newTable(floatCol("X", concat(presentValues, absentValues)));
        checkKeys(setTable, 7, probeTable, presentValues.length);
    }

    @Test
    public void testDoubleKeys() {
        final double[] setValues = {NULL_DOUBLE, Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0,
                Double.POSITIVE_INFINITY, Double.NaN};
        // NaN matches NaN, and -0.0 and 0.0 are one key, as they are for ==.
        final double[] presentValues = {NULL_DOUBLE, Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0,
                Double.POSITIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L)};
        final double[] absentValues = {-2.0, 2.0, Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE,
                Math.nextUp(NULL_DOUBLE)};

        final Table setTable = newTable(doubleCol("X", setValues));
        final Table probeTable = newTable(doubleCol("X", concat(presentValues, absentValues)));
        checkKeys(setTable, 7, probeTable, presentValues.length);
    }

    /**
     * Build sets from {@code setTable}'s column X, alone and in compound keys of up to four columns at each position,
     * the other columns holding one value, and check that each holds {@code expectedSize} keys, that the first
     * {@code presentCount} rows of {@code probeTable} match, and that the rest do not. Two columns use a pregenerated
     * kernel, three a kernel compiled when needed, and four one over {@code ArrayTuple}s.
     */
    private static void checkKeys(
            final Table setTable,
            final int expectedSize,
            final Table probeTable,
            final int presentCount) {
        final Table setWithConstant = setTable.update("C = 7");
        final Table probeWithConstant = probeTable.update("C = 7");
        for (int columns = 1; columns <= 4; ++columns) {
            for (int position = 0; position < columns; ++position) {
                final ColumnSource<?>[] setSources = new ColumnSource[columns];
                final ColumnSource<?>[] probeSources = new ColumnSource[columns];
                for (int ci = 0; ci < columns; ++ci) {
                    final String name = ci == position ? "X" : "C";
                    setSources[ci] = setWithConstant.getColumnSource(name);
                    probeSources[ci] = probeWithConstant.getColumnSource(name);
                }
                final String description = columns + " columns, X at " + position;
                checkKeys(description, SetKernel.create(setSources, setWithConstant.getRowSet(), false),
                        expectedSize, probeWithConstant, probeSources, presentCount);
            }
        }
    }

    private static void checkKeys(
            final String description,
            final SetKernel keySet,
            final int expectedSize,
            final Table probeTable,
            final ColumnSource<?>[] probeSources,
            final int presentCount) {
        assertEquals(description, expectedSize, keySet.size());

        final int probeSize = probeTable.intSize();
        // noinspection unchecked
        final WritableChunk<Values>[] keyChunks = new WritableChunk[probeSources.length];
        try (final WritableLongChunk<OrderedRowKeys> rowKeys = WritableLongChunk.makeWritableChunk(probeSize);
                final WritableLongChunk<OrderedRowKeys> results = WritableLongChunk.makeWritableChunk(probeSize)) {
            for (int ci = 0; ci < probeSources.length; ++ci) {
                keyChunks[ci] = probeSources[ci].getChunkType().makeWritableChunk(probeSize);
                try (final ColumnSource.FillContext fillContext = probeSources[ci].makeFillContext(probeSize)) {
                    probeSources[ci].fillChunk(fillContext, keyChunks[ci], probeTable.getRowSet());
                }
            }
            probeTable.getRowSet().fillRowKeyChunk(rowKeys);

            keySet.matchValues(keyChunks, rowKeys, results, true);
            assertEquals(description, presentCount, results.size());
            for (int ii = 0; ii < presentCount; ++ii) {
                assertEquals(description + ", present row " + ii, ii, results.get(ii));
            }

            keySet.matchValues(keyChunks, rowKeys, results, false);
            assertEquals(description, probeSize - presentCount, results.size());
            for (int ii = presentCount; ii < probeSize; ++ii) {
                assertEquals(description + ", absent row " + ii, ii, results.get(ii - presentCount));
            }
        } finally {
            SafeCloseable.closeAll(keyChunks);
        }
    }

    private static float[] concat(final float[] first, final float[] second) {
        final float[] result = new float[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static double[] concat(final double[] first, final double[] second) {
        final double[] result = new double[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
