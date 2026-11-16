//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.snapshot.SnapshotUtils;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DelegatingColumnSourceTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final long[] ROW_KEYS = {3, 0, 2, 0};

    @Test
    public void testFillsUnorderedKeysThroughADelegateThatDoes() {
        final ColumnSource<Long> delegate = ArrayBackedColumnSource.getMemoryColumnSource(new long[] {10, 11, 12, 13});
        final DelegatingColumnSource<Long, Long> wrapper = new DelegatingColumnSource<>(long.class, null, delegate);
        assertTrue(FillUnordered.providesFillUnordered(wrapper));
        assertArrayEquals(new long[] {13, 10, 12, 10}, fillUnordered(wrapper, false));
    }

    @Test
    public void testFillsUnorderedPreviousKeysThroughTheDelegate() {
        final WritableColumnSource<Long> delegate =
                ArrayBackedColumnSource.getMemoryColumnSource(new long[] {10, 11, 12, 13});
        delegate.startTrackingPrevValues();
        final DelegatingColumnSource<Long, Long> wrapper = new DelegatingColumnSource<>(long.class, null, delegate);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            for (int ii = 0; ii < 4; ++ii) {
                delegate.set(ii, 20L + ii);
            }
            assertArrayEquals(new long[] {13, 10, 12, 10}, fillUnordered(wrapper, true));
            assertArrayEquals(new long[] {23, 20, 22, 20}, fillUnordered(wrapper, false));
        });
    }

    @Test
    public void testDoesNotFillUnorderedKeysThroughADelegateThatDoesNot() {
        final ColumnSource<Long> delegate = TableTools.emptyTable(4).view("X = ii").getColumnSource("X", long.class);
        assertFalse(FillUnordered.providesFillUnordered(delegate));
        assertFalse(FillUnordered.providesFillUnordered(new DelegatingColumnSource<>(long.class, null, delegate)));
    }

    /**
     * A snapshot's trigger column of vectors fills direct vectors, which its delegate's unordered fills would not, so
     * it does not fill unordered keys although its delegate does.
     */
    @Test
    public void testDirectVectorSourceDoesNotFillUnorderedKeys() {
        final ColumnSource<IntVector> vectors = ArrayBackedColumnSource.getMemoryColumnSource(
                new IntVector[] {new IntVectorDirect(1, 2), new IntVectorDirect(3)}, IntVector.class, int.class);
        assertTrue(FillUnordered.providesFillUnordered(vectors));
        final ColumnSource<?> direct = SnapshotUtils.maybeTransformToDirectVectorColumnSource(vectors);
        assertTrue(direct instanceof DelegatingColumnSource);
        assertFalse(FillUnordered.providesFillUnordered(direct));
    }

    /**
     * Fill {@link #ROW_KEYS}, out of order and repeated, through {@code wrapper}.
     */
    private static long[] fillUnordered(final DelegatingColumnSource<Long, Long> wrapper, final boolean usePrev) {
        try (final ChunkSource.FillContext context = wrapper.makeFillContext(ROW_KEYS.length);
                final WritableLongChunk<RowKeys> keys = WritableLongChunk.writableChunkWrap(ROW_KEYS.clone());
                final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(ROW_KEYS.length)) {
            dest.setSize(0);
            if (usePrev) {
                wrapper.fillPrevChunkUnordered(context, dest, keys);
            } else {
                wrapper.fillChunkUnordered(context, dest, keys);
            }
            final long[] values = new long[dest.size()];
            dest.copyToTypedArray(0, values, 0, values.length);
            return values;
        }
    }
}
