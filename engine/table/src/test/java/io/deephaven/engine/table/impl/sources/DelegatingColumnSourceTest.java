//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DelegatingColumnSourceTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testFillsUnorderedKeysThroughADelegateThatDoes() {
        final ColumnSource<Long> delegate = ArrayBackedColumnSource.getMemoryColumnSource(new long[] {10, 11, 12, 13});
        final DelegatingColumnSource<Long, Long> wrapper = new DelegatingColumnSource<>(long.class, null, delegate);
        assertTrue(FillUnordered.providesFillUnordered(wrapper));

        final long[] rowKeys = {3, 0, 2, 0};
        try (final ChunkSource.FillContext context = wrapper.makeFillContext(rowKeys.length);
                final WritableLongChunk<RowKeys> keys = WritableLongChunk.writableChunkWrap(rowKeys);
                final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(rowKeys.length)) {
            wrapper.fillChunkUnordered(context, dest, keys);
            assertEquals(rowKeys.length, dest.size());
            for (int ii = 0; ii < rowKeys.length; ++ii) {
                assertEquals(10 + rowKeys[ii], dest.get(ii));
            }
        }
    }

    @Test
    public void testDoesNotFillUnorderedKeysThroughADelegateThatDoesNot() {
        final ColumnSource<Long> delegate = TableTools.emptyTable(4).view("X = ii").getColumnSource("X", long.class);
        assertFalse(FillUnordered.providesFillUnordered(delegate));
        assertFalse(FillUnordered.providesFillUnordered(new DelegatingColumnSource<>(long.class, null, delegate)));
    }
}
