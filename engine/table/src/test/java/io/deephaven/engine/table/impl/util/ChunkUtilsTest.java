//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ChunkUtilsTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static IntegerArraySource source(final int... values) {
        final IntegerArraySource source = new IntegerArraySource();
        source.ensureCapacity(values.length);
        for (int ii = 0; ii < values.length; ++ii) {
            source.set(ii, values[ii]);
        }
        return source;
    }

    @Test
    public void testCopyDataColumns() {
        final IntegerArraySource first = source(1, 2, 3);
        final IntegerArraySource second = source(10, 20, 30);
        final IntegerArraySource firstDest = new IntegerArraySource();
        final IntegerArraySource secondDest = new IntegerArraySource();
        try (final RowSet srcRows = RowSetFactory.fromKeys(0, 2);
                final RowSet destRows = RowSetFactory.fromKeys(5, 6)) {
            ChunkUtils.copyData(new ChunkSource.WithPrev[] {first, second}, srcRows,
                    new WritableColumnSource[] {firstDest, secondDest}, destRows, false);
        }
        assertEquals(1, firstDest.getInt(5));
        assertEquals(3, firstDest.getInt(6));
        assertEquals(10, secondDest.getInt(5));
        assertEquals(30, secondDest.getInt(6));
    }

    @Test
    public void testCopyDataRejectsSourceThatIsADestination() {
        final IntegerArraySource first = source(1, 2, 3);
        final IntegerArraySource second = source(10, 20, 30);
        final IntegerArraySource firstDest = new IntegerArraySource();
        // the second column's source is the first column's destination
        final ChunkSource.WithPrev<? extends Values>[] sources = new ChunkSource.WithPrev[] {first, firstDest};
        final WritableColumnSource<?>[] destinations = new WritableColumnSource[] {firstDest, second};
        try (final RowSet rows = RowSetFactory.flat(3)) {
            assertThrows(IllegalArgumentException.class,
                    () -> ChunkUtils.copyData(sources, rows, destinations, rows, false));
        }
    }
}
