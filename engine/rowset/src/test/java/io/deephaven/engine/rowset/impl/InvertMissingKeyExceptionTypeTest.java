//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;

/**
 * {@link RowSet#invert} of a key the rowset does not hold is rejected with an {@link IllegalArgumentException},
 * whichever backing holds the rowset and whichever container holds the block. A caller telling a stale key apart from
 * an internal error relies on the type being the same everywhere.
 */
public class InvertMissingKeyExceptionTypeTest {

    private static final long MAX = Long.MAX_VALUE;

    private static Class<?> exceptionFrom(final WritableRowSet a, final long[][] missingKeys) {
        try (a; final WritableRowSet keys = rspOf(missingKeys)) {
            try (final WritableRowSet inverted = a.invert(keys, MAX)) {
                return null;
            }
        } catch (Throwable t) {
            return t.getClass();
        }
    }

    private static void checkAllBackings(final long[][] ranges, final long[][]... missingKeySets) {
        final Supplier<?>[] backings = {
                () -> ranges.length == 1 ? singleRangeOf(ranges[0][0], ranges[0][1]) : sortedRangesOf(ranges),
                () -> sortedRangesOf(ranges),
                () -> rspOf(ranges),
        };
        for (final long[][] missing : missingKeySets) {
            for (final Supplier<?> backing : backings) {
                final WritableRowSet a = (WritableRowSet) backing.get();
                final String name = ((WritableRowSetImpl) a).getInnerSet().getClass().getSimpleName();
                final Class<?> thrown = exceptionFrom(a, missing);
                assertEquals(name + " invert of missing keys " + missing[0][0] + "-" + missing[0][1],
                        IllegalArgumentException.class, thrown);
            }
        }
    }

    /** One contiguous run inside a block, which the bitmap holds in a single range container. */
    @Test
    public void testSingleRangeContainerBlock() {
        checkAllBackings(new long[][] {{61942, 64110}},
                new long[][] {{0, 2}}, new long[][] {{64111, 64111}}, new long[][] {{61940, 61941}});
    }

    /** Scattered keys inside a block, which the bitmap holds in an array container. */
    @Test
    public void testArrayContainerBlock() {
        checkAllBackings(new long[][] {{100, 100}, {200, 200}, {300, 310}},
                new long[][] {{150, 150}}, new long[][] {{0, 0}}, new long[][] {{311, 311}});
    }

    /** A key in a block the rowset does not touch at all. */
    @Test
    public void testMissingBlock() {
        checkAllBackings(new long[][] {{100, 110}, {5 * 65536L, 5 * 65536L + 3}},
                new long[][] {{2 * 65536L, 2 * 65536L}}, new long[][] {{9 * 65536L, 9 * 65536L}});
    }
}
