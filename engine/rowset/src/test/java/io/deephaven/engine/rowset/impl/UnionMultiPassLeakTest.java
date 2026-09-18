//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * A union that needs more than one pass absorbs the accumulators the earlier pass built. Those are the only references
 * to them, and insertion borrows its argument rather than consuming it, so the union has to close them. Leaking one
 * holds a copy-on-write reference to whichever input it came from, which defeats copy-on-write for that input long
 * after the union has been closed.
 */
public class UnionMultiPassLeakTest {

    /**
     * Disjoint sets that all span the key space, so nothing appends and nothing duplicates. The merge can only pair
     * them, which is what forces the passes this test is about.
     */
    private static List<RowSet> interleaved(final int setCount, final int rangesPerSet) {
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[setCount];
        for (int ii = 0; ii < setCount; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        for (int range = 0; range < setCount * rangesPerSet; ++range) {
            final long start = range * 10L;
            builders[range % setCount].appendRange(start, start + 4);
        }
        final List<RowSet> rowSets = new ArrayList<>(setCount);
        for (int ii = 0; ii < setCount; ++ii) {
            rowSets.add(builders[ii].build());
        }
        return rowSets;
    }

    private static int refCount(final RowSet rowSet) {
        return ((WritableRowSetImpl) rowSet).refCount();
    }

    @Test
    public void inputsAreReleasedAfterAMultiPassUnion() {
        // 17 sets take five passes: 17 -> 9 -> 5 -> 3 -> 2 -> 1.
        final List<RowSet> rowSets = interleaved(17, 7);
        try {
            for (final RowSet rowSet : rowSets) {
                assertEquals(1, refCount(rowSet));
            }
            try (final WritableRowSet union = RowSetFactory.union(rowSets)) {
                assertEquals(17 * 7 * 5, union.size());
            }
            for (final RowSet rowSet : rowSets) {
                // Every intermediate accumulator held a reference to the input it was built from.
                assertEquals(1, refCount(rowSet));
            }
        } finally {
            rowSets.forEach(RowSet::close);
        }
    }

    @Test
    public void inputsAreReleasedAfterASinglePassUnion() {
        final List<RowSet> rowSets = new ArrayList<>();
        for (int ii = 0; ii < 9; ++ii) {
            rowSets.add(RowSetFactory.fromRange(ii * 100L, ii * 100L + 49));
        }
        try {
            try (final WritableRowSet union = RowSetFactory.union(rowSets)) {
                assertEquals(9 * 50, union.size());
            }
            for (final RowSet rowSet : rowSets) {
                assertEquals(1, refCount(rowSet));
            }
        } finally {
            rowSets.forEach(RowSet::close);
        }
    }
}
