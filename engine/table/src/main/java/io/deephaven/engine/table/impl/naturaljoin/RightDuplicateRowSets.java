//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import static io.deephaven.engine.table.impl.IncrementalNaturalJoinStateManager.FIRST_DUPLICATE;

/**
 * The right row sets of the keys that have more than one right row, for the incremental natural join state managers.
 * <p>
 * A key's right state is a single long: a right row key, a marker for no right row, or, for a key with several right
 * rows, a token at or below {@link io.deephaven.engine.table.impl.IncrementalNaturalJoinStateManager#FIRST_DUPLICATE}
 * that names a location here. Locations are recycled, so a key with a single right row allocates nothing.
 */
final class RightDuplicateRowSets {
    private final ObjectArraySource<WritableRowSet> rowSets = new ObjectArraySource<>(WritableRowSet.class);
    private long nextLocation = 0;
    private final LongArrayList freeLocations = new LongArrayList();

    /**
     * @param rightState a right state at or below {@code FIRST_DUPLICATE}
     * @return the location of the duplicate row set that the state names
     */
    static long locationFromState(final long rightState) {
        Assert.leq(rightState, "rightState", FIRST_DUPLICATE);
        return -rightState + FIRST_DUPLICATE;
    }

    /**
     * @param location a duplicate location
     * @return the right state that names it
     */
    static long stateFromLocation(final long location) {
        return -location + FIRST_DUPLICATE;
    }

    /**
     * Store a new duplicate row set.
     *
     * @param duplicates the row set, which this takes ownership of
     * @return the right state that names it
     */
    long allocate(final WritableRowSet duplicates) {
        final long location;
        if (freeLocations.isEmpty()) {
            rowSets.ensureCapacity(nextLocation + 1);
            location = nextLocation++;
        } else {
            location = freeLocations.removeLong(freeLocations.size() - 1);
        }
        rowSets.set(location, duplicates);
        return stateFromLocation(location);
    }

    /**
     * @param rightState a right state that names a duplicate row set
     * @return the duplicate row set
     */
    WritableRowSet get(final long rightState) {
        return rowSets.getUnsafe(locationFromState(rightState));
    }

    /**
     * @param location a duplicate location
     * @return the duplicate row set at the location
     */
    WritableRowSet getAtLocation(final long location) {
        return rowSets.getUnsafe(location);
    }

    /**
     * Close the duplicate row set that {@code rightState} names, and recycle its location.
     */
    void free(final long rightState) {
        final long location = locationFromState(rightState);
        final WritableRowSet duplicates = rowSets.getAndSetUnsafe(location, null);
        if (duplicates != null) {
            duplicates.close();
        }
        freeLocations.add(location);
    }

    /**
     * Compact every duplicate row set.
     */
    void compactAll() {
        for (long ii = 0; ii < nextLocation; ++ii) {
            final WritableRowSet rowSet = rowSets.getUnsafe(ii);
            if (rowSet != null) {
                rowSet.compact();
            }
        }
    }
}
