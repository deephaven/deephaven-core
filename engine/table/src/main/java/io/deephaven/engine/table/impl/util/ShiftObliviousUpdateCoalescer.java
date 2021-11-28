package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ShiftObliviousListener;

/**
 * Helper utility for coalescing multiple {@link ShiftObliviousListener updates}.
 */
public class ShiftObliviousUpdateCoalescer {

    private static final boolean VALIDATE_COALESCED_UPDATES = Configuration.getInstance()
            .getBooleanWithDefault("ShiftObliviousUpdateCoalescer.validateCoalescedUpdates", true);

    private WritableRowSet added, modified, removed;

    public ShiftObliviousUpdateCoalescer() {
        reset();
    }

    /**
     * The class assumes ownership of one reference to the indices passed; the caller should ensure to RowSet.copy()
     * them before passing them if they are shared.
     */
    public ShiftObliviousUpdateCoalescer(final TrackingWritableRowSet added, final TrackingWritableRowSet removed,
            final TrackingWritableRowSet modified) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
    }

    public void update(final RowSet addedOnUpdate, final RowSet removedOnUpdate,
            final RowSet modifiedOnUpdate) {
        // Note: extract removes matching ranges from the source RowSet
        try (final RowSet addedBack = this.removed.extract(addedOnUpdate);
                final RowSet actuallyAdded = addedOnUpdate.minus(addedBack)) {
            this.added.insert(actuallyAdded);
            this.modified.insert(addedBack);
        }

        // Things we've added, but are now removing. Do not aggregate these as removed since client never saw them.
        try (final RowSet additionsRemoved = this.added.extract(removedOnUpdate);
                final RowSet actuallyRemoved = removedOnUpdate.minus(additionsRemoved)) {
            this.removed.insert(actuallyRemoved);
        }

        // If we've removed it, it should no longer be modified.
        this.modified.remove(removedOnUpdate);

        // And anything modified, should be added to the modified set; unless we've previously added it.
        try (final RowSet actuallyModified = modifiedOnUpdate.minus(this.added)) {
            this.modified.insert(actuallyModified);
        }

        if (VALIDATE_COALESCED_UPDATES
                && (this.added.overlaps(this.modified) || this.added.overlaps(this.removed)
                        || this.removed.overlaps(modified))) {
            final String assertionMessage = "Coalesced overlaps detected: " +
                    "added=" + added +
                    ", removed=" + removed +
                    ", modified=" + modified +
                    ", addedOnUpdate=" + addedOnUpdate +
                    ", removedOnUpdate=" + removedOnUpdate +
                    ", modifiedOnUpdate=" + modifiedOnUpdate +
                    "addedIntersectRemoved=" + added.intersect(removed) +
                    "addedIntersectModified=" + added.intersect(modified) +
                    "removedIntersectModified=" + removed.intersect(modified);
            Assert.assertion(false, assertionMessage);
        }
    }

    public RowSet takeAdded() {
        final RowSet r = added;
        added = RowSetFactory.empty();
        return r;
    }

    public RowSet takeRemoved() {
        final RowSet r = removed;
        removed = RowSetFactory.empty();
        return r;
    }

    public RowSet takeModified() {
        final RowSet r = modified;
        modified = RowSetFactory.empty();
        return r;
    }

    public void reset() {
        added = RowSetFactory.empty();
        modified = RowSetFactory.empty();
        removed = RowSetFactory.empty();
    }
}
