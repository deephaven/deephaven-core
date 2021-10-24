package io.deephaven.engine.v2.utils;

import io.deephaven.base.verify.Assert;

/**
 * Helper utility for coalescing multiple {@link io.deephaven.engine.v2.Listener updates}.
 */
public class ShiftObliviousUpdateCoalescer {

    private TrackingMutableRowSet added, modified, removed;

    public ShiftObliviousUpdateCoalescer() {
        reset();
    }

    /**
     * The class assumes ownership of one reference to the indices passed; the caller should ensure to TrackingMutableRowSet.clone()
     * them before passing them if they are shared.
     */
    public ShiftObliviousUpdateCoalescer(final TrackingMutableRowSet added, final TrackingMutableRowSet removed, final TrackingMutableRowSet modified) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
    }

    public void update(final TrackingMutableRowSet addedOnUpdate, final TrackingMutableRowSet removedOnUpdate, final TrackingMutableRowSet modifiedOnUpdate) {
        // Note: extract removes matching ranges from the source rowSet
        try (final TrackingMutableRowSet addedBack = this.removed.extract(addedOnUpdate);
             final TrackingMutableRowSet actuallyAdded = addedOnUpdate.minus(addedBack)) {
            this.added.insert(actuallyAdded);
            this.modified.insert(addedBack);
        }

        // Things we've added, but are now removing. Do not aggregate these as removed since client never saw them.
        try (final TrackingMutableRowSet additionsRemoved = this.added.extract(removedOnUpdate);
             final TrackingMutableRowSet actuallyRemoved = removedOnUpdate.minus(additionsRemoved)) {
            this.removed.insert(actuallyRemoved);
        }

        // If we've removed it, it should no longer be modified.
        this.modified.remove(removedOnUpdate);

        // And anything modified, should be added to the modified set; unless we've previously added it.
        try (final TrackingMutableRowSet actuallyModified = modifiedOnUpdate.minus(this.added)) {
            this.modified.insert(actuallyModified);
        }

        if (TrackingMutableRowSet.VALIDATE_COALESCED_UPDATES && (this.added.overlaps(this.modified) || this.added.overlaps(this.removed)
                || this.removed.overlaps(modified))) {
            final String assertionMessage = "Coalesced overlaps detected: " +
                    "added=" + added.toString() +
                    ", removed=" + removed.toString() +
                    ", modified=" + modified.toString() +
                    ", addedOnUpdate=" + addedOnUpdate.toString() +
                    ", removedOnUpdate=" + removedOnUpdate.toString() +
                    ", modifiedOnUpdate=" + modifiedOnUpdate.toString() +
                    "addedIntersectRemoved=" + added.intersect(removed).toString() +
                    "addedIntersectModified=" + added.intersect(modified).toString() +
                    "removedIntersectModified=" + removed.intersect(modified).toString();
            Assert.assertion(false, assertionMessage);
        }
    }

    public TrackingMutableRowSet takeAdded() {
        final TrackingMutableRowSet r = added;
        added = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        return r;
    }

    public TrackingMutableRowSet takeRemoved() {
        final TrackingMutableRowSet r = removed;
        removed = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        return r;
    }

    public TrackingMutableRowSet takeModified() {
        final TrackingMutableRowSet r = modified;
        modified = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        return r;
    }

    public void reset() {
        added = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        modified = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        removed = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
    }
}
