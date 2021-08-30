package io.deephaven.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.rowset.Index;

/**
 * Tool for coalescing multiple {@link Index} updates for legacy (not shift-aware) listeners.
 */
public
class LegacyIndexUpdateCoalescer {
    private Index added, modified, removed;

    public LegacyIndexUpdateCoalescer() {
        reset();
    }

    /**
     * The class assumes ownership of one reference to the indices passed; the caller should ensure to Index.clone()
     * them before passing them if they are shared.
     */
    public LegacyIndexUpdateCoalescer(Index added, Index removed, Index modified) {
        this.added = added;
        this.removed = removed;
        this.modified = modified;
    }

    public void update(final Index addedOnUpdate, final Index removedOnUpdate, final Index modifiedOnUpdate) {
        // Note: extract removes matching ranges from the source index
        try (final Index addedBack = this.removed.extract(addedOnUpdate);
             final Index actuallyAdded = addedOnUpdate.minus(addedBack)) {
            this.added.insert(actuallyAdded);
            this.modified.insert(addedBack);
        }

        // Things we've added, but are now removing. Do not aggregate these as removed since client never saw them.
        try (final Index additionsRemoved = this.added.extract(removedOnUpdate);
             final Index actuallyRemoved = removedOnUpdate.minus(additionsRemoved)) {
            this.removed.insert(actuallyRemoved);
        }

        // If we've removed it, it should no longer be modified.
        this.modified.remove(removedOnUpdate);

        // And anything modified, should be added to the modified set; unless we've previously added it.
        try (final Index actuallyModified = modifiedOnUpdate.minus(this.added)) {
            this.modified.insert(actuallyModified);
        }

        if (Index.VALIDATE_COALESCED_UPDATES && (this.added.overlaps(this.modified) || this.added.overlaps(this.removed)
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

    public Index takeAdded() {
        final Index r = added;
        added = Index.FACTORY.getEmptyIndex();
        return r;
    }

    public Index takeRemoved() {
        final Index r = removed;
        removed = Index.FACTORY.getEmptyIndex();
        return r;
    }

    public Index takeModified() {
        final Index r = modified;
        modified = Index.FACTORY.getEmptyIndex();
        return r;
    }

    public void reset() {
        added = Index.FACTORY.getEmptyIndex();
        modified = Index.FACTORY.getEmptyIndex();
        removed = Index.FACTORY.getEmptyIndex();
    }
}
