/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

/**
 * Shift-aware listener for table changes.
 */
public interface ShiftAwareListener extends ListenerBase {

    /**
     * A shift aware update structure, containing the rows and columns that were added, modified, removed, and shifted
     * on a given cycle.
     */
    class Update implements LogOutputAppendable {
        /**
         * rows added (post-shift keyspace)
         */
        public Index added;

        /**
         * rows removed (pre-shift keyspace)
         */
        public Index removed;

        /**
         * rows modified (post-shift keyspace)
         */
        public Index modified;

        /**
         * rows that shifted to new indices
         */
        public IndexShiftData shifted;

        /**
         * the set of columns that might have changed for rows in the {@code modified()} index
         */
        public ModifiedColumnSet modifiedColumnSet;

        // Cached version of prevModified index.
        private volatile Index prevModified;

        // Field updater for refCount, so we can avoid creating an {@link java.util.concurrent.atomic.AtomicInteger} for
        // each instance.
        private static final AtomicIntegerFieldUpdater<Update> REFERENCE_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(Update.class, "refCount");

        // Ensure that we clean up only after all copies of the update are released.
        private volatile int refCount = 1;

        public Update() {}

        public Update(final Index added, final Index removed, final Index modified, final IndexShiftData shifted,
                final ModifiedColumnSet modifiedColumnSet) {
            this.added = added;
            this.removed = removed;
            this.modified = modified;
            this.shifted = shifted;
            this.modifiedColumnSet = modifiedColumnSet;
        }

        /**
         * Increment the reference count on this object.
         * 
         * @return {@code this} for convenience
         */
        public Update acquire() {
            if (REFERENCE_COUNT_UPDATER.incrementAndGet(this) == 1) {
                // This doubles as a memory barrier read for the writes in reset().
                Assert.eqNull(prevModified, "prevModified");
            }
            return this;
        }

        /**
         * Decrement the reference count on this object.
         */
        public void release() {
            int newRefCount = REFERENCE_COUNT_UPDATER.decrementAndGet(this);
            if (newRefCount > 0) {
                return;
            }
            Assert.eqZero(newRefCount, "newRefCount");
            reset();
        }

        /**
         * @return true if no changes occurred in this update
         */
        public boolean empty() {
            return added.empty() && removed.empty() && modified.empty() && shifted.empty();
        }

        /**
         * @return true if all internal state is initialized
         */
        public boolean valid() {
            return added != null && removed != null && modified != null && shifted != null && modifiedColumnSet != null;
        }

        /**
         * Make a deep copy of this object.
         */
        public Update copy() {
            final ModifiedColumnSet newMCS;
            if (modifiedColumnSet == ModifiedColumnSet.ALL || modifiedColumnSet == ModifiedColumnSet.EMPTY) {
                newMCS = modifiedColumnSet;
            } else {
                newMCS = new ModifiedColumnSet(modifiedColumnSet);
                newMCS.setAll(modifiedColumnSet);
            }
            return new Update(added.clone(), removed.clone(), modified.clone(), shifted, newMCS);
        }

        /**
         * @return a cached copy of the modified index in pre-shift keyspace
         */
        public Index getModifiedPreShift() {
            if (shifted.empty()) {
                return modified;
            }
            Index localPrevModified = prevModified;
            if (localPrevModified == null) {
                synchronized (this) {
                    localPrevModified = prevModified;
                    if (localPrevModified == null) {
                        localPrevModified = modified.clone();
                        shifted.unapply(localPrevModified);
                        // this volatile write ensures prevModified is visible only after it is shifted
                        prevModified = localPrevModified;
                    }
                }
            }
            return localPrevModified;
        }

        /**
         * This helper iterates through the modified index and supplies both the pre-shift and post-shift keys per row.
         * 
         * @param consumer a consumer to feed the modified pre-shift and post-shift key values to.
         */
        public void forAllModified(final BiConsumer<Long, Long> consumer) {
            final Index prevModified = getModifiedPreShift();
            final Index.Iterator it = modified.iterator();
            final Index.Iterator pit = prevModified.iterator();

            while (it.hasNext() && pit.hasNext()) {
                consumer.accept(pit.nextLong(), it.next());
            }

            if (it.hasNext() || pit.hasNext()) {
                throw new IllegalStateException("IndexShiftData.forAllModified(modified) generated an invalid set.");
            }
        }

        private void reset() {
            if (added != null) {
                added.close();
                added = null;
            }
            if (removed != null) {
                removed.close();
                removed = null;
            }
            if (modified != null) {
                modified.close();
                modified = null;
            }
            if (prevModified != null) {
                prevModified.close();
            }
            shifted = null;
            modifiedColumnSet = null;
            // This doubles as a memory barrier write prior to the read in acquire(). It must remain last.
            prevModified = null;
        }

        @Override
        public String toString() {
            return new LogOutputStringImpl().append(this).toString();
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append('{')
                    .append("added=").append(added)
                    .append(", removed=").append(removed)
                    .append(", modified=").append(modified)
                    .append(", shifted=").append(shifted == null ? "{}" : shifted.toString())
                    .append(", modifiedColumnSet=")
                    .append(modifiedColumnSet == null ? "{EMPTY}" : modifiedColumnSet.toString())
                    .append("}");
        }
    }

    /**
     * Process notification of table changes.
     *
     * @param upstream The set of upstream table updates.
     */
    void onUpdate(Update upstream);

    /**
     * Creates a notification for the table changes.
     *
     * @param upstream The set of upstream table updates.
     * @return table change notification
     */
    NotificationQueue.IndexUpdateNotification getNotification(Update upstream);
}
