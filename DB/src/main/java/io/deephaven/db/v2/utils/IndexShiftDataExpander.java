package io.deephaven.db.v2.utils;

import io.deephaven.base.Base64;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.util.SafeCloseable;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.function.BiConsumer;

/**
 * Expands {@link ShiftAwareListener#onUpdate(ShiftAwareListener.Update)}'s Update into a backward compatible ARM
 * (added, removed, modified) by expanding keyspace shifts.
 *
 * Using this is almost always less efficient than using the Update directly.
 */
public class IndexShiftDataExpander implements SafeCloseable {

    private final ShiftAwareListener.Update update;

    /**
     * Generates the backwards compatible ARM from an ARMS update.
     * 
     * @param update The usptream update.
     */
    public IndexShiftDataExpander(final ShiftAwareListener.Update update, final Index sourceIndex) {
        // do we even need changes?
        if (update.shifted.empty() && !update.added.overlaps(update.removed)) {
            this.update = update.acquire();
            return;
        }

        try {
            this.update = new ShiftAwareListener.Update();

            // Compute added and removed using the old definitions explicitly.
            try (final Index prevIndex = sourceIndex.getPrevIndex()) {
                this.update.added = sourceIndex.minus(prevIndex);
                this.update.removed = prevIndex.minus(sourceIndex);
            }

            // Conceptually we can group modifies into two: a) modifies that were not part of any shift, and b) modifies
            // that are now at a shift destination. Group A is in upstream's modified set already. Group B indices
            // either existed last cycle or it did not. If it existed last cycle, then it should remain in the modified
            // set.
            // If it did not exist last cycle then it is accounted for in `this.update.added`. The is one more group of
            // modified rows. These are rows that existed in both previous and current indexes but were shifted.
            // Thus we need to add mods for shifted rows and remove any rows that are added (by old definition).
            this.update.modified = update.modified.clone();

            // Expand shift destinations to paint rows that might need to be considered modified.
            final Index.SequentialBuilder addedByShiftB = Index.FACTORY.getSequentialBuilder();
            final Index.SequentialBuilder removedByShiftB = Index.FACTORY.getSequentialBuilder();

            for (int idx = 0; idx < update.shifted.size(); ++idx) {
                final long start = update.shifted.getBeginRange(idx);
                final long end = update.shifted.getEndRange(idx);
                final long delta = update.shifted.getShiftDelta(idx);
                addedByShiftB.appendRange(start + delta, end + delta);
                removedByShiftB.appendRange(start, end);
            }

            // consider all rows that are in a shift region as modified (if they still exist)
            try (final Index addedByShift = addedByShiftB.getIndex();
                    final Index rmByShift = removedByShiftB.getIndex()) {
                addedByShift.insert(rmByShift);
                addedByShift.retain(sourceIndex);
                this.update.modified.insert(addedByShift);
            }

            // remove all rows we define as added (i.e. modified rows that were actually shifted into a new index)
            try (final Index absoluteModified = update.removed.intersect(update.added)) {
                this.update.modified.insert(absoluteModified);
            }
            this.update.modified.remove(this.update.added);
        } catch (Exception e) {
            throw new RuntimeException("Could not expand update: " + update.toString(), e);
        }
    }

    /**
     * Fetch the resulting index of added values.
     * 
     * @return added index
     */
    public Index getAdded() {
        return update.added;
    }

    /**
     * Fetch the resulting index of removed values.
     * 
     * @return removed index
     */
    public Index getRemoved() {
        return update.removed;
    }

    /**
     * Fetch the resulting index of modified values.
     * 
     * @return modified index
     */
    public Index getModified() {
        return update.modified;
    }

    @Override
    public void close() {
        if (this != EMPTY) {
            this.update.release();
        }
    }

    /**
     * Immutable, re-usable {@link IndexShiftDataExpander} for an empty set of changes.
     */
    public static IndexShiftDataExpander EMPTY = new IndexShiftDataExpander(new ShiftAwareListener.Update(
            Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(),
            IndexShiftData.EMPTY,
            ModifiedColumnSet.ALL), Index.FACTORY.getEmptyIndex());

    /**
     * Perform backwards compatible validation checks.
     * 
     * @param sourceIndex the underlying index that apply to added/removed/modified
     */
    public void validate(final Index sourceIndex) {
        final boolean previousContainsAdds;
        final boolean previousMissingRemovals;
        final boolean previousMissingModifications;
        try (final ReadOnlyIndex prevIndex = sourceIndex.getPrevIndex()) {
            previousContainsAdds = update.added.overlaps(prevIndex);
            previousMissingRemovals = !update.removed.subsetOf(prevIndex);
            previousMissingModifications = !update.modified.subsetOf(prevIndex);
        }
        final boolean currentMissingAdds = !update.added.subsetOf(sourceIndex);
        final boolean currentContainsRemovals = update.removed.overlaps(sourceIndex);
        final boolean currentMissingModifications = !update.modified.subsetOf(sourceIndex);

        if (!previousContainsAdds && !previousMissingRemovals && !previousMissingModifications &&
                !currentMissingAdds && !currentContainsRemovals && !currentMissingModifications) {
            return;
        }

        // Excuse the sloppiness in Index closing after this point, we're planning to crash the process anyway...

        String serializedIndices = null;
        if (BaseTable.PRINT_SERIALIZED_UPDATE_OVERLAPS) {
            // The indices are really rather complicated, if we fail this check let's generate a serialized
            // representation
            // of them that can later be loaded into a debugger. If this fails, we'll ignore it and continue with our
            // regularly scheduled exception.
            try {
                final StringBuilder outputBuffer = new StringBuilder();
                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

                final BiConsumer<String, Object> append = (name, obj) -> {
                    try {
                        objectOutputStream.writeObject(obj);
                        outputBuffer.append(name);
                        outputBuffer.append(Base64.byteArrayToBase64(byteArrayOutputStream.toByteArray()));
                        byteArrayOutputStream.reset();
                        objectOutputStream.reset();
                    } catch (final Exception ignored) {
                    }
                };

                append.accept("getIndex().getPrevIndex=", sourceIndex.getPrevIndex());
                append.accept("getIndex()=", sourceIndex.getPrevIndex());
                append.accept("added=", update.added);
                append.accept("removed=", update.removed);
                append.accept("modified=", update.modified);

                serializedIndices = outputBuffer.toString();
            } catch (final Exception ignored) {
            }
        }

        // If we're still here, we know that things are off the rails, and we want to fire the assertion
        final Index addedIntersectPrevious = update.added.intersect(sourceIndex.getPrevIndex());
        final Index removalsMinusPrevious = update.removed.minus(sourceIndex.getPrevIndex());
        final Index modifiedMinusPrevious = update.modified.minus(sourceIndex.getPrevIndex());
        final Index addedMinusCurrent = update.added.minus(sourceIndex);
        final Index removedIntersectCurrent = update.removed.intersect(sourceIndex);
        final Index modifiedMinusCurrent = update.modified.minus(sourceIndex);

        // Everything is messed up for this table, print out the indices in an easy to understand way
        final String indexUpdateErrorMessage = new LogOutputStringImpl().append("Index update error detected: ")
                .append(LogOutput::nl).append("\t          previousIndex=").append(sourceIndex.getPrevIndex())
                .append(LogOutput::nl).append("\t           currentIndex=").append(sourceIndex)
                .append(LogOutput::nl).append("\t         updateToExpand=").append(update)
                .append(LogOutput::nl).append("\t                  added=").append(update.added)
                .append(LogOutput::nl).append("\t                removed=").append(update.removed)
                .append(LogOutput::nl).append("\t               modified=").append(update.modified)
                .append(LogOutput::nl).append("\t         shifted.size()=").append(update.shifted.size())
                .append(LogOutput::nl).append("\t addedIntersectPrevious=").append(addedIntersectPrevious)
                .append(LogOutput::nl).append("\t  removalsMinusPrevious=").append(removalsMinusPrevious)
                .append(LogOutput::nl).append("\t  modifiedMinusPrevious=").append(modifiedMinusPrevious)
                .append(LogOutput::nl).append("\t      addedMinusCurrent=").append(addedMinusCurrent)
                .append(LogOutput::nl).append("\tremovedIntersectCurrent=").append(removedIntersectCurrent)
                .append(LogOutput::nl).append("\t   modifiedMinusCurrent=").append(modifiedMinusCurrent).toString();

        final Logger log = ProcessEnvironment.getDefaultLog();
        log.error().append(indexUpdateErrorMessage).endl();

        if (serializedIndices != null) {
            log.error().append("Index update error detected: serialized data=").append(serializedIndices).endl();
        }

        Assert.assertion(false, "!(previousContainsAdds || previousMissingRemovals || " +
                "previousMissingModifications || currentMissingAdds || currentContainsRemovals || " +
                "currentMissingModifications)", indexUpdateErrorMessage);
    }
}
