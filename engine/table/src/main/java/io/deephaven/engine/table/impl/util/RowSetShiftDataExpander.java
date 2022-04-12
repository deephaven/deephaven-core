package io.deephaven.engine.table.impl.util;

import io.deephaven.base.Base64;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.function.BiConsumer;

/**
 * Expands {@link TableUpdateListener#onUpdate(TableUpdate)}'s Update into a backward compatible ARM (added, removed,
 * modified) by expanding keyspace shifts.
 *
 * Using this is almost always less efficient than using the Update directly.
 */
public class RowSetShiftDataExpander implements SafeCloseable {

    private static final Logger log = LoggerFactory.getLogger(RowSetShiftDataExpander.class);

    private final RowSet added;
    private final RowSet removed;
    private final WritableRowSet modified;

    /**
     * Generates the backwards compatible ARM from an ARMS update.
     * 
     * @param update The usptream update.
     */
    public RowSetShiftDataExpander(final TableUpdate update, final TrackingRowSet sourceRowSet) {
        // do we even need changes?
        if (update.shifted().empty() && !update.added().overlaps(update.removed())) {
            added = update.added().copy();
            removed = update.removed().copy();
            modified = update.modified().copy();
            return;
        }

        try {
            // Compute added and removed using the old definitions explicitly.
            try (final RowSet prevRowSet = sourceRowSet.copyPrev()) {
                added = sourceRowSet.minus(prevRowSet);
                removed = prevRowSet.minus(sourceRowSet);
            }

            // Conceptually we can group modifies into two: a) modifies that were not part of any shift, and b) modifies
            // that are now at a shift destination. Group A is in upstream's modified set already. Group B indices
            // either existed last cycle or it did not. If it existed last cycle, then it should remain in the modified
            // set.
            // If it did not exist last cycle then it is accounted for in `this.update.added`. The is one more group of
            // modified rows. These are rows that existed in both previous and current indexes but were shifted.
            // Thus we need to add mods for shifted rows and remove any rows that are added (by old definition).
            modified = update.modified().copy();

            // Expand shift destinations to paint rows that might need to be considered modified.
            final RowSetBuilderSequential addedByShiftB = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removedByShiftB = RowSetFactory.builderSequential();

            for (int idx = 0; idx < update.shifted().size(); ++idx) {
                final long start = update.shifted().getBeginRange(idx);
                final long end = update.shifted().getEndRange(idx);
                final long delta = update.shifted().getShiftDelta(idx);
                addedByShiftB.appendRange(start + delta, end + delta);
                removedByShiftB.appendRange(start, end);
            }

            // consider all rows that are in a shift region as modified (if they still exist)
            try (final WritableRowSet addedByShift = addedByShiftB.build();
                    final RowSet rmByShift = removedByShiftB.build()) {
                addedByShift.insert(rmByShift);
                addedByShift.retain(sourceRowSet);
                modified.insert(addedByShift);
            }

            // remove all rows we define as added (i.e. modified rows that were actually shifted into a new row key)
            try (final RowSet absoluteModified = update.removed().intersect(update.added())) {
                modified.insert(absoluteModified);
            }
            modified.remove(added);
        } catch (Exception e) {
            throw new RuntimeException("Could not expand update: " + update, e);
        }
    }

    /**
     * Fetch the resulting RowSet of added values.
     * 
     * @return added RowSet
     */
    public RowSet getAdded() {
        return added;
    }

    /**
     * Fetch the resulting RowSet of removed values.
     * 
     * @return removed RowSet
     */
    public RowSet getRemoved() {
        return removed;
    }

    /**
     * Fetch the resulting RowSet of modified values.
     * 
     * @return modified RowSet
     */
    public RowSet getModified() {
        return modified;
    }

    @Override
    public void close() {
        if (this != EMPTY) {
            added.close();
            removed.close();
            modified.close();
        }
    }

    /**
     * Immutable, re-usable {@link RowSetShiftDataExpander} for an empty set of changes.
     */
    public static final RowSetShiftDataExpander EMPTY = new RowSetShiftDataExpander(
            new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.ALL),
            RowSetFactory.empty().toTracking());

    /**
     * Perform backwards compatible validation checks.
     * 
     * @param update The update originally passed at construction time, used only for logging debug info on error
     * @param sourceRowSet The underlying RowSet that applies to added/removed/modified
     */
    public void validate(final TableUpdate update, final TrackingRowSet sourceRowSet) {
        final boolean previousContainsAdds;
        final boolean previousMissingRemovals;
        final boolean previousMissingModifications;
        try (final RowSet prevIndex = sourceRowSet.copyPrev()) {
            previousContainsAdds = added.overlaps(prevIndex);
            previousMissingRemovals = !removed.subsetOf(prevIndex);
            previousMissingModifications = !modified.subsetOf(prevIndex);
        }
        final boolean currentMissingAdds = !added.subsetOf(sourceRowSet);
        final boolean currentContainsRemovals = removed.overlaps(sourceRowSet);
        final boolean currentMissingModifications = !modified.subsetOf(sourceRowSet);

        if (!previousContainsAdds && !previousMissingRemovals && !previousMissingModifications &&
                !currentMissingAdds && !currentContainsRemovals && !currentMissingModifications) {
            return;
        }

        // Excuse the sloppiness in RowSet closing after this point, we're planning to crash the process
        // anyway...

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

                append.accept("build().copyPrev=", sourceRowSet.copyPrev());
                append.accept("build()=", sourceRowSet.copyPrev());
                append.accept("added=", added);
                append.accept("removed=", removed);
                append.accept("modified=", modified);

                serializedIndices = outputBuffer.toString();
            } catch (final Exception ignored) {
            }
        }

        // If we're still here, we know that things are off the rails, and we want to fire the assertion
        final RowSet addedIntersectPrevious = added.intersect(sourceRowSet.copyPrev());
        final RowSet removalsMinusPrevious = removed.minus(sourceRowSet.copyPrev());
        final RowSet modifiedMinusPrevious = modified.minus(sourceRowSet.copyPrev());
        final RowSet addedMinusCurrent = added.minus(sourceRowSet);
        final RowSet removedIntersectCurrent = removed.intersect(sourceRowSet);
        final RowSet modifiedMinusCurrent = modified.minus(sourceRowSet);

        // Everything is messed up for this table, print out the indices in an easy to understand way
        final String indexUpdateErrorMessage = new LogOutputStringImpl()
                .append("RowSet update error detected: ")
                .append(LogOutput::nl).append("\t          previousIndex=").append(sourceRowSet.copyPrev())
                .append(LogOutput::nl).append("\t           currentIndex=").append(sourceRowSet)
                .append(LogOutput::nl).append("\t         updateToExpand=").append(update)
                .append(LogOutput::nl).append("\t         shifted.size()=").append(update.shifted().size())
                .append(LogOutput::nl).append("\t                  added=").append(added)
                .append(LogOutput::nl).append("\t                removed=").append(removed)
                .append(LogOutput::nl).append("\t                modified=").append(modified)
                .append(LogOutput::nl).append("\t addedIntersectPrevious=").append(addedIntersectPrevious)
                .append(LogOutput::nl).append("\t  removalsMinusPrevious=").append(removalsMinusPrevious)
                .append(LogOutput::nl).append("\t   modifiedMinusPrevious=").append(modifiedMinusPrevious)
                .append(LogOutput::nl).append("\t      addedMinusCurrent=").append(addedMinusCurrent)
                .append(LogOutput::nl).append("\tremovedIntersectCurrent=").append(removedIntersectCurrent)
                .append(LogOutput::nl).append("\t    modifiedMinusCurrent=").append(modifiedMinusCurrent).toString();

        log.error().append(indexUpdateErrorMessage).endl();

        if (serializedIndices != null) {
            log.error().append("RowSet update error detected: serialized data=")
                    .append(serializedIndices).endl();
        }

        Assert.assertion(false, "!(previousContainsAdds || previousMissingRemovals || " +
                "previousMissingModifications || currentMissingAdds || currentContainsRemovals || " +
                "currentMissingModifications)", indexUpdateErrorMessage);
    }
}
