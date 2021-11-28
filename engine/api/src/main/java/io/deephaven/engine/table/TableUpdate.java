package io.deephaven.engine.table;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;

import java.util.function.BiConsumer;

/**
 * Table update propagation structure, describing the rows and columns that were added, removed, modified, and shifted
 * on a given cycle.
 */
public interface TableUpdate extends LogOutputAppendable {

    @Override
    default LogOutput append(LogOutput logOutput) {
        return logOutput.append('{')
                .append("added=").append(added())
                .append(", removed=").append(removed())
                .append(", modified=").append(modified())
                .append(", shifted=").append(shifted() == null ? "{}" : shifted().toString())
                .append(", modifiedColumnSet=")
                .append(modifiedColumnSet() == null ? "{EMPTY}" : modifiedColumnSet().toString())
                .append("}");
    }

    /**
     * Increment the reference count on this object.
     *
     * @return {@code this} for convenience
     */
    TableUpdate acquire();

    /**
     * Decrement the reference count on this object.
     */
    void release();

    /**
     * @return true if no changes occurred in this update
     */
    default boolean empty() {
        return added().isEmpty()
                && removed().isEmpty()
                && modified().isEmpty()
                && shifted().empty();
    }

    /**
     * @return true if all internal state is initialized
     */
    default boolean valid() {
        return added() != null
                && removed() != null
                && modified() != null
                && shifted() != null
                && modifiedColumnSet() != null;
    }

    /**
     * @return a cached copy of the modified RowSet in pre-shift keyspace
     */
    RowSet getModifiedPreShift();

    /**
     * This helper iterates through the modified RowSet and supplies both the pre-shift and post-shift keys per row.
     *
     * @param consumer a consumer to feed the modified pre-shift and post-shift key values to.
     */
    default void forAllModified(final BiConsumer<Long, Long> consumer) {
        final RowSet prevModified = getModifiedPreShift();
        final RowSet.Iterator it = modified().iterator();
        final RowSet.Iterator pit = prevModified.iterator();

        while (it.hasNext() && pit.hasNext()) {
            consumer.accept(pit.nextLong(), it.next());
        }

        Assert.assertion(!it.hasNext(), "!it.hasNext()");
        Assert.assertion(!pit.hasNext(), "!pit.hasNext()");
    }

    /**
     * rows added (post-shift keyspace)
     */
    RowSet added();

    /**
     * rows removed (pre-shift keyspace)
     */
    RowSet removed();

    /**
     * rows modified (post-shift keyspace)
     */
    RowSet modified();

    /**
     * rows that shifted to new indices
     */
    RowSetShiftData shifted();

    /**
     * the set of columns that might have changed for rows in the {@code modified()} RowSet
     */
    ModifiedColumnSet modifiedColumnSet();
}
