//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import org.jetbrains.annotations.NotNull;

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
     * <p>
     * A TableUpdate is only valid during the update cycle that it was created on. If a TableUpdate is held across cycle
     * boundaries behavior is undefined.
     * </p>
     *
     * <p>
     * You must call {@link #release()} to decrement the reference count.
     * </p>
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
    boolean valid();

    /**
     * @return a cached copy of the modified RowSet in pre-shift keyspace
     */
    @NotNull
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
     * 
     * <p>
     * A {@link #valid()} update never returns a null RowSet, but the returned rowset may be {@link RowSet#isEmpty()
     * empty}.
     * </p>
     *
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire()} the update or make a {@link RowSet#copy() copy} of the RowSet.
     * </p>
     */
    @NotNull
    RowSet added();

    /**
     * rows removed (pre-shift keyspace)
     *
     * <p>
     * A {@link #valid()} update never returns a null RowSet, but the returned rowset may be {@link RowSet#isEmpty()
     * empty}.
     * </p>
     *
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire()} the update or make a {@link RowSet#copy() copy} of the RowSet.
     * </p>
     */
    @NotNull
    RowSet removed();

    /**
     * rows modified (post-shift keyspace)
     *
     * <p>
     * A {@link #valid()} update never returns a null RowSet, but the returned rowset may be {@link RowSet#isEmpty()
     * empty}.
     * </p>
     *
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire()} the update or make a {@link RowSet#copy() copy} of the RowSet.
     * </p>
     */
    @NotNull
    RowSet modified();

    /**
     * rows that shifted to new indices
     */
    @NotNull
    RowSetShiftData shifted();

    /**
     * the set of columns that might have changed for rows in the {@code modified()} RowSet
     */
    @NotNull
    ModifiedColumnSet modifiedColumnSet();
}
