//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.updategraph.NotificationQueue;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;

/**
 * Table update propagation structure, describing the rows and columns that were added, removed, modified, and shifted
 * on a given cycle.
 * <p>
 * {@link TableUpdateListener TableUpdateListeners} must not mutate TableUpdates they receive.
 * <p>
 * A TableUpdate received by a TableUpdateListener is only valid during the updating phase of the cycle that it was
 * created on. Listeners should be very careful to ensure that any deferred or asynchronous usage respects this
 * constraint.
 * <p>
 * All deferred or asynchronous usage must be guarded by a successful {@link #acquire()}, and terminated by a
 * {@link #release()}. In the typical pattern, the TableUpdateListener acquires the TableUpdate when
 * {@link TableUpdateListener#getNotification(TableUpdate) making a notification}, and releases it when the notification
 * has been completely {@link NotificationQueue.Notification#run() run}. Users typically extend a base
 * TableUpdateListener implementation that handles this on their behalf, but must be sure not to inadvertently use the
 * TableUpdate outside its expected context without taking additional precautions.
 * <p>
 *
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
     * Acquire a reference count for this TableUpdate that will keep it from being cleaned up until it is
     * {@link #release() released}. Code that calls this method must be sure to call {@link #release()} when the
     * TableUpdate is no longer needed. Acquiring a TableUpdate does not change the constraint that it must not be used
     * outside the updating phase for which it was created.
     *
     * @return {@code this} for convenience
     */
    TableUpdate acquire();

    /**
     * Release a previously-acquired reference count for this object.
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
     * Throws an {@link io.deephaven.base.verify.AssertionFailure} if this update is not structurally valid, i.e. its
     * accessors will return non-{@code null}, usable data structures. This does not test for usage outside the
     * appropriate updating phase.
     */
    void validate() throws AssertionFailure;

    /**
     * @return a cached copy of the modified RowSet in pre-shift keyspace
     */
    @NotNull
    RowSet getModifiedPreShift();

    /**
     * This helper iterates through the modified RowSet and supplies both the pre-shift and post-shift keys per row.
     *
     * @param consumer A consumer to feed the modified pre-shift and post-shift key values to
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
     * Rows added (in post-shift keyspace).
     * <p>
     * A {@link #validate()} update never returns a null RowSet, but the returned RowSet may be {@link RowSet#isEmpty()
     * empty}.
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire() acquire} the update or make a {@link RowSet#copy() copy} of the RowSet.
     */
    @NotNull
    RowSet added();

    /**
     * Rows removed (in pre-shift keyspace).
     * <p>
     * A {@link #validate()} update never returns a null RowSet, but the returned RowSet may be {@link RowSet#isEmpty()
     * empty}.
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire() acquire} the update or make a {@link RowSet#copy() copy} of the RowSet.
     */
    @NotNull
    RowSet removed();

    /**
     * Rows modified (in post-shift keyspace).
     * <p>
     * A {@link #validate()} update never returns a null RowSet, but the returned RowSet may be {@link RowSet#isEmpty()
     * empty}.
     * <p>
     * Note that the TableUpdate object still retains ownership of the returned {@link RowSet} object. The caller must
     * not close the returned RowSet. To use the RowSet beyond the scope of a notification, the caller must
     * {@link #acquire() acquire} the update or make a {@link RowSet#copy() copy} of the RowSet.
     */
    @NotNull
    RowSet modified();

    /**
     * Rows that shifted to new row keys.
     */
    @NotNull
    RowSetShiftData shifted();

    /**
     * The set of columns that might have changed for rows in the {@link #modified()} RowSet.
     */
    @NotNull
    ModifiedColumnSet modifiedColumnSet();
}
