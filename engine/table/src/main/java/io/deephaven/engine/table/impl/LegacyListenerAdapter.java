//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.reference.SimpleReference;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.util.RowSetShiftDataExpander;
import io.deephaven.engine.updategraph.EmptyErrorNotification;
import io.deephaven.engine.updategraph.EmptyNotification;
import io.deephaven.engine.updategraph.NotificationQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.stream.Stream;

/**
 * {@link TableUpdateListener} that wraps and delegates to a {@link ShiftObliviousListener}.
 * <p>
 * It's expected that the legacy listener is responsible for managing its parent from a liveness perspective, removing
 * itself upon destruction, and ensuring reachability to its parent. This wrapper can simply delegate matters of
 * liveness to the legacy listener.
 */
class LegacyListenerAdapter implements TableUpdateListener, SimpleReference<TableUpdateListener> {

    private final WeakReference<ShiftObliviousListener> legacyListenerReference;
    private final TrackingRowSet sourceRowSet;

    /**
     * Create a new LegacyListenerAdapter.
     *
     * @param legacyListener The {@link ShiftObliviousListener} to wrap
     * @param sourceRowSet The parent table's {@link TrackingRowSet} to use for expansion and validation
     */
    LegacyListenerAdapter(
            @NotNull final ShiftObliviousListener legacyListener, @NotNull final TrackingRowSet sourceRowSet) {
        this.legacyListenerReference = new WeakReference<>(legacyListener);
        this.sourceRowSet = sourceRowSet;
    }

    @Override
    public void onFailure(@NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
        throw new IllegalStateException(
                "Unexpected invocation: should never be called; getErrorNotification delegates to the wrapped ShiftObliviousListener");
    }

    @Override
    public NotificationQueue.ErrorNotification getErrorNotification(
            @NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        if (legacyListener == null) {
            return new EmptyErrorNotification();
        }
        return legacyListener.getErrorNotification(originalException, sourceEntry);
    }

    @Override
    public void onUpdate(@NotNull final TableUpdate upstream) {
        throw new IllegalStateException(
                "Unexpected invocation: getNotification delegates to the wrapped ShiftObliviousListener");
    }

    @Override
    public NotificationQueue.Notification getNotification(@NotNull final TableUpdate upstream) {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        if (legacyListener == null) {
            return new EmptyNotification();
        }
        // We can safely assume we're getting a TableUpdateImpl, since it's the only implementation.
        // If we later want to loft getExpander to the TableUpdate interface, we'll need to extract a
        // RowSetShiftDataExpander interface to live in engine-api.
        final RowSetShiftDataExpander expander = ((TableUpdateImpl) upstream).getExpander(sourceRowSet);
        return legacyListener.getNotification(expander.getAdded(), expander.getRemoved(), expander.getModified());
    }

    @Override
    public TableUpdateListener get() {
        return legacyListenerReference.get() == null ? null : this;
    }

    @Override
    public void clear() {
        legacyListenerReference.clear();
    }

    /**
     * Test if {@code otherLegacyListener} is the same as our wrapped listener.
     *
     * @param otherLegacyListener The listener to test
     * @return Whether {@code otherLegacyListener} is our wrapped listener
     */
    boolean matches(@NotNull final ShiftObliviousListener otherLegacyListener) {
        return legacyListenerReference.get() == otherLegacyListener;
    }

    @Override
    public boolean tryManage(@NotNull final LivenessReferent referent) {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        return legacyListener != null && legacyListener.tryManage(referent);
    }

    @Override
    public boolean tryUnmanage(@NotNull final LivenessReferent referent) {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        return legacyListener != null && legacyListener.tryUnmanage(referent);
    }

    @Override
    public boolean tryUnmanage(@NotNull final Stream<? extends LivenessReferent> referents) {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        return legacyListener != null && legacyListener.tryUnmanage(referents);
    }

    @Override
    public boolean tryRetainReference() {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        return legacyListener != null && legacyListener.tryRetainReference();
    }

    @Override
    public void dropReference() {
        final ShiftObliviousListener legacyListener = legacyListenerReference.get();
        if (legacyListener != null) {
            legacyListener.dropReference();
        }
    }

    @Override
    public WeakReference<? extends LivenessReferent> getWeakReference() {
        return legacyListenerReference;
    }
}
