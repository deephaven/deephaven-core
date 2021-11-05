/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.base.cache.RetentionCache;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.liveness.Liveness;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * This class is used for Listeners that represent "leaf" nodes in the update propagation tree.
 *
 * It provides an optional retention cache, to prevent listeners from being garbage collected.
 *
 * For creating internally ticking table nodes, instead use {@link BaseTable.ShiftObliviousListenerImpl}
 */
public abstract class ShiftObliviousInstrumentedListenerAdapter extends ShiftObliviousInstrumentedListener {

    private static final RetentionCache<ShiftObliviousInstrumentedListenerAdapter> RETENTION_CACHE =
            new RetentionCache<>();

    private final boolean retain;

    @ReferentialIntegrity
    protected final Table source;

    /**
     * Create an instrumented listener for source. No description is provided.
     *
     * @param source The source table this listener will subscribe to - needed for preserving referential integrity
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     *        In most scenarios, it's better to specify {@code false} and keep a reference in the calling code.
     */
    public ShiftObliviousInstrumentedListenerAdapter(@NotNull final Table source, final boolean retain) {
        this(null, source, retain);
    }

    /**
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table this listener will subscribe to - needed for preserving referential integrity.
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     *        In most scenarios, it's better to specify {@code false} and keep a reference in the calling code.
     */
    public ShiftObliviousInstrumentedListenerAdapter(@Nullable final String description, @NotNull final Table source,
            final boolean retain) {
        super(description);
        this.source = Require.neqNull(source, "source");
        if (this.retain = retain) {
            RETENTION_CACHE.retain(this);
            if (Liveness.DEBUG_MODE_ENABLED) {
                Liveness.log.info().append("LivenessDebug: ShiftObliviousInstrumentedListenerAdapter ")
                        .append(Utils.REFERENT_FORMATTER, this).append(" created with retention enabled").endl();
            }
        }
        manage(source);
    }

    @Override
    public abstract void onUpdate(RowSet added, RowSet removed, RowSet modified);

    /**
     * Called when the source table produces an error
     *
     * @param originalException the original throwable that caused this error
     * @param sourceEntry the performance tracker entry that was active when the error occurred
     */
    @Override
    public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
        try {
            AsyncErrorLogger.log(DBTimeUtils.currentTime(), sourceEntry, sourceEntry, originalException);
            AsyncClientErrorNotifier.reportError(originalException);
        } catch (IOException e) {
            throw new RuntimeException("Exception in " + sourceEntry.toString(), originalException);
        }
    }

    @Override
    public boolean canExecute(final long step) {
        return source.satisfied(step);
    }

    @Override
    protected void destroy() {
        source.removeUpdateListener(this);
        source.removeDirectUpdateListener(this);
        if (retain) {
            RETENTION_CACHE.forget(this);
        }
    }
}
