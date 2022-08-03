/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.updategraph.UpdateCommitter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class AddsToRingsListener extends BaseTable.ListenerImpl {

    enum Init {
        NONE, FROM_PREVIOUS, FROM_CURRENT
    }

    static Table of(SwapListener swapListener, Table parent, int capacity, Init init) {
        if (swapListener == null && init == Init.NONE) {
            throw new IllegalArgumentException(String.format(
                    "Trying to initialize %s against a static table, but init=NONE; no data will be filled in this case.",
                    AddsToRingsListener.class.getName()));
        }
        final Map<String, ? extends ColumnSource<?>> sourceMap = parent.getColumnSourceMap();
        final int numColumns = sourceMap.size();
        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>(numColumns);
        final ColumnSource<?>[] sources = new ColumnSource[numColumns];
        final boolean[] sourceHasUnboundedFillContexts = new boolean[numColumns];
        final RingColumnSource<?>[] rings = new RingColumnSource[numColumns];
        int ix = 0;
        for (Map.Entry<String, ? extends ColumnSource<?>> e : sourceMap.entrySet()) {
            final String name = e.getKey();

            final ColumnSource<?> original = e.getValue();

            // for the source columns, we would like to read primitives instead of objects in cases where it is possible
            final ColumnSource<?> source = ReinterpretUtils.maybeConvertToPrimitive(original);

            final boolean sourceSupportsUnboundedFill;
            try (final FillContext tmpFillContext = source.makeFillContext(1)) {
                sourceSupportsUnboundedFill = tmpFillContext.supportsUnboundedFill();
            }

            // for the destination sources, we know they are array backed sources that will actually store primitives
            // and we can fill efficiently
            final RingColumnSource<?> ring = RingColumnSource.of(capacity, source.getType(), source.getComponentType());

            // Re-interpret back to the original type
            final ColumnSource<?> output =
                    source == original ? ring : ReinterpretUtils.convertToOriginal(original.getType(), ring);

            sources[ix] = source;
            sourceHasUnboundedFillContexts[ix] = sourceSupportsUnboundedFill;
            rings[ix] = ring;
            resultMap.put(name, output);
            ++ix;
        }

        final WritableRowSet initialRowSet = init(init, parent, sources, sourceHasUnboundedFillContexts, rings);
        final QueryTable result = new QueryTable(initialRowSet.toTracking(), resultMap);
        if (swapListener == null) {
            result.setRefreshing(false);
        } else {
            result.setRefreshing(true);
            result.addParentReference(swapListener);
        }
        final AddsToRingsListener listener = new AddsToRingsListener(
                "AddsToRingsListener", parent, result, sources, sourceHasUnboundedFillContexts, rings);
        if (swapListener != null) {
            swapListener.setListenerAndResult(listener, result);
        }
        return result;
    }

    private static WritableRowSet init(Init init, Table parent, ColumnSource<?>[] sources,
            boolean[] sourceHasUnboundedFillContexts, RingColumnSource<?>[] rings) {
        if (init == Init.NONE) {
            return RowSetFactory.empty();
        }
        final boolean usePrev = init == Init.FROM_PREVIOUS;
        try (final RowSet prevToClose = usePrev ? parent.getRowSet().copyPrev() : null) {
            final RowSet srcKeys = usePrev ? prevToClose : parent.getRowSet();
            if (srcKeys.isEmpty()) {
                return RowSetFactory.empty();
            }
            for (int i = 0; i < rings.length; ++i) {
                final ChunkSource<? extends Values> source = usePrev ? sources[i].getPrevSource() : sources[i];
                if (sourceHasUnboundedFillContexts[i]) {
                    rings[i].appendUnbounded(source, srcKeys);
                } else {
                    rings[i].appendBounded(source, srcKeys);
                }
                rings[i].bringPreviousUpToDate();
            }
        }
        return rings[0].rowSet();
    }

    private final ColumnSource<?>[] sources;
    private final boolean[] sourceHasUnboundedFillContexts;
    private final RingColumnSource<?>[] rings;
    private final UpdateCommitter<AddsToRingsListener> prevFlusher;

    private AddsToRingsListener(
            String description,
            Table parent,
            BaseTable dependent,
            ColumnSource<?>[] sources,
            boolean[] sourceHasUnboundedFillContexts,
            RingColumnSource<?>[] rings) {
        super(description, parent, Objects.requireNonNull(dependent));
        this.sources = Objects.requireNonNull(sources);
        this.sourceHasUnboundedFillContexts = sourceHasUnboundedFillContexts;
        this.rings = Objects.requireNonNull(rings);
        if (sources.length != sourceHasUnboundedFillContexts.length) {
            throw new IllegalArgumentException();
        }
        if (sources.length != rings.length) {
            throw new IllegalArgumentException();
        }
        if (sources.length == 0) {
            throw new IllegalArgumentException();
        }
        if (!dependent.getRowSet().isWritable()) {
            throw new IllegalArgumentException("Expected writable row set");
        }
        final int capacity = rings[0].capacity();
        for (RingColumnSource<?> ring : rings) {
            if (ring.capacity() != capacity) {
                throw new IllegalArgumentException();
            }
        }
        prevFlusher = new UpdateCommitter<>(this, AddsToRingsListener::bringPreviousUpToDate);
    }

    private WritableRowSet resultRowSet() {
        return getDependent().getRowSet().writableCast();
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
            throw new IllegalStateException("Not expecting modifies or shifts");
        }
        // Ignoring any removes
        if (upstream.added().isEmpty()) {
            return;
        }
        append(upstream.added());
    }

    private void append(RowSet added) {
        for (int i = 0; i < rings.length; ++i) {
            if (sourceHasUnboundedFillContexts[i]) {
                rings[i].appendUnbounded(sources[i], added);
            } else {
                rings[i].appendBounded(sources[i], added);
            }
        }
        prevFlusher.maybeActivate();
        final TableUpdate update = rings[0].tableUpdate();
        resultRowSet().update(update.added(), update.removed());
        getDependent().notifyListeners(update);
    }

    private void bringPreviousUpToDate() {
        for (RingColumnSource<?> ring : rings) {
            ring.bringPreviousUpToDate();
        }
    }
}
