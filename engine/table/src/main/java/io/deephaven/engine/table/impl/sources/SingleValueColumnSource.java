//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

public abstract class SingleValueColumnSource<T> extends AbstractColumnSource<T>
        implements WritableColumnSource<T>, ChunkSink<Values>, InMemoryColumnSource,
        RowKeyAgnosticChunkSource<Values> {

    protected transient long changeTime;
    protected boolean isTrackingPrevValues;

    SingleValueColumnSource(@NotNull final Class<T> type) {
        this(type, null);
    }

    SingleValueColumnSource(@NotNull final Class<T> type, @Nullable final Class<?> elementType) {
        super(type, elementType);
    }

    @Override
    public final void startTrackingPrevValues() {
        isTrackingPrevValues = true;
    }

    public static <T> SingleValueColumnSource<T> getSingleValueColumnSource(Class<T> type) {
        return getSingleValueColumnSource(type, null);
    }

    public static <T> SingleValueColumnSource<T> getSingleValueColumnSource(Class<T> type, Class<?> componentType) {
        SingleValueColumnSource<?> result;
        if (type == Byte.class || type == byte.class) {
            result = new ByteSingleValueSource();
        } else if (type == Character.class || type == char.class) {
            result = new CharacterSingleValueSource();
        } else if (type == Double.class || type == double.class) {
            result = new DoubleSingleValueSource();
        } else if (type == Float.class || type == float.class) {
            result = new FloatSingleValueSource();
        } else if (type == Integer.class || type == int.class) {
            result = new IntegerSingleValueSource();
        } else if (type == Long.class || type == long.class) {
            result = new LongSingleValueSource();
        } else if (type == Short.class || type == short.class) {
            result = new ShortSingleValueSource();
        } else if (type == Boolean.class || type == boolean.class) {
            result = new BooleanSingleValueSource();
        } else {
            result = new ObjectSingleValueSource<>(type, componentType);
        }
        // noinspection unchecked
        return (SingleValueColumnSource<T>) result;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    public void set(char value) {
        throw new UnsupportedOperationException();
    }

    public void set(byte value) {
        throw new UnsupportedOperationException();
    }

    public void set(double value) {
        throw new UnsupportedOperationException();
    }

    public void set(float value) {
        throw new UnsupportedOperationException();
    }

    public void set(short value) {
        throw new UnsupportedOperationException();
    }

    public void set(long value) {
        throw new UnsupportedOperationException();
    }

    public void set(int value) {
        throw new UnsupportedOperationException();
    }

    public void set(T value) {
        throw new UnsupportedOperationException();
    }

    public void setNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void setNull(long key) {
        setNull();
    }

    @Override
    public final void setNull(RowSequence rowSequence) {
        if (!rowSequence.isEmpty()) {
            setNull();
        }
    }

    @Override
    public final void ensureCapacity(long capacity, boolean nullFilled) {
        // Do nothing
    }

    @Override
    public FillFromContext makeFillFromContext(int chunkCapacity) {
        return DEFAULT_FILL_FROM_INSTANCE;
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        return new SingleValuePushdownHelper.FilterContext(filter, filterSources);
    }

    @Override
    public void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        onComplete.accept(PushdownResult.SINGLE_VALUE_COLUMN_COST);
    }

    @Override
    public void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        if (selection.isEmpty()) {
            onComplete.accept(PushdownResult.allNoMatch(selection));
            return;
        }

        final SingleValuePushdownHelper.FilterContext filterCtx = (SingleValuePushdownHelper.FilterContext) context;

        // Chunk filtering has lower overhead than creating a dummy table.
        if (filterCtx.supportsChunkFiltering()) {
            final Supplier<Chunk<Values>> chunkSupplier = usePrev ? this::getPrevValueChunk : this::getValueChunk;
            final PushdownResult result =
                    SingleValuePushdownHelper.pushdownChunkFilter(selection, filterCtx, chunkSupplier);
            onComplete.accept(result);
            return;
        }

        // Chunk filtering is not supported, so test against a dummy table.
        final PushdownResult result = SingleValuePushdownHelper.pushdownTableFilter(filter, selection, usePrev, this);
        onComplete.accept(result);
    }

    /**
     * Returns a chunk containing the value for this column source.
     */
    protected abstract Chunk<Values> getValueChunk();

    /**
     * Returns a chunk containing the previous value for this columnSource.
     */
    protected abstract Chunk<Values> getPrevValueChunk();
}
