package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for explicitly boxing a primitive into a more complex type, e.g. {@code byte} as
 * {@link Boolean} or {@code long} as {@link DBDateTime}.
 */
public abstract class BoxedColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
        implements MutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    final ColumnSource<?> originalSource;

    BoxedColumnSource(@NotNull final Class<DATA_TYPE> dataType, @NotNull final ColumnSource<?> originalSource) {
        super(dataType);
        this.originalSource = originalSource;
    }

    @Override
    public abstract DATA_TYPE get(long index);

    @Override
    public abstract DATA_TYPE getPrev(long index);

    abstract void transformChunk(@NotNull final Chunk<? extends Values> source,
            @NotNull final WritableChunk<? super Values> destination);

    private static final class BoxedFillContext implements FillContext {

        private final GetContext originalGetContext;

        private BoxedFillContext(@NotNull final ColumnSource<?> originalSource, final int chunkCapacity,
                final SharedContext sharedContext) {
            originalGetContext = originalSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public final void close() {
            originalGetContext.close();
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new BoxedFillContext(originalSource, chunkCapacity, sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final Chunk<? extends Values> originalChunk =
                originalSource.getChunk(((BoxedFillContext) context).originalGetContext, orderedKeys);
        transformChunk(originalChunk, destination);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final Chunk<? extends Values> originalChunk =
                originalSource.getPrevChunk(((BoxedFillContext) context).originalGetContext, orderedKeys);
        transformChunk(originalChunk, destination);
    }

    @Override
    public final boolean isImmutable() {
        return originalSource.isImmutable();
    }

    @Override
    public final <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return originalSource.getType() == alternateDataType;
    }

    @Override
    protected final <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) originalSource;
    }

    static final class OfBoolean extends BoxedColumnSource<Boolean> {

        OfBoolean(@NotNull final ColumnSource<Byte> originalSource) {
            super(Boolean.class, originalSource);
        }

        @Override
        public final Boolean get(final long index) {
            return BooleanUtils.byteAsBoolean(originalSource.getByte(index));
        }

        @Override
        public final Boolean getPrev(final long index) {
            return BooleanUtils.byteAsBoolean(originalSource.getPrevByte(index));
        }

        @Override
        final void transformChunk(@NotNull final Chunk<? extends Values> source,
                @NotNull final WritableChunk<? super Values> destination) {
            final ByteChunk<? extends Values> typedSource = source.asByteChunk();
            final WritableObjectChunk<Boolean, ? super Values> typedDestination = destination.asWritableObjectChunk();

            final int sourceSize = typedSource.size();
            for (int pi = 0; pi < sourceSize; ++pi) {
                typedDestination.set(pi, BooleanUtils.byteAsBoolean(typedSource.get(pi)));
            }
            typedDestination.setSize(sourceSize);
        }
    }

    public static final class OfDateTime extends BoxedColumnSource<DBDateTime> {

        public OfDateTime(@NotNull final ColumnSource<Long> originalSource) {
            super(DBDateTime.class, originalSource);
            Assert.eq(originalSource.getType(), "originalSource.getType()", long.class);
        }

        @Override
        public final DBDateTime get(final long index) {
            return DBTimeUtils.nanosToTime(originalSource.getLong(index));
        }

        @Override
        public final DBDateTime getPrev(final long index) {
            return DBTimeUtils.nanosToTime(originalSource.getPrevLong(index));
        }

        @Override
        final void transformChunk(@NotNull final Chunk<? extends Values> source,
                @NotNull final WritableChunk<? super Values> destination) {
            final LongChunk<? extends Values> typedSource = source.asLongChunk();
            final WritableObjectChunk<DBDateTime, ? super Values> typedDestination =
                    destination.asWritableObjectChunk();

            final int sourceSize = typedSource.size();
            for (int pi = 0; pi < sourceSize; ++pi) {
                typedDestination.set(pi, DBTimeUtils.nanosToTime(typedSource.get(pi)));
            }
            typedDestination.setSize(sourceSize);
        }
    }
}
