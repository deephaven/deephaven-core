package io.deephaven.engine.table.impl.by;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

/**
 * {@link ColumnSource} implementation that delegates to the main and alternate sources for our incremental open
 * addressed hash table key columns that swap back and forth between a "main" and "alternate" source.  Note that
 * the main and alternate swap back and forth, from the perspective of this column source the main source is
 * addressed by zero; and the alternate source is addressed starting at {@link #ALTERNATE_SWITCH_MASK}.  Neither
 * source may have addresses greater than {@link #ALTERNATE_INNER_MASK}.
 */
public class AlternatingColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
        implements ColumnSource<DATA_TYPE> {
    public static final long ALTERNATE_SWITCH_MASK = 0x4000_0000;
    public static final long ALTERNATE_INNER_MASK = 0x3fff_ffff;

    private static class SourceHolder<DATA_TYPE> {
        private ColumnSource<DATA_TYPE> mainSource;
        private ColumnSource<DATA_TYPE> alternateSource;

        private SourceHolder(ColumnSource<DATA_TYPE> mainSource, ColumnSource<DATA_TYPE> alternateSource) {
            this.mainSource = mainSource;
            this.alternateSource = alternateSource;
        }
    }

    private final SourceHolder<DATA_TYPE> sourceHolder;
    private final WeakReferenceManager<Consumer<SourceHolder>> sourceHolderListeners = new WeakReferenceManager<>();

    AlternatingColumnSource(@NotNull final Class<DATA_TYPE> dataType,
            @Nullable final Class<?> componentType,
            final SourceHolder sourceHolder) {
        super(dataType, componentType);
        this.sourceHolder = sourceHolder;
    }

    AlternatingColumnSource(@NotNull final Class<DATA_TYPE> dataType,
            @Nullable final Class<?> componentType,
            final ColumnSource<DATA_TYPE> mainSource,
            final ColumnSource<DATA_TYPE> alternateSource) {
        this(dataType, componentType, new SourceHolder(mainSource, alternateSource));
    }

    public AlternatingColumnSource(@NotNull final ColumnSource<DATA_TYPE> mainSource,
            final ColumnSource<DATA_TYPE> alternateSource) {
        this(mainSource.getType(), mainSource.getComponentType(), mainSource, alternateSource);
    }

    void setSources(ColumnSource<DATA_TYPE> mainSource, ColumnSource<DATA_TYPE> alternateSource) {
        sourceHolder.mainSource = mainSource;
        sourceHolder.alternateSource = alternateSource;
        sourceHolderListeners.forEachValidReference(x -> x.accept(sourceHolder));
    }

    private static class AlternatingFillContext implements FillContext {

        final FillContext mainFillContext;
        final FillContext alternateFillContext;
        final ShiftedRowSequence alternateShiftedRowSequence;
        final ResettableWritableChunk<Any> alternateDestinationSlice;

        private AlternatingFillContext(@Nullable final ColumnSource<?> mainSource,
                @Nullable final ColumnSource<?> alternateSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            // TODO: Implement a proper shareable context to use when combining fills from the main and overflow
            // sources. Current usage is "safe" because sources are only exposed through this wrapper, and all
            // sources at a given level will split their keys the same, but this is not ideal.
            if (mainSource != null) {
                mainFillContext = mainSource.makeFillContext(chunkCapacity, sharedContext);
            } else {
                mainFillContext = null;
            }
            if (alternateSource != null) {
                alternateFillContext = alternateSource.makeFillContext(chunkCapacity, sharedContext);
                alternateShiftedRowSequence = new ShiftedRowSequence();
                alternateDestinationSlice = alternateSource.getChunkType().makeResettableWritableChunk();
            } else {
                alternateFillContext = null;
                alternateShiftedRowSequence = null;
                alternateDestinationSlice = null;
            }
        }

        @Override
        public void close() {
            if (mainFillContext != null) {
                mainFillContext.close();
            }
            if (alternateFillContext != null) {
                alternateFillContext.close();
                alternateShiftedRowSequence.close();
                alternateDestinationSlice.close();
            }
        }
    }

    private static final class AlternatingGetContext extends AlternatingFillContext implements GetContext {

        private final GetContext mainGetContext;
        private final GetContext alternateGetContext;
        private final WritableChunk<Values> mergeChunk;

        private AlternatingGetContext(@Nullable final ColumnSource<?> mainSource,
                @Nullable final ColumnSource<?> alternateSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            super(mainSource, alternateSource, chunkCapacity, sharedContext);
            if (mainSource != null) {
                mainGetContext = mainSource.makeGetContext(chunkCapacity, sharedContext);
            } else {
                mainGetContext = null;
            }
            if (alternateSource != null) {
                alternateGetContext = alternateSource.makeGetContext(chunkCapacity, sharedContext);
            } else {
                alternateGetContext = null;
            }
            if (mainGetContext != null && alternateGetContext != null) {
                mergeChunk = mainSource.getChunkType().makeWritableChunk(chunkCapacity);
            } else {
                mergeChunk = null;
            }
        }

        @Override
        public void close() {
            super.close();
            if (mainGetContext != null) {
                mainGetContext.close();
            }
            if (alternateGetContext != null) {
                alternateGetContext.close();
            }
            if (mergeChunk != null) {
                mergeChunk.close();
            }
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AlternatingFillContext(sourceHolder.mainSource, sourceHolder.alternateSource, chunkCapacity,
                sharedContext);
    }

    @Override
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AlternatingGetContext(sourceHolder.mainSource, sourceHolder.alternateSource, chunkCapacity,
                sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final AlternatingFillContext typedContext = (AlternatingFillContext) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            sourceHolder.mainSource.fillChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            sourceHolder.alternateSource.fillChunk(typedContext.alternateFillContext, destination,
                    typedContext.alternateShiftedRowSequence);
            typedContext.alternateShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillChunk(@NotNull final AlternatingFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstOverflowChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, ALTERNATE_SWITCH_MASK - 1)) {
            firstOverflowChunkPosition = mainRowSequenceSlice.intSize();
            sourceHolder.mainSource.fillChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence overflowRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.alternateShiftedRowSequence.reset(overflowRowSequenceSlice, -ALTERNATE_SWITCH_MASK);
            sourceHolder.alternateSource.fillChunk(typedContext.alternateFillContext,
                    typedContext.alternateDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.alternateShiftedRowSequence);
        }
        typedContext.alternateDestinationSlice.clear();
        typedContext.alternateShiftedRowSequence.clear();
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final AlternatingFillContext typedContext = (AlternatingFillContext) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            sourceHolder.mainSource.fillPrevChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            sourceHolder.alternateSource.fillPrevChunk(typedContext.alternateFillContext, destination,
                    typedContext.alternateShiftedRowSequence);
            typedContext.alternateShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillPrevChunk(@NotNull final AlternatingFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstOverflowChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, ALTERNATE_SWITCH_MASK - 1)) {
            firstOverflowChunkPosition = mainRowSequenceSlice.intSize();
            sourceHolder.mainSource.fillPrevChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence overflowRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.alternateShiftedRowSequence.reset(overflowRowSequenceSlice, -ALTERNATE_SWITCH_MASK);
            sourceHolder.alternateSource.fillPrevChunk(typedContext.alternateFillContext,
                    typedContext.alternateDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.alternateShiftedRowSequence);
            typedContext.alternateDestinationSlice.clear();
            typedContext.alternateShiftedRowSequence.clear();
        }
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final AlternatingGetContext typedContext = (AlternatingGetContext) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return sourceHolder.mainSource.getChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            return sourceHolder.alternateSource.getChunk(typedContext.alternateGetContext,
                    typedContext.alternateShiftedRowSequence);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final AlternatingGetContext typedContext = (AlternatingGetContext) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return sourceHolder.mainSource.getPrevChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            return sourceHolder.alternateSource.getPrevChunk(typedContext.alternateGetContext,
                    typedContext.alternateShiftedRowSequence);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final DATA_TYPE get(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.get(innerLocation(index))
                : sourceHolder.mainSource.get(index);
    }

    @Override
    public final Boolean getBoolean(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getBoolean(innerLocation(index))
                : sourceHolder.mainSource.getBoolean(index);
    }

    @Override
    public final byte getByte(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getByte(innerLocation(index))
                : sourceHolder.mainSource.getByte(index);
    }

    @Override
    public final char getChar(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getChar(innerLocation(index))
                : sourceHolder.mainSource.getChar(index);
    }

    @Override
    public final double getDouble(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getDouble(innerLocation(index))
                : sourceHolder.mainSource.getDouble(index);
    }

    @Override
    public final float getFloat(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getFloat(innerLocation(index))
                : sourceHolder.mainSource.getFloat(index);
    }

    @Override
    public final int getInt(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getInt(innerLocation(index))
                : sourceHolder.mainSource.getInt(index);
    }

    @Override
    public final long getLong(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getLong(innerLocation(index))
                : sourceHolder.mainSource.getLong(index);
    }

    @Override
    public final short getShort(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getShort(innerLocation(index))
                : sourceHolder.mainSource.getShort(index);
    }

    @Override
    public final DATA_TYPE getPrev(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrev(innerLocation(index))
                : sourceHolder.mainSource.getPrev(index);
    }

    @Override
    public final Boolean getPrevBoolean(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevBoolean(innerLocation(index))
                : sourceHolder.mainSource.getPrevBoolean(index);
    }

    @Override
    public final byte getPrevByte(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevByte(innerLocation(index))
                : sourceHolder.mainSource.getPrevByte(index);
    }

    @Override
    public final char getPrevChar(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevChar(innerLocation(index))
                : sourceHolder.mainSource.getPrevChar(index);
    }

    @Override
    public final double getPrevDouble(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevDouble(innerLocation(index))
                : sourceHolder.mainSource.getPrevDouble(index);
    }

    @Override
    public final float getPrevFloat(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevFloat(innerLocation(index))
                : sourceHolder.mainSource.getPrevFloat(index);
    }

    @Override
    public final int getPrevInt(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevInt(innerLocation(index))
                : sourceHolder.mainSource.getPrevInt(index);
    }

    @Override
    public final long getPrevLong(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevLong(innerLocation(index))
                : sourceHolder.mainSource.getPrevLong(index);
    }

    @Override
    public final short getPrevShort(final long index) {
        return isAlternate(index) ? sourceHolder.alternateSource.getPrevShort(innerLocation(index))
                : sourceHolder.mainSource.getPrevShort(index);
    }

    @Override
    public final void startTrackingPrevValues() {
        sourceHolder.mainSource.startTrackingPrevValues();
        sourceHolder.alternateSource.startTrackingPrevValues();
    }

    @Override
    public final boolean isImmutable() {
        return sourceHolder.mainSource.isImmutable() && sourceHolder.alternateSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (sourceHolder.alternateSource == null) {
            return sourceHolder.mainSource.allowsReinterpret(alternateDataType);
        }
        if (sourceHolder.mainSource == null) {
            return sourceHolder.alternateSource.allowsReinterpret(alternateDataType);
        }
        return sourceHolder.mainSource.allowsReinterpret(alternateDataType)
                && sourceHolder.alternateSource.allowsReinterpret(alternateDataType);
    }

    private static final class Reinterpreted<DATA_TYPE> extends AlternatingColumnSource<DATA_TYPE>
            implements Consumer<SourceHolder> {
        private Reinterpreted(@NotNull final Class<DATA_TYPE> dataType,
                @NotNull final AlternatingColumnSource<?> original,
                @NotNull final SourceHolder<DATA_TYPE> sourceHolder) {
            super(dataType, null, sourceHolder);
            original.sourceHolderListeners.add(this);
        }

        @Override
        public void accept(SourceHolder sourceHolder) {
            final SourceHolder<DATA_TYPE> reinterpreted =
                    AlternatingColumnSource.reinterpretSourceHolder(sourceHolder, getType());
            setSources(reinterpreted.mainSource, reinterpreted.alternateSource);
        }
    }

    private static <ALTERNATE_DATA_TYPE, DATA_TYPE> SourceHolder<ALTERNATE_DATA_TYPE> reinterpretSourceHolder(
            SourceHolder<DATA_TYPE> sourceHolder, Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        final ColumnSource<ALTERNATE_DATA_TYPE> newMain =
                sourceHolder.mainSource == null ? null : sourceHolder.mainSource.reinterpret(alternateDataType);
        final ColumnSource<ALTERNATE_DATA_TYPE> newAlternate = sourceHolder.alternateSource == null ? null
                : sourceHolder.alternateSource.reinterpret(alternateDataType);
        return new SourceHolder<>(newMain, newAlternate);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return new Reinterpreted<>(alternateDataType, this, reinterpretSourceHolder(sourceHolder, alternateDataType));
    }

    public static boolean isAlternate(final long index) {
        return (index & ALTERNATE_SWITCH_MASK) != 0;
    }

    public static int innerLocation(final long hashSlot) {
        return (int) (hashSlot & ALTERNATE_INNER_MASK);
    }
}
