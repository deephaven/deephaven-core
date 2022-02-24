package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;

/**
 * {@link ColumnSource} implementation that delegates to the main and alternate sources for our incremental open
 * addressed hash table key columns that swap back and forth between a "main" and "alternate" source. Note that the main
 * and alternate swap back and forth, from the perspective of this column source the main source is addressed by zero;
 * and the alternate source is addressed starting at {@link #ALTERNATE_SWITCH_MASK}. Neither source may have addresses
 * greater than {@link #ALTERNATE_INNER_MASK}.
 */
public class AlternatingColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
        implements ColumnSource<DATA_TYPE>, FillUnordered {
    public static final long ALTERNATE_SWITCH_MASK = 0x4000_0000;
    public static final long ALTERNATE_INNER_MASK = 0x3fff_ffff;

    private ColumnSource<DATA_TYPE> mainSource;
    private ColumnSource<DATA_TYPE> alternateSource;

    private final WeakReferenceManager<BiConsumer<ColumnSource<DATA_TYPE>, ColumnSource<DATA_TYPE>>> sourceHolderListeners =
            new WeakReferenceManager<>();

    public AlternatingColumnSource(@NotNull final Class<DATA_TYPE> dataType,
            @Nullable final Class<?> componentType,
            final ColumnSource<DATA_TYPE> mainSource,
            final ColumnSource<DATA_TYPE> alternateSource) {
        super(dataType, componentType);
        this.mainSource = mainSource;
        this.alternateSource = alternateSource;
    }

    public AlternatingColumnSource(@NotNull final ColumnSource<DATA_TYPE> mainSource,
            final ColumnSource<DATA_TYPE> alternateSource) {
        this(mainSource.getType(), mainSource.getComponentType(), mainSource, alternateSource);
    }

    public void setSources(ColumnSource<DATA_TYPE> mainSource, ColumnSource<DATA_TYPE> alternateSource) {
        this.mainSource = mainSource;
        this.alternateSource = alternateSource;
        sourceHolderListeners.forEachValidReference(x -> x.accept(mainSource, alternateSource));
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AlternatingFillContextWithUnordered(mainSource, alternateSource,
                chunkCapacity,
                sharedContext);
    }

    @Override
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AlternatingGetContext(mainSource, alternateSource, chunkCapacity,
                sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final AlternatingFillContextWithUnordered typedContext = (AlternatingFillContextWithUnordered) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Alternate locations are always after main locations, so there are no responsive alternate locations
            mainSource.fillChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before alternate locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            alternateSource.fillChunk(typedContext.alternateFillContext, destination,
                    typedContext.alternateShiftedRowSequence);
            typedContext.alternateShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and alternate locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillChunk(@NotNull final BaseAlternatingFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstAlternateChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, ALTERNATE_SWITCH_MASK - 1)) {
            firstAlternateChunkPosition = mainRowSequenceSlice.intSize();
            mainSource.fillChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromAlternate = totalSize - firstAlternateChunkPosition;

        // Set destination size ahead of time, so that resetting our alternate destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence alternateRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstAlternateChunkPosition, sizeFromAlternate)) {
            typedContext.alternateShiftedRowSequence.reset(alternateRowSequenceSlice, -ALTERNATE_SWITCH_MASK);
            alternateSource.fillChunk(typedContext.alternateFillContext,
                    typedContext.alternateDestinationSlice.resetFromChunk(destination, firstAlternateChunkPosition,
                            sizeFromAlternate),
                    typedContext.alternateShiftedRowSequence);
        }
        typedContext.alternateDestinationSlice.clear();
        typedContext.alternateShiftedRowSequence.clear();
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final AlternatingFillContextWithUnordered typedContext = (AlternatingFillContextWithUnordered) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Alternate locations are always after main locations, so there are no responsive alternate locations
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before alternate locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            alternateSource.fillPrevChunk(typedContext.alternateFillContext, destination,
                    typedContext.alternateShiftedRowSequence);
            typedContext.alternateShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and alternate locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillPrevChunk(@NotNull final BaseAlternatingFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstAlternateChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, ALTERNATE_SWITCH_MASK - 1)) {
            firstAlternateChunkPosition = mainRowSequenceSlice.intSize();
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromAlternate = totalSize - firstAlternateChunkPosition;

        // Set destination size ahead of time, so that resetting our alternate destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence alternateRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstAlternateChunkPosition, sizeFromAlternate)) {
            typedContext.alternateShiftedRowSequence.reset(alternateRowSequenceSlice, -ALTERNATE_SWITCH_MASK);
            alternateSource.fillPrevChunk(typedContext.alternateFillContext,
                    typedContext.alternateDestinationSlice.resetFromChunk(destination, firstAlternateChunkPosition,
                            sizeFromAlternate),
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
            // Alternate locations are always after main locations, so there are no responsive alternate locations
            return mainSource.getChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before alternate locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            return alternateSource.getChunk(typedContext.alternateGetContext,
                    typedContext.alternateShiftedRowSequence);
        }
        // We're going to have to mix main and alternate locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final AlternatingGetContext typedContext = (AlternatingGetContext) context;
        if (!isAlternate(rowSequence.lastRowKey())) {
            // Alternate locations are always after main locations, so there are no responsive alternate locations
            return mainSource.getPrevChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isAlternate(rowSequence.firstRowKey())) {
            // Main locations are always before alternate locations, so there are no responsive main locations
            typedContext.alternateShiftedRowSequence.reset(rowSequence, -ALTERNATE_SWITCH_MASK);
            return alternateSource.getPrevChunk(typedContext.alternateGetContext,
                    typedContext.alternateShiftedRowSequence);
        }
        // We're going to have to mix main and alternate locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final DATA_TYPE get(final long index) {
        return isAlternate(index) ? alternateSource.get(innerLocation(index))
                : mainSource.get(index);
    }

    @Override
    public final Boolean getBoolean(final long index) {
        return isAlternate(index) ? alternateSource.getBoolean(innerLocation(index))
                : mainSource.getBoolean(index);
    }

    @Override
    public final byte getByte(final long index) {
        return isAlternate(index) ? alternateSource.getByte(innerLocation(index))
                : mainSource.getByte(index);
    }

    @Override
    public final char getChar(final long index) {
        return isAlternate(index) ? alternateSource.getChar(innerLocation(index))
                : mainSource.getChar(index);
    }

    @Override
    public final double getDouble(final long index) {
        return isAlternate(index) ? alternateSource.getDouble(innerLocation(index))
                : mainSource.getDouble(index);
    }

    @Override
    public final float getFloat(final long index) {
        return isAlternate(index) ? alternateSource.getFloat(innerLocation(index))
                : mainSource.getFloat(index);
    }

    @Override
    public final int getInt(final long index) {
        return isAlternate(index) ? alternateSource.getInt(innerLocation(index))
                : mainSource.getInt(index);
    }

    @Override
    public final long getLong(final long index) {
        return isAlternate(index) ? alternateSource.getLong(innerLocation(index))
                : mainSource.getLong(index);
    }

    @Override
    public final short getShort(final long index) {
        return isAlternate(index) ? alternateSource.getShort(innerLocation(index))
                : mainSource.getShort(index);
    }

    @Override
    public final DATA_TYPE getPrev(final long index) {
        return isAlternate(index) ? alternateSource.getPrev(innerLocation(index))
                : mainSource.getPrev(index);
    }

    @Override
    public final Boolean getPrevBoolean(final long index) {
        return isAlternate(index) ? alternateSource.getPrevBoolean(innerLocation(index))
                : mainSource.getPrevBoolean(index);
    }

    @Override
    public final byte getPrevByte(final long index) {
        return isAlternate(index) ? alternateSource.getPrevByte(innerLocation(index))
                : mainSource.getPrevByte(index);
    }

    @Override
    public final char getPrevChar(final long index) {
        return isAlternate(index) ? alternateSource.getPrevChar(innerLocation(index))
                : mainSource.getPrevChar(index);
    }

    @Override
    public final double getPrevDouble(final long index) {
        return isAlternate(index) ? alternateSource.getPrevDouble(innerLocation(index))
                : mainSource.getPrevDouble(index);
    }

    @Override
    public final float getPrevFloat(final long index) {
        return isAlternate(index) ? alternateSource.getPrevFloat(innerLocation(index))
                : mainSource.getPrevFloat(index);
    }

    @Override
    public final int getPrevInt(final long index) {
        return isAlternate(index) ? alternateSource.getPrevInt(innerLocation(index))
                : mainSource.getPrevInt(index);
    }

    @Override
    public final long getPrevLong(final long index) {
        return isAlternate(index) ? alternateSource.getPrevLong(innerLocation(index))
                : mainSource.getPrevLong(index);
    }

    @Override
    public final short getPrevShort(final long index) {
        return isAlternate(index) ? alternateSource.getPrevShort(innerLocation(index))
                : mainSource.getPrevShort(index);
    }

    @Override
    public final void startTrackingPrevValues() {
        mainSource.startTrackingPrevValues();
        alternateSource.startTrackingPrevValues();
    }

    @Override
    public final boolean isImmutable() {
        return mainSource.isImmutable() && alternateSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateSource == null) {
            return mainSource.allowsReinterpret(alternateDataType);
        }
        if (mainSource == null) {
            return alternateSource.allowsReinterpret(alternateDataType);
        }
        return mainSource.allowsReinterpret(alternateDataType)
                && alternateSource.allowsReinterpret(alternateDataType);
    }

    private static final class Reinterpreted<DATA_TYPE, ORIGINAL_DATA_TYPE> extends AlternatingColumnSource<DATA_TYPE>
            implements BiConsumer<ColumnSource<ORIGINAL_DATA_TYPE>, ColumnSource<ORIGINAL_DATA_TYPE>> {
        private Reinterpreted(@NotNull final Class<DATA_TYPE> dataType,
                @NotNull final AlternatingColumnSource<ORIGINAL_DATA_TYPE> original,
                final ColumnSource<DATA_TYPE> main,
                final ColumnSource<DATA_TYPE> alternate) {
            super(dataType, null, main, alternate);
            original.sourceHolderListeners.add(this);
        }

        @Override
        public void accept(ColumnSource<ORIGINAL_DATA_TYPE> main, ColumnSource<ORIGINAL_DATA_TYPE> alternateSource) {
            setSources(reinterpretInner(main, getType()), reinterpretInner(alternateSource, getType()));
        }
    }

    private static <ALTERNATE_DATA_TYPE, DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpretInner(
            ColumnSource<DATA_TYPE> source, Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return source == null ? null : source.reinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return new Reinterpreted<>(alternateDataType, this, reinterpretInner(mainSource, alternateDataType),
                reinterpretInner(alternateSource, alternateDataType));
    }

    public static boolean isAlternate(final long index) {
        return (index & ALTERNATE_SWITCH_MASK) != 0;
    }

    public static int innerLocation(final long hashSlot) {
        return (int) (hashSlot & ALTERNATE_INNER_MASK);
    }


    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final AlternatingFillContextWithUnordered typedContext = (AlternatingFillContextWithUnordered) context;
        if (alternateSource == null) {
            ((FillUnordered) mainSource).fillChunkUnordered(typedContext.mainFillContext, dest, keys);
        } else if (mainSource == null) {
            doFillAlternateUnorderedDirect(dest, keys, typedContext, false);
        } else {
            final int mainSize = populateInnerKeysMain(keys, typedContext);
            if (mainSize == keys.size()) {
                ((FillUnordered) mainSource).fillChunkUnordered(typedContext.mainFillContext, dest, keys);
            } else if (mainSize == 0) {
                doFillAlternateUnorderedDirect(dest, keys, typedContext, false);
            } else {
                typedContext.innerValues.setSize(keys.size());
                ((FillUnordered) mainSource).fillChunkUnordered(typedContext.mainFillContext,
                        typedContext.innerValues, typedContext.innerKeys);
                populateInnerKeysAlternate(keys, typedContext);

                ((FillUnordered) alternateSource).fillChunkUnordered(typedContext.alternateFillContext,
                        typedContext.innerSlice, typedContext.innerKeys);

                typedContext.mergeKernel.mergeContext(dest, keys, typedContext.innerValues, mainSize);
            }
        }
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final AlternatingFillContextWithUnordered typedContext = (AlternatingFillContextWithUnordered) context;
        if (alternateSource == null) {
            ((FillUnordered) mainSource).fillPrevChunkUnordered(typedContext.mainFillContext, dest, keys);
        } else if (mainSource == null) {
            doFillAlternateUnorderedDirect(dest, keys, typedContext, true);
        } else {
            final int mainSize = populateInnerKeysMain(keys, typedContext);
            if (mainSize == keys.size()) {
                ((FillUnordered) mainSource).fillPrevChunkUnordered(typedContext.mainFillContext, dest,
                        keys);
            } else if (mainSize == 0) {
                doFillAlternateUnorderedDirect(dest, keys, typedContext, true);
            } else {
                typedContext.innerValues.setSize(keys.size());
                ((FillUnordered) mainSource).fillPrevChunkUnordered(typedContext.mainFillContext,
                        typedContext.innerValues, typedContext.innerKeys);
                // fill the alternate into the back half of the chunk
                populateInnerKeysAlternate(keys, typedContext);
                ((FillUnordered) alternateSource).fillPrevChunkUnordered(typedContext.alternateFillContext,
                        typedContext.innerSlice, typedContext.innerKeys);

                typedContext.mergeKernel.mergeContext(dest, keys, typedContext.innerValues, mainSize);
            }
        }
    }

    private int populateInnerKeysMain(@NotNull LongChunk<? extends RowKeys> keys,
            AlternatingFillContextWithUnordered typedContext) {
        typedContext.innerKeys.setSize(0);
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long outerKey = keys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                typedContext.innerKeys.add(keys.get(ii));
            }
        }
        return typedContext.innerKeys.size();
    }

    private void populateInnerKeysAlternate(@NotNull LongChunk<? extends RowKeys> keys,
            AlternatingFillContextWithUnordered typedContext) {
        // fill the alternate into the back half of the chunk
        typedContext.innerValues.setSize(keys.size());
        typedContext.innerSlice.resetFromChunk((WritableChunk) typedContext.innerValues, typedContext.innerKeys.size(),
                keys.size() - typedContext.innerKeys.size());
        typedContext.innerKeys.setSize(0);
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long outerKey = keys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) != 0) {
                typedContext.innerKeys.add(keys.get(ii) & ALTERNATE_INNER_MASK);
            }
        }
    }

    private void doFillAlternateUnorderedDirect(@NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys, AlternatingFillContextWithUnordered typedContext,
            boolean usePrev) {
        typedContext.innerKeys.setSize(keys.size());
        for (int ii = 0; ii < keys.size(); ++ii) {
            typedContext.innerKeys.set(ii, keys.get(ii) & ALTERNATE_INNER_MASK);
        }
        if (usePrev) {
            ((FillUnordered) alternateSource).fillPrevChunkUnordered(typedContext.alternateFillContext,
                    dest, typedContext.innerKeys);
        } else {
            ((FillUnordered) alternateSource).fillChunkUnordered(typedContext.alternateFillContext, dest,
                    typedContext.innerKeys);
        }
    }


    @Override
    public boolean providesFillUnordered() {
        return (mainSource == null || FillUnordered.providesFillUnordered(mainSource)) &&
                (alternateSource == null
                        || FillUnordered.providesFillUnordered(alternateSource));
    }
}
