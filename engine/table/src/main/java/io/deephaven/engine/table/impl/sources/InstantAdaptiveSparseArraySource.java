//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A sparse in-memory column source of {@link Instant Instants}, which stores the epoch nanoseconds of each value in a
 * {@link LongAdaptiveSparseArraySource}. It may be reinterpreted as that long source.
 */
public class InstantAdaptiveSparseArraySource extends AbstractColumnSource<Instant>
        implements MutableColumnSourceGetDefaults.ForLongAsInstant, WritableColumnSource<Instant>,
        FillUnordered<Values>, InMemoryColumnSource, PossiblyImmutableColumnSource, RowSetShiftCallback,
        SparseBlockColumnSource, WritableSourceWithPrepareForParallelPopulation, ConvertibleTimeSource {

    private final LongAdaptiveSparseArraySource nanoSource;

    public InstantAdaptiveSparseArraySource() {
        super(Instant.class);
        nanoSource = new LongAdaptiveSparseArraySource();
    }

    // region Getters & Setters
    @Override
    public long getLong(final long rowKey) {
        return nanoSource.getLong(rowKey);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        return nanoSource.getPrevLong(rowKey);
    }

    @Override
    public void set(final long key, final Instant value) {
        nanoSource.set(key, DateTimeUtils.epochNanos(value));
    }

    @Override
    public void set(final long key, final long value) {
        nanoSource.set(key, value);
    }

    @Override
    public void setNull(final long key) {
        nanoSource.setNull(key);
    }

    @Override
    public void setNull(@NotNull final RowSequence rowSequence) {
        nanoSource.setNull(rowSequence);
    }

    @Override
    public void ensureCapacity(final long capacity, final boolean nullFilled) {
        nanoSource.ensureCapacity(capacity, nullFilled);
    }
    // endregion Getters & Setters

    // region Structure
    @Override
    public void startTrackingPrevValues() {
        nanoSource.startTrackingPrevValues();
    }

    @Override
    public boolean isImmutable() {
        return nanoSource.isImmutable();
    }

    @Override
    public void setImmutable() {
        nanoSource.setImmutable();
    }

    @Override
    public void shift(final RowSet rowSet, final long shiftDelta) {
        nanoSource.shift(rowSet, shiftDelta);
    }

    @Override
    public void clearBlocks(final RowSet blocksToClear, final RowSet removeBlocks2, final RowSet removeBlocks1,
            final boolean empty) {
        nanoSource.clearBlocks(blocksToClear, removeBlocks2, removeBlocks1, empty);
    }

    @TestUseOnly
    @Override
    public long estimateSize() {
        return nanoSource.estimateSize();
    }

    @Override
    public int parallelPopulationAlignment() {
        return nanoSource.parallelPopulationAlignment();
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence) {
        nanoSource.prepareForParallelPopulation(rowSequence);
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence, final boolean alignedDivisions) {
        nanoSource.prepareForParallelPopulation(rowSequence, alignedDivisions);
    }
    // endregion Structure

    // region Chunking
    private class ToInstantFillContext implements FillContext {
        private final GetContext nanoGetContext;
        private final FillContext nanoFillContext;
        private final WritableLongChunk<Values> nanoChunk;

        private ToInstantFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            nanoGetContext = nanoSource.makeGetContext(chunkCapacity, sharedContext);
            nanoFillContext = nanoSource.makeFillContext(chunkCapacity, sharedContext);
            nanoChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            nanoGetContext.close();
            nanoFillContext.close();
            nanoChunk.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToInstantFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ToInstantFillContext fillContext = (ToInstantFillContext) context;
        convertToInstant(destination, nanoSource.getChunk(fillContext.nanoGetContext, rowSequence));
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToInstantFillContext fillContext = (ToInstantFillContext) context;
        convertToInstant(destination, nanoSource.getPrevChunk(fillContext.nanoGetContext, rowSequence).asLongChunk());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void fillChunkUnordered(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final ToInstantFillContext fillContext = (ToInstantFillContext) context;
        fillContext.nanoChunk.setSize(keys.size());
        nanoSource.fillChunkUnordered(fillContext.nanoFillContext, fillContext.nanoChunk, keys);
        convertToInstant(dest, fillContext.nanoChunk);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest, @NotNull final LongChunk<? extends RowKeys> keys) {
        final ToInstantFillContext fillContext = (ToInstantFillContext) context;
        fillContext.nanoChunk.setSize(keys.size());
        nanoSource.fillPrevChunkUnordered(fillContext.nanoFillContext, fillContext.nanoChunk, keys);
        convertToInstant(dest, fillContext.nanoChunk);
    }

    private static void convertToInstant(@NotNull final WritableChunk<? super Values> destination,
            @NotNull final LongChunk<? extends Values> nanoChunk) {
        final WritableObjectChunk<Instant, ? super Values> instantDestination = destination.asWritableObjectChunk();
        final int size = nanoChunk.size();
        for (int ii = 0; ii < size; ++ii) {
            instantDestination.set(ii, DateTimeUtils.epochNanosToInstant(nanoChunk.get(ii)));
        }
        instantDestination.setSize(size);
    }

    private class ToNanosFillFromContext implements FillFromContext {
        private final FillFromContext nanoFillFromContext;
        private final WritableLongChunk<Values> nanoChunk;

        private ToNanosFillFromContext(final int chunkCapacity) {
            nanoFillFromContext = nanoSource.makeFillFromContext(chunkCapacity);
            nanoChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            nanoFillFromContext.close();
            nanoChunk.close();
        }

        private LongChunk<Values> convert(@NotNull final ObjectChunk<Instant, ? extends Values> src) {
            final int size = src.size();
            for (int ii = 0; ii < size; ++ii) {
                nanoChunk.set(ii, DateTimeUtils.epochNanos(src.get(ii)));
            }
            nanoChunk.setSize(size);
            return nanoChunk;
        }
    }

    @Override
    public FillFromContext makeFillFromContext(final int chunkCapacity) {
        return new ToNanosFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(@NotNull final FillFromContext context, @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        final ToNanosFillFromContext fillFromContext = (ToNanosFillFromContext) context;
        nanoSource.fillFromChunk(fillFromContext.nanoFillFromContext,
                fillFromContext.convert(src.asObjectChunk()), rowSequence);
    }

    @Override
    public void fillFromChunkUnordered(@NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src, @NotNull final LongChunk<RowKeys> keys) {
        final ToNanosFillFromContext fillFromContext = (ToNanosFillFromContext) context;
        nanoSource.fillFromChunkUnordered(fillFromContext.nanoFillFromContext,
                fillFromContext.convert(src.asObjectChunk()), keys);
    }
    // endregion Chunking

    // region Reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Long.class
                || alternateDataType == Instant.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == Instant.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        }
        return (ColumnSource<ALTERNATE_DATA_TYPE>) nanoSource;
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new LongAsZonedDateTimeColumnSource(nanoSource, Require.neqNull(zone, "zone"));
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return new LongAsLocalDateColumnSource(nanoSource, zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return new LongAsLocalTimeColumnSource(nanoSource, zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return this;
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return nanoSource;
    }
    // endregion Reinterpretation
}
