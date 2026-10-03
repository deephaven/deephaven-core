//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ReverseOperation;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.reverse.ReverseKernel;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * This column source wraps another column source, and returns the values in the opposite order. It must be paired with
 * a ReverseOperation (that can be shared among reversed column sources) that implements the RowSet transformations for
 * this source.
 * <p>
 * Reinterpretation and time conversion delegate to the wrapped source, and the results are reversed views of the
 * wrapped source's results. A source wrapping a {@link ConvertibleTimeSource.Zoned} is itself
 * {@link ConvertibleTimeSource.Zoned}, with the same zone; use {@link #create(ColumnSource, ReverseOperation)} to
 * construct instances.
 */
public class ReversedColumnSource<T> extends AbstractColumnSource<T> implements ConvertibleTimeSource {
    private final ColumnSource<T> innerSource;
    private final ReverseOperation indexReverser;
    private long maxInnerIndex = 0;

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType();
    }

    /**
     * Create a reversed view of {@code innerSource}.
     *
     * @param innerSource the source to reverse
     * @param indexReverser the operation that transforms row keys between the reversed and inner sources
     * @return a reversed view of {@code innerSource}, which is a {@link ConvertibleTimeSource.Zoned} if and only if
     *         {@code innerSource} is
     */
    public static <T> ReversedColumnSource<T> create(
            @NotNull final ColumnSource<T> innerSource,
            @NotNull final ReverseOperation indexReverser) {
        if (innerSource instanceof ConvertibleTimeSource.Zoned) {
            return new ZonedReversedColumnSource<>(innerSource, indexReverser,
                    ((ConvertibleTimeSource.Zoned) innerSource).getZone());
        }
        return new ReversedColumnSource<>(innerSource, indexReverser);
    }

    private ReversedColumnSource(@NotNull ColumnSource<T> innerSource, @NotNull ReverseOperation indexReverser) {
        super(innerSource.getType());
        this.innerSource = innerSource;
        this.indexReverser = indexReverser;
    }

    private static final class ZonedReversedColumnSource<T> extends ReversedColumnSource<T>
            implements ConvertibleTimeSource.Zoned {
        private final ZoneId zone;

        private ZonedReversedColumnSource(
                @NotNull final ColumnSource<T> innerSource,
                @NotNull final ReverseOperation indexReverser,
                @NotNull final ZoneId zone) {
            super(innerSource, indexReverser);
            this.zone = zone;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }
    }

    @Override
    public void startTrackingPrevValues() {
        // Nothing to do.
    }

    @Override
    public T get(long rowKey) {
        return innerSource.get(indexReverser.transform(rowKey));
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        return innerSource.getBoolean(indexReverser.transform(rowKey));
    }

    @Override
    public byte getByte(long rowKey) {
        return innerSource.getByte(indexReverser.transform(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        return innerSource.getChar(indexReverser.transform(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        return innerSource.getDouble(indexReverser.transform(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        return innerSource.getFloat(indexReverser.transform(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        return innerSource.getInt(indexReverser.transform(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        return innerSource.getLong(indexReverser.transform(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        return innerSource.getShort(indexReverser.transform(rowKey));
    }

    @Override
    public T getPrev(long rowKey) {
        return innerSource.getPrev(indexReverser.transformPrev(rowKey));
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        return innerSource.getPrevBoolean(indexReverser.transformPrev(rowKey));
    }

    @Override
    public byte getPrevByte(long rowKey) {
        return innerSource.getPrevByte(indexReverser.transformPrev(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        return innerSource.getPrevChar(indexReverser.transformPrev(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        return innerSource.getPrevDouble(indexReverser.transformPrev(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        return innerSource.getPrevFloat(indexReverser.transformPrev(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        return innerSource.getPrevInt(indexReverser.transformPrev(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        return innerSource.getPrevLong(indexReverser.transformPrev(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        return innerSource.getPrevShort(indexReverser.transformPrev(rowKey));
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    private class FillContext implements ColumnSource.FillContext {
        final ColumnSource.FillContext innerContext;
        final ReverseKernel reverseKernel = ReverseKernel.makeReverseKernel(getChunkType());

        FillContext(int chunkCapacity) {
            this.innerContext = innerSource.makeFillContext(chunkCapacity);
        }

        @Override
        public final void close() {
            innerContext.close();
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull ColumnSource.FillContext _context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final FillContext context = (FillContext) _context;
        final RowSequence reversedIndex = indexReverser.transform(rowSequence.asRowSet());
        innerSource.fillChunk(context.innerContext, destination, reversedIndex);
        context.reverseKernel.reverse(destination);
    }

    @Override
    public void fillPrevChunk(@NotNull ColumnSource.FillContext _context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        final FillContext context = (FillContext) _context;
        final RowSequence reversedIndex = indexReverser.transformPrev(rowSequence.asRowSet());
        innerSource.fillPrevChunk(context.innerContext, destination, reversedIndex);
        context.reverseKernel.reverse(destination);
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return innerSource.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return create(innerSource.reinterpret(alternateDataType), indexReverser);
    }

    @Override
    public boolean supportsTimeConversion() {
        return innerSource instanceof ConvertibleTimeSource
                && ((ConvertibleTimeSource) innerSource).supportsTimeConversion();
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return create(((ConvertibleTimeSource) innerSource).toZonedDateTime(zone), indexReverser);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return create(((ConvertibleTimeSource) innerSource).toLocalDate(zone), indexReverser);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return create(((ConvertibleTimeSource) innerSource).toLocalTime(zone), indexReverser);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return create(((ConvertibleTimeSource) innerSource).toInstant(), indexReverser);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return create(((ConvertibleTimeSource) innerSource).toEpochNano(), indexReverser);
    }
}
