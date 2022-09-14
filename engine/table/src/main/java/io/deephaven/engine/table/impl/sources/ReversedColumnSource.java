/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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

/**
 * This column source wraps another column source, and returns the values in the opposite order. It must be paired with
 * a ReverseOperation (that can be shared among reversed column sources) that implements the RowSet transformations for
 * this source.
 */
public class ReversedColumnSource<T> extends AbstractColumnSource<T> {
    private final ColumnSource<T> innerSource;
    private final ReverseOperation indexReverser;
    private long maxInnerIndex = 0;

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType();
    }

    public ReversedColumnSource(@NotNull ColumnSource<T> innerSource, @NotNull ReverseOperation indexReverser) {
        super(innerSource.getType());
        this.innerSource = innerSource;
        this.indexReverser = indexReverser;
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
}
