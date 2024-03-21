//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.ElementSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.Context;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

/**
 * The Formula class is used within a FormulaColumn to compute individual table cell values.
 */
public abstract class Formula implements ElementSource {
    public TrackingRowSet getRowSet() {
        return __rowSet;
    }

    protected final TrackingRowSet __rowSet;

    protected Formula(TrackingRowSet __rowSet) {
        this.__rowSet = __rowSet;
    }

    @Override
    public abstract Object getPrev(long rowKey);

    @Override
    public abstract Object get(long rowKey);

    @Override
    public Boolean getBoolean(long rowKey) {
        return (Boolean) get(rowKey);
    }

    @Override
    public byte getByte(long rowKey) {
        final Byte aByte = (Byte) get(rowKey);
        return aByte == null ? NULL_BYTE : aByte;
    }

    @Override
    public char getChar(long rowKey) {
        final Character character = (Character) get(rowKey);
        return character == null ? NULL_CHAR : character;
    }

    @Override
    public double getDouble(long rowKey) {
        final Double aDouble = (Double) get(rowKey);
        return aDouble == null ? NULL_DOUBLE : aDouble;
    }

    @Override
    public float getFloat(long rowKey) {
        final Float aFloat = (Float) get(rowKey);
        return aFloat == null ? NULL_FLOAT : aFloat;
    }

    @Override
    public int getInt(long rowKey) {
        final Integer integer = (Integer) get(rowKey);
        return integer == null ? NULL_INT : integer;
    }

    @Override
    public long getLong(long rowKey) {
        final Long aLong = (Long) get(rowKey);
        return aLong == null ? NULL_LONG : aLong;
    }

    @Override
    public short getShort(long rowKey) {
        final Short aShort = (Short) get(rowKey);
        return aShort == null ? NULL_SHORT : aShort;
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        return (Boolean) getPrev(rowKey);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        final Byte aByte = (Byte) getPrev(rowKey);
        return aByte == null ? NULL_BYTE : aByte;
    }

    @Override
    public char getPrevChar(long rowKey) {
        final Character character = (Character) getPrev(rowKey);
        return character == null ? NULL_CHAR : character;
    }

    @Override
    public double getPrevDouble(long rowKey) {
        final Double aDouble = (Double) getPrev(rowKey);
        return aDouble == null ? NULL_DOUBLE : aDouble;
    }

    @Override
    public float getPrevFloat(long rowKey) {
        final Float aFloat = (Float) getPrev(rowKey);
        return aFloat == null ? NULL_FLOAT : aFloat;
    }

    public int getPrevInt(long rowKey) {
        final Integer integer = (Integer) getPrev(rowKey);
        return integer == null ? NULL_INT : integer;
    }

    @Override
    public long getPrevLong(long rowKey) {
        final Long aLong = (Long) getPrev(rowKey);
        return aLong == null ? NULL_LONG : aLong;
    }

    @Override
    public short getPrevShort(long rowKey) {
        final Short aShort = (Short) getPrev(rowKey);
        return aShort == null ? NULL_SHORT : aShort;
    }

    public interface GetContext extends Context {
    }
    public interface FillContext extends Context {
    }

    private static class FormulaGetContext implements GetContext {
        final WritableChunk<Values> sourceChunk;
        final FillContext fillContext;

        FormulaGetContext(final ChunkType chunkType, final FillContext fillContext, final int chunkCapacity) {
            this.sourceChunk = chunkType.makeWritableChunk(chunkCapacity);
            this.fillContext = fillContext;
        }

        @Override
        public void close() {
            fillContext.close();
            sourceChunk.close();
        }
    }

    public GetContext makeGetContext(final int chunkCapacity) {
        return new FormulaGetContext(getChunkType(), makeFillContext(chunkCapacity), chunkCapacity);
    }

    public abstract FillContext makeFillContext(final int chunkCapacity);

    public Chunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        final FormulaGetContext formulaGetContext = (FormulaGetContext) context;
        final WritableChunk<Values> sourceChunk = formulaGetContext.sourceChunk;
        fillChunk(formulaGetContext.fillContext, sourceChunk, rowSequence);
        return sourceChunk;
    }


    public Chunk<Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final FormulaGetContext formulaGetContext = (FormulaGetContext) context;
        final WritableChunk<Values> sourceChunk = formulaGetContext.sourceChunk;
        fillPrevChunk(formulaGetContext.fillContext, sourceChunk, rowSequence);
        return sourceChunk;
    }

    public abstract void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence);

    public abstract void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence);

    protected abstract ChunkType getChunkType();
}
