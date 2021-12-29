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
    public abstract Object getPrev(long key);

    @Override
    public abstract Object get(long key);

    @Override
    public Boolean getBoolean(long key) {
        return (Boolean) get(key);
    }

    @Override
    public byte getByte(long key) {
        final Byte aByte = (Byte) get(key);
        return aByte == null ? NULL_BYTE : aByte;
    }

    @Override
    public char getChar(long key) {
        final Character character = (Character) get(key);
        return character == null ? NULL_CHAR : character;
    }

    @Override
    public double getDouble(long key) {
        final Double aDouble = (Double) get(key);
        return aDouble == null ? NULL_DOUBLE : aDouble;
    }

    @Override
    public float getFloat(long key) {
        final Float aFloat = (Float) get(key);
        return aFloat == null ? NULL_FLOAT : aFloat;
    }

    @Override
    public int getInt(long key) {
        final Integer integer = (Integer) get(key);
        return integer == null ? NULL_INT : integer;
    }

    @Override
    public long getLong(long key) {
        final Long aLong = (Long) get(key);
        return aLong == null ? NULL_LONG : aLong;
    }

    @Override
    public short getShort(long key) {
        final Short aShort = (Short) get(key);
        return aShort == null ? NULL_SHORT : aShort;
    }

    @Override
    public Boolean getPrevBoolean(long key) {
        return (Boolean) getPrev(key);
    }

    @Override
    public byte getPrevByte(long key) {
        final Byte aByte = (Byte) getPrev(key);
        return aByte == null ? NULL_BYTE : aByte;
    }

    @Override
    public char getPrevChar(long key) {
        final Character character = (Character) getPrev(key);
        return character == null ? NULL_CHAR : character;
    }

    @Override
    public double getPrevDouble(long key) {
        final Double aDouble = (Double) getPrev(key);
        return aDouble == null ? NULL_DOUBLE : aDouble;
    }

    @Override
    public float getPrevFloat(long key) {
        final Float aFloat = (Float) getPrev(key);
        return aFloat == null ? NULL_FLOAT : aFloat;
    }

    public int getPrevInt(long key) {
        final Integer integer = (Integer) getPrev(key);
        return integer == null ? NULL_INT : integer;
    }

    @Override
    public long getPrevLong(long key) {
        final Long aLong = (Long) getPrev(key);
        return aLong == null ? NULL_LONG : aLong;
    }

    @Override
    public short getPrevShort(long key) {
        final Short aShort = (Short) getPrev(key);
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
