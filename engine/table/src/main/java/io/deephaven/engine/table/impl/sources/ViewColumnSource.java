/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class ViewColumnSource<T> extends AbstractColumnSource<T> {
    private final Formula formula;

    private final boolean isStateless;

    public ViewColumnSource(Class<T> type, Formula formula, boolean isStateless) {
        super(type);
        this.formula = formula;
        this.isStateless = isStateless;
    }

    public ViewColumnSource(Class<T> type, Class elementType, Formula formula,
            boolean isStateless) {
        super(type, elementType);
        this.formula = formula;
        this.isStateless = isStateless;
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }

    @Override
    public T get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        // noinspection unchecked
        return (T) formula.get(rowKey);
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return formula.getBoolean(rowKey);
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return formula.getByte(rowKey);
    }

    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return formula.getChar(rowKey);
    }

    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return formula.getDouble(rowKey);
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return formula.getFloat(rowKey);
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return formula.getInt(rowKey);
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return formula.getLong(rowKey);
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return formula.getShort(rowKey);
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        // noinspection unchecked
        return (T) formula.getPrev(rowKey);
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return formula.getPrevBoolean(rowKey);
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return formula.getPrevByte(rowKey);
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return formula.getPrevChar(rowKey);
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return formula.getPrevDouble(rowKey);
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return formula.getPrevFloat(rowKey);
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return formula.getPrevInt(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return formula.getPrevLong(rowKey);
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return formula.getPrevShort(rowKey);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedState) {
        return new VCSGetContext(formula.makeGetContext(chunkCapacity));
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedState) {
        return new VCSFillContext(formula.makeFillContext(chunkCapacity));
    }

    @Override
    public Chunk<Values> getChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        return formula.getChunk(((VCSGetContext) context).underlyingGetContext, rowSequence);

    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        return formula.getPrevChunk(((VCSGetContext) context).underlyingGetContext, rowSequence);

    }

    @Override
    public void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        formula.fillChunk(((VCSFillContext) context).underlyingFillContext, destination, rowSequence);
    }


    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        formula.fillPrevChunk(((VCSFillContext) context).underlyingFillContext, destination, rowSequence);
    }

    public static class VCSGetContext implements GetContext {
        private final Formula.GetContext underlyingGetContext;

        public VCSGetContext(Formula.GetContext underlyingGetContext) {
            this.underlyingGetContext = underlyingGetContext;
        }

        @Override
        public void close() {
            underlyingGetContext.close();
        }
    }

    public static class VCSFillContext implements FillContext {
        private final Formula.FillContext underlyingFillContext;

        public VCSFillContext(Formula.FillContext underlyingFillContext) {
            this.underlyingFillContext = underlyingFillContext;
        }

        @Override
        public void close() {
            underlyingFillContext.close();
        }
    }

    @Override
    public boolean isStateless() {
        return isStateless;
    }
}
