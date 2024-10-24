//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.api.updateby.spec.CumCountSpec;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class PrimitiveCumCountOperator extends BaseCumCountOperator {

    @Override
    protected ValueCountFunction createValueCountFunction(
            final Chunk<? extends Values> chunk,
            final CumCountSpec.CumCountType countType) {

        if (columnType == byte.class || columnType == Byte.class) {
            return createByteValueCountFunction(chunk.asByteChunk(), countType);
        }
        if (columnType == char.class || columnType == Character.class) {
            return createCharValueCountFunction(chunk.asCharChunk(), countType);
        }
        if (columnType == double.class || columnType == Double.class) {
            return createDoubleValueCountFunction(chunk.asDoubleChunk(), countType);
        }
        if (columnType == float.class || columnType == Float.class) {
            return createFloatValueCountFunction(chunk.asFloatChunk(), countType);
        }
        if (columnType == int.class || columnType == Integer.class) {
            return createIntValueCountFunction(chunk.asIntChunk(), countType);
        }
        if (columnType == long.class || columnType == Long.class) {
            return createLongValueCountFunction(chunk.asLongChunk(), countType);
        }
        if (columnType == short.class || columnType == Short.class) {
            return createShortValueCountFunction(chunk.asShortChunk(), countType);
        }
        throw new UnsupportedOperationException("PrimitiveCumCountOperator - Unsupported column type: " + columnType);
    }

    // region ValueCountFunction implementations

    private ValueCountFunction createByteValueCountFunction(
            @NotNull final ByteChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return index -> valueChunk.get(index) != NULL_BYTE;
            case NULL:
                return index -> valueChunk.get(index) == NULL_BYTE;
            case NEGATIVE:
                return index -> {
                    final byte val = valueChunk.get(index);
                    return val != NULL_BYTE && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_BYTE is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createCharValueCountFunction(
            @NotNull final CharChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
            case NEGATIVE: // char is never negative
            case FINITE:
                return index -> valueChunk.get(index) != NULL_CHAR;
            case NULL:
                return index -> valueChunk.get(index) == NULL_CHAR;
            case POSITIVE:
                return index -> {
                    final char val = valueChunk.get(index);
                    return val != NULL_CHAR && val > 0;
                };
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createDoubleValueCountFunction(
            @NotNull final DoubleChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
                return index -> valueChunk.get(index) != NULL_DOUBLE;
            case NULL:
                return index -> valueChunk.get(index) == NULL_DOUBLE;
            case NEGATIVE:
                return index -> {
                    final double val = valueChunk.get(index);
                    return val != NULL_DOUBLE && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_DOUBLE is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
                return index -> Double.isNaN(valueChunk.get(index));
            case INFINITE:
                return index -> Double.isInfinite(valueChunk.get(index));
            case FINITE:
                return index -> {
                    final double val = valueChunk.get(index);
                    return val != NULL_DOUBLE && Double.isFinite(val);
                };
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createFloatValueCountFunction(
            @NotNull final FloatChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
                return index -> valueChunk.get(index) != NULL_FLOAT;
            case NULL:
                return index -> valueChunk.get(index) == NULL_FLOAT;
            case NEGATIVE:
                return index -> {
                    final float val = valueChunk.get(index);
                    return val != NULL_FLOAT && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_FLOAT is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
                return index -> Float.isNaN(valueChunk.get(index));
            case INFINITE:
                return index -> Float.isInfinite(valueChunk.get(index));
            case FINITE:
                return index -> {
                    final float val = valueChunk.get(index);
                    return val != NULL_FLOAT && Float.isFinite(val);
                };
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createIntValueCountFunction(
            @NotNull final IntChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return index -> valueChunk.get(index) != NULL_INT;
            case NULL:
                return index -> valueChunk.get(index) == NULL_INT;
            case NEGATIVE:
                return index -> {
                    final int val = valueChunk.get(index);
                    return val != NULL_INT && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_INT is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createLongValueCountFunction(
            @NotNull final LongChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return index -> valueChunk.get(index) != NULL_LONG;
            case NULL:
                return index -> valueChunk.get(index) == NULL_LONG;
            case NEGATIVE:
                return index -> {
                    final long val = valueChunk.get(index);
                    return val != NULL_LONG && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_LONG is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    private ValueCountFunction createShortValueCountFunction(
            @NotNull final ShortChunk<?> valueChunk,
            final CumCountSpec.CumCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return index -> valueChunk.get(index) != NULL_SHORT;
            case NULL:
                return index -> valueChunk.get(index) == NULL_SHORT;
            case NEGATIVE:
                return index -> {
                    final short val = valueChunk.get(index);
                    return val != NULL_SHORT && val < 0;
                };
            case POSITIVE:
                return index -> valueChunk.get(index) > 0; // NULL_SHORT is negative
            case ZERO:
                return index -> valueChunk.get(index) == 0;
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("PrimitiveCumCountOperator -Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }
    // endregion

    public PrimitiveCumCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final CumCountSpec spec,
            @NotNull final Class<?> columnType) {
        super(pair, spec, columnType);
    }

    @Override
    public UpdateByOperator copy() {
        return new PrimitiveCumCountOperator(pair, spec, columnType);
    }
}
