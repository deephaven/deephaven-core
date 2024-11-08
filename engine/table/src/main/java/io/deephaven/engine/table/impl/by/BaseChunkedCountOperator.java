//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.spec.AggSpecCountValues;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.engine.util.NullSafeAddition.plusLong;

abstract class BaseChunkedCountOperator implements IterativeChunkedAggregationOperator {
    /*
     * This class leverages assumptions about null values for the various primitive types to optimize count operations.
     * This assert ensures that the assumptions are regularly verified against future changes.
     */
    static {
        assert QueryConstants.NULL_BYTE < 0;
        assert QueryConstants.NULL_CHAR > 0; // null char is the only positive value
        assert QueryConstants.NULL_SHORT < 0;
        assert QueryConstants.NULL_INT < 0;
        assert QueryConstants.NULL_LONG < 0;
        assert QueryConstants.NULL_FLOAT < 0;
        assert QueryConstants.NULL_DOUBLE < 0;
    }

    private final String resultName;
    final LongArraySource resultColumnSource;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    BaseChunkedCountOperator(@NotNull final String resultName) {
        this.resultName = resultName;
        this.resultColumnSource = new LongArraySource();
    }

    protected abstract int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values);

    @Override
    public void addChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(values, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values, destination, 0, values.size());
    }

    private boolean addChunk(
            Chunk<? extends Values> values,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int count = doCount(chunkStart, chunkSize, values);
        if (count > 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            return true;
        }
        return false;
    }

    @Override
    public void removeChunk(
            BucketedContext context,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(values, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean removeChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys,
            long destination) {
        return removeChunk(values, destination, 0, values.size());
    }

    private boolean removeChunk(
            Chunk<? extends Values> values,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int count = doCount(chunkStart, chunkSize, values);
        if (count > 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), -count));
            return true;
        }
        return false;
    }

    @Override
    public void modifyChunk(
            BucketedContext context,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(previousValues, newValues, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean modifyChunk(
            SingletonContext context,
            int chunkSize,
            Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys,
            long destination) {
        return modifyChunk(previousValues, newValues, destination, 0, newValues.size());
    }

    private boolean modifyChunk(
            Chunk<? extends Values> oldValues,
            Chunk<? extends Values> newValues,
            long destination,
            int chunkStart,
            int chunkSize) {
        final int oldCount = doCount(chunkStart, chunkSize, oldValues);
        final int newCount = doCount(chunkStart, chunkSize, newValues);
        final int count = newCount - oldCount;
        if (count != 0) {
            resultColumnSource.set(destination, plusLong(resultColumnSource.getUnsafe(destination), count));
            return true;
        }
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumnSource.ensureCapacity(tableSize, false);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, resultColumnSource);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumnSource.startTrackingPrevValues();
    }

    // region Count Interfaces and Functions
    @FunctionalInterface
    public interface ByteCountFunction {
        boolean count(byte value);
    }

    @FunctionalInterface
    public interface CharCountFunction {
        boolean count(char value);
    }

    @FunctionalInterface
    public interface ShortCountFunction {
        boolean count(short value);
    }

    @FunctionalInterface
    public interface IntCountFunction {
        boolean count(int value);
    }

    @FunctionalInterface
    public interface LongCountFunction {
        boolean count(long value);
    }

    @FunctionalInterface
    public interface FloatCountFunction {
        boolean count(float value);
    }

    @FunctionalInterface
    public interface DoubleCountFunction {
        boolean count(double value);
    }

    @FunctionalInterface
    public interface BooleanCountFunction {
        boolean count(byte value);
    }

    @FunctionalInterface
    public interface ObjectCountFunction {
        boolean count(Object value);
    }

    @FunctionalInterface
    public interface BigDecimalCountFunction {
        boolean count(BigDecimal value);
    }

    @FunctionalInterface
    public interface BigIntegerCountFunction {
        boolean count(BigInteger value);
    }

    ByteCountFunction getByteCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_BYTE;
            case NULL:
                return value -> value == QueryConstants.NULL_BYTE;
            case POSITIVE:
                return value -> value > 0; // NULL_BYTE is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_BYTE && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for byte data type (" + resultName + ")");
        }
    }

    CharCountFunction getCharCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_CHAR;
            case NULL:
                return value -> value == QueryConstants.NULL_CHAR;
            case POSITIVE:
                return value -> value != QueryConstants.NULL_CHAR && value > 0;
            case ZERO:
                return value -> value == 0;
            case NEGATIVE: // char is unsigned
            case NAN:
            case INFINITE:
                return value -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for char data type (" + resultName + ")");
        }
    }

    ShortCountFunction getShortCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_SHORT;
            case NULL:
                return value -> value == QueryConstants.NULL_SHORT;
            case POSITIVE:
                return value -> value > 0;// NULL_SHORT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_SHORT && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for short data type (" + resultName + ")");
        }
    }

    IntCountFunction getIntCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_INT;
            case NULL:
                return value -> value == QueryConstants.NULL_INT;
            case POSITIVE:
                return value -> value > 0; // NULL_INT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_INT && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for int data type (" + resultName + ")");
        }
    }

    LongCountFunction getLongCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_LONG;
            case NULL:
                return value -> value == QueryConstants.NULL_LONG;
            case POSITIVE:
                return value -> value > 0; // NULL_LONG is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_LONG && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for long data type (" + resultName + ")");
        }
    }

    FloatCountFunction getFloatCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
                return value -> value != QueryConstants.NULL_FLOAT;
            case NULL:
                return value -> value == QueryConstants.NULL_FLOAT;
            case POSITIVE:
                return value -> value > 0; // NULL_FLOAT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_FLOAT && value < 0;
            case NAN:
                return Float::isNaN;
            case INFINITE:
                return Float::isInfinite;
            case FINITE:
                return value -> value != QueryConstants.NULL_FLOAT && Float.isFinite(value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for float data type (" + resultName + ")");
        }
    }

    DoubleCountFunction getDoubleCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
                return value -> value != QueryConstants.NULL_DOUBLE;
            case NULL:
                return value -> value == QueryConstants.NULL_DOUBLE;
            case POSITIVE:
                return value -> value > 0; // NULL_DOUBLE is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_DOUBLE && value < 0;
            case NAN:
                return Double::isNaN;
            case INFINITE:
                return Double::isInfinite;
            case FINITE:
                return value -> value != QueryConstants.NULL_DOUBLE && Double.isFinite(value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for double data type (" + resultName + ")");
        }
    }

    ObjectCountFunction getObjectCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for object data type (" + resultName + ")");
        }
    }

    BigDecimalCountFunction getBigDecimalCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            case POSITIVE:
                return value -> value != null && value.signum() > 0;
            case ZERO:
                return value -> value != null && value.signum() == 0;
            case NEGATIVE:
                return value -> value != null && value.signum() < 0;
            case NAN:
            case INFINITE:
                return index -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for BigDecimal data type");
        }
    }

    BigIntegerCountFunction getBigIntegerCountFunction(@NotNull final AggSpecCountValues.AggCountType countType) {
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            case POSITIVE:
                return value -> value != null && value.signum() > 0;
            case ZERO:
                return value -> value != null && value.signum() == 0;
            case NEGATIVE:
                return value -> value != null && value.signum() < 0;
            case NAN:
            case INFINITE:
                return index -> false;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for BigInteger data type");
        }
    }
    // endregion Count Interfaces and Functions
}
