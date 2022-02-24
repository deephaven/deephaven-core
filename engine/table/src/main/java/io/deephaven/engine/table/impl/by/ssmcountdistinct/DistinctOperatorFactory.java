package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.configuration.Configuration;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.*;
import io.deephaven.util.type.TypeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.util.QueryConstants.*;

/**
 * A factory interface to create {@link IterativeChunkedAggregationOperator operators} for the
 * {@link io.deephaven.engine.table.impl.by.AggType#Distinct}
 * {@link io.deephaven.engine.table.impl.by.AggType#CountDistinct},
 * {@link io.deephaven.engine.table.impl.by.AggType#Unique} and their rollup counterparts
 */
public interface DistinctOperatorFactory {
    int NODE_SIZE = Configuration.getInstance().getIntegerWithDefault("DistinctOperatorFactory.nodeSize", 4096);

    /**
     * Create an {@link IterativeChunkedAggregationOperator operator} for the
     * {@link io.deephaven.engine.table.impl.by.AggType#CountDistinct} aggregation.
     *
     * @param type the type of the column
     * @param resultName the name of the result column
     * @param countNulls true if null values should be counted as important values, or false if they should be ignored.
     * @param exposeInternal true if the underlying SSM state should be exposed as a column (for use with rollups)
     * @param isRollup true if the returned operator should be suitable for second or higher levels of rollup
     *        aggregation
     *
     * @return an appropriate operator.
     */
    static IterativeChunkedAggregationOperator createCountDistinct(Class<?> type, String resultName, boolean countNulls,
            boolean exposeInternal, boolean isRollup) {
        if (type == Byte.class || type == byte.class) {
            return isRollup ? new ByteRollupCountDistinctOperator(resultName, countNulls)
                    : new ByteChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return isRollup ? new CharRollupCountDistinctOperator(resultName, countNulls)
                    : new CharChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return isRollup ? new DoubleRollupCountDistinctOperator(resultName, countNulls)
                    : new DoubleChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return isRollup ? new FloatRollupCountDistinctOperator(resultName, countNulls)
                    : new FloatChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return isRollup ? new IntRollupCountDistinctOperator(resultName, countNulls)
                    : new IntChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            return isRollup ? new LongRollupCountDistinctOperator(resultName, countNulls)
                    : new LongChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return isRollup ? new ShortRollupCountDistinctOperator(resultName, countNulls)
                    : new ShortChunkedCountDistinctOperator(resultName, countNulls, exposeInternal);
        } else {
            return isRollup ? new ObjectRollupCountDistinctOperator(type, resultName, countNulls)
                    : new ObjectChunkedCountDistinctOperator(type, resultName, countNulls, exposeInternal);
        }
    }

    /**
     * Create an {@link IterativeChunkedAggregationOperator operator} for the
     * {@link io.deephaven.engine.table.impl.by.AggType#Distinct} aggregation.
     *
     * @param type the type of the column
     * @param resultName the name of the result column
     * @param countNulls true if null values should be counted as important values, or false if they should be ignored.
     * @param exposeInternal true if the underlying SSM state should be exposed as a column (for use with rollups)
     * @param isRollup true if the returned operator should be suitable for second or higher levels of rollup
     *        aggregation
     *
     * @return an appropriate operator.
     */
    static IterativeChunkedAggregationOperator createDistinct(Class<?> type, String resultName, boolean countNulls,
            boolean exposeInternal, boolean isRollup) {
        if (type == Byte.class || type == byte.class) {
            return isRollup ? new ByteRollupDistinctOperator(resultName, countNulls)
                    : new ByteChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return isRollup ? new CharRollupDistinctOperator(resultName, countNulls)
                    : new CharChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return isRollup ? new DoubleRollupDistinctOperator(resultName, countNulls)
                    : new DoubleChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return isRollup ? new FloatRollupDistinctOperator(resultName, countNulls)
                    : new FloatChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return isRollup ? new IntRollupDistinctOperator(resultName, countNulls)
                    : new IntChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            return isRollup ? new LongRollupDistinctOperator(type, resultName, countNulls)
                    : new LongChunkedDistinctOperator(type, resultName, countNulls, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return isRollup ? new ShortRollupDistinctOperator(resultName, countNulls)
                    : new ShortChunkedDistinctOperator(resultName, countNulls, exposeInternal);
        } else {
            return isRollup ? new ObjectRollupDistinctOperator(type, resultName, countNulls)
                    : new ObjectChunkedDistinctOperator(type, resultName, countNulls, exposeInternal);
        }
    }

    /**
     * Create an {@link IterativeChunkedAggregationOperator operator} for the
     * {@link io.deephaven.engine.table.impl.by.AggType#Unique} aggregation.
     *
     * @param type the type of the column
     * @param resultName the name of the result column
     * @param countNulls true if null values should be counted as important values, or false if they should be ignored.
     * @param exposeInternal true if the underlying SSM state should be exposed as a column (for use with rollups)
     * @param isRollup true if the returned operator should be suitable for second or higher levels of rollup
     *        aggregation
     *
     * @return an appropriate operator.
     */
    static IterativeChunkedAggregationOperator createUnique(Class<?> type, String resultName, boolean countNulls,
            Object onlyNullsSentinel, Object nonUniqueSentinel,
            boolean exposeInternal, boolean isRollup) {
        checkType(resultName, "Only Nulls Sentinel", type, onlyNullsSentinel);
        checkType(resultName, "Non Unique Sentinel", type, nonUniqueSentinel);

        if (type == Byte.class || type == byte.class) {
            final byte onsAsType = (onlyNullsSentinel == null) ? NULL_BYTE : ((Number) onlyNullsSentinel).byteValue();
            final byte nusAsType = (nonUniqueSentinel == null) ? NULL_BYTE : ((Number) nonUniqueSentinel).byteValue();
            return isRollup ? new ByteRollupUniqueOperator(resultName, countNulls, onsAsType, nusAsType)
                    : new ByteChunkedUniqueOperator(resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Character.class || type == char.class) {
            return isRollup
                    ? new CharRollupUniqueOperator(resultName, countNulls,
                            io.deephaven.util.type.TypeUtils.unbox((Character) onlyNullsSentinel),
                            io.deephaven.util.type.TypeUtils.unbox((Character) nonUniqueSentinel))
                    : new CharChunkedUniqueOperator(resultName, countNulls, exposeInternal,
                            io.deephaven.util.type.TypeUtils.unbox((Character) onlyNullsSentinel),
                            io.deephaven.util.type.TypeUtils.unbox((Character) nonUniqueSentinel));
        } else if (type == Double.class || type == double.class) {
            final double onsAsType =
                    (onlyNullsSentinel == null) ? NULL_DOUBLE : ((Number) onlyNullsSentinel).doubleValue();
            final double nusAsType =
                    (nonUniqueSentinel == null) ? NULL_DOUBLE : ((Number) nonUniqueSentinel).doubleValue();
            return isRollup ? new DoubleRollupUniqueOperator(resultName, countNulls, onsAsType, nusAsType)
                    : new DoubleChunkedUniqueOperator(resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Float.class || type == float.class) {
            final float onsAsType =
                    (onlyNullsSentinel == null) ? NULL_FLOAT : ((Number) onlyNullsSentinel).floatValue();
            final float nusAsType =
                    (nonUniqueSentinel == null) ? NULL_FLOAT : ((Number) nonUniqueSentinel).floatValue();
            return isRollup ? new FloatRollupUniqueOperator(resultName, countNulls, onsAsType, nusAsType)
                    : new FloatChunkedUniqueOperator(resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Integer.class || type == int.class) {
            final int onsAsType = (onlyNullsSentinel == null) ? NULL_INT : ((Number) onlyNullsSentinel).intValue();
            final int nusAsType = (nonUniqueSentinel == null) ? NULL_INT : ((Number) nonUniqueSentinel).intValue();
            return isRollup ? new IntRollupUniqueOperator(resultName, countNulls, onsAsType, nusAsType)
                    : new IntChunkedUniqueOperator(resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            final long onsAsType;
            final long nusAsType;
            if (type == DateTime.class) {
                onsAsType = (onlyNullsSentinel == null) ? NULL_LONG : ((DateTime) onlyNullsSentinel).getNanos();
                nusAsType = (nonUniqueSentinel == null) ? NULL_LONG : ((DateTime) nonUniqueSentinel).getNanos();
            } else {
                onsAsType = (onlyNullsSentinel == null) ? NULL_LONG : ((Number) onlyNullsSentinel).longValue();
                nusAsType = (nonUniqueSentinel == null) ? NULL_LONG : ((Number) nonUniqueSentinel).longValue();
            }

            return isRollup ? new LongRollupUniqueOperator(type, resultName, countNulls, onsAsType, nusAsType)
                    : new LongChunkedUniqueOperator(type, resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Short.class || type == short.class) {
            final short onsAsType =
                    (onlyNullsSentinel == null) ? NULL_SHORT : ((Number) onlyNullsSentinel).shortValue();
            final short nusAsType =
                    (nonUniqueSentinel == null) ? NULL_SHORT : ((Number) nonUniqueSentinel).shortValue();
            return isRollup ? new ShortRollupUniqueOperator(resultName, countNulls, onsAsType, nusAsType)
                    : new ShortChunkedUniqueOperator(resultName, countNulls, exposeInternal, onsAsType, nusAsType);
        } else {
            return isRollup
                    ? new ObjectRollupUniqueOperator(type, resultName, countNulls, onlyNullsSentinel, nonUniqueSentinel)
                    : new ObjectChunkedUniqueOperator(type, resultName, countNulls, exposeInternal, onlyNullsSentinel,
                            nonUniqueSentinel);
        }
    }

    static void checkType(String resultColName, String valueIntent, Class<?> expected, Object value) {
        expected = io.deephaven.util.type.TypeUtils.getBoxedType(expected);

        if (value != null && !expected.isAssignableFrom(value.getClass())) {
            if (io.deephaven.util.type.TypeUtils.isNumeric(expected) && TypeUtils.isNumeric(value.getClass())) {
                if (checkNumericCompatibility((Number) value, expected)) {
                    return;
                }

                throw new IllegalArgumentException("For result column `" + resultColName + "' the " + valueIntent + " '"
                        + value + "' is larger than can be represented with a " + expected.getName());
            }

            throw new IllegalArgumentException("For result column `" + resultColName + "' the " + valueIntent
                    + " must be of type " + expected.getName() + " but is " + value.getClass().getName());
        }
    }

    static boolean checkNumericCompatibility(Number value, Class<?> expected) {
        if (expected == Byte.class) {
            return Byte.MIN_VALUE <= value.longValue() && value.longValue() <= Byte.MAX_VALUE;
        } else if (expected == Short.class) {
            return Short.MIN_VALUE <= value.longValue() && value.longValue() <= Short.MAX_VALUE;
        } else if (expected == Integer.class) {
            return Integer.MIN_VALUE <= value.longValue() && value.longValue() <= Integer.MAX_VALUE;
        } else if (expected == Long.class) {
            return new BigInteger(value.toString()).compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0 &&
                    new BigInteger(value.toString()).compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0;
        } else if (expected == Float.class) {
            return value.getClass() != Double.class;
        } else if (expected == Double.class) {
            return value.getClass() != BigDecimal.class;
        } else if (expected == BigDecimal.class || expected == BigInteger.class) {
            return true;
        }

        return false;
    }
}
