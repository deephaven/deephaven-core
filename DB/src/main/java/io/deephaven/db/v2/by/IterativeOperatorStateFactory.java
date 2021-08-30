/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.by.ssmcountdistinct.DistinctOperatorFactory;
import io.deephaven.db.v2.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.db.v2.by.ssmpercentile.SsmChunkedPercentileOperator;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Creates iterative operators for the supplied type.
 */
public abstract class IterativeOperatorStateFactory extends ReaggregatableStatefactory
    implements IterativeChunkedOperatorFactory {

    IterativeOperatorStateFactory() {}

    @Override
    public abstract IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
        boolean exposeInternalColumns);

    static IterativeChunkedAggregationOperator getSumChunked(Class type, String name) {
        if (type == Boolean.class || type == boolean.class) {
            return new BooleanChunkedSumOperator(name);
        } else if (type == Byte.class || type == byte.class) {
            return new ByteChunkedSumOperator(false, name);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedSumOperator(false, name);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedSumOperator(false, name);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedSumOperator(false, name);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedSumOperator(false, name);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedSumOperator(false, name);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedSumOperator(false, name);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedSumOperator(false, name);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedSumOperator(false, name);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    static IterativeChunkedAggregationOperator getMinMaxChunked(Class type, boolean minimum,
        boolean isStreamOrAddOnly, String name) {
        if (!isStreamOrAddOnly) {
            return new SsmChunkedMinMaxOperator(type, minimum, name);
        } else {
            if (type == Byte.class || type == byte.class) {
                return new ByteChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Character.class || type == char.class) {
                return new CharChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Double.class || type == double.class) {
                return new DoubleChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Float.class || type == float.class) {
                return new FloatChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Integer.class || type == int.class) {
                return new IntChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Long.class || type == long.class || type == DBDateTime.class) {
                return new LongChunkedAddOnlyMinMaxOperator(type, minimum, name);
            } else if (type == Short.class || type == short.class) {
                return new ShortChunkedAddOnlyMinMaxOperator(minimum, name);
            } else if (type == Boolean.class || type == boolean.class) {
                return new BooleanChunkedAddOnlyMinMaxOperator(minimum, name);
            } else {
                return new ObjectChunkedAddOnlyMinMaxOperator(type, minimum, name);
            }
        }
    }

    static IterativeChunkedAggregationOperator getPercentileChunked(Class type, double percentile,
        boolean averageMedian, String name) {
        return new SsmChunkedPercentileOperator(type, percentile, averageMedian, name);
    }

    static IterativeChunkedAggregationOperator getCountDistinctChunked(Class type, String name,
        boolean countNulls, boolean exposeInternal, boolean isRollup) {
        return DistinctOperatorFactory.createCountDistinct(type, name, countNulls, exposeInternal,
            isRollup);
    }

    static IterativeChunkedAggregationOperator getDistinctChunked(Class type, String name,
        boolean countNulls, boolean exposeInternal, boolean isRollup) {
        return DistinctOperatorFactory.createDistinct(type, name, countNulls, exposeInternal,
            isRollup);
    }

    static IterativeChunkedAggregationOperator getUniqueChunked(Class type, String name,
        boolean countNulls, boolean exposeInternal, Object noKeyValue, Object nonUniqueValue,
        boolean isRollup) {
        return DistinctOperatorFactory.createUnique(type, name, countNulls, exposeInternal,
            noKeyValue, nonUniqueValue, isRollup);
    }

    static IterativeChunkedAggregationOperator getAbsSumChunked(Class type, String name) {
        if (type == Boolean.class || type == boolean.class) {
            return new BooleanChunkedSumOperator(name);
        } else if (type == Byte.class || type == byte.class) {
            return new ByteChunkedSumOperator(true, name);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedSumOperator(true, name);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedSumOperator(true, name);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedSumOperator(true, name);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedSumOperator(true, name);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedSumOperator(true, name);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedSumOperator(true, name);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedSumOperator(true, name);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedSumOperator(true, name);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    static IterativeChunkedAggregationOperator getAvgChunked(Class type, String name,
        boolean exposeInternalColumns) {
        if (type == Byte.class || type == byte.class) {
            return new ByteChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedAvgOperator(name, exposeInternalColumns);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedAvgOperator(name, exposeInternalColumns);
        } else if (AvgState.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException();
        } else if (AvgStateWithNan.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    static IterativeChunkedAggregationOperator getVarChunked(Class type, boolean std, String name,
        boolean exposeInternalColumns) {
        if (type == Byte.class || type == byte.class) {
            return new ByteChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedVarOperator(std, name, exposeInternalColumns);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedVarOperator(std, name, exposeInternalColumns);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    @Override
    boolean supportsRollup() {
        return false;
    }

    @Override
    ReaggregatableStatefactory forRollup() {
        throw new UnsupportedOperationException();
    }

    @Override
    ReaggregatableStatefactory rollupFactory() {
        throw new UnsupportedOperationException();
    }
}
