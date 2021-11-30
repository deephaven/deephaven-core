package io.deephaven.engine.table.impl;

import io.deephaven.function.*;
import io.deephaven.vector.*;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.libs.GroovyStaticImports;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;

public class QueryTableAggregationTestFormulaStaticMethods {
    public static ByteVector abs(ByteVector values) {
        final byte[] result = new byte[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = ByteNumericPrimitives.abs(values.get(ii));
        }
        return new ByteVectorDirect(result);
    }

    public static ShortVector abs(ShortVector values) {
        final short[] result = new short[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = ShortNumericPrimitives.abs(values.get(ii));
        }
        return new ShortVectorDirect(result);
    }

    public static IntVector abs(IntVector values) {
        final int[] result = new int[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = IntegerNumericPrimitives.abs(values.get(ii));
        }
        return new IntVectorDirect(result);
    }

    public static LongVector abs(LongVector values) {
        final long[] result = new long[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = LongNumericPrimitives.abs(values.get(ii));
        }
        return new LongVectorDirect(result);
    }

    public static FloatVector abs(FloatVector values) {
        final float[] result = new float[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = FloatNumericPrimitives.abs(values.get(ii));
        }
        return new FloatVectorDirect(result);
    }

    public static DoubleVector abs(DoubleVector values) {
        final double[] result = new double[values.intSize()];
        for (int ii = 0; ii < values.size(); ++ii) {
            result[ii] = DoubleNumericPrimitives.abs(values.get(ii));
        }
        return new DoubleVectorDirect(result);
    }

    public static long countChar(CharVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final char c = values.get(ii);
            if (c != QueryConstants.NULL_CHAR) {
                count++;
            }
        }
        return count;
    }

    public static long countByte(ByteVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final byte c = values.get(ii);
            if (c != QueryConstants.NULL_BYTE) {
                count++;
            }
        }
        return count;
    }

    public static long countShort(ShortVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final short c = values.get(ii);
            if (c != QueryConstants.NULL_SHORT) {
                count++;
            }
        }
        return count;
    }

    public static long countInt(IntVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final int c = values.get(ii);
            if (c != QueryConstants.NULL_INT) {
                count++;
            }
        }
        return count;
    }

    public static long countLong(LongVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final long c = values.get(ii);
            if (c != QueryConstants.NULL_LONG) {
                count++;
            }
        }
        return count;
    }

    public static long countFloat(FloatVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final float c = values.get(ii);
            if (c != QueryConstants.NULL_FLOAT) {
                count++;
            }
        }
        return count;
    }

    public static long countDouble(DoubleVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final double c = values.get(ii);
            if (c != QueryConstants.NULL_DOUBLE) {
                count++;
            }
        }
        return count;
    }

    public static long countObject(ObjectVector values) {
        if (values.size() == 0) {
            return 0;
        }
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object o = values.get(ii);
            if (o != null) {
                count++;
            }
        }
        return count;
    }

    public static long sumChar(CharVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_LONG;
        }
        long sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final char c = values.get(ii);
            if (c != QueryConstants.NULL_CHAR) {
                sum += c;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_LONG : sum;
    }

    public static long sumByte(ByteVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_LONG;
        }
        long sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final byte b = values.get(ii);
            if (b != QueryConstants.NULL_BYTE) {
                sum += b;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_LONG : sum;
    }

    public static long sumShort(ShortVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_LONG;
        }
        long sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final short s = values.get(ii);
            if (s != QueryConstants.NULL_SHORT) {
                sum += s;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_LONG : sum;
    }

    public static long sumInt(IntVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_LONG;
        }
        long sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final int v = values.get(ii);
            if (v != QueryConstants.NULL_INT) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_LONG : sum;
    }

    public static double sumDouble(DoubleVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        double sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final double v = values.get(ii);
            if (v != QueryConstants.NULL_DOUBLE) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_DOUBLE : sum;
    }

    public static float sumFloat(FloatVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_FLOAT;
        }
        float sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final float v = values.get(ii);
            if (v != QueryConstants.NULL_FLOAT) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_FLOAT : sum;
    }

    public static long sumBool(ObjectVector<Boolean> values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_LONG;
        }
        long sum = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final Boolean b = values.get(ii);
            if (b != null) {
                if (b) {
                    sum++;
                }
                count++;
            }
        }
        return count == 0 ? QueryConstants.NULL_LONG : sum;
    }

    public static BigInteger sumBigInt(ObjectVector<BigInteger> values) {
        if (values.size() == 0) {
            return null;
        }
        BigInteger sum = BigInteger.ZERO;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final BigInteger v = values.get(ii);
            if (v != null) {
                sum = sum.add(v);
                count++;
            }
        }
        return count == 0 ? null : sum;
    }

    public static BigInteger absSumBigInt(ObjectVector<BigInteger> values) {
        if (values.size() == 0) {
            return null;
        }
        BigInteger sum = BigInteger.ZERO;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final BigInteger v = values.get(ii);
            if (v != null) {
                sum = sum.add(v.abs());
                count++;
            }
        }
        return count == 0 ? null : sum;
    }

    public static BigDecimal sumBigDec(ObjectVector<BigDecimal> values) {
        if (values.size() == 0) {
            return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final BigDecimal v = values.get(ii);
            if (v != null) {
                sum = sum.add(v);
                count++;
            }
        }
        return count == 0 ? null : sum;
    }

    public static BigDecimal absSumBigDec(ObjectVector<BigDecimal> values) {
        if (values.size() == 0) {
            return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final BigDecimal v = values.get(ii);
            if (v != null) {
                sum = sum.add(v.abs());
                count++;
            }
        }
        return count == 0 ? null : sum;
    }

    public static double varChar(CharVector values) {
        double sum = 0;
        double sum2 = 0;
        int count = 0;

        for (int ii = 0; ii < values.size(); ++ii) {
            final char c = values.get(ii);
            if (c != QueryConstants.NULL_CHAR) {
                sum += c;
                sum2 += c * c;
                count++;
            }
        }

        return (sum2 - sum * sum / count) / (count - 1);
    }

    public static BigDecimal varBigInt(ObjectVector<BigInteger> values) {
        BigInteger sum = BigInteger.ZERO;
        BigInteger sum2 = BigInteger.ZERO;
        int count = 0;

        for (int ii = 0; ii < values.size(); ++ii) {
            final BigInteger v = values.get(ii);
            if (v != null) {
                sum = sum.add(v);
                sum2 = sum2.add(v.pow(2));
                count++;
            }
        }

        if (count <= 1) {
            return null;
        }
        final BigDecimal countMinus1 = BigDecimal.valueOf(count - 1);
        return new BigDecimal(sum2)
                .subtract(new BigDecimal(sum.pow(2)).divide(BigDecimal.valueOf(count), BigDecimal.ROUND_HALF_UP))
                .divide(countMinus1, BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal varBigDec(ObjectVector<BigDecimal> values) {
        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal sum2 = BigDecimal.ZERO;
        int count = 0;

        for (int ii = 0; ii < values.size(); ++ii) {
            final BigDecimal v = values.get(ii);
            if (v != null) {
                sum = sum.add(v);
                sum2 = sum2.add(v.pow(2));
                count++;
            }
        }

        if (count <= 1) {
            return null;
        }
        final BigDecimal countMinus1 = BigDecimal.valueOf(count - 1);
        return sum2.subtract(sum.pow(2).divide(BigDecimal.valueOf(count), BigDecimal.ROUND_HALF_UP)).divide(countMinus1,
                BigDecimal.ROUND_HALF_UP);
    }

    public static char minChar(CharVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_CHAR;
        }
        char min = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final char c = values.get(ii);
            if (c != QueryConstants.NULL_CHAR) {
                if (count++ == 0) {
                    min = c;
                } else if (CharComparisons.lt(c, min)) {
                    min = c;
                }
            }
        }
        return count == 0 ? QueryConstants.NULL_CHAR : min;
    }

    public static char maxChar(CharVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_CHAR;
        }
        char max = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final char c = values.get(ii);
            if (c != QueryConstants.NULL_CHAR) {
                if (count++ == 0) {
                    max = c;
                } else if (CharComparisons.gt(c, max)) {
                    max = c;
                }
            }
        }
        return count == 0 ? QueryConstants.NULL_CHAR : max;
    }

    public static Object minObj(ObjectVector values) {
        if (values.size() == 0) {
            return null;
        }
        Object min = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object v = values.get(ii);
            if (v != null) {
                if (count++ == 0) {
                    min = v;
                } else if (ObjectComparisons.lt(v, min)) {
                    min = v;
                }
            }
        }
        return count == 0 ? null : min;
    }

    public static Object maxObj(ObjectVector values) {
        if (values.size() == 0) {
            return null;
        }
        Object max = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object v = values.get(ii);
            if (v != null) {
                if (count++ == 0) {
                    max = v;
                } else if (ObjectComparisons.gt(v, max)) {
                    max = v;
                }
            }
        }
        return count == 0 ? null : max;
    }

    public static double minDouble(DoubleVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        double min = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final double v = values.get(ii);
            if (v != QueryConstants.NULL_DOUBLE) { // TODO: the existing aggregator doesn't handle this &&
                                                   // !Double.isNaN(v)) {
                if (count++ == 0) {
                    min = v;
                } else if (DoubleComparisons.lt(v, min)) {
                    min = v;
                }
            }
        }
        return count == 0 ? QueryConstants.NULL_DOUBLE : min;
    }

    public static double maxDouble(DoubleVector values) {
        if (values.size() == 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        double min = 0;
        int count = 0;
        for (int ii = 0; ii < values.size(); ++ii) {
            final double v = values.get(ii);
            if (v != QueryConstants.NULL_DOUBLE) { // TODO: the existing aggregator doesn't handle this &&
                                                   // !Double.isNaN(v)) {
                if (count++ == 0) {
                    min = v;
                } else if (DoubleComparisons.gt(v, min)) {
                    min = v;
                }
            }
        }
        return count == 0 ? QueryConstants.NULL_DOUBLE : min;
    }

    static String sumFunction(String col) {
        switch (col) {
            case "charCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumChar";
            case "boolCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumBool";
            case "byteCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumByte";
            case "shortCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumShort";
            case "intCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumInt";
            case "bigI":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumBigInt";
            case "bigD":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumBigDec";
            case "doubleCol":
            case "doubleNanCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumDouble";
            case "floatCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumFloat";
            default:
                return "(long)sum";
        }
    }

    static String minFunction(String col) {
        switch (col) {
            case "charCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".minChar(" + col + ")";
            case "doubleNanCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".minDouble(" + col
                        + ")";
            case "Sym":
                return "(String)" + QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".minObj("
                        + col + ")";
            default:
                return GroovyStaticImports.class.getCanonicalName() + ".min(" + col + ")";
        }
    }

    static String maxFunction(String col) {
        switch (col) {
            case "charCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".maxChar(" + col + ")";
            case "doubleNanCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".maxDouble(" + col
                        + ")";
            case "Sym":
                return "(String)" + QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".maxObj("
                        + col + ")";
            default:
                return GroovyStaticImports.class.getCanonicalName() + ".max(" + col + ")";
        }
    }

    static String varFunction(String col) {
        switch (col) {
            case "charCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".varChar(" + col + ")";
            case "bigI":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".varBigInt(" + col
                        + ")";
            case "bigD":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".varBigDec(" + col
                        + ")";
            default:
                return "var(" + col + ")";
        }
    }

    static String stdFunction(String col) {
        switch (col) {
            case "bigI":
            case "bigD":
                return "io.deephaven.util.BigDecimalUtils.sqrt(" + varFunction(col) + ", 10)";
            default:
                return "Math.sqrt(" + varFunction(col) + ")";
        }
    }

    static String countFunction(String col) {
        switch (col) {
            case "charCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countChar";
            case "byteCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countByte";
            case "shortCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countShort";
            case "intCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countInt";
            case "bigI":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countObject";
            case "bigD":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countObject";
            case "doubleCol":
            case "doubleNanCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countDouble";
            case "floatCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countFloat";
            case "longCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".countLong";
            default:
                throw new IllegalArgumentException();
        }
    }

    static String absSumFunction(String col, String expr) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "charCol":
                return className + ".sumChar(" + expr + ")";
            case "boolCol":
                return className + ".sumBool(" + expr + ")";
            case "byteCol":
                return className + ".sumByte(" + className + ".abs(" + expr + "))";
            case "shortCol":
                return className + ".sumShort(" + className + ".abs(" + expr + "))";
            case "intCol":
                return className + ".sumInt(" + className + ".abs(" + expr + "))";
            case "bigI":
                return className + ".absSumBigInt(" + expr + ")";
            case "bigD":
                return className + ".absSumBigDec(" + expr + ")";
            case "doubleCol":
            case "doubleNanCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumDouble("
                        + className + ".abs(" + expr + "))";
            case "floatCol":
                return QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName() + ".sumFloat(" + className
                        + ".abs(" + expr + "))";
            default:
                return "(long)sum(" + className + ".abs(" + expr + "))";
        }
    }
}
