//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.vector.*;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static io.deephaven.function.Basic.isNull;

public class QueryTableAggregationTestFormulaStaticMethods {


    @SuppressWarnings("unused")
    public static long sumBool(ObjectVector<Boolean> values) {
        if (values.isEmpty()) {
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

    @SuppressWarnings("unused")
    public static BigInteger sumBigInt(ObjectVector<BigInteger> values) {
        if (values.isEmpty()) {
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

    @SuppressWarnings("unused")
    public static BigInteger absSumBigInt(ObjectVector<BigInteger> values) {
        if (values.isEmpty()) {
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

    @SuppressWarnings("unused")
    public static BigDecimal sumBigDec(ObjectVector<BigDecimal> values) {
        if (values.isEmpty()) {
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

    @SuppressWarnings("unused")
    public static BigDecimal absSumBigDec(ObjectVector<BigDecimal> values) {
        if (values.isEmpty()) {
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


    @SuppressWarnings("unused")
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
                .subtract(new BigDecimal(sum.pow(2)).divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP))
                .divide(countMinus1, RoundingMode.HALF_UP);
    }

    @SuppressWarnings("unused")
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
        return sum2.subtract(sum.pow(2).divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP)).divide(countMinus1,
                RoundingMode.HALF_UP);
    }


    @SuppressWarnings("unused")
    public static BigDecimal avgBigInt(ObjectVector<BigInteger> bigIntegerObjectVector) {
        if (bigIntegerObjectVector == null || bigIntegerObjectVector.isEmpty()) {
            return null;
        }

        BigDecimal sum = new BigDecimal(0);
        long count = 0;

        final long n = bigIntegerObjectVector.size();

        for (long i = 0; i < n; i++) {
            BigInteger val = bigIntegerObjectVector.get(i);
            if (!isNull(val)) {
                final BigDecimal decVal = new BigDecimal(val);
                sum = sum.add(decVal);
                count++;
            }
        }
        if (count == 0) {
            return null;
        }
        return sum.divide(new BigDecimal(count), RoundingMode.HALF_UP);
    }

    @SuppressWarnings("unused")
    public static BigDecimal avgBigDec(ObjectVector<BigDecimal> bigDecimalObjectVector) {
        if (bigDecimalObjectVector == null || bigDecimalObjectVector.isEmpty()) {
            return null;
        }

        BigDecimal sum = new BigDecimal(0);
        long count = 0;

        final long n = bigDecimalObjectVector.size();

        for (long i = 0; i < n; i++) {
            BigDecimal val = bigDecimalObjectVector.get(i);
            if (!isNull(val)) {
                sum = sum.add(val);
                count++;
            }
        }
        if (count == 0) {
            return null;
        }
        return sum.divide(new BigDecimal(count), RoundingMode.HALF_UP);
    }

    static String sumFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "boolCol":
                return className + ".sumBool(" + col + ")";
            case "bigI":
                return className + ".sumBigInt(" + col + ")";
            case "bigD":
                return className + ".sumBigDec(" + col + ")";
            default:
                return "sum(" + col + ")";
        }
    }

    static String minFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "Sym":
            case "boolCol":
            case "bigI":
            case "bigD":
            case "dt":
                return "minObj(" + col + ")";
            default:
                return "min(" + col + ")";
        }
    }

    static String maxFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "Sym":
            case "boolCol":
            case "bigI":
            case "bigD":
            case "dt":
                return "maxObj(" + col + ")";
            default:
                return "max(" + col + ")";
        }
    }

    static String varFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "bigI":
                return className + ".varBigInt(" + col + ")";
            case "bigD":
                return className + ".varBigDec(" + col + ")";
            default:
                return "var(" + col + ")";
        }
    }

    static String stdFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "bigI":
            case "bigD":
                return "io.deephaven.util.BigDecimalUtils.sqrt(" + varFunction(col) + ", 10)";
            default:
                return "std(" + col + ")";
        }
    }

    static String countFunction(String col) {
        switch (col) {
            case "Sym":
            case "bigI":
            case "bigD":
                return "countObj(" + col + ")";
            default:
                return "count(" + col + ")";
        }
    }

    static String absSumFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "boolCol":
                return className + ".sumBool(" + col + ")";
            case "bigI":
                return className + ".absSumBigInt(" + col + ")";
            case "bigD":
                return className + ".absSumBigDec(" + col + ")";
            default:
                return "absSum(" + col + ")";
        }
    }

    static String avgFunction(String col) {
        final String className = QueryTableAggregationTestFormulaStaticMethods.class.getCanonicalName();
        switch (col) {
            case "bigI":
                return className + ".avgBigInt(" + col + ")";
            case "bigD":
                return className + ".avgBigDec(" + col + ")";
            default:
                return "avg(" + col + ")";
        }
    }

    static String wavgFunction(String col, String wcol) {
        return "wavg(" + col + "," + wcol + ")";
    }
}
