/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.lang;

import io.deephaven.util.QueryConstants;

import junit.framework.TestCase;

import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

@SuppressWarnings({"unused", "WeakerAccess", "NumericOverflow"})
public final class TestQueryLanguageFunctionUtils extends TestCase {

    public static void test_plus_int_int() {
        final int value1 = 42;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_double() {
        final int value1 = 42;
        final double value2 = 42d;
        final int zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_long() {
        final int value1 = 42;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_float() {
        final int value1 = 42;
        final float value2 = 42f;
        final int zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_char() {
        final int value1 = 42;
        final char value2 = '0';
        final int zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_byte() {
        final int value1 = 42;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_int_short() {
        final int value1 = 42;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_int() {
        final double value1 = 42d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_double() {
        final double value1 = 42d;
        final double value2 = 42d;
        final double zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_long() {
        final double value1 = 42d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_float() {
        final double value1 = 42d;
        final float value2 = 42f;
        final double zero1 = 0;
        final float zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_char() {
        final double value1 = 42d;
        final char value2 = '0';
        final double zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_byte() {
        final double value1 = 42d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_double_short() {
        final double value1 = 42d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_int() {
        final long value1 = 42L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_double() {
        final long value1 = 42L;
        final double value2 = 42d;
        final long zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_long() {
        final long value1 = 42L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_float() {
        final long value1 = 42L;
        final float value2 = 42f;
        final long zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_char() {
        final long value1 = 42L;
        final char value2 = '0';
        final long zero1 = 0;
        final char zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_byte() {
        final long value1 = 42L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_long_short() {
        final long value1 = 42L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_int() {
        final float value1 = 42f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_double() {
        final float value1 = 42f;
        final double value2 = 42d;
        final float zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_long() {
        final float value1 = 42f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_float() {
        final float value1 = 42f;
        final float value2 = 42f;
        final float zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_char() {
        final float value1 = 42f;
        final char value2 = '0';
        final float zero1 = 0;
        final char zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_byte() {
        final float value1 = 42f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_float_short() {
        final float value1 = 42f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_int() {
        final char value1 = '0';
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_double() {
        final char value1 = '0';
        final double value2 = 42d;
        final char zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_long() {
        final char value1 = '0';
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_float() {
        final char value1 = '0';
        final float value2 = 42f;
        final char zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_char() {
        final char value1 = '0';
        final char value2 = '0';
        final char zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_byte() {
        final char value1 = '0';
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_char_short() {
        final char value1 = '0';
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_int() {
        final byte value1 = (byte) 42;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_double() {
        final byte value1 = (byte) 42;
        final double value2 = 42d;
        final byte zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_long() {
        final byte value1 = (byte) 42;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_float() {
        final byte value1 = (byte) 42;
        final float value2 = 42f;
        final byte zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_char() {
        final byte value1 = (byte) 42;
        final char value2 = '0';
        final byte zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_byte() {
        final byte value1 = (byte) 42;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_byte_short() {
        final byte value1 = (byte) 42;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_int() {
        final short value1 = (short) 42;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_double() {
        final short value1 = (short) 42;
        final double value2 = 42d;
        final short zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_long() {
        final short value1 = (short) 42;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_float() {
        final short value1 = (short) 42;
        final float value2 = 42f;
        final short zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_char() {
        final short value1 = (short) 42;
        final char value2 = '0';
        final short zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_byte() {
        final short value1 = (short) 42;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_plus_short_short() {
        final short value1 = (short) 42;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.plus(value1, value2);
            expectedResult = value1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(value1, value2), value1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(zero1, value2);
            expectedResult = zero1 + value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.plus(zero1, value2), zero1+value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.plus(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_int() {
        final int value1 = 42;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_double() {
        final int value1 = 42;
        final double value2 = 42d;
        final int zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_long() {
        final int value1 = 42;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_float() {
        final int value1 = 42;
        final float value2 = 42f;
        final int zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_char() {
        final int value1 = 42;
        final char value2 = '0';
        final int zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_byte() {
        final int value1 = 42;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_int_short() {
        final int value1 = 42;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_int() {
        final double value1 = 42d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_double() {
        final double value1 = 42d;
        final double value2 = 42d;
        final double zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_long() {
        final double value1 = 42d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_float() {
        final double value1 = 42d;
        final float value2 = 42f;
        final double zero1 = 0;
        final float zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_char() {
        final double value1 = 42d;
        final char value2 = '0';
        final double zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_byte() {
        final double value1 = 42d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_double_short() {
        final double value1 = 42d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_int() {
        final long value1 = 42L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_double() {
        final long value1 = 42L;
        final double value2 = 42d;
        final long zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_long() {
        final long value1 = 42L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_float() {
        final long value1 = 42L;
        final float value2 = 42f;
        final long zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_char() {
        final long value1 = 42L;
        final char value2 = '0';
        final long zero1 = 0;
        final char zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_byte() {
        final long value1 = 42L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_long_short() {
        final long value1 = 42L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_int() {
        final float value1 = 42f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_double() {
        final float value1 = 42f;
        final double value2 = 42d;
        final float zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_long() {
        final float value1 = 42f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_float() {
        final float value1 = 42f;
        final float value2 = 42f;
        final float zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_char() {
        final float value1 = 42f;
        final char value2 = '0';
        final float zero1 = 0;
        final char zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_byte() {
        final float value1 = 42f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_float_short() {
        final float value1 = 42f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_int() {
        final char value1 = '0';
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_double() {
        final char value1 = '0';
        final double value2 = 42d;
        final char zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_long() {
        final char value1 = '0';
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_float() {
        final char value1 = '0';
        final float value2 = 42f;
        final char zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_char() {
        final char value1 = '0';
        final char value2 = '0';
        final char zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_byte() {
        final char value1 = '0';
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_char_short() {
        final char value1 = '0';
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_int() {
        final byte value1 = (byte) 42;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_double() {
        final byte value1 = (byte) 42;
        final double value2 = 42d;
        final byte zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_long() {
        final byte value1 = (byte) 42;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_float() {
        final byte value1 = (byte) 42;
        final float value2 = 42f;
        final byte zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_char() {
        final byte value1 = (byte) 42;
        final char value2 = '0';
        final byte zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_byte() {
        final byte value1 = (byte) 42;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_byte_short() {
        final byte value1 = (byte) 42;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_int() {
        final short value1 = (short) 42;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_double() {
        final short value1 = (short) 42;
        final double value2 = 42d;
        final short zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_long() {
        final short value1 = (short) 42;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_float() {
        final short value1 = (short) 42;
        final float value2 = 42f;
        final short zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_char() {
        final short value1 = (short) 42;
        final char value2 = '0';
        final short zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_byte() {
        final short value1 = (short) 42;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_minus_short_short() {
        final short value1 = (short) 42;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.minus(value1, value2);
            expectedResult = value1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(value1, value2), value1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(zero1, value2);
            expectedResult = zero1 - value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.minus(zero1, value2), zero1-value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.minus(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_int() {
        final int value1 = 42;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_double() {
        final int value1 = 42;
        final double value2 = 42d;
        final int zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_long() {
        final int value1 = 42;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_float() {
        final int value1 = 42;
        final float value2 = 42f;
        final int zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_char() {
        final int value1 = 42;
        final char value2 = '0';
        final int zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_byte() {
        final int value1 = 42;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_int_short() {
        final int value1 = 42;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_int() {
        final double value1 = 42d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_double() {
        final double value1 = 42d;
        final double value2 = 42d;
        final double zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_long() {
        final double value1 = 42d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_float() {
        final double value1 = 42d;
        final float value2 = 42f;
        final double zero1 = 0;
        final float zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_char() {
        final double value1 = 42d;
        final char value2 = '0';
        final double zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_byte() {
        final double value1 = 42d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_double_short() {
        final double value1 = 42d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_int() {
        final long value1 = 42L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_double() {
        final long value1 = 42L;
        final double value2 = 42d;
        final long zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_long() {
        final long value1 = 42L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_float() {
        final long value1 = 42L;
        final float value2 = 42f;
        final long zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_char() {
        final long value1 = 42L;
        final char value2 = '0';
        final long zero1 = 0;
        final char zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_byte() {
        final long value1 = 42L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_long_short() {
        final long value1 = 42L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_int() {
        final float value1 = 42f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_double() {
        final float value1 = 42f;
        final double value2 = 42d;
        final float zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_long() {
        final float value1 = 42f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_float() {
        final float value1 = 42f;
        final float value2 = 42f;
        final float zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_char() {
        final float value1 = 42f;
        final char value2 = '0';
        final float zero1 = 0;
        final char zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_byte() {
        final float value1 = 42f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_float_short() {
        final float value1 = 42f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_int() {
        final char value1 = '0';
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_double() {
        final char value1 = '0';
        final double value2 = 42d;
        final char zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_long() {
        final char value1 = '0';
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_float() {
        final char value1 = '0';
        final float value2 = 42f;
        final char zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_char() {
        final char value1 = '0';
        final char value2 = '0';
        final char zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_byte() {
        final char value1 = '0';
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_char_short() {
        final char value1 = '0';
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_int() {
        final byte value1 = (byte) 42;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_double() {
        final byte value1 = (byte) 42;
        final double value2 = 42d;
        final byte zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_long() {
        final byte value1 = (byte) 42;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_float() {
        final byte value1 = (byte) 42;
        final float value2 = 42f;
        final byte zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_char() {
        final byte value1 = (byte) 42;
        final char value2 = '0';
        final byte zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_byte() {
        final byte value1 = (byte) 42;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_byte_short() {
        final byte value1 = (byte) 42;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_int() {
        final short value1 = (short) 42;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_double() {
        final short value1 = (short) 42;
        final double value2 = 42d;
        final short zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_long() {
        final short value1 = (short) 42;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_float() {
        final short value1 = (short) 42;
        final float value2 = 42f;
        final short zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_char() {
        final short value1 = (short) 42;
        final char value2 = '0';
        final short zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_byte() {
        final short value1 = (short) 42;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_multiply_short_short() {
        final short value1 = (short) 42;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.multiply(value1, value2);
            expectedResult = value1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, value2), value1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(zero1, value2);
            expectedResult = zero1 * value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.multiply(zero1, value2), zero1*value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.multiply(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_int() {
        final int value1 = 42;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_double() {
        final int value1 = 42;
        final double value2 = 42d;
        final int zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_long() {
        final int value1 = 42;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_float() {
        final int value1 = 42;
        final float value2 = 42f;
        final int zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_char() {
        final int value1 = 42;
        final char value2 = '0';
        final int zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_byte() {
        final int value1 = 42;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_int_short() {
        final int value1 = 42;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_int() {
        final double value1 = 42d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_double() {
        final double value1 = 42d;
        final double value2 = 42d;
        final double zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_long() {
        final double value1 = 42d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_float() {
        final double value1 = 42d;
        final float value2 = 42f;
        final double zero1 = 0;
        final float zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_char() {
        final double value1 = 42d;
        final char value2 = '0';
        final double zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_byte() {
        final double value1 = 42d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_double_short() {
        final double value1 = 42d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_int() {
        final long value1 = 42L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_double() {
        final long value1 = 42L;
        final double value2 = 42d;
        final long zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_long() {
        final long value1 = 42L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_float() {
        final long value1 = 42L;
        final float value2 = 42f;
        final long zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_char() {
        final long value1 = 42L;
        final char value2 = '0';
        final long zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_byte() {
        final long value1 = 42L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_long_short() {
        final long value1 = 42L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_int() {
        final float value1 = 42f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_double() {
        final float value1 = 42f;
        final double value2 = 42d;
        final float zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_long() {
        final float value1 = 42f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_float() {
        final float value1 = 42f;
        final float value2 = 42f;
        final float zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_char() {
        final float value1 = 42f;
        final char value2 = '0';
        final float zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_byte() {
        final float value1 = 42f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_float_short() {
        final float value1 = 42f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_int() {
        final char value1 = '0';
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_double() {
        final char value1 = '0';
        final double value2 = 42d;
        final char zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_long() {
        final char value1 = '0';
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_float() {
        final char value1 = '0';
        final float value2 = 42f;
        final char zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_char() {
        final char value1 = '0';
        final char value2 = '0';
        final char zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_byte() {
        final char value1 = '0';
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_char_short() {
        final char value1 = '0';
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_int() {
        final byte value1 = (byte) 42;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_double() {
        final byte value1 = (byte) 42;
        final double value2 = 42d;
        final byte zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_long() {
        final byte value1 = (byte) 42;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_float() {
        final byte value1 = (byte) 42;
        final float value2 = 42f;
        final byte zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_char() {
        final byte value1 = (byte) 42;
        final char value2 = '0';
        final byte zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_byte() {
        final byte value1 = (byte) 42;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_byte_short() {
        final byte value1 = (byte) 42;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_int() {
        final short value1 = (short) 42;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_double() {
        final short value1 = (short) 42;
        final double value2 = 42d;
        final short zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_long() {
        final short value1 = (short) 42;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_float() {
        final short value1 = (short) 42;
        final float value2 = 42f;
        final short zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_char() {
        final short value1 = (short) 42;
        final char value2 = '0';
        final short zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_byte() {
        final short value1 = (short) 42;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_divide_short_short() {
        final short value1 = (short) 42;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.divide(value1, value2);
            expectedResult = value1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(value1, value2), value1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(zero1, value2);
            expectedResult = zero1 / (double) value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.divide(zero1, value2), zero1/(double)value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.divide(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_int() {
        final int value1 = 42;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_double() {
        final int value1 = 42;
        final double value2 = 42d;
        final int zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_long() {
        final int value1 = 42;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_float() {
        final int value1 = 42;
        final float value2 = 42f;
        final int zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_char() {
        final int value1 = 42;
        final char value2 = '0';
        final int zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_byte() {
        final int value1 = 42;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_int_short() {
        final int value1 = 42;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_int() {
        final double value1 = 42d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_double() {
        final double value1 = 42d;
        final double value2 = 42d;
        final double zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_long() {
        final double value1 = 42d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_float() {
        final double value1 = 42d;
        final float value2 = 42f;
        final double zero1 = 0;
        final float zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_char() {
        final double value1 = 42d;
        final char value2 = '0';
        final double zero1 = 0;
        final char zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_byte() {
        final double value1 = 42d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_double_short() {
        final double value1 = 42d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_int() {
        final long value1 = 42L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_INT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_double() {
        final long value1 = 42L;
        final double value2 = 42d;
        final long zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_long() {
        final long value1 = 42L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_float() {
        final long value1 = 42L;
        final float value2 = 42f;
        final long zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_char() {
        final long value1 = 42L;
        final char value2 = '0';
        final long zero1 = 0;
        final char zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_byte() {
        final long value1 = 42L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_long_short() {
        final long value1 = 42L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_int() {
        final float value1 = 42f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_double() {
        final float value1 = 42f;
        final double value2 = 42d;
        final float zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_long() {
        final float value1 = 42f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_float() {
        final float value1 = 42f;
        final float value2 = 42f;
        final float zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_char() {
        final float value1 = 42f;
        final char value2 = '0';
        final float zero1 = 0;
        final char zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_byte() {
        final float value1 = 42f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_float_short() {
        final float value1 = 42f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_int() {
        final char value1 = '0';
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_double() {
        final char value1 = '0';
        final double value2 = 42d;
        final char zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_long() {
        final char value1 = '0';
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_float() {
        final char value1 = '0';
        final float value2 = 42f;
        final char zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_char() {
        final char value1 = '0';
        final char value2 = '0';
        final char zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_byte() {
        final char value1 = '0';
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_char_short() {
        final char value1 = '0';
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_int() {
        final byte value1 = (byte) 42;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_double() {
        final byte value1 = (byte) 42;
        final double value2 = 42d;
        final byte zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_long() {
        final byte value1 = (byte) 42;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_float() {
        final byte value1 = (byte) 42;
        final float value2 = 42f;
        final byte zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_char() {
        final byte value1 = (byte) 42;
        final char value2 = '0';
        final byte zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_byte() {
        final byte value1 = (byte) 42;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_byte_short() {
        final byte value1 = (byte) 42;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_int() {
        final short value1 = (short) 42;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_double() {
        final short value1 = (short) 42;
        final double value2 = 42d;
        final short zero1 = 0;
        final double zero2 = 0;

        double dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Double.compare(dbResult, expectedResult);
            description = "Double.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE);
            expectedResult = QueryConstants.NULL_DOUBLE;
            compareResult = Double.compare(dbResult, expectedResult);
            description =
                    "Double.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE), QueryConstants.NULL_DOUBLE)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_long() {
        final short value1 = (short) 42;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        long dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Long.compare(dbResult, expectedResult);
            description = "Long.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG);
            expectedResult = QueryConstants.NULL_LONG;
            compareResult = Long.compare(dbResult, expectedResult);
            description =
                    "Long.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG), QueryConstants.NULL_LONG)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_float() {
        final short value1 = (short) 42;
        final float value2 = 42f;
        final short zero1 = 0;
        final float zero2 = 0;

        float dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Float.compare(dbResult, expectedResult);
            description = "Float.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT);
            expectedResult = QueryConstants.NULL_FLOAT;
            compareResult = Float.compare(dbResult, expectedResult);
            description =
                    "Float.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT), QueryConstants.NULL_FLOAT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_char() {
        final short value1 = (short) 42;
        final char value2 = '0';
        final short zero1 = 0;
        final char zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_byte() {
        final short value1 = (short) 42;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_remainder_short_short() {
        final short value1 = (short) 42;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        int dbResult = -1, expectedResult = -1;
        int compareResult;
        String description;

        try {
            dbResult = QueryLanguageFunctionUtils.remainder(value1, value2);
            expectedResult = value1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, value2), value1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(value1, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(zero1, value2);
            expectedResult = zero1 % value2;
            compareResult = Integer.compare(dbResult, expectedResult);
            description = "Integer.compare(QueryLanguageFunctionUtils.remainder(zero1, value2), zero1%value2)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, value2), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);

            dbResult = QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT);
            expectedResult = QueryConstants.NULL_INT;
            compareResult = Integer.compare(dbResult, expectedResult);
            description =
                    "Integer.compare(QueryLanguageFunctionUtils.remainder(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT), QueryConstants.NULL_INT)";
            TestCase.assertEquals(description, 0, compareResult);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Comparison failure: dbResult=" + dbResult + ", expectedResult=" + expectedResult, ex);
        }

    }

    public static void test_compare_int_int_compare() {
        final int value1 = 1;
        final int value2 = 42;
        final int zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_double_compare() {
        final int value1 = 1;
        final double value2 = 42.0d;
        final int zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_long_compare() {
        final int value1 = 1;
        final long value2 = 42L;
        final int zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_float_compare() {
        final int value1 = 1;
        final float value2 = 42.0f;
        final int zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_char_compare() {
        final int value1 = 1;
        final char value2 = '1';
        final int zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_byte_compare() {
        final int value1 = 1;
        final byte value2 = (byte) 42;
        final int zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_int_short_compare() {
        final int value1 = 1;
        final short value2 = (short) 42;
        final int zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_INT, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_int_compare() {
        final double value1 = 0.01d;
        final int value2 = 42;
        final double zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_double_compare() {
        final double value1 = 0.01d;
        final double value2 = 42.0d;
        final double zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_long_compare() {
        final double value1 = 0.01d;
        final long value2 = 42L;
        final double zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_float_compare() {
        final double value1 = 0.01d;
        final float value2 = 42.0f;
        final double zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_char_compare() {
        final double value1 = 0.01d;
        final char value2 = '1';
        final double zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_byte_compare() {
        final double value1 = 0.01d;
        final byte value2 = (byte) 42;
        final double zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_double_short_compare() {
        final double value1 = 0.01d;
        final short value2 = (short) 42;
        final double zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_int_compare() {
        final long value1 = 1L;
        final int value2 = 42;
        final long zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_double_compare() {
        final long value1 = 1L;
        final double value2 = 42.0d;
        final long zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_long_compare() {
        final long value1 = 1L;
        final long value2 = 42L;
        final long zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_float_compare() {
        final long value1 = 1L;
        final float value2 = 42.0f;
        final long zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_char_compare() {
        final long value1 = 1L;
        final char value2 = '1';
        final long zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_byte_compare() {
        final long value1 = 1L;
        final byte value2 = (byte) 42;
        final long zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_long_short_compare() {
        final long value1 = 1L;
        final short value2 = (short) 42;
        final long zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_LONG, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_int_compare() {
        final float value1 = 0.01f;
        final int value2 = 42;
        final float zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_double_compare() {
        final float value1 = 0.01f;
        final double value2 = 42.0d;
        final float zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_long_compare() {
        final float value1 = 0.01f;
        final long value2 = 42L;
        final float zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_float_compare() {
        final float value1 = 0.01f;
        final float value2 = 42.0f;
        final float zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_char_compare() {
        final float value1 = 0.01f;
        final char value2 = '1';
        final float zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_byte_compare() {
        final float value1 = 0.01f;
        final byte value2 = (byte) 42;
        final float zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_float_short_compare() {
        final float value1 = 0.01f;
        final short value2 = (short) 42;
        final float zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_FLOAT, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_int_compare() {
        final char value1 = (char) 1;
        final int value2 = 42;
        final char zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_double_compare() {
        final char value1 = (char) 1;
        final double value2 = 42.0d;
        final char zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_long_compare() {
        final char value1 = (char) 1;
        final long value2 = 42L;
        final char zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_float_compare() {
        final char value1 = (char) 1;
        final float value2 = 42.0f;
        final char zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_char_compare() {
        final char value1 = (char) 1;
        final char value2 = '1';
        final char zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_byte_compare() {
        final char value1 = (char) 1;
        final byte value2 = (byte) 42;
        final char zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_char_short_compare() {
        final char value1 = (char) 1;
        final short value2 = (short) 42;
        final char zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_CHAR, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_int_compare() {
        final byte value1 = (byte) 1;
        final int value2 = 42;
        final byte zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_double_compare() {
        final byte value1 = (byte) 1;
        final double value2 = 42.0d;
        final byte zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_long_compare() {
        final byte value1 = (byte) 1;
        final long value2 = 42L;
        final byte zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_float_compare() {
        final byte value1 = (byte) 1;
        final float value2 = 42.0f;
        final byte zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_char_compare() {
        final byte value1 = (byte) 1;
        final char value2 = '1';
        final byte zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_byte_compare() {
        final byte value1 = (byte) 1;
        final byte value2 = (byte) 42;
        final byte zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_byte_short_compare() {
        final byte value1 = (byte) 1;
        final short value2 = (short) 42;
        final byte zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_int_compare() {
        final short value1 = (short) 1;
        final int value2 = 42;
        final short zero1 = 0;
        final int zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_INT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_double_compare() {
        final short value1 = (short) 1;
        final double value2 = 42.0d;
        final short zero1 = 0;
        final double zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_DOUBLE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_long_compare() {
        final short value1 = (short) 1;
        final long value2 = 42L;
        final short zero1 = 0;
        final long zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_LONG));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_float_compare() {
        final short value1 = (short) 1;
        final float value2 = 42.0f;
        final short zero1 = 0;
        final float zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_FLOAT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_char_compare() {
        final short value1 = (short) 1;
        final char value2 = '1';
        final short zero1 = 0;
        final char zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_CHAR));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_byte_compare() {
        final short value1 = (short) 1;
        final byte value2 = (byte) 42;
        final short zero1 = 0;
        final byte zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_BYTE));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }

    public static void test_compare_short_short_compare() {
        final short value1 = (short) 1;
        final short value2 = (short) 42;
        final short zero1 = 0;
        final short zero2 = 0;

        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Float.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Float.NaN, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, Double.NaN));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(Double.NaN, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero1, zero2));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(zero2, zero1));
        TestCase.assertEquals(0,
                QueryLanguageFunctionUtils.compareTo(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value1, value1));
        TestCase.assertEquals(0, QueryLanguageFunctionUtils.compareTo(value2, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(value1, value2));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(value2, value1));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value1, value2));
        TestCase.assertEquals(-1, QueryLanguageFunctionUtils.compareTo(-value2, value1));
        TestCase.assertEquals(1, QueryLanguageFunctionUtils.compareTo(-value1, -value2));
    }


    public static void test_bigdecimal_plus() {
        final BigDecimal bv1 = BigDecimal.valueOf(10.005);
        final BigDecimal bv2 = BigDecimal.valueOf(10.5);
        final byte yv2 = (byte) 10;
        final int iv2 = 10;
        final short sv2 = (short) 10;
        final long lv2 = 10;
        final float fv2 = 0.5F;
        final double dv2 = 0.5;
        final BigDecimal[] results = new BigDecimal[] {
                plus(bv1, bv2),
                plus(plus(bv1, yv2), fv2),
                plus(plus(yv2, bv1), fv2),
                plus(plus(bv1, sv2), fv2),
                plus(plus(sv2, bv1), fv2),
                plus(plus(bv1, iv2), fv2),
                plus(plus(iv2, bv1), fv2),
                plus(plus(bv1, fv2), iv2),
                plus(plus(fv2, bv1), iv2),
                plus(plus(bv1, lv2), dv2),
                plus(plus(lv2, bv1), dv2),
                plus(plus(bv1, dv2), lv2),
                plus(plus(dv2, bv1), lv2),
        };
        final BigDecimal expected = BigDecimal.valueOf(20.505);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }
    }

    public static void test_bigdecimal_minus() {
        final BigDecimal bv1Left = BigDecimal.valueOf(100);
        final BigDecimal bv1Right = BigDecimal.valueOf(10);
        final BigDecimal[] results = new BigDecimal[] {
                minus(bv1Left, bv1Right),
                minus(bv1Left, (byte) 10),
                minus((byte) 100, bv1Right),
                minus(bv1Left, (short) 10),
                minus((short) 100, bv1Right),
                minus(bv1Left, 10),
                minus(100, bv1Right),
                minus(bv1Left, 10L),
                minus(100L, bv1Right),
                minus(bv1Left, 10.0F),
                minus(100.0F, bv1Right),
                minus(bv1Left, 10.0),
                minus(100.0, bv1Right),
        };
        final BigDecimal expected = BigDecimal.valueOf(90);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }
    }

    public static void test_bigdecimal_multiply() {
        final BigDecimal bv1 = BigDecimal.valueOf(10.005);
        final BigDecimal bv2 = BigDecimal.valueOf(2);
        final byte yv2 = (byte) 2;
        final short sv2 = (short) 2;
        final int iv2 = 2;
        final long lv2 = 2;
        final float fv2 = 2.0F;
        final double dv2 = 2.0;
        final BigDecimal[] results = new BigDecimal[] {
                multiply(bv1, bv2),
                multiply(bv1, yv2),
                multiply(yv2, bv1),
                multiply(bv1, sv2),
                multiply(sv2, bv1),
                multiply(bv1, iv2),
                multiply(iv2, bv1),
                multiply(bv1, fv2),
                multiply(fv2, bv1),
                multiply(bv1, lv2),
                multiply(lv2, bv1),
                multiply(bv1, dv2),
                multiply(dv2, bv1),
        };
        final BigDecimal expected = new BigDecimal(BigInteger.valueOf(2001), 2);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }
    }

    public static void test_bigdecimal_divide() {
        final BigDecimal bv1Left = BigDecimal.valueOf(1).setScale(3);
        final BigDecimal bv1Right = BigDecimal.valueOf(3).setScale(3);
        final BigDecimal[] results = new BigDecimal[] {
                divide(bv1Left, bv1Right),
                divide(bv1Left, (byte) 3),
                divide((byte) 1, bv1Right),
                divide(bv1Left, (short) 3),
                divide((short) 1, bv1Right),
                divide(bv1Left, 3),
                divide(1, bv1Right),
                divide(bv1Left, 3L),
                divide(1L, bv1Right),
                divide(bv1Left, 3.0F),
                divide(1.0F, bv1Right),
                divide(bv1Left, 3.0),
                divide(1.0, bv1Right),
        };
        long ex = 0;
        for (int i = 0; i < DEFAULT_SCALE; ++i) {
            ex = ex * 10 + 3;
        }
        final BigDecimal expected = new BigDecimal(BigInteger.valueOf(ex), DEFAULT_SCALE);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }

        final int biggerScale = DEFAULT_SCALE + 2;
        final BigDecimal bv2Left = BigDecimal.valueOf(1).setScale(biggerScale);
        ex = 0;
        for (int i = 0; i < biggerScale; ++i) {
            ex = ex * 10 + 3;
        }
        final BigDecimal expected2 = new BigDecimal(BigInteger.valueOf(ex), biggerScale);
        TestCase.assertEquals(0, expected2.compareTo(divide(bv2Left, 3.0)));
        TestCase.assertEquals(0, expected2.compareTo(divide(bv2Left, 3.0F)));

        final BigDecimal bv2Right = BigDecimal.valueOf(3).setScale(biggerScale);
        TestCase.assertEquals(0, expected2.compareTo(divide(1.0, bv2Right)));
        TestCase.assertEquals(0, expected2.compareTo(divide(1.0F, bv2Right)));
    }

    public static void test_bigdecimal_divide_floating_actual() {
        final BigDecimal bv1Left = BigDecimal.valueOf(0.5);
        final BigDecimal bv1Right = BigDecimal.valueOf(0.125);
        final BigDecimal[] results = new BigDecimal[] {
                divide(bv1Left, bv1Right),
                divide(bv1Left, 0.125),
                divide(0.5, bv1Right),
                divide(bv1Left, 0.125F),
                divide(0.5F, bv1Right),
        };
        final BigDecimal expected = new BigDecimal(BigInteger.valueOf(4)).setScale(DEFAULT_SCALE, ROUNDING_MODE);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }
    }

    public static void test_bigdecimal_arithmetic_op_primitives_null() {
        final BigDecimal bv1 = BigDecimal.valueOf(10.005);
        final byte yv1 = QueryConstants.NULL_BYTE;
        final short sv1 = QueryConstants.NULL_SHORT;
        final int iv1 = QueryConstants.NULL_INT;
        final long lv1 = QueryConstants.NULL_LONG;
        final float fv1 = QueryConstants.NULL_FLOAT;
        final double dv1 = QueryConstants.NULL_DOUBLE;
        final BigDecimal[] results = new BigDecimal[] {
                plus(bv1, yv1),
                plus(yv1, bv1),
                plus(bv1, sv1),
                plus(sv1, bv1),
                plus(bv1, iv1),
                plus(iv1, bv1),
                plus(bv1, fv1),
                plus(fv1, bv1),
                plus(bv1, lv1),
                plus(lv1, bv1),
                plus(bv1, dv1),
                plus(dv1, bv1),
                minus(bv1, yv1),
                minus(yv1, bv1),
                minus(bv1, sv1),
                minus(sv1, bv1),
                minus(bv1, iv1),
                minus(iv1, bv1),
                minus(bv1, fv1),
                minus(fv1, bv1),
                minus(bv1, lv1),
                minus(lv1, bv1),
                minus(bv1, dv1),
                minus(dv1, bv1),
                multiply(bv1, yv1),
                multiply(yv1, bv1),
                multiply(bv1, sv1),
                multiply(sv1, bv1),
                multiply(bv1, iv1),
                multiply(iv1, bv1),
                multiply(bv1, fv1),
                multiply(fv1, bv1),
                multiply(bv1, lv1),
                multiply(lv1, bv1),
                multiply(bv1, dv1),
                multiply(dv1, bv1),
                divide(bv1, yv1),
                divide(yv1, bv1),
                divide(bv1, sv1),
                divide(sv1, bv1),
                divide(bv1, iv1),
                divide(iv1, bv1),
                divide(bv1, fv1),
                divide(fv1, bv1),
                divide(bv1, lv1),
                divide(lv1, bv1),
                divide(bv1, dv1),
                divide(dv1, bv1),
        };
        for (BigDecimal r : results) {
            TestCase.assertNull(r);
        }
    }

    public static void test_bigdecimal_airthmetic_op_bigdecimal_null() {
        final BigDecimal bvnull = null;
        final BigDecimal bv1 = BigDecimal.valueOf(100.05);
        final byte yv1 = (byte) 10;
        final short sv1 = (short) 10;
        final int iv1 = 10;
        final long lv1 = 10L;
        final float fv1 = 0.5F;
        final double dv1 = 0.5;
        final BigDecimal[] results = new BigDecimal[] {
                plus(bvnull, yv1),
                plus(yv1, bvnull),
                plus(bvnull, sv1),
                plus(sv1, bvnull),
                plus(bvnull, bv1),
                plus(bv1, bvnull),
                plus(bvnull, iv1),
                plus(iv1, bvnull),
                plus(bvnull, fv1),
                plus(fv1, bvnull),
                plus(bvnull, lv1),
                plus(lv1, bvnull),
                plus(bvnull, dv1),
                plus(dv1, bvnull),
                minus(bvnull, yv1),
                minus(yv1, bvnull),
                minus(bvnull, sv1),
                minus(sv1, bvnull),
                minus(bvnull, yv1),
                minus(yv1, bvnull),
                minus(bvnull, sv1),
                minus(sv1, bvnull),
                minus(bvnull, bv1),
                minus(bv1, bvnull),
                minus(bvnull, iv1),
                minus(iv1, bvnull),
                minus(bvnull, fv1),
                minus(fv1, bvnull),
                minus(bvnull, lv1),
                minus(lv1, bvnull),
                minus(bvnull, dv1),
                minus(dv1, bvnull),
                multiply(bvnull, yv1),
                multiply(yv1, bvnull),
                multiply(bvnull, sv1),
                multiply(sv1, bvnull),
                multiply(bvnull, bv1),
                multiply(bv1, bvnull),
                multiply(bvnull, iv1),
                multiply(iv1, bvnull),
                multiply(bvnull, fv1),
                multiply(fv1, bvnull),
                multiply(bvnull, lv1),
                multiply(lv1, bvnull),
                multiply(bvnull, dv1),
                multiply(dv1, bvnull),
                divide(bvnull, yv1),
                divide(yv1, bvnull),
                divide(bvnull, sv1),
                divide(sv1, bvnull),
                divide(bvnull, bv1),
                divide(bv1, bvnull),
                divide(bvnull, iv1),
                divide(iv1, bvnull),
                divide(bvnull, fv1),
                divide(fv1, bvnull),
                divide(bvnull, lv1),
                divide(lv1, bvnull),
                divide(bvnull, dv1),
                divide(dv1, bvnull),
        };
        for (BigDecimal r : results) {
            TestCase.assertNull(r);
        }
    }

    public static void test_bigdecimal_nans() {
        final BigDecimal bv1 = BigDecimal.valueOf(100.05);
        final BigDecimal bvnull = null;
        final float vfnan = Float.NaN;
        final double vdnan = Double.NaN;

        TestCase.assertEquals(-1, compareTo(bv1, vfnan)); // rhs NaN
        TestCase.assertEquals(1, compareTo(vfnan, bv1)); // lhs NaN
        TestCase.assertEquals(-1, compareTo(bvnull, vfnan)); // lhs null and rhs NaN
        TestCase.assertEquals(1, compareTo(vfnan, bvnull)); // lhs NaN and rhs null

        TestCase.assertEquals(-1, compareTo(bv1, vdnan)); // rhs NaN
        TestCase.assertEquals(1, compareTo(vdnan, bv1)); // lhs NaN
        TestCase.assertEquals(-1, compareTo(bvnull, vdnan)); // lhs null and rhs NaN
        TestCase.assertEquals(1, compareTo(vdnan, bvnull)); // lhs NaN and rhs null

        TestCase.assertFalse(eq(bv1, vfnan));
        TestCase.assertFalse(eq(vfnan, bv1));
        TestCase.assertFalse(eq(bvnull, vfnan));
        TestCase.assertFalse(eq(vfnan, bvnull));
        TestCase.assertFalse(eq(bv1, vdnan));
        TestCase.assertFalse(eq(vdnan, bv1));
        TestCase.assertFalse(eq(bvnull, vdnan));
        TestCase.assertFalse(eq(vdnan, bvnull));
    }

    public static void test_bigdecimal_compare() {
        final BigDecimal bv10 = BigDecimal.valueOf(10);
        final BigDecimal bv11 = BigDecimal.valueOf(11);
        final BigDecimal bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertEquals(-1, compareTo(bv10, bv11));
        TestCase.assertEquals(-1, compareTo(bv10, yv11));
        TestCase.assertEquals(-1, compareTo(bv10, sv11));
        TestCase.assertEquals(-1, compareTo(bv10, iv11));
        TestCase.assertEquals(-1, compareTo(bv10, lv11));
        TestCase.assertEquals(-1, compareTo(bv10, fv11));
        TestCase.assertEquals(-1, compareTo(bv10, dv11));
        TestCase.assertEquals(0, compareTo(bv10, bv10));
        TestCase.assertEquals(0, compareTo(bv10, yv10));
        TestCase.assertEquals(0, compareTo(bv10, sv10));
        TestCase.assertEquals(0, compareTo(bv10, iv10));
        TestCase.assertEquals(0, compareTo(bv10, lv10));
        TestCase.assertEquals(0, compareTo(bv10, fv10));
        TestCase.assertEquals(0, compareTo(bv10, dv10));
        TestCase.assertEquals(1, compareTo(bv11, bv10));
        TestCase.assertEquals(1, compareTo(bv11, yv10));
        TestCase.assertEquals(1, compareTo(bv11, sv10));
        TestCase.assertEquals(1, compareTo(bv11, iv10));
        TestCase.assertEquals(1, compareTo(bv11, lv10));
        TestCase.assertEquals(1, compareTo(bv11, fv10));
        TestCase.assertEquals(1, compareTo(bv11, dv10));

        TestCase.assertEquals(-1, compareTo(yv10, bv11));
        TestCase.assertEquals(-1, compareTo(sv10, bv11));
        TestCase.assertEquals(-1, compareTo(iv10, bv11));
        TestCase.assertEquals(-1, compareTo(lv10, bv11));
        TestCase.assertEquals(-1, compareTo(fv10, bv11));
        TestCase.assertEquals(-1, compareTo(dv10, bv11));
        TestCase.assertEquals(0, compareTo(yv10, bv10));
        TestCase.assertEquals(0, compareTo(sv10, bv10));
        TestCase.assertEquals(0, compareTo(iv10, bv10));
        TestCase.assertEquals(0, compareTo(lv10, bv10));
        TestCase.assertEquals(0, compareTo(fv10, bv10));
        TestCase.assertEquals(0, compareTo(dv10, bv10));
        TestCase.assertEquals(1, compareTo(yv11, bv10));
        TestCase.assertEquals(1, compareTo(sv11, bv10));
        TestCase.assertEquals(1, compareTo(iv11, bv10));
        TestCase.assertEquals(1, compareTo(lv11, bv10));
        TestCase.assertEquals(1, compareTo(fv11, bv10));
        TestCase.assertEquals(1, compareTo(dv11, bv10));

        TestCase.assertEquals(-1, compareTo(bvnull, bv10));
        TestCase.assertEquals(-1, compareTo(bvnull, yv10));
        TestCase.assertEquals(-1, compareTo(bvnull, sv10));
        TestCase.assertEquals(-1, compareTo(bvnull, iv10));
        TestCase.assertEquals(-1, compareTo(bvnull, lv10));
        TestCase.assertEquals(-1, compareTo(bvnull, fv10));
        TestCase.assertEquals(-1, compareTo(bvnull, dv10));
        TestCase.assertEquals(0, compareTo(bvnull, yvnull));
        TestCase.assertEquals(0, compareTo(bvnull, svnull));
        TestCase.assertEquals(0, compareTo(bvnull, ivnull));
        TestCase.assertEquals(0, compareTo(bvnull, lvnull));
        TestCase.assertEquals(0, compareTo(bvnull, fvnull));
        TestCase.assertEquals(0, compareTo(bvnull, dvnull));
        TestCase.assertEquals(1, compareTo(bv10, bvnull));
        TestCase.assertEquals(1, compareTo(bv10, yvnull));
        TestCase.assertEquals(1, compareTo(bv10, svnull));
        TestCase.assertEquals(1, compareTo(bv10, ivnull));
        TestCase.assertEquals(1, compareTo(bv10, lvnull));
        TestCase.assertEquals(1, compareTo(bv10, fvnull));
        TestCase.assertEquals(1, compareTo(bv10, dvnull));

        TestCase.assertEquals(-1, compareTo(yvnull, bv10));
        TestCase.assertEquals(-1, compareTo(svnull, bv10));
        TestCase.assertEquals(-1, compareTo(ivnull, bv10));
        TestCase.assertEquals(-1, compareTo(lvnull, bv10));
        TestCase.assertEquals(-1, compareTo(fvnull, bv10));
        TestCase.assertEquals(-1, compareTo(dvnull, bv10));
        TestCase.assertEquals(0, compareTo(yvnull, bvnull));
        TestCase.assertEquals(0, compareTo(svnull, bvnull));
        TestCase.assertEquals(0, compareTo(ivnull, bvnull));
        TestCase.assertEquals(0, compareTo(lvnull, bvnull));
        TestCase.assertEquals(0, compareTo(fvnull, bvnull));
        TestCase.assertEquals(0, compareTo(dvnull, bvnull));
        TestCase.assertEquals(1, compareTo(yv10, bvnull));
        TestCase.assertEquals(1, compareTo(sv10, bvnull));
        TestCase.assertEquals(1, compareTo(iv10, bvnull));
        TestCase.assertEquals(1, compareTo(lv10, bvnull));
        TestCase.assertEquals(1, compareTo(fv10, bvnull));
        TestCase.assertEquals(1, compareTo(dv10, bvnull));
    }

    public static void test_bigdecimal_eq() {
        final BigDecimal bv10 = BigDecimal.valueOf(10);
        final BigDecimal bv11 = BigDecimal.valueOf(11);
        final BigDecimal bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertTrue(eq(bv10, bv10));
        TestCase.assertTrue(eq(bv10, yv10));
        TestCase.assertTrue(eq(bv10, sv10));
        TestCase.assertTrue(eq(bv10, iv10));
        TestCase.assertTrue(eq(bv10, lv10));
        TestCase.assertTrue(eq(bv10, fv10));
        TestCase.assertTrue(eq(bv10, dv10));
        TestCase.assertTrue(eq(yv10, bv10));
        TestCase.assertTrue(eq(sv10, bv10));
        TestCase.assertTrue(eq(iv10, bv10));
        TestCase.assertTrue(eq(lv10, bv10));
        TestCase.assertTrue(eq(fv10, bv10));
        TestCase.assertTrue(eq(dv10, bv10));

        TestCase.assertFalse(eq(bv10, bv11));
        TestCase.assertFalse(eq(bv10, yv11));
        TestCase.assertFalse(eq(bv10, sv11));
        TestCase.assertFalse(eq(bv10, iv11));
        TestCase.assertFalse(eq(bv10, lv11));
        TestCase.assertFalse(eq(bv10, fv11));
        TestCase.assertFalse(eq(bv10, dv11));
        TestCase.assertFalse(eq(yv10, bv11));
        TestCase.assertFalse(eq(sv10, bv11));
        TestCase.assertFalse(eq(iv10, bv11));
        TestCase.assertFalse(eq(lv10, bv11));
        TestCase.assertFalse(eq(fv10, bv11));
        TestCase.assertFalse(eq(dv10, bv11));

        TestCase.assertFalse(eq(bv10, bvnull));
        TestCase.assertFalse(eq(bv10, yvnull));
        TestCase.assertFalse(eq(bv10, svnull));
        TestCase.assertFalse(eq(bv10, ivnull));
        TestCase.assertFalse(eq(bv10, lvnull));
        TestCase.assertFalse(eq(bv10, fvnull));
        TestCase.assertFalse(eq(bv10, dvnull));
        TestCase.assertFalse(eq(yvnull, bv11));
        TestCase.assertFalse(eq(svnull, bv11));
        TestCase.assertFalse(eq(ivnull, bv11));
        TestCase.assertFalse(eq(lvnull, bv11));
        TestCase.assertFalse(eq(fvnull, bv11));
        TestCase.assertFalse(eq(dvnull, bv11));
        TestCase.assertFalse(eq(bvnull, yv11));
        TestCase.assertFalse(eq(bvnull, sv11));
        TestCase.assertFalse(eq(bvnull, iv11));
        TestCase.assertFalse(eq(bvnull, lv11));
        TestCase.assertFalse(eq(bvnull, fv11));
        TestCase.assertFalse(eq(bvnull, dv11));
        TestCase.assertFalse(eq(yv10, bvnull));
        TestCase.assertFalse(eq(sv10, bvnull));
        TestCase.assertFalse(eq(iv10, bvnull));
        TestCase.assertFalse(eq(lv10, bvnull));
        TestCase.assertFalse(eq(fv10, bvnull));
        TestCase.assertFalse(eq(dv10, bvnull));

        TestCase.assertTrue(eq(bvnull, bvnull));
        TestCase.assertTrue(eq(bvnull, yvnull));
        TestCase.assertTrue(eq(bvnull, svnull));
        TestCase.assertTrue(eq(bvnull, ivnull));
        TestCase.assertTrue(eq(bvnull, lvnull));
        TestCase.assertTrue(eq(bvnull, fvnull));
        TestCase.assertTrue(eq(bvnull, dvnull));
        TestCase.assertTrue(eq(yvnull, bvnull));
        TestCase.assertTrue(eq(svnull, bvnull));
        TestCase.assertTrue(eq(ivnull, bvnull));
        TestCase.assertTrue(eq(lvnull, bvnull));
        TestCase.assertTrue(eq(fvnull, bvnull));
        TestCase.assertTrue(eq(dvnull, bvnull));
    }

    public static void test_bigdecimal_comps() {
        final BigDecimal bv10 = BigDecimal.valueOf(10);
        final BigDecimal bv11 = BigDecimal.valueOf(11);
        final BigDecimal bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertTrue(less(bv10, bv11));
        TestCase.assertTrue(less(bv10, yv11));
        TestCase.assertTrue(less(bv10, sv11));
        TestCase.assertTrue(less(bv10, iv11));
        TestCase.assertTrue(less(bv10, lv11));
        TestCase.assertTrue(less(bv10, fv11));
        TestCase.assertTrue(less(bv10, dv11));
        TestCase.assertTrue(less(yv10, bv11));
        TestCase.assertTrue(less(sv10, bv11));
        TestCase.assertTrue(less(iv10, bv11));
        TestCase.assertTrue(less(lv10, bv11));
        TestCase.assertTrue(less(fv10, bv11));
        TestCase.assertTrue(less(dv10, bv11));
        TestCase.assertFalse(less(bv10, bv10));
        TestCase.assertFalse(less(bv10, yv10));
        TestCase.assertFalse(less(bv10, sv10));
        TestCase.assertFalse(less(bv10, iv10));
        TestCase.assertFalse(less(bv10, lv10));
        TestCase.assertFalse(less(bv10, fv10));
        TestCase.assertFalse(less(bv10, dv10));
        TestCase.assertFalse(less(yv10, bv10));
        TestCase.assertFalse(less(sv10, bv10));
        TestCase.assertFalse(less(iv10, bv10));
        TestCase.assertFalse(less(lv10, bv10));
        TestCase.assertFalse(less(fv10, bv10));
        TestCase.assertFalse(less(dv10, bv10));
        TestCase.assertFalse(less(bv11, bv10));
        TestCase.assertFalse(less(bv11, yv10));
        TestCase.assertFalse(less(bv11, sv10));
        TestCase.assertFalse(less(bv11, iv10));
        TestCase.assertFalse(less(bv11, lv10));
        TestCase.assertFalse(less(bv11, fv10));
        TestCase.assertFalse(less(bv11, dv10));
        TestCase.assertFalse(less(yv11, bv10));
        TestCase.assertFalse(less(sv11, bv10));
        TestCase.assertFalse(less(iv11, bv10));
        TestCase.assertFalse(less(lv11, bv10));
        TestCase.assertFalse(less(fv11, bv10));
        TestCase.assertFalse(less(dv11, bv10));

        TestCase.assertTrue(lessEquals(bv10, bv10));
        TestCase.assertTrue(lessEquals(bv10, yv10));
        TestCase.assertTrue(lessEquals(bv10, sv10));
        TestCase.assertTrue(lessEquals(bv10, iv10));
        TestCase.assertTrue(lessEquals(bv10, lv10));
        TestCase.assertTrue(lessEquals(bv10, fv10));
        TestCase.assertTrue(lessEquals(bv10, dv10));
        TestCase.assertTrue(lessEquals(yv10, bv10));
        TestCase.assertTrue(lessEquals(sv10, bv10));
        TestCase.assertTrue(lessEquals(iv10, bv10));
        TestCase.assertTrue(lessEquals(lv10, bv10));
        TestCase.assertTrue(lessEquals(fv10, bv10));
        TestCase.assertTrue(lessEquals(dv10, bv10));
        TestCase.assertTrue(lessEquals(bv10, bv11));
        TestCase.assertTrue(lessEquals(bv10, yv11));
        TestCase.assertTrue(lessEquals(bv10, sv11));
        TestCase.assertTrue(lessEquals(bv10, iv11));
        TestCase.assertTrue(lessEquals(bv10, lv11));
        TestCase.assertTrue(lessEquals(bv10, fv11));
        TestCase.assertTrue(lessEquals(bv10, dv11));
        TestCase.assertTrue(lessEquals(yv10, bv11));
        TestCase.assertTrue(lessEquals(sv10, bv11));
        TestCase.assertTrue(lessEquals(iv10, bv11));
        TestCase.assertTrue(lessEquals(lv10, bv11));
        TestCase.assertTrue(lessEquals(fv10, bv11));
        TestCase.assertTrue(lessEquals(dv10, bv11));
        TestCase.assertFalse(less(bv11, bv10));
        TestCase.assertFalse(less(bv11, yv10));
        TestCase.assertFalse(less(bv11, sv10));
        TestCase.assertFalse(less(bv11, iv10));
        TestCase.assertFalse(less(bv11, lv10));
        TestCase.assertFalse(less(bv11, fv10));
        TestCase.assertFalse(less(bv11, dv10));
        TestCase.assertFalse(less(yv11, bv10));
        TestCase.assertFalse(less(sv11, bv10));
        TestCase.assertFalse(less(iv11, bv10));
        TestCase.assertFalse(less(lv11, bv10));
        TestCase.assertFalse(less(fv11, bv10));
        TestCase.assertFalse(less(dv11, bv10));

        TestCase.assertFalse(greater(bv10, bv11));
        TestCase.assertFalse(greater(bv10, yv11));
        TestCase.assertFalse(greater(bv10, sv11));
        TestCase.assertFalse(greater(bv10, iv11));
        TestCase.assertFalse(greater(bv10, lv11));
        TestCase.assertFalse(greater(bv10, fv11));
        TestCase.assertFalse(greater(bv10, dv11));
        TestCase.assertFalse(greater(yv10, bv11));
        TestCase.assertFalse(greater(sv10, bv11));
        TestCase.assertFalse(greater(iv10, bv11));
        TestCase.assertFalse(greater(lv10, bv11));
        TestCase.assertFalse(greater(fv10, bv11));
        TestCase.assertFalse(greater(dv10, bv11));
        TestCase.assertFalse(greater(bv10, bv10));
        TestCase.assertFalse(greater(bv10, yv10));
        TestCase.assertFalse(greater(bv10, sv10));
        TestCase.assertFalse(greater(bv10, iv10));
        TestCase.assertFalse(greater(bv10, lv10));
        TestCase.assertFalse(greater(bv10, fv10));
        TestCase.assertFalse(greater(bv10, dv10));
        TestCase.assertFalse(greater(yv10, bv10));
        TestCase.assertFalse(greater(sv10, bv10));
        TestCase.assertFalse(greater(iv10, bv10));
        TestCase.assertFalse(greater(lv10, bv10));
        TestCase.assertFalse(greater(fv10, bv10));
        TestCase.assertFalse(greater(dv10, bv10));
        TestCase.assertTrue(greater(bv11, bv10));
        TestCase.assertTrue(greater(bv11, yv10));
        TestCase.assertTrue(greater(bv11, sv10));
        TestCase.assertTrue(greater(bv11, iv10));
        TestCase.assertTrue(greater(bv11, lv10));
        TestCase.assertTrue(greater(bv11, fv10));
        TestCase.assertTrue(greater(bv11, dv10));
        TestCase.assertTrue(greater(yv11, bv10));
        TestCase.assertTrue(greater(sv11, bv10));
        TestCase.assertTrue(greater(iv11, bv10));
        TestCase.assertTrue(greater(lv11, bv10));
        TestCase.assertTrue(greater(fv11, bv10));
        TestCase.assertTrue(greater(dv11, bv10));

        TestCase.assertFalse(greaterEquals(bv10, bv11));
        TestCase.assertFalse(greaterEquals(bv10, yv11));
        TestCase.assertFalse(greaterEquals(bv10, sv11));
        TestCase.assertFalse(greaterEquals(bv10, iv11));
        TestCase.assertFalse(greaterEquals(bv10, lv11));
        TestCase.assertFalse(greaterEquals(bv10, fv11));
        TestCase.assertFalse(greaterEquals(bv10, dv11));
        TestCase.assertFalse(greaterEquals(yv10, bv11));
        TestCase.assertFalse(greaterEquals(sv10, bv11));
        TestCase.assertFalse(greaterEquals(iv10, bv11));
        TestCase.assertFalse(greaterEquals(lv10, bv11));
        TestCase.assertFalse(greaterEquals(fv10, bv11));
        TestCase.assertFalse(greaterEquals(dv10, bv11));
        TestCase.assertTrue(greaterEquals(bv10, bv10));
        TestCase.assertTrue(greaterEquals(bv10, yv10));
        TestCase.assertTrue(greaterEquals(bv10, sv10));
        TestCase.assertTrue(greaterEquals(bv10, iv10));
        TestCase.assertTrue(greaterEquals(bv10, lv10));
        TestCase.assertTrue(greaterEquals(bv10, fv10));
        TestCase.assertTrue(greaterEquals(bv10, dv10));
        TestCase.assertTrue(greaterEquals(yv10, bv10));
        TestCase.assertTrue(greaterEquals(sv10, bv10));
        TestCase.assertTrue(greaterEquals(iv10, bv10));
        TestCase.assertTrue(greaterEquals(lv10, bv10));
        TestCase.assertTrue(greaterEquals(fv10, bv10));
        TestCase.assertTrue(greaterEquals(dv10, bv10));
        TestCase.assertTrue(greaterEquals(bv11, bv10));
        TestCase.assertTrue(greaterEquals(bv11, yv10));
        TestCase.assertTrue(greaterEquals(bv11, sv10));
        TestCase.assertTrue(greaterEquals(bv11, iv10));
        TestCase.assertTrue(greaterEquals(bv11, lv10));
        TestCase.assertTrue(greaterEquals(bv11, fv10));
        TestCase.assertTrue(greaterEquals(bv11, dv10));
        TestCase.assertTrue(greaterEquals(yv11, bv10));
        TestCase.assertTrue(greaterEquals(sv11, bv10));
        TestCase.assertTrue(greaterEquals(iv11, bv10));
        TestCase.assertTrue(greaterEquals(lv11, bv10));
        TestCase.assertTrue(greaterEquals(fv11, bv10));
        TestCase.assertTrue(greaterEquals(dv11, bv10));
    }

    public static void sameBiOrBd(final Object expected, final Object result) {
        if (expected instanceof BigDecimal) {
            TestCase.assertTrue(result instanceof BigDecimal);
            final BigDecimal exBd = (BigDecimal) expected;
            final BigDecimal reBd = (BigDecimal) result;
            assertEquals(0, exBd.compareTo(reBd));
            return;
        }

        TestCase.assertTrue(result instanceof BigInteger);
        final BigInteger exBi = (BigInteger) expected;
        TestCase.assertTrue(result instanceof BigInteger);
        final BigInteger reBi = (BigInteger) result;
        assertEquals(0, exBi.compareTo(reBi));
    }

    public static void test_biginteger_plus() {
        final BigInteger bv1 = BigInteger.valueOf(10);
        final BigInteger bv2 = BigInteger.valueOf(5);
        final byte yv2 = (byte) 5;
        final int iv2 = 5;
        final short sv2 = (short) 5;
        final long lv2 = 5;
        final float fv2 = 5.0F;
        final double dv2 = 5.0;
        final BigInteger bie = BigInteger.valueOf(15);
        final BigDecimal bde = BigDecimal.valueOf(15);
        final Object[] expectedAndresult = new Object[] {
                bie, plus(bv1, bv2),
                bie, plus(bv1, yv2),
                bie, plus(yv2, bv1),
                bie, plus(bv1, sv2),
                bie, plus(sv2, bv1),
                bie, plus(bv1, iv2),
                bie, plus(iv2, bv1),
                bie, plus(lv2, bv1),
                bie, plus(bv1, lv2),
                bde, plus(bv1, fv2),
                bde, plus(fv2, bv1),
                bde, plus(bv1, dv2),
                bde, plus(dv2, bv1),
        };
        for (int i = 0; i < expectedAndresult.length; i += 2) {
            final Object expected = expectedAndresult[i];
            final Object result = expectedAndresult[i + 1];
            sameBiOrBd(expected, result);
        }
    }

    public static void test_biginteger_minus() {
        final BigInteger bv1Left = BigInteger.valueOf(100);
        final BigInteger bv1Right = BigInteger.valueOf(10);
        final BigInteger bie = BigInteger.valueOf(90);
        final BigDecimal bde = BigDecimal.valueOf(90);
        final Object[] expectedAndresult = new Object[] {
                bie, minus(bv1Left, bv1Right),
                bie, minus(bv1Left, (byte) 10),
                bie, minus((byte) 100, bv1Right),
                bie, minus(bv1Left, (short) 10),
                bie, minus((short) 100, bv1Right),
                bie, minus(bv1Left, 10),
                bie, minus(100, bv1Right),
                bie, minus(bv1Left, 10L),
                bie, minus(100L, bv1Right),
                bde, minus(bv1Left, 10.0F),
                bde, minus(100.0F, bv1Right),
                bde, minus(bv1Left, 10.0),
                bde, minus(100.0, bv1Right),
        };
        for (int i = 0; i < expectedAndresult.length; i += 2) {
            final Object expected = expectedAndresult[i];
            final Object result = expectedAndresult[i + 1];
            sameBiOrBd(expected, result);
        }
    }

    public static void test_biginteger_multiply() {
        final BigInteger bv1 = BigInteger.valueOf(1000);
        final BigInteger bv2 = BigInteger.valueOf(2);
        final byte yv2 = (byte) 2;
        final short sv2 = (short) 2;
        final int iv2 = 2;
        final long lv2 = 2;
        final float fv2 = 2.0F;
        final double dv2 = 2.0;
        final BigInteger bie = BigInteger.valueOf(2000);
        final BigDecimal bde = BigDecimal.valueOf(2000);
        final Object[] expectedAndresult = new Object[] {
                bie, multiply(bv1, bv2),
                bie, multiply(bv1, yv2),
                bie, multiply(yv2, bv1),
                bie, multiply(bv1, sv2),
                bie, multiply(sv2, bv1),
                bie, multiply(bv1, iv2),
                bie, multiply(iv2, bv1),
                bie, multiply(bv1, lv2),
                bie, multiply(lv2, bv1),
                bde, multiply(bv1, fv2),
                bde, multiply(fv2, bv1),
                bde, multiply(bv1, dv2),
                bde, multiply(dv2, bv1),
        };
        for (int i = 0; i < expectedAndresult.length; i += 2) {
            final Object expected = expectedAndresult[i];
            final Object result = expectedAndresult[i + 1];
            sameBiOrBd(expected, result);
        }
    }

    public static void test_biginteger_divide() {
        final BigInteger bv1Left = BigInteger.valueOf(91);
        final BigInteger bv1Right = BigInteger.valueOf(2);
        final BigDecimal[] results = new BigDecimal[] {
                divide(bv1Left, bv1Right),
                divide(bv1Left, (byte) 2),
                divide((byte) 91, bv1Right),
                divide(bv1Left, (short) 2),
                divide((short) 91, bv1Right),
                divide(bv1Left, 2),
                divide(91, bv1Right),
                divide(bv1Left, 2L),
                divide(91L, bv1Right),
                divide(bv1Left, 2.0F),
                divide(91.0F, bv1Right),
                divide(bv1Left, 2.0),
                divide(91.0, bv1Right),
        };
        final BigDecimal expected = BigDecimal.valueOf(45.5)
                .setScale(
                        QueryLanguageFunctionUtils.DEFAULT_SCALE,
                        RoundingMode.HALF_UP);
        for (BigDecimal r : results) {
            TestCase.assertEquals(0, expected.compareTo(r));
        }
    }

    public static void test_biginteger_divide_floating_actual() {
        long vAt = 1;
        for (int i = 0; i < DEFAULT_SCALE; ++i) {
            vAt *= 10;
        }
        final long[] vs = new long[] {vAt, vAt * 10};
        for (final long v : vs) {
            final BigInteger bv1Left = BigInteger.valueOf(1);
            final BigInteger bv1Right = BigInteger.valueOf(v);
            final BigDecimal[] results = new BigDecimal[] {
                    divide(bv1Left, bv1Right),
                    divide(bv1Left, (double) v),
                    divide(1.0, bv1Right),
                    divide(1.0F, bv1Right),
            };
            final BigDecimal expected =
                    new BigDecimal(bv1Left).divide(BigDecimal.valueOf(v), DEFAULT_SCALE, ROUNDING_MODE);
            for (BigDecimal r : results) {
                TestCase.assertEquals(0, expected.compareTo(r));
            }
        }
    }

    public static void test_biginteger_arithmetic_op_primitives_null() {
        final BigInteger bv1 = BigInteger.valueOf(10);
        final byte yv1 = QueryConstants.NULL_BYTE;
        final short sv1 = QueryConstants.NULL_SHORT;
        final int iv1 = QueryConstants.NULL_INT;
        final long lv1 = QueryConstants.NULL_LONG;
        final float fv1 = QueryConstants.NULL_FLOAT;
        final double dv1 = QueryConstants.NULL_DOUBLE;
        final Object[] results = new Object[] {
                plus(bv1, yv1),
                plus(yv1, bv1),
                plus(bv1, sv1),
                plus(sv1, bv1),
                plus(bv1, iv1),
                plus(iv1, bv1),
                plus(bv1, fv1),
                plus(fv1, bv1),
                plus(bv1, lv1),
                plus(lv1, bv1),
                plus(bv1, dv1),
                plus(dv1, bv1),
                minus(bv1, yv1),
                minus(yv1, bv1),
                minus(bv1, sv1),
                minus(sv1, bv1),
                minus(bv1, iv1),
                minus(iv1, bv1),
                minus(bv1, fv1),
                minus(fv1, bv1),
                minus(bv1, lv1),
                minus(lv1, bv1),
                minus(bv1, dv1),
                minus(dv1, bv1),
                multiply(bv1, yv1),
                multiply(yv1, bv1),
                multiply(bv1, sv1),
                multiply(sv1, bv1),
                multiply(bv1, iv1),
                multiply(iv1, bv1),
                multiply(bv1, fv1),
                multiply(fv1, bv1),
                multiply(bv1, lv1),
                multiply(lv1, bv1),
                multiply(bv1, dv1),
                multiply(dv1, bv1),
                divide(bv1, yv1),
                divide(yv1, bv1),
                divide(bv1, sv1),
                divide(sv1, bv1),
                divide(bv1, iv1),
                divide(iv1, bv1),
                divide(bv1, fv1),
                divide(fv1, bv1),
                divide(bv1, lv1),
                divide(lv1, bv1),
                divide(bv1, dv1),
                divide(dv1, bv1),
        };
        for (Object r : results) {
            TestCase.assertNull(r);
        }
    }

    public static void test_biginteger_airthmetic_op_biginteger_null() {
        final BigInteger bvnull = null;
        final BigInteger bv1 = BigInteger.valueOf(100);
        final byte yv1 = (byte) 10;
        final short sv1 = (short) 10;
        final int iv1 = 10;
        final long lv1 = 10L;
        final float fv1 = 0.5F;
        final double dv1 = 0.5;
        final Object[] results = new Object[] {
                plus(bvnull, yv1),
                plus(yv1, bvnull),
                plus(bvnull, sv1),
                plus(sv1, bvnull),
                plus(bvnull, bv1),
                plus(bv1, bvnull),
                plus(bvnull, iv1),
                plus(iv1, bvnull),
                plus(bvnull, fv1),
                plus(fv1, bvnull),
                plus(bvnull, lv1),
                plus(lv1, bvnull),
                plus(bvnull, dv1),
                plus(dv1, bvnull),
                minus(bvnull, yv1),
                minus(yv1, bvnull),
                minus(bvnull, sv1),
                minus(sv1, bvnull),
                minus(bvnull, yv1),
                minus(yv1, bvnull),
                minus(bvnull, sv1),
                minus(sv1, bvnull),
                minus(bvnull, bv1),
                minus(bv1, bvnull),
                minus(bvnull, iv1),
                minus(iv1, bvnull),
                minus(bvnull, fv1),
                minus(fv1, bvnull),
                minus(bvnull, lv1),
                minus(lv1, bvnull),
                minus(bvnull, dv1),
                minus(dv1, bvnull),
                multiply(bvnull, yv1),
                multiply(yv1, bvnull),
                multiply(bvnull, sv1),
                multiply(sv1, bvnull),
                multiply(bvnull, bv1),
                multiply(bv1, bvnull),
                multiply(bvnull, iv1),
                multiply(iv1, bvnull),
                multiply(bvnull, fv1),
                multiply(fv1, bvnull),
                multiply(bvnull, lv1),
                multiply(lv1, bvnull),
                multiply(bvnull, dv1),
                multiply(dv1, bvnull),
                divide(bvnull, yv1),
                divide(yv1, bvnull),
                divide(bvnull, sv1),
                divide(sv1, bvnull),
                divide(bvnull, bv1),
                divide(bv1, bvnull),
                divide(bvnull, iv1),
                divide(iv1, bvnull),
                divide(bvnull, fv1),
                divide(fv1, bvnull),
                divide(bvnull, lv1),
                divide(lv1, bvnull),
                divide(bvnull, dv1),
                divide(dv1, bvnull),
        };
        for (Object r : results) {
            TestCase.assertNull(r);
        }
    }

    public static void test_biginteger_nans() {
        final BigInteger bv1 = BigInteger.valueOf(100);
        final BigInteger bvnull = null;
        final float vfnan = Float.NaN;
        final double vdnan = Double.NaN;

        TestCase.assertEquals(-1, compareTo(bv1, vfnan)); // rhs NaN
        TestCase.assertEquals(1, compareTo(vfnan, bv1)); // lhs NaN
        TestCase.assertEquals(-1, compareTo(bvnull, vfnan)); // lhs null and rhs NaN
        TestCase.assertEquals(1, compareTo(vfnan, bvnull)); // lhs NaN and rhs null

        TestCase.assertEquals(-1, compareTo(bv1, vdnan)); // rhs NaN
        TestCase.assertEquals(1, compareTo(vdnan, bv1)); // lhs NaN
        TestCase.assertEquals(-1, compareTo(bvnull, vdnan)); // lhs null and rhs NaN
        TestCase.assertEquals(1, compareTo(vdnan, bvnull)); // lhs NaN and rhs null

        TestCase.assertFalse(eq(bv1, vfnan));
        TestCase.assertFalse(eq(vfnan, bv1));
        TestCase.assertFalse(eq(bvnull, vfnan));
        TestCase.assertFalse(eq(vfnan, bvnull));
        TestCase.assertFalse(eq(bv1, vdnan));
        TestCase.assertFalse(eq(vdnan, bv1));
        TestCase.assertFalse(eq(bvnull, vdnan));
        TestCase.assertFalse(eq(vdnan, bvnull));
    }

    public static void test_biginteger_compare() {
        final BigInteger bv10 = BigInteger.valueOf(10);
        final BigInteger bv11 = BigInteger.valueOf(11);
        final BigInteger bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertEquals(-1, compareTo(bv10, bv11));
        TestCase.assertEquals(-1, compareTo(bv10, yv11));
        TestCase.assertEquals(-1, compareTo(bv10, sv11));
        TestCase.assertEquals(-1, compareTo(bv10, iv11));
        TestCase.assertEquals(-1, compareTo(bv10, lv11));
        TestCase.assertEquals(-1, compareTo(bv10, fv11));
        TestCase.assertEquals(-1, compareTo(bv10, dv11));
        TestCase.assertEquals(0, compareTo(bv10, bv10));
        TestCase.assertEquals(0, compareTo(bv10, yv10));
        TestCase.assertEquals(0, compareTo(bv10, sv10));
        TestCase.assertEquals(0, compareTo(bv10, iv10));
        TestCase.assertEquals(0, compareTo(bv10, lv10));
        TestCase.assertEquals(0, compareTo(bv10, fv10));
        TestCase.assertEquals(0, compareTo(bv10, dv10));
        TestCase.assertEquals(1, compareTo(bv11, bv10));
        TestCase.assertEquals(1, compareTo(bv11, yv10));
        TestCase.assertEquals(1, compareTo(bv11, sv10));
        TestCase.assertEquals(1, compareTo(bv11, iv10));
        TestCase.assertEquals(1, compareTo(bv11, lv10));
        TestCase.assertEquals(1, compareTo(bv11, fv10));
        TestCase.assertEquals(1, compareTo(bv11, dv10));

        TestCase.assertEquals(-1, compareTo(yv10, bv11));
        TestCase.assertEquals(-1, compareTo(sv10, bv11));
        TestCase.assertEquals(-1, compareTo(iv10, bv11));
        TestCase.assertEquals(-1, compareTo(lv10, bv11));
        TestCase.assertEquals(-1, compareTo(fv10, bv11));
        TestCase.assertEquals(-1, compareTo(dv10, bv11));
        TestCase.assertEquals(0, compareTo(yv10, bv10));
        TestCase.assertEquals(0, compareTo(sv10, bv10));
        TestCase.assertEquals(0, compareTo(iv10, bv10));
        TestCase.assertEquals(0, compareTo(lv10, bv10));
        TestCase.assertEquals(0, compareTo(fv10, bv10));
        TestCase.assertEquals(0, compareTo(dv10, bv10));
        TestCase.assertEquals(1, compareTo(yv11, bv10));
        TestCase.assertEquals(1, compareTo(sv11, bv10));
        TestCase.assertEquals(1, compareTo(iv11, bv10));
        TestCase.assertEquals(1, compareTo(lv11, bv10));
        TestCase.assertEquals(1, compareTo(fv11, bv10));
        TestCase.assertEquals(1, compareTo(dv11, bv10));

        TestCase.assertEquals(-1, compareTo(bvnull, bv10));
        TestCase.assertEquals(-1, compareTo(bvnull, yv10));
        TestCase.assertEquals(-1, compareTo(bvnull, sv10));
        TestCase.assertEquals(-1, compareTo(bvnull, iv10));
        TestCase.assertEquals(-1, compareTo(bvnull, lv10));
        TestCase.assertEquals(-1, compareTo(bvnull, fv10));
        TestCase.assertEquals(-1, compareTo(bvnull, dv10));
        TestCase.assertEquals(0, compareTo(bvnull, yvnull));
        TestCase.assertEquals(0, compareTo(bvnull, svnull));
        TestCase.assertEquals(0, compareTo(bvnull, ivnull));
        TestCase.assertEquals(0, compareTo(bvnull, lvnull));
        TestCase.assertEquals(0, compareTo(bvnull, fvnull));
        TestCase.assertEquals(0, compareTo(bvnull, dvnull));
        TestCase.assertEquals(1, compareTo(bv10, bvnull));
        TestCase.assertEquals(1, compareTo(bv10, yvnull));
        TestCase.assertEquals(1, compareTo(bv10, svnull));
        TestCase.assertEquals(1, compareTo(bv10, ivnull));
        TestCase.assertEquals(1, compareTo(bv10, lvnull));
        TestCase.assertEquals(1, compareTo(bv10, fvnull));
        TestCase.assertEquals(1, compareTo(bv10, dvnull));

        TestCase.assertEquals(-1, compareTo(yvnull, bv10));
        TestCase.assertEquals(-1, compareTo(svnull, bv10));
        TestCase.assertEquals(-1, compareTo(ivnull, bv10));
        TestCase.assertEquals(-1, compareTo(lvnull, bv10));
        TestCase.assertEquals(-1, compareTo(fvnull, bv10));
        TestCase.assertEquals(-1, compareTo(dvnull, bv10));
        TestCase.assertEquals(0, compareTo(yvnull, bvnull));
        TestCase.assertEquals(0, compareTo(svnull, bvnull));
        TestCase.assertEquals(0, compareTo(ivnull, bvnull));
        TestCase.assertEquals(0, compareTo(lvnull, bvnull));
        TestCase.assertEquals(0, compareTo(fvnull, bvnull));
        TestCase.assertEquals(0, compareTo(dvnull, bvnull));
        TestCase.assertEquals(1, compareTo(yv10, bvnull));
        TestCase.assertEquals(1, compareTo(sv10, bvnull));
        TestCase.assertEquals(1, compareTo(iv10, bvnull));
        TestCase.assertEquals(1, compareTo(lv10, bvnull));
        TestCase.assertEquals(1, compareTo(fv10, bvnull));
        TestCase.assertEquals(1, compareTo(dv10, bvnull));
    }

    public static void test_biginteger_eq() {
        final BigInteger bv10 = BigInteger.valueOf(10);
        final BigInteger bv11 = BigInteger.valueOf(11);
        final BigInteger bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertTrue(eq(bv10, bv10));
        TestCase.assertTrue(eq(bv10, yv10));
        TestCase.assertTrue(eq(bv10, sv10));
        TestCase.assertTrue(eq(bv10, iv10));
        TestCase.assertTrue(eq(bv10, lv10));
        TestCase.assertTrue(eq(bv10, fv10));
        TestCase.assertTrue(eq(bv10, dv10));
        TestCase.assertTrue(eq(yv10, bv10));
        TestCase.assertTrue(eq(sv10, bv10));
        TestCase.assertTrue(eq(iv10, bv10));
        TestCase.assertTrue(eq(lv10, bv10));
        TestCase.assertTrue(eq(fv10, bv10));
        TestCase.assertTrue(eq(dv10, bv10));

        TestCase.assertFalse(eq(bv10, bv11));
        TestCase.assertFalse(eq(bv10, yv11));
        TestCase.assertFalse(eq(bv10, sv11));
        TestCase.assertFalse(eq(bv10, iv11));
        TestCase.assertFalse(eq(bv10, lv11));
        TestCase.assertFalse(eq(bv10, fv11));
        TestCase.assertFalse(eq(bv10, dv11));
        TestCase.assertFalse(eq(yv10, bv11));
        TestCase.assertFalse(eq(sv10, bv11));
        TestCase.assertFalse(eq(iv10, bv11));
        TestCase.assertFalse(eq(lv10, bv11));
        TestCase.assertFalse(eq(fv10, bv11));
        TestCase.assertFalse(eq(dv10, bv11));

        TestCase.assertFalse(eq(bv10, bvnull));
        TestCase.assertFalse(eq(bv10, yvnull));
        TestCase.assertFalse(eq(bv10, svnull));
        TestCase.assertFalse(eq(bv10, ivnull));
        TestCase.assertFalse(eq(bv10, lvnull));
        TestCase.assertFalse(eq(bv10, fvnull));
        TestCase.assertFalse(eq(bv10, dvnull));
        TestCase.assertFalse(eq(yvnull, bv11));
        TestCase.assertFalse(eq(svnull, bv11));
        TestCase.assertFalse(eq(ivnull, bv11));
        TestCase.assertFalse(eq(lvnull, bv11));
        TestCase.assertFalse(eq(fvnull, bv11));
        TestCase.assertFalse(eq(dvnull, bv11));
        TestCase.assertFalse(eq(bvnull, yv11));
        TestCase.assertFalse(eq(bvnull, sv11));
        TestCase.assertFalse(eq(bvnull, iv11));
        TestCase.assertFalse(eq(bvnull, lv11));
        TestCase.assertFalse(eq(bvnull, fv11));
        TestCase.assertFalse(eq(bvnull, dv11));
        TestCase.assertFalse(eq(yv10, bvnull));
        TestCase.assertFalse(eq(sv10, bvnull));
        TestCase.assertFalse(eq(iv10, bvnull));
        TestCase.assertFalse(eq(lv10, bvnull));
        TestCase.assertFalse(eq(fv10, bvnull));
        TestCase.assertFalse(eq(dv10, bvnull));

        TestCase.assertTrue(eq(bvnull, bvnull));
        TestCase.assertTrue(eq(bvnull, yvnull));
        TestCase.assertTrue(eq(bvnull, svnull));
        TestCase.assertTrue(eq(bvnull, ivnull));
        TestCase.assertTrue(eq(bvnull, lvnull));
        TestCase.assertTrue(eq(bvnull, fvnull));
        TestCase.assertTrue(eq(bvnull, dvnull));
        TestCase.assertTrue(eq(yvnull, bvnull));
        TestCase.assertTrue(eq(svnull, bvnull));
        TestCase.assertTrue(eq(ivnull, bvnull));
        TestCase.assertTrue(eq(lvnull, bvnull));
        TestCase.assertTrue(eq(fvnull, bvnull));
        TestCase.assertTrue(eq(dvnull, bvnull));
    }

    public static void test_biginteger_comps() {
        final BigInteger bv10 = BigInteger.valueOf(10);
        final BigInteger bv11 = BigInteger.valueOf(11);
        final BigInteger bvnull = null;
        final byte yv10 = (byte) 10;
        final byte yv11 = (byte) 11;
        final byte yvnull = QueryConstants.NULL_BYTE;
        final short sv10 = (short) 10;
        final short sv11 = (short) 11;
        final short svnull = QueryConstants.NULL_SHORT;
        final int iv10 = 10;
        final int iv11 = 11;
        final int ivnull = QueryConstants.NULL_INT;
        final long lv10 = 10L;
        final long lv11 = 11L;
        final long lvnull = QueryConstants.NULL_LONG;
        final float fv10 = 10.0F;
        final float fv11 = 11.0F;
        final float fvnull = QueryConstants.NULL_FLOAT;
        final double dv10 = 10.0;
        final double dv11 = 11.0;
        final double dvnull = QueryConstants.NULL_DOUBLE;

        TestCase.assertTrue(less(bv10, bv11));
        TestCase.assertTrue(less(bv10, yv11));
        TestCase.assertTrue(less(bv10, sv11));
        TestCase.assertTrue(less(bv10, iv11));
        TestCase.assertTrue(less(bv10, lv11));
        TestCase.assertTrue(less(bv10, fv11));
        TestCase.assertTrue(less(bv10, dv11));
        TestCase.assertTrue(less(yv10, bv11));
        TestCase.assertTrue(less(sv10, bv11));
        TestCase.assertTrue(less(iv10, bv11));
        TestCase.assertTrue(less(lv10, bv11));
        TestCase.assertTrue(less(fv10, bv11));
        TestCase.assertTrue(less(dv10, bv11));
        TestCase.assertFalse(less(bv10, bv10));
        TestCase.assertFalse(less(bv10, yv10));
        TestCase.assertFalse(less(bv10, sv10));
        TestCase.assertFalse(less(bv10, iv10));
        TestCase.assertFalse(less(bv10, lv10));
        TestCase.assertFalse(less(bv10, fv10));
        TestCase.assertFalse(less(bv10, dv10));
        TestCase.assertFalse(less(yv10, bv10));
        TestCase.assertFalse(less(sv10, bv10));
        TestCase.assertFalse(less(iv10, bv10));
        TestCase.assertFalse(less(lv10, bv10));
        TestCase.assertFalse(less(fv10, bv10));
        TestCase.assertFalse(less(dv10, bv10));
        TestCase.assertFalse(less(bv11, bv10));
        TestCase.assertFalse(less(bv11, yv10));
        TestCase.assertFalse(less(bv11, sv10));
        TestCase.assertFalse(less(bv11, iv10));
        TestCase.assertFalse(less(bv11, lv10));
        TestCase.assertFalse(less(bv11, fv10));
        TestCase.assertFalse(less(bv11, dv10));
        TestCase.assertFalse(less(yv11, bv10));
        TestCase.assertFalse(less(sv11, bv10));
        TestCase.assertFalse(less(iv11, bv10));
        TestCase.assertFalse(less(lv11, bv10));
        TestCase.assertFalse(less(fv11, bv10));
        TestCase.assertFalse(less(dv11, bv10));

        TestCase.assertTrue(lessEquals(bv10, bv10));
        TestCase.assertTrue(lessEquals(bv10, yv10));
        TestCase.assertTrue(lessEquals(bv10, sv10));
        TestCase.assertTrue(lessEquals(bv10, iv10));
        TestCase.assertTrue(lessEquals(bv10, lv10));
        TestCase.assertTrue(lessEquals(bv10, fv10));
        TestCase.assertTrue(lessEquals(bv10, dv10));
        TestCase.assertTrue(lessEquals(yv10, bv10));
        TestCase.assertTrue(lessEquals(sv10, bv10));
        TestCase.assertTrue(lessEquals(iv10, bv10));
        TestCase.assertTrue(lessEquals(lv10, bv10));
        TestCase.assertTrue(lessEquals(fv10, bv10));
        TestCase.assertTrue(lessEquals(dv10, bv10));
        TestCase.assertTrue(lessEquals(bv10, bv11));
        TestCase.assertTrue(lessEquals(bv10, yv11));
        TestCase.assertTrue(lessEquals(bv10, sv11));
        TestCase.assertTrue(lessEquals(bv10, iv11));
        TestCase.assertTrue(lessEquals(bv10, lv11));
        TestCase.assertTrue(lessEquals(bv10, fv11));
        TestCase.assertTrue(lessEquals(bv10, dv11));
        TestCase.assertTrue(lessEquals(yv10, bv11));
        TestCase.assertTrue(lessEquals(sv10, bv11));
        TestCase.assertTrue(lessEquals(iv10, bv11));
        TestCase.assertTrue(lessEquals(lv10, bv11));
        TestCase.assertTrue(lessEquals(fv10, bv11));
        TestCase.assertTrue(lessEquals(dv10, bv11));
        TestCase.assertFalse(less(bv11, bv10));
        TestCase.assertFalse(less(bv11, yv10));
        TestCase.assertFalse(less(bv11, sv10));
        TestCase.assertFalse(less(bv11, iv10));
        TestCase.assertFalse(less(bv11, lv10));
        TestCase.assertFalse(less(bv11, fv10));
        TestCase.assertFalse(less(bv11, dv10));
        TestCase.assertFalse(less(yv11, bv10));
        TestCase.assertFalse(less(sv11, bv10));
        TestCase.assertFalse(less(iv11, bv10));
        TestCase.assertFalse(less(lv11, bv10));
        TestCase.assertFalse(less(fv11, bv10));
        TestCase.assertFalse(less(dv11, bv10));

        TestCase.assertFalse(greater(bv10, bv11));
        TestCase.assertFalse(greater(bv10, yv11));
        TestCase.assertFalse(greater(bv10, sv11));
        TestCase.assertFalse(greater(bv10, iv11));
        TestCase.assertFalse(greater(bv10, lv11));
        TestCase.assertFalse(greater(bv10, fv11));
        TestCase.assertFalse(greater(bv10, dv11));
        TestCase.assertFalse(greater(yv10, bv11));
        TestCase.assertFalse(greater(sv10, bv11));
        TestCase.assertFalse(greater(iv10, bv11));
        TestCase.assertFalse(greater(lv10, bv11));
        TestCase.assertFalse(greater(fv10, bv11));
        TestCase.assertFalse(greater(dv10, bv11));
        TestCase.assertFalse(greater(bv10, bv10));
        TestCase.assertFalse(greater(bv10, yv10));
        TestCase.assertFalse(greater(bv10, sv10));
        TestCase.assertFalse(greater(bv10, iv10));
        TestCase.assertFalse(greater(bv10, lv10));
        TestCase.assertFalse(greater(bv10, fv10));
        TestCase.assertFalse(greater(bv10, dv10));
        TestCase.assertFalse(greater(yv10, bv10));
        TestCase.assertFalse(greater(sv10, bv10));
        TestCase.assertFalse(greater(iv10, bv10));
        TestCase.assertFalse(greater(lv10, bv10));
        TestCase.assertFalse(greater(fv10, bv10));
        TestCase.assertFalse(greater(dv10, bv10));
        TestCase.assertTrue(greater(bv11, bv10));
        TestCase.assertTrue(greater(bv11, yv10));
        TestCase.assertTrue(greater(bv11, sv10));
        TestCase.assertTrue(greater(bv11, iv10));
        TestCase.assertTrue(greater(bv11, lv10));
        TestCase.assertTrue(greater(bv11, fv10));
        TestCase.assertTrue(greater(bv11, dv10));
        TestCase.assertTrue(greater(yv11, bv10));
        TestCase.assertTrue(greater(sv11, bv10));
        TestCase.assertTrue(greater(iv11, bv10));
        TestCase.assertTrue(greater(lv11, bv10));
        TestCase.assertTrue(greater(fv11, bv10));
        TestCase.assertTrue(greater(dv11, bv10));

        TestCase.assertFalse(greaterEquals(bv10, bv11));
        TestCase.assertFalse(greaterEquals(bv10, yv11));
        TestCase.assertFalse(greaterEquals(bv10, sv11));
        TestCase.assertFalse(greaterEquals(bv10, iv11));
        TestCase.assertFalse(greaterEquals(bv10, lv11));
        TestCase.assertFalse(greaterEquals(bv10, fv11));
        TestCase.assertFalse(greaterEquals(bv10, dv11));
        TestCase.assertFalse(greaterEquals(yv10, bv11));
        TestCase.assertFalse(greaterEquals(sv10, bv11));
        TestCase.assertFalse(greaterEquals(iv10, bv11));
        TestCase.assertFalse(greaterEquals(lv10, bv11));
        TestCase.assertFalse(greaterEquals(fv10, bv11));
        TestCase.assertFalse(greaterEquals(dv10, bv11));
        TestCase.assertTrue(greaterEquals(bv10, bv10));
        TestCase.assertTrue(greaterEquals(bv10, yv10));
        TestCase.assertTrue(greaterEquals(bv10, sv10));
        TestCase.assertTrue(greaterEquals(bv10, iv10));
        TestCase.assertTrue(greaterEquals(bv10, lv10));
        TestCase.assertTrue(greaterEquals(bv10, fv10));
        TestCase.assertTrue(greaterEquals(bv10, dv10));
        TestCase.assertTrue(greaterEquals(yv10, bv10));
        TestCase.assertTrue(greaterEquals(sv10, bv10));
        TestCase.assertTrue(greaterEquals(iv10, bv10));
        TestCase.assertTrue(greaterEquals(lv10, bv10));
        TestCase.assertTrue(greaterEquals(fv10, bv10));
        TestCase.assertTrue(greaterEquals(dv10, bv10));
        TestCase.assertTrue(greaterEquals(bv11, bv10));
        TestCase.assertTrue(greaterEquals(bv11, yv10));
        TestCase.assertTrue(greaterEquals(bv11, sv10));
        TestCase.assertTrue(greaterEquals(bv11, iv10));
        TestCase.assertTrue(greaterEquals(bv11, lv10));
        TestCase.assertTrue(greaterEquals(bv11, fv10));
        TestCase.assertTrue(greaterEquals(bv11, dv10));
        TestCase.assertTrue(greaterEquals(yv11, bv10));
        TestCase.assertTrue(greaterEquals(sv11, bv10));
        TestCase.assertTrue(greaterEquals(iv11, bv10));
        TestCase.assertTrue(greaterEquals(lv11, bv10));
        TestCase.assertTrue(greaterEquals(fv11, bv10));
        TestCase.assertTrue(greaterEquals(dv11, bv10));
    }
}
