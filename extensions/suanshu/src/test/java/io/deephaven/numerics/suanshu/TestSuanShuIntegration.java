/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.numerics.suanshu;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.util.QueryConstants;
import com.numericalmethod.suanshu.matrix.doubles.Matrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.dense.DenseMatrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.mathoperation.SimpleMatrixMathOperation;
import com.numericalmethod.suanshu.vector.doubles.Vector;
import com.numericalmethod.suanshu.vector.doubles.dense.DenseVector;

import java.math.BigDecimal;

import static io.deephaven.numerics.suanshu.SuanShuIntegration.ssMat;
import static io.deephaven.numerics.suanshu.SuanShuIntegration.ssVec;

/**
 * Testcases for {@link SuanShuIntegration}
 */
public class TestSuanShuIntegration extends BaseArrayTestCase {

    public void testConvertByteArrayToVector() throws Exception {
        final byte[] bytes = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        final double[] doubles = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
        final Vector actual = ssVec(bytes);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6,
            (byte) 7, (byte) 8, (byte) 9, (byte) 10, (byte) 11, (byte) 12, (byte) 13, (byte) 14));
    }

    public void testConvertDbByteArrayToVector() throws Exception {
        final DbByteArrayDirect dbByteArrayDirect =
            new DbByteArrayDirect(new byte[] {1, 2, 3, 4, QueryConstants.NULL_BYTE, 6});
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final Vector actual = ssVec(dbByteArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertShortArrayToVector() throws Exception {
        final short[] shorts = new short[] {1, 2, 3, 4, 5, 6};
        final double[] doubles = new double[] {1, 2, 3, 4, 5, 6};
        final Vector actual = ssVec(shorts);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected,
            ssVec((short) 1, (short) 2, (short) 3, (short) 4, (short) 5, (short) 6));
    }

    public void testConvertDbShortArrayToVector() throws Exception {
        final DbShortArrayDirect dbShortArrayDirect =
            new DbShortArrayDirect(new short[] {1, 2, 3, 4, QueryConstants.NULL_SHORT, 6});
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final Vector actual = ssVec(dbShortArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertIntArrayToVector() throws Exception {
        final int[] ints = new int[] {-1, 2, -3, 4, 5, 6};
        final double[] doubles = new double[] {-1, 2, -3, 4, 5, 6};
        final Vector actual = ssVec(ints);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(-1, 2, -3, 4, 5, 6));
    }

    public void testConvertDbIntArrayToVector() throws Exception {
        final DbIntArrayDirect dbIntArrayDirect =
            new DbIntArrayDirect(-1, 2, -3, 4, QueryConstants.NULL_INT, 6);
        final double[] doubles = new double[] {-1, 2, -3, 4, Double.NaN, 6};
        final Vector actual = ssVec(dbIntArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertFloatArrayToVector() throws Exception {
        final float[] floats = new float[] {1.2f, -562, -23.069f, 4.56f, 5.89f, 6f};
        final double[] doubles = new double[] {1.2f, -562, -23.069f, 4.56f, 5.89f, 6f};
        final Vector actual = ssVec(floats);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(1.2f, -562, -23.069f, 4.56f, 5.89f, 6f));
    }

    public void testConvertDbFloatArrayToVector() throws Exception {
        final DbFloatArrayDirect dbFloatArrayDirect =
            new DbFloatArrayDirect(1.2f, -562, -23.069f, 4.56f, QueryConstants.NULL_FLOAT, 6f);
        final double[] doubles = new double[] {1.2f, -562, -23.069f, 4.56f, Double.NaN, 6f};
        final Vector actual = ssVec(dbFloatArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertLongArrayToVector() throws Exception {
        final long[] longs = new long[] {-126564L, 256746545L, 3545678945136L, 4544L, 5L, 6654845L};
        final double[] doubles =
            new double[] {-126564d, 256746545d, 3545678945136d, 4544d, 5d, 6654845L};
        final Vector actual = ssVec(longs);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(-126564L, 256746545L, 3545678945136L, 4544L, 5L, 6654845L));
    }

    public void testConvertDbLongArrayToVector() throws Exception {
        final DbLongArrayDirect dbLongArrayDirect = new DbLongArrayDirect(-126564L, 256746545L,
            3545678945136L, 4544L, QueryConstants.NULL_LONG, 6654845L);
        final double[] doubles =
            new double[] {-126564L, 256746545L, 3545678945136L, 4544L, Double.NaN, 6654845L};
        final Vector actual = ssVec(dbLongArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertDoubleArrayToVector() throws Exception {
        final double[] doublesActual =
            new double[] {2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d};
        final double[] doubles =
            new double[] {2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d};
        final Vector actual = ssVec(doublesActual);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected,
            ssVec(2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d));
    }

    public void testConvertDbDoubleArrayToVector() throws Exception {
        final DbDoubleArrayDirect dbDoubleArrayDirect = new DbDoubleArrayDirect(2.365d, 2125.5698d,
            -98231.2656897451d, QueryConstants.NULL_DOUBLE, 3457836.7283648723d);
        final double[] doubles =
            new double[] {2.365d, 2125.5698d, -98231.2656897451d, Double.NaN, 3457836.7283648723d};
        final Vector actual = ssVec(dbDoubleArrayDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertDbNumberArrayToVector() throws Exception {
        DbArray dbArray = new DbArrayDirect<>(BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO,
            BigDecimal.ONE, null, BigDecimal.ONE);
        double[] doubles = new double[] {0, 1, 0, 1, Double.NaN, 1};
        Vector actual = ssVec(dbArray);
        testAbstractVector(actual);
        DenseVector expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        dbArray = new DbArrayDirect<>(2.365d, 2125.5698d, -98231.2656897451d,
            QueryConstants.NULL_DOUBLE, 3457836.7283648723d);
        doubles =
            new double[] {2.365d, 2125.5698d, -98231.2656897451d, Double.NaN, 3457836.7283648723d};
        actual = ssVec(dbArray);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        dbArray = new DbArrayDirect<>((byte) 1, (short) 2, 3, 4f, 5l, 6d);
        doubles = new double[] {1, 2, 3, 4, 5, 6};
        actual = ssVec(dbArray);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        dbArray = new DbArrayDirect<>(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT,
            QueryConstants.NULL_INT, QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG,
            QueryConstants.NULL_DOUBLE);
        doubles =
            new double[] {Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
        actual = ssVec(dbArray);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());
    }

    public void testConvertDbNumberArrayToVectorException() throws Exception {
        DbArray dbArray = new DbArrayDirect<>("A", "B");
        try {
            ssVec(dbArray);
            fail("Should throw " + RequirementFailure.class);
        } catch (final Exception cce) {
            assertTrue(cce instanceof RequirementFailure);
        }

        dbArray = new DbArrayDirect<>(1, "B");
        try {
            ssVec(dbArray);
            fail("Should throw " + RequirementFailure.class);
        } catch (final Exception cce) {
            assertTrue(cce instanceof RequirementFailure);
        }
    }

    public void testConvertNumberArrayToVector() throws Exception {
        final Integer[] integers = new Integer[] {1, 2, 3, 4, null, 6};
        final Double[] doubleArrays = new Double[] {1d, 2d, 3d, 4d, null, 6d};
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final Vector actualInteger = ssVec(integers);
        testAbstractVector(actualInteger);
        final Vector actualDoubles = ssVec(doubleArrays);
        testAbstractVector(actualDoubles);
        final DenseVector expected = new DenseVector(doubles);
        assertTrue(expected.equals(actualInteger));
        assertTrue(expected.equals(actualDoubles));
        assertTrue(expected.equals(ssVec(1, 2, 3, 4, null, 6)));
    }

    public void testConvertByte2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final byte[][] bytes = new byte[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final Matrix actual = ssMat(bytes);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbByteArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                    {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final DbByteArray[] dbByteArrays =
            new DbByteArray[] {new DbByteArrayDirect(new byte[] {1, 2, 3, 4, 5, 6}),
                    new DbByteArrayDirect(
                        new byte[] {11, 12, 13, 14, 15, QueryConstants.NULL_BYTE}),
                    new DbByteArrayDirect(new byte[] {21, 22, 23, 24, 25, 26}),
                    new DbByteArrayDirect(new byte[] {31, 32, 33, 34, 35, 36})};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(dbByteArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertShort2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final short[][] shorts = new short[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        final Matrix actual = ssMat(shorts);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbShortArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                    {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final DbShortArray[] dbShortArrays =
            new DbShortArray[] {new DbShortArrayDirect(new short[] {1, 2, 3, 4, 5, 6}),
                    new DbShortArrayDirect(
                        new short[] {11, 12, 13, 14, 15, QueryConstants.NULL_SHORT}),
                    new DbShortArrayDirect(new short[] {21, 22, 23, 24, 25, 26}),
                    new DbShortArrayDirect(new short[] {31, 32, 33, 34, 35, 36})};
        final Matrix actual = ssMat(dbShortArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertInt2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final int[][] ints = new int[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        final Matrix actual = ssMat(ints);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbIntArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                    {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final DbIntArray[] dbIntArrays = new DbIntArray[] {new DbIntArrayDirect(1, 2, 3, 4, 5, 6),
                new DbIntArrayDirect(11, 12, 13, 14, 15, QueryConstants.NULL_INT),
                new DbIntArrayDirect(21, 22, 23, 24, 25, 26),
                new DbIntArrayDirect(31, 32, 33, 34, 35, 36)};

        final Matrix actual = ssMat(dbIntArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertFloat2dArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, 16f},
                    {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final float[][] floats =
            new float[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, 16f},
                    {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};
        final Matrix actual = ssMat(floats);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbFloatArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, Double.NaN},
                    {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final DbFloatArray[] dbFloatArrays =
            new DbFloatArray[] {new DbFloatArrayDirect(1f, 2f, 3f, 4f, 5f, 6f),
                    new DbFloatArrayDirect(11f, 12f, 13f, 14f, 15f, QueryConstants.NULL_FLOAT),
                    new DbFloatArrayDirect(21f, 22f, 23f, 24f, 25f, 26f),
                    new DbFloatArrayDirect(31f, 32f, 33f, 34f, 35f, 36f)};
        final Matrix actual = ssMat(dbFloatArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertLong2dArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1L, 2L, 3L, 4L, 5L, 6L}, {11L, 12L, 13L, 14L, 15L, 16L},
                    {21L, 22L, 23L, 24L, 25L, 26L}, {31L, 32L, 33L, 34L, 35L, 36L}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final long[][] longs =
            new long[][] {{1L, 2L, 3L, 4L, 5L, 6L}, {11L, 12L, 13L, 14L, 15L, 16L},
                    {21L, 22L, 23L, 24L, 25L, 26L}, {31L, 32L, 33L, 34L, 35L, 36L}};
        final Matrix actual = ssMat(longs);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbLongArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1l, 2l, 3l, 4l, 5l, 6l}, {11l, 12l, 13l, 14l, 15l, Double.NaN},
                    {21l, 22l, 23l, 24l, 25l, 26l}, {31l, 32l, 33l, 34l, 35l, 36l}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final DbLongArray[] dbLongArrays =
            new DbLongArray[] {new DbLongArrayDirect(1l, 2l, 3l, 4l, 5l, 6l),
                    new DbLongArrayDirect(11l, 12l, 13l, 14l, 15l, QueryConstants.NULL_LONG),
                    new DbLongArrayDirect(21l, 22l, 23l, 24l, 25l, 26l),
                    new DbLongArrayDirect(31l, 32l, 33l, 34l, 35l, 36l)};
        final Matrix actual = ssMat(dbLongArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDouble2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(doubles);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbDoubleArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                    {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final DbDoubleArray[] dbDoubleArrays =
            new DbDoubleArray[] {new DbDoubleArrayDirect(1d, 2d, 3d, 4d, 5d, 6d),
                    new DbDoubleArrayDirect(11d, 12d, 13d, 14d, 15d, QueryConstants.NULL_DOUBLE),
                    new DbDoubleArrayDirect(21d, 22d, 23d, 24d, 25d, 26d),
                    new DbDoubleArrayDirect(31d, 32d, 33d, 34d, 35d, 36d)};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(dbDoubleArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbArrayBasesToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6},
                {1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16}, {21, 22, 23, 24, 25, 26},
                {31, 32, 33, 34, 35, 36}, {0, 1, 0, 1, 0, 1}};

        final DbArrayBase[] dbArrayBases =
            new DbArrayBase[] {new DbByteArrayDirect(new byte[] {1, 2, 3, 4, 5, 6}),
                    new DbShortArrayDirect(new short[] {1, 2, 3, 4, 5, 6}),
                    new DbDoubleArrayDirect(1d, 2d, 3d, 4d, 5d, 6d),
                    new DbIntArrayDirect(11, 12, 13, 14, 15, 16),
                    new DbLongArrayDirect(21l, 22l, 23l, 24l, 25l, 26l),
                    new DbFloatArrayDirect(31f, 32f, 33f, 34f, 35f, 36f),
                    new DbArrayDirect<>(BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO,
                        BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE)};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(dbArrayBases);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbArraysToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, Double.NaN, 6},
                {1, 2, 3, 4, Double.NaN, 6}, {1, 2, 3, 4, Double.NaN, 6},
                {11, 12, 13, 14, Double.NaN, 16}, {21, 22, 23, 24, Double.NaN, 26},
                {31, 32, 33, 34, Double.NaN, 36}, {0, 1, 0, 1, Double.NaN, 1}};

        final DbArray dbArrays = new DbArrayDirect(
            new DbByteArrayDirect(new byte[] {1, 2, 3, 4, QueryConstants.NULL_BYTE, 6}),
            new DbShortArrayDirect(new short[] {1, 2, 3, 4, QueryConstants.NULL_SHORT, 6}),
            new DbDoubleArrayDirect(1d, 2d, 3d, 4d, QueryConstants.NULL_DOUBLE, 6d),
            new DbIntArrayDirect(11, 12, 13, 14, QueryConstants.NULL_INT, 16),
            new DbLongArrayDirect(21l, 22l, 23l, 24l, QueryConstants.NULL_LONG, 26l),
            new DbFloatArrayDirect(31f, 32f, 33f, 34f, QueryConstants.NULL_FLOAT, 36f),
            new DbArrayDirect<>(BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE,
                null, BigDecimal.ONE));
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(dbArrays);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDbArrayBasesToMatrixException() throws Exception {

        DbArrayBase[] dbArrayBases = new DbArrayBase[] {new DbArrayDirect<>(1, 2, "3", 4, 5, 6)};
        try {
            ssMat(dbArrayBases);
            fail("Error expected as ssMat() does not allow conversion for DbArrayDirect<Object>");
        } catch (final UnsupportedOperationException usoe) {
            assertTrue(usoe.getMessage().contains("must be numeric"));
        }

        dbArrayBases = new DbArrayBase[] {new DbArrayDirect<>("1", "2", "3")};
        try {
            ssMat(dbArrayBases);
            fail("Error expected as ssMat() does not allow conversion for DbArrayDirect<String>");
        } catch (final UnsupportedOperationException usoe) {
            assertTrue(usoe.getMessage().contains("must be numeric"));
        }
    }

    public void testConvertNumber2dArrayToMatrix() throws Exception {
        final double[][] doubles =
            new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                    {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final Integer[][] integers = new Integer[][] {{1, 2, 3, 4, 5, 6},
                {11, 12, 13, 14, 15, null}, {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(integers);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
        testMatEquals(expected, ssMat(ssVec(1, 2, 3, 4, 5, 6).toArray(),
            ssVec(11, 12, 13, 14, 15, null).toArray(),
            ssVec(21, 22, 23, 24, 25, 26).toArray(),
            ssVec(31, 32, 33, 34, 35, 36).toArray()));
    }

    private void testMatEquals(final Matrix expected, final Matrix actual) {
        assertEquals(expected.nCols(), actual.nCols());
        assertEquals(expected.nRows(), actual.nRows());
        for (int i = 1; i <= expected.nRows(); i++) {
            testVecEquals(expected.getRow(i), actual.getRow(i));
            for (int j = 1; j <= expected.nCols(); j++) {
                testVecEquals(expected.getColumn(j), actual.getColumn(j));
                assertEquals(expected.get(i, j), actual.get(i, j));
            }
        }
        if (actual instanceof SuanShuIntegration.AbstractMatrix) {
            assertEquals(expected.toString(), ((SuanShuIntegration.AbstractMatrix) actual).show());
        }
    }

    private void testVecEquals(final Vector expected, final Vector actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 1; i <= expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), 0);
        }
        assertEquals(expected.toArray(), actual.toArray());
        if (actual instanceof SuanShuIntegration.AbstractVector) {
            assertEquals(expected.toString(), ((SuanShuIntegration.AbstractVector) actual).show());
        }
    }

    private void testAbstractVector(final Vector vector) {
        if (vector.size() > 0) {
            try {
                vector.set(1, 5d);
                fail("Expecting immutable vector, but it allowed to set data.");
            } catch (final UnsupportedOperationException usoe) {
                assertTrue(usoe.getMessage().contains("Setting"));
            }
            assertEquals(new DenseVector(new double[vector.size()]), vector.ZERO());
            testMutableVector(vector.deepCopy());
        }
    }

    private void testMutableVector(final Vector vector) {
        final double data = 5d;
        final int index = 1;
        if (vector.size() > 0) {
            try {
                vector.set(index, data);
                assertEquals(data, vector.get(index));
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        }
    }

    public void testAbstractMatrix(final Matrix matrix) {
        if (matrix.nCols() > 0 && matrix.nRows() > 0) {
            try {
                matrix.set(1, 1, 5d);
                fail("Expecting immutable matrix, but it allowed to set data.");
            } catch (final UnsupportedOperationException usoe) {
                assertTrue(usoe.getMessage().contains("Setting"));
            }
        }
        // testZero
        testMatEquals(new DenseMatrix(matrix.nRows(), matrix.nCols()).ZERO(), matrix.ZERO());
        // testOne
        testMatEquals(new DenseMatrix(matrix.nRows(), matrix.nCols()).ONE(), matrix.ONE());
        testMutableMatrix(matrix.deepCopy());
        for (int i = 1; i <= matrix.nRows(); i++) {
            testAbstractVector(matrix.getRow(i));
        }
        for (int i = 1; i <= matrix.nCols(); i++) {
            testAbstractVector(matrix.getColumn(i));
        }
    }

    public void testMutableMatrix(final Matrix matrix) {
        // testset
        final double data = 5d;
        final int rowIndex = 1;
        final int colIndex = 1;
        if (matrix.nCols() > 0 && matrix.nRows() > 0) {
            try {
                matrix.set(rowIndex, colIndex, data);
                assertEquals(data, matrix.get(rowIndex, colIndex));
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        }
    }

    public void testssMatAmbiguity() {
        try {
            ssMat(new DbDoubleArrayDirect(1d, 2d, 3d, 4d, 5d, 6d));
        } catch (Exception e) {
            fail("Fail for \n ssMat(new DbDoubleArrayDirect(1d, 2d, 3d, 4d, 5d, 6d));");
        }

        try {
            ssMat(new int[] {1, 2, 3, 4, 5, 6});
        } catch (Exception e) {
            fail("Fail for \n ssMat(new int[]{1, 2, 3, 4, 5, 6})");
        }

        try {
            ssMat(new Integer[] {1, 2, 3, 4, 5, 6});
        } catch (Exception e) {
            fail("Fail for \n ssMat(new Integer[]{1, 2, 3, 4, 5, 6})");
        }
    }
}
