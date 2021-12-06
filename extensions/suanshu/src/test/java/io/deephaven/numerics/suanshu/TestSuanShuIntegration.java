/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.numerics.suanshu;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.vector.*;
import io.deephaven.util.QueryConstants;
import com.numericalmethod.suanshu.matrix.doubles.Matrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.dense.DenseMatrix;
import com.numericalmethod.suanshu.matrix.doubles.matrixtype.mathoperation.SimpleMatrixMathOperation;
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
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(bytes);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8,
                (byte) 9, (byte) 10, (byte) 11, (byte) 12, (byte) 13, (byte) 14));
    }

    public void testConvertDhByteVectorToVector() throws Exception {
        final ByteVectorDirect dhByteVectorDirect =
                new ByteVectorDirect(new byte[] {1, 2, 3, 4, QueryConstants.NULL_BYTE, 6});
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhByteVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertShortArrayToVector() throws Exception {
        final short[] shorts = new short[] {1, 2, 3, 4, 5, 6};
        final double[] doubles = new double[] {1, 2, 3, 4, 5, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(shorts);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec((short) 1, (short) 2, (short) 3, (short) 4, (short) 5, (short) 6));
    }

    public void testConvertDhShortVectorToVector() throws Exception {
        final ShortVectorDirect dhShortVectorDirect =
                new ShortVectorDirect(new short[] {1, 2, 3, 4, QueryConstants.NULL_SHORT, 6});
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhShortVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertIntArrayToVector() throws Exception {
        final int[] ints = new int[] {-1, 2, -3, 4, 5, 6};
        final double[] doubles = new double[] {-1, 2, -3, 4, 5, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(ints);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(-1, 2, -3, 4, 5, 6));
    }

    public void testConvertDhIntVectorToVector() throws Exception {
        final IntVectorDirect dhIntVectorDirect = new IntVectorDirect(-1, 2, -3, 4, QueryConstants.NULL_INT, 6);
        final double[] doubles = new double[] {-1, 2, -3, 4, Double.NaN, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhIntVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertFloatArrayToVector() throws Exception {
        final float[] floats = new float[] {1.2f, -562, -23.069f, 4.56f, 5.89f, 6f};
        final double[] doubles = new double[] {1.2f, -562, -23.069f, 4.56f, 5.89f, 6f};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(floats);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(1.2f, -562, -23.069f, 4.56f, 5.89f, 6f));
    }

    public void testConvertDhFloatVectorToVector() throws Exception {
        final FloatVectorDirect dhFloatVectorDirect =
                new FloatVectorDirect(1.2f, -562, -23.069f, 4.56f, QueryConstants.NULL_FLOAT, 6f);
        final double[] doubles = new double[] {1.2f, -562, -23.069f, 4.56f, Double.NaN, 6f};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhFloatVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertLongArrayToVector() throws Exception {
        final long[] longs = new long[] {-126564L, 256746545L, 3545678945136L, 4544L, 5L, 6654845L};
        final double[] doubles = new double[] {-126564d, 256746545d, 3545678945136d, 4544d, 5d, 6654845L};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(longs);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(-126564L, 256746545L, 3545678945136L, 4544L, 5L, 6654845L));
    }

    public void testConvertDhLongVectorToVector() throws Exception {
        final LongVectorDirect dhLongVectorDirect =
                new LongVectorDirect(-126564L, 256746545L, 3545678945136L, 4544L, QueryConstants.NULL_LONG, 6654845L);
        final double[] doubles = new double[] {-126564L, 256746545L, 3545678945136L, 4544L, Double.NaN, 6654845L};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhLongVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertDoubleArrayToVector() throws Exception {
        final double[] doublesActual = new double[] {2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d};
        final double[] doubles = new double[] {2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(doublesActual);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
        testVecEquals(expected, ssVec(2.365d, 2125.5698d, -98231.2656897451d, 697, 3457836.7283648723d));
    }

    public void testConvertDhDoubleVectorToVector() throws Exception {
        final DoubleVectorDirect dhDoubleVectorDirect = new DoubleVectorDirect(2.365d, 2125.5698d, -98231.2656897451d,
                QueryConstants.NULL_DOUBLE, 3457836.7283648723d);
        final double[] doubles = new double[] {2.365d, 2125.5698d, -98231.2656897451d, Double.NaN, 3457836.7283648723d};
        final com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(dhDoubleVectorDirect);
        testAbstractVector(actual);
        final DenseVector expected = new DenseVector(doubles);
        testVecEquals(expected, actual);
    }

    public void testConvertDhNumberVectorToVector() throws Exception {
        ObjectVector vector =
                new ObjectVectorDirect<>(BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE, null,
                        BigDecimal.ONE);
        double[] doubles = new double[] {0, 1, 0, 1, Double.NaN, 1};
        com.numericalmethod.suanshu.vector.doubles.Vector actual = ssVec(vector);
        testAbstractVector(actual);
        DenseVector expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        vector = new ObjectVectorDirect<>(2.365d, 2125.5698d, -98231.2656897451d, QueryConstants.NULL_DOUBLE,
                3457836.7283648723d);
        doubles = new double[] {2.365d, 2125.5698d, -98231.2656897451d, Double.NaN, 3457836.7283648723d};
        actual = ssVec(vector);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        vector = new ObjectVectorDirect<>((byte) 1, (short) 2, 3, 4f, 5l, 6d);
        doubles = new double[] {1, 2, 3, 4, 5, 6};
        actual = ssVec(vector);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());

        vector = new ObjectVectorDirect<>(QueryConstants.NULL_BYTE, QueryConstants.NULL_SHORT, QueryConstants.NULL_INT,
                QueryConstants.NULL_FLOAT, QueryConstants.NULL_LONG, QueryConstants.NULL_DOUBLE);
        doubles = new double[] {Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
        actual = ssVec(vector);
        testAbstractVector(actual);
        expected = new DenseVector(doubles);
        assertTrue(expected.equals(actual));
        assertEquals(expected.toArray(), actual.toArray());
    }

    public void testConvertDhNumberVectorToVectorException() throws Exception {
        ObjectVector vector = new ObjectVectorDirect<>("A", "B");
        try {
            ssVec(vector);
            fail("Should throw " + RequirementFailure.class);
        } catch (final Exception cce) {
            assertTrue(cce instanceof RequirementFailure);
        }

        vector = new ObjectVectorDirect<>(1, "B");
        try {
            ssVec(vector);
            fail("Should throw " + RequirementFailure.class);
        } catch (final Exception cce) {
            assertTrue(cce instanceof RequirementFailure);
        }
    }

    public void testConvertNumberArrayToVector() throws Exception {
        final Integer[] integers = new Integer[] {1, 2, 3, 4, null, 6};
        final Double[] doubleArrays = new Double[] {1d, 2d, 3d, 4d, null, 6d};
        final double[] doubles = new double[] {1, 2, 3, 4, Double.NaN, 6};
        final com.numericalmethod.suanshu.vector.doubles.Vector actualInteger = ssVec(integers);
        testAbstractVector(actualInteger);
        final com.numericalmethod.suanshu.vector.doubles.Vector actualDoubles = ssVec(doubleArrays);
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

        final byte[][] bytes = new byte[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16}, {21, 22, 23, 24, 25, 26},
                {31, 32, 33, 34, 35, 36}};

        final Matrix actual = ssMat(bytes);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDhByteVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final ByteVector[] byteVectors = new ByteVector[] {new ByteVectorDirect(new byte[] {1, 2, 3, 4, 5, 6}),
                new ByteVectorDirect(new byte[] {11, 12, 13, 14, 15, QueryConstants.NULL_BYTE}),
                new ByteVectorDirect(new byte[] {21, 22, 23, 24, 25, 26}),
                new ByteVectorDirect(new byte[] {31, 32, 33, 34, 35, 36})};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(byteVectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertShort2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final short[][] shorts = new short[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16}, {21, 22, 23, 24, 25, 26},
                {31, 32, 33, 34, 35, 36}};
        final Matrix actual = ssMat(shorts);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDhShortVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final ShortVector[] shortVectors = new ShortVector[] {new ShortVectorDirect(new short[] {1, 2, 3, 4, 5, 6}),
                new ShortVectorDirect(new short[] {11, 12, 13, 14, 15, QueryConstants.NULL_SHORT}),
                new ShortVectorDirect(new short[] {21, 22, 23, 24, 25, 26}),
                new ShortVectorDirect(new short[] {31, 32, 33, 34, 35, 36})};
        final Matrix actual = ssMat(shortVectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertInt2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final int[][] ints = new int[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, 16}, {21, 22, 23, 24, 25, 26},
                {31, 32, 33, 34, 35, 36}};
        final Matrix actual = ssMat(ints);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDhIntVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final IntVector[] intVectors = new IntVector[] {new IntVectorDirect(1, 2, 3, 4, 5, 6),
                new IntVectorDirect(11, 12, 13, 14, 15, QueryConstants.NULL_INT),
                new IntVectorDirect(21, 22, 23, 24, 25, 26), new IntVectorDirect(31, 32, 33, 34, 35, 36)};

        final Matrix actual = ssMat(intVectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertFloat2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, 16f},
                {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final float[][] floats = new float[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, 16f},
                {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};
        final Matrix actual = ssMat(floats);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDhFloatVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1f, 2f, 3f, 4f, 5f, 6f}, {11f, 12f, 13f, 14f, 15f, Double.NaN},
                {21f, 22f, 23f, 24f, 25f, 26f}, {31f, 32f, 33f, 34f, 35f, 36f}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final FloatVector[] floatVectors = new FloatVector[] {new FloatVectorDirect(1f, 2f, 3f, 4f, 5f, 6f),
                new FloatVectorDirect(11f, 12f, 13f, 14f, 15f, QueryConstants.NULL_FLOAT),
                new FloatVectorDirect(21f, 22f, 23f, 24f, 25f, 26f),
                new FloatVectorDirect(31f, 32f, 33f, 34f, 35f, 36f)};
        final Matrix actual = ssMat(floatVectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertLong2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1L, 2L, 3L, 4L, 5L, 6L}, {11L, 12L, 13L, 14L, 15L, 16L},
                {21L, 22L, 23L, 24L, 25L, 26L}, {31L, 32L, 33L, 34L, 35L, 36L}};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final long[][] longs = new long[][] {{1L, 2L, 3L, 4L, 5L, 6L}, {11L, 12L, 13L, 14L, 15L, 16L},
                {21L, 22L, 23L, 24L, 25L, 26L}, {31L, 32L, 33L, 34L, 35L, 36L}};
        final Matrix actual = ssMat(longs);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertDhLongVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1l, 2l, 3l, 4l, 5l, 6l}, {11l, 12l, 13l, 14l, 15l, Double.NaN},
                {21l, 22l, 23l, 24l, 25l, 26l}, {31l, 32l, 33l, 34l, 35l, 36l}};

        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final LongVector[] longVectors = new LongVector[] {new LongVectorDirect(1l, 2l, 3l, 4l, 5l, 6l),
                new LongVectorDirect(11l, 12l, 13l, 14l, 15l, QueryConstants.NULL_LONG),
                new LongVectorDirect(21l, 22l, 23l, 24l, 25l, 26l),
                new LongVectorDirect(31l, 32l, 33l, 34l, 35l, 36l)};
        final Matrix actual = ssMat(longVectors);
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

    public void testConvertDhDoubleVectorToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final DoubleVector[] doubleVectors = new DoubleVector[] {new DoubleVectorDirect(1d, 2d, 3d, 4d, 5d, 6d),
                new DoubleVectorDirect(11d, 12d, 13d, 14d, 15d, QueryConstants.NULL_DOUBLE),
                new DoubleVectorDirect(21d, 22d, 23d, 24d, 25d, 26d),
                new DoubleVectorDirect(31d, 32d, 33d, 34d, 35d, 36d)};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(doubleVectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertVectorBasesToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6},
                {11, 12, 13, 14, 15, 16}, {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}, {0, 1, 0, 1, 0, 1}};

        final Vector[] vectors = new Vector[] {new ByteVectorDirect(new byte[] {1, 2, 3, 4, 5, 6}),
                new ShortVectorDirect(new short[] {1, 2, 3, 4, 5, 6}), new DoubleVectorDirect(1d, 2d, 3d, 4d, 5d, 6d),
                new IntVectorDirect(11, 12, 13, 14, 15, 16), new LongVectorDirect(21l, 22l, 23l, 24l, 25l, 26l),
                new FloatVectorDirect(31f, 32f, 33f, 34f, 35f, 36f), new ObjectVectorDirect<>(BigDecimal.ZERO,
                        BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE)};
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(vectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertVectorsToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, Double.NaN, 6}, {1, 2, 3, 4, Double.NaN, 6},
                {1, 2, 3, 4, Double.NaN, 6}, {11, 12, 13, 14, Double.NaN, 16}, {21, 22, 23, 24, Double.NaN, 26},
                {31, 32, 33, 34, Double.NaN, 36}, {0, 1, 0, 1, Double.NaN, 1}};

        final ObjectVector vectors =
                new ObjectVectorDirect(new ByteVectorDirect(new byte[] {1, 2, 3, 4, QueryConstants.NULL_BYTE, 6}),
                        new ShortVectorDirect(new short[] {1, 2, 3, 4, QueryConstants.NULL_SHORT, 6}),
                        new DoubleVectorDirect(1d, 2d, 3d, 4d, QueryConstants.NULL_DOUBLE, 6d),
                        new IntVectorDirect(11, 12, 13, 14, QueryConstants.NULL_INT, 16),
                        new LongVectorDirect(21l, 22l, 23l, 24l, QueryConstants.NULL_LONG, 26l),
                        new FloatVectorDirect(31f, 32f, 33f, 34f, QueryConstants.NULL_FLOAT, 36f),
                        new ObjectVectorDirect<>(BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.ONE, null,
                                BigDecimal.ONE));
        Matrix expected = new DenseMatrix(doubles);
        expected = new SimpleMatrixMathOperation().transpose(expected);

        final Matrix actual = ssMat(vectors);
        testAbstractMatrix(actual);
        testMatEquals(expected, actual);
    }

    public void testConvertVectorBasesToMatrixException() throws Exception {

        Vector[] vectors = new Vector[] {new ObjectVectorDirect<>(1, 2, "3", 4, 5, 6)};
        try {
            ssMat(vectors);
            fail("Error expected as ssMat() does not allow conversion for ObjectVectorDirect<Object>");
        } catch (final UnsupportedOperationException usoe) {
            assertTrue(usoe.getMessage().contains("must be numeric"));
        }

        vectors = new Vector[] {new ObjectVectorDirect<>("1", "2", "3")};
        try {
            ssMat(vectors);
            fail("Error expected as ssMat() does not allow conversion for ObjectVectorDirect<String>");
        } catch (final UnsupportedOperationException usoe) {
            assertTrue(usoe.getMessage().contains("must be numeric"));
        }
    }

    public void testConvertNumber2dArrayToMatrix() throws Exception {
        final double[][] doubles = new double[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, Double.NaN},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};

        final Integer[][] integers = new Integer[][] {{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14, 15, null},
                {21, 22, 23, 24, 25, 26}, {31, 32, 33, 34, 35, 36}};
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

    private void testVecEquals(final com.numericalmethod.suanshu.vector.doubles.Vector expected,
            final com.numericalmethod.suanshu.vector.doubles.Vector actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 1; i <= expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), 0);
        }
        assertEquals(expected.toArray(), actual.toArray());
        if (actual instanceof SuanShuIntegration.AbstractVector) {
            assertEquals(expected.toString(), ((SuanShuIntegration.AbstractVector) actual).show());
        }
    }

    private void testAbstractVector(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
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

    private void testMutableVector(final com.numericalmethod.suanshu.vector.doubles.Vector vector) {
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
            ssMat(new DoubleVectorDirect(1d, 2d, 3d, 4d, 5d, 6d));
        } catch (Exception e) {
            fail("Fail for \n ssMat(new DoubleVectorDirect(1d, 2d, 3d, 4d, 5d, 6d));");
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
