package io.deephaven.integrations.learn.gather;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.InMemoryTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.function.BiFunction;
import java.util.function.Function;

public class NumPyTest {

    private static InMemoryTable table;
    private static final String[] boolColNames = {"bool1", "bool2"};
    private static final boolean[][] boolData = {
            new boolean[] {true, true, false, false},
            new boolean[] {true, false, true, false}
    };
    private static final String[] byteColNames = {"byte1", "byte2"};
    private static final byte[][] byteData = {
            new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4},
            new byte[] {(byte) 5, (byte) 6, (byte) 7, (byte) 8}
    };
    private static final String[] shortColNames = {"short1", "short2"};
    private static final short[][] shortData = {
            new short[] {(short) -1, (short) -2, (short) -3, (short) -4},
            new short[] {(short) -5, (short) -6, (short) -7, (short) -8}
    };
    private static final String[] intColNames = {"int1", "int2"};
    private static final int[][] intData = {
            new int[] {100, 200, -100, -200},
            new int[] {-300, -400, 300, 400},
    };
    private static final String[] longColNames = {"long1", "long2"};
    private static final long[][] longData = {
            new long[] {1L, 100L, 10000L, 1000000L},
            new long[] {9L, 999L, 99999L, 9999999L},
    };
    private static final String[] floatColNames = {"float1", "float2"};
    private static final float[][] floatData = {
            new float[] {3.14F, 2.73F, 1.5F, 0.63F},
            new float[] {0.1F, 0.2F, 0.3F, 0.4F},
    };
    private static final String[] doubleColNames = {"double1", "double2"};
    private static final double[][] doubleData = {
            new double[] {3.14, 2.73, 1.5, 0.63},
            new double[] {0.1, 0.2, 0.3, 0.4}
    };
    private static final String[] columnNames = new String[] {
            boolColNames[0], boolColNames[1],
            byteColNames[0], byteColNames[1],
            shortColNames[0], shortColNames[1],
            intColNames[0], intColNames[1],
            longColNames[0], longColNames[1],
            floatColNames[0], floatColNames[1],
            doubleColNames[0], doubleColNames[1]
    };
    private static final Object[] columnData = new Object[] {
            boolData[0], boolData[1],
            byteData[0], byteData[1],
            shortData[0], shortData[1],
            intData[0], intData[1],
            longData[0], longData[1],
            floatData[0], floatData[1],
            doubleData[0], doubleData[1]
    };

    @BeforeClass
    public static void setup() {
        table = new InMemoryTable(columnNames, columnData);
    }

    public static ColumnSource<?>[] getColSet(final String[] colNames) {
        ColumnSource<?>[] rst = new ColumnSource[2];

        for (int i = 0; i < 2; i++) {
            rst[i] = table.getColumnSource(colNames[i]);
        }

        return rst;
    }

    private static <T> void assertRowMajor(BiFunction<Integer, Integer, T> expected, Function<Integer, T> actual) {
        // Data should be stored in row-major order
        int idx = 0;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 2; i++) {
                Assert.assertEquals("i=" + i + " j=" + j, expected.apply(i, j), actual.apply(idx));
                idx++;
            }
        }
    }


    private static <T> void assertColumnMajor(BiFunction<Integer, Integer, T> expected, Function<Integer, T> actual) {
        // Data should be stored in column-major order
        int idx = 0;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 4; j++) {
                Assert.assertEquals("i=" + i + " j=" + j, expected.apply(i, j), actual.apply(idx));
                idx++;
            }
        }
    }

    @Test
    public void byteTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(byteColNames);
        byte[] resultColumnMajor = NumPy.tensorBuffer2DByte(rowSet, colSet, true);
        assertColumnMajor((i, j) -> byteData[i][j], i -> resultColumnMajor[i]);
        byte[] resultRowMajor = NumPy.tensorBuffer2DByte(rowSet, colSet, false);
        assertRowMajor((i, j) -> byteData[i][j], i -> resultRowMajor[i]);
    }

    @Test
    public void shortTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(shortColNames);
        short[] resultColumnMajor = NumPy.tensorBuffer2DShort(rowSet, colSet, true);
        assertColumnMajor((i, j) -> shortData[i][j], i -> resultColumnMajor[i]);
        short[] resultRowMajor = NumPy.tensorBuffer2DShort(rowSet, colSet, false);
        assertRowMajor((i, j) -> shortData[i][j], i -> resultRowMajor[i]);
    }

    @Test
    public void intTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(intColNames);
        int[] resultColumnMajor = NumPy.tensorBuffer2DInt(rowSet, colSet, true);
        assertColumnMajor((i, j) -> intData[i][j], i -> resultColumnMajor[i]);
        int[] resultRowMajor = NumPy.tensorBuffer2DInt(rowSet, colSet, false);
        assertRowMajor((i, j) -> intData[i][j], i -> resultRowMajor[i]);
    }

    @Test
    public void longTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(longColNames);
        long[] resultColumnMajor = NumPy.tensorBuffer2DLong(rowSet, colSet, true);
        assertColumnMajor((i, j) -> longData[i][j], i -> resultColumnMajor[i]);
        long[] resultRowMajor = NumPy.tensorBuffer2DLong(rowSet, colSet, false);
        assertRowMajor((i, j) -> longData[i][j], i -> resultRowMajor[i]);
    }

    @Test
    public void floatTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(floatColNames);
        float[] resultColumnMajor = NumPy.tensorBuffer2DFloat(rowSet, colSet, true);
        assertColumnMajor((i, j) -> floatData[i][j], i -> resultColumnMajor[i]);
        float[] resultRowMajor = NumPy.tensorBuffer2DFloat(rowSet, colSet, false);
        assertRowMajor((i, j) -> floatData[i][j], i -> resultRowMajor[i]);
    }

    @Test
    public void doubleTestMethod() {
        RowSequence rowSet = table.getRowSet();
        ColumnSource<?>[] colSet = getColSet(doubleColNames);
        double[] resultColumnMajor = NumPy.tensorBuffer2DDouble(rowSet, colSet, true);
        assertColumnMajor((i, j) -> doubleData[i][j], i -> resultColumnMajor[i]);
        double[] resultRowMajor = NumPy.tensorBuffer2DDouble(rowSet, colSet, false);
        assertRowMajor((i, j) -> doubleData[i][j], i -> resultRowMajor[i]);
    }

}
