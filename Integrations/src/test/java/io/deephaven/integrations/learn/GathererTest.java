package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Arrays;

public class GathererTest {

    private static InMemoryTable table;
    private static String[] columnNames = new String[] {
            "bool1", "bool2",
            "byte1", "byte2",
            "short1", "short2",
            "int1", "int2",
            "long1", "long2",
            "float1", "float2",
            "double1", "double2"
    };
    private static Object[] columnData = new Object[] {
            new boolean[] {true, true, false, false},
            new boolean[] {true, false, true, false},
            new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4},
            new byte[] {(byte) 5, (byte) 6, (byte) 7, (byte) 8},
            new short[] {(short) -1, (short) -2, (short) -3, (short) -4},
            new short[] {(short) -5, (short) -6, (short) -7, (short) -8},
            new int[] {100, 200, -100, -200},
            new int[] {-300, -400, 300, 400},
            new long[] {1L, 100L, 10000L, 1000000L},
            new long[] {9L, 999L, 99999L, 9999999L},
            new float[] {3.14F, 2.73F, 1.5F, 0.63F},
            new float[] {0.1F, 0.2F, 0.3F, 0.4F},
            new double[] {3.14, 2.73, 1.5, 0.63},
            new double[] {0.1, 0.2, 0.3, 0.4}
    };

    @BeforeClass
    public static void setup() {
        table = new InMemoryTable(columnNames, columnData);
    }

    public static IndexSet makeIndexSet(final InMemoryTable t) {
        final IndexSet is = new IndexSet(t.intSize());

        for (long idx : t.getIndex()) {
            is.add(idx);
        }

        return is;
    }

    @Test
    public void booleanTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] boolColNames = Arrays.copyOfRange(columnNames, 0, 2);
        ColumnSource<?>[] boolColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            boolColSet[i] = table.getColumnSource(boolColNames[i]);
        }

        boolean[] result = Gatherer.tensorBuffer2DBoolean(idxSet, boolColSet);

        boolean[] firstBoolCol = (boolean[]) columnData[0];
        boolean[] secondBoolCol = (boolean[]) columnData[1];

        boolean[] firstBoolResult = Arrays.copyOfRange(result, 0, 4);
        boolean[] secondBoolResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertTrue(Arrays.equals(firstBoolResult, firstBoolCol));
        Assert.assertTrue(Arrays.equals(secondBoolResult, secondBoolCol));

    }

    @Test
    public void byteTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] byteColNames = Arrays.copyOfRange(columnNames, 2, 4);
        ColumnSource<?>[] byteColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            byteColSet[i] = table.getColumnSource(byteColNames[i]);
        }

        byte[] result = Gatherer.tensorBuffer2DByte(idxSet, byteColSet);

        byte[] firstByteCol = (byte[]) columnData[2];
        byte[] secondByteCol = (byte[]) columnData[3];

        byte[] firstByteResult = Arrays.copyOfRange(result, 0, 4);
        byte[] secondByteResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertArrayEquals(firstByteResult, firstByteCol);
        Assert.assertArrayEquals(secondByteResult, secondByteCol);
    }

    @Test
    public void shortTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] shortColNames = Arrays.copyOfRange(columnNames, 4, 6);
        ColumnSource<?>[] shortColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            shortColSet[i] = table.getColumnSource(shortColNames[i]);
        }

        short[] result = Gatherer.tensorBuffer2DShort(idxSet, shortColSet);

        short[] firstShortCol = (short[]) columnData[4];
        short[] secondShortCol = (short[]) columnData[5];

        short[] firstShortResult = Arrays.copyOfRange(result, 0, 4);
        short[] secondShortResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertArrayEquals(firstShortResult, firstShortCol);
        Assert.assertArrayEquals(secondShortResult, secondShortCol);
    }

    @Test
    public void intTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] intColNames = Arrays.copyOfRange(columnNames, 6, 8);
        ColumnSource<?>[] intColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            intColSet[i] = table.getColumnSource(intColNames[i]);
        }

        int[] result = Gatherer.tensorBuffer2DInt(idxSet, intColSet);

        int[] firstIntCol = (int[]) columnData[6];
        int[] secondIntCol = (int[]) columnData[7];

        int[] firstIntResult = Arrays.copyOfRange(result, 0, 4);
        int[] secondIntResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertArrayEquals(firstIntResult, firstIntCol);
        Assert.assertArrayEquals(secondIntResult, secondIntCol);
    }

    @Test
    public void longTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] longColNames = Arrays.copyOfRange(columnNames, 8, 10);
        ColumnSource<?>[] longColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            longColSet[i] = table.getColumnSource(longColNames[i]);
        }

        long[] result = Gatherer.tensorBuffer2DLong(idxSet, longColSet);

        long[] firstLongCol = (long[]) columnData[8];
        long[] secondLongCol = (long[]) columnData[9];

        long[] firstLongResult = Arrays.copyOfRange(result, 0, 4);
        long[] secondLongResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertArrayEquals(firstLongResult, firstLongCol);
        Assert.assertArrayEquals(secondLongResult, secondLongCol);
    }

    @Test
    public void floatTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] floatColNames = Arrays.copyOfRange(columnNames, 10, 12);
        ColumnSource<?>[] floatColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            floatColSet[i] = table.getColumnSource(floatColNames[i]);
        }

        float[] result = Gatherer.tensorBuffer2DFloat(idxSet, floatColSet);

        float[] firstFloatCol = (float[]) columnData[10];
        float[] secondFloatCol = (float[]) columnData[11];
        float[] firstFloatResult = Arrays.copyOfRange(result, 0, 4);
        float[] secondFloatResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertTrue(Arrays.equals(firstFloatResult, firstFloatCol));
        Assert.assertTrue(Arrays.equals(secondFloatResult, secondFloatCol));
    }

    @Test
    public void doubleTestMethod() {
        IndexSet idxSet = makeIndexSet(table);

        String[] doubleColNames = Arrays.copyOfRange(columnNames, 12, 14);
        ColumnSource<?>[] doubleColSet = new ColumnSource[2];
        for (int i = 0; i < 2; i++) {
            doubleColSet[i] = table.getColumnSource(doubleColNames[i]);
        }

        double[] result = Gatherer.tensorBuffer2DDouble(idxSet, doubleColSet);

        double[] firstDoubleCol = (double[]) columnData[12];
        double[] secondDoubleCol = (double[]) columnData[13];

        double[] firstDoubleResult = Arrays.copyOfRange(result, 0, 4);
        double[] secondDoubleResult = Arrays.copyOfRange(result, 4, 8);

        Assert.assertTrue(Arrays.equals(firstDoubleResult, firstDoubleCol));
        Assert.assertTrue(Arrays.equals(secondDoubleResult, secondDoubleCol));
    }

}
