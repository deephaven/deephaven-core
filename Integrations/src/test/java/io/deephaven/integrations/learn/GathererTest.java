package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Arrays;

public class GathererTest {

    private static InMemoryTable table;

    @BeforeClass
    public static void setup() {
        table = new InMemoryTable(
                new String[] {"bool1", "bool2",
                        "byte1", "byte2",
                        "short1", "short2",
                        "int1", "int2",
                        "long1", "long2",
                        "float1", "float2",
                        "double1", "double2"},
                new Object[] {
                        new boolean[] {true, true, false, false},
                        new boolean[] {true, false, true, false},
                        new byte[] {(byte)1, (byte)2, (byte)3, (byte)4},
                        new byte[] {(byte)5, (byte)6, (byte)7, (byte)8},
                        new short[] {(short)-1, (short)-2, (short)-3, (short)-4},
                        new short[] {(short)-5, (short)-6, (short)-7, (short)-8},
                        new int[] {100, 200, -100, -200},
                        new int[] {-300, -400, 300, 400},
                        new long[] {1L, 100L, 10000L, 1000000L},
                        new long[] {9L, 999L, 99999L, 9999999L},
                        new float[] {3.14F, 2.73F, 1.5F, 0.63F},
                        new float[] {0.1F, 0.2F, 0.3F, 0.4F},
                        new double[] {3.14, 2.73, 1.5, 0.63},
                        new double[] {0.1, 0.2, 0.3, 0.4}
                }
        );
    }

    public static IndexSet makeIndexSet(final InMemoryTable t) {
        final IndexSet is = new IndexSet(t.intSize());

        for (long idx : t.getIndex()) {
            is.add(idx);
        }

        return is;
    }

    @Test
    public void gatherTestMethod() {

        boolean[] booleanArray = {true, true, false, false, true, false, true, false};
        byte[] byteArray = {(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};
        short[] shortArray = {(short)-1, (short)-2, (short)-3, (short)-4, (short)-5, (short)-6, (short)-7, (short)-8};
        int[] intArray = {100, 200, -100, -200, -300, -400, 300, 400};
        long[] longArray = {1L, 100L, 10000L, 1000000L, 9L, 999L, 99999L, 9999999L};
        float[] floatArray = {3.14F, 2.73F, 1.5F, 0.63F, 0.1F, 0.2F, 0.3F, 0.4F};
        double[] doubleArray = {3.14, 2.73, 1.5, 0.63, 0.1, 0.2, 0.3, 0.4};

        int nColsPerType = 2;

        String[] boolColNames = {"bool1", "bool2"};
        String[] byteColNames = {"byte1", "byte2"};
        String[] shortColNames = {"short1", "short2"};
        String[] intColNames = {"int1", "int2"};
        String[] longColNames = {"long1", "long2"};
        String[] floatColNames = {"float1", "float2"};
        String[] doubleColNames = {"double1", "double2"};

        ColumnSource<?>[] boolColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] byteColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] shortColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] intColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] longColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] floatColSet = new ColumnSource[nColsPerType];
        ColumnSource<?>[] doubleColSet = new ColumnSource[nColsPerType];

        for (int i = 0; i < nColsPerType; i++) {
            boolColSet[i] = table.getColumnSource(boolColNames[i]);
            byteColSet[i] = table.getColumnSource(byteColNames[i]);
            shortColSet[i] = table.getColumnSource(shortColNames[i]);
            intColSet[i] = table.getColumnSource(intColNames[i]);
            longColSet[i] = table.getColumnSource(longColNames[i]);
            floatColSet[i] = table.getColumnSource(floatColNames[i]);
            doubleColSet[i] = table.getColumnSource(doubleColNames[i]);
        }

        IndexSet idxSet = makeIndexSet(table);

        boolean[] booleanTensor = Gatherer.tensorBuffer2DBoolean(idxSet, boolColSet);
        byte[] byteTensor = Gatherer.tensorBuffer2DByte(idxSet, boolColSet);
        short[] shortTensor = Gatherer.tensorBuffer2DShort(idxSet, shortColSet);
        int[] intTensor = Gatherer.tensorBuffer2DInt(idxSet, intColSet);
        long[] longTensor = Gatherer.tensorBuffer2DLong(idxSet, longColSet);
        float[] floatTensor = Gatherer.tensorBuffer2DFloat(idxSet, floatColSet);
        double[] doubleTensor = Gatherer.tensorBuffer2DDouble(idxSet, doubleColSet);

        Assert.assertTrue(Arrays.equals(booleanTensor, booleanArray));
        Assert.assertArrayEquals(byteTensor, byteArray);
        Assert.assertArrayEquals(shortTensor, shortArray);
        Assert.assertArrayEquals(intTensor, intArray);
        Assert.assertArrayEquals(longTensor, longArray);
        Assert.assertTrue(Arrays.equals(floatTensor, floatArray));
        Assert.assertTrue(Arrays.equals(doubleTensor, doubleArray));

    }


}