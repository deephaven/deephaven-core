package io.deephaven.integrations.numpy;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;

public class Java2NumpyCopyTest extends BaseArrayTestCase {

    final private String file = Configuration.getInstance().getDevRootPath() + "/Integrations/src/test/java/io/deephaven/integrations/numpy/dh.jpg";
    final private String q1 = Configuration.getInstance().getDevRootPath() + "/Integrations/src/test/java/io/deephaven/integrations/numpy/quadrant1.jpg";
    final private String q2 = Configuration.getInstance().getDevRootPath() + "/Integrations/src/test/java/io/deephaven/integrations/numpy/quadrant2.png";

    public void testTableType() {
        final Table tNum = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)1.0");
        final Table tImg = TableTools.emptyTable(10).update("D=io.deephaven.dbtypes.DbImage.newInstance(`" + file + "`)");
        final Table tFail = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)1.0", "D=io.deephaven.dbtypes.DbImage.newInstance(`" + file + "`)");

        assertEquals(Java2NumpyCopy.TableType.NUMBER, Java2NumpyCopy.tableType(tNum));
        assertEquals(Java2NumpyCopy.TableType.IMAGE, Java2NumpyCopy.tableType(tImg));

        try {
            Java2NumpyCopy.tableType(tFail);
            fail("Should have thrown an exception!");
        } catch (UnsupportedOperationException e) {
            //pass
        }
    }

    public void testCopySliceDouble() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final double[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final double[] rst = new double[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceFloat() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final float[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final float[] rst = new float[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceByte() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final byte[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final byte[] rst = new byte[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceShort() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final short[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final short[] rst = new short[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceInt() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final int[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final int[] rst = new int[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceLong() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final long[] target = {2, 1, 2, 3, 1, 2, 4, 1, 2};
        final long[] rst = new long[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testCopySliceBoolean() {
        final Table t = TableTools.emptyTable(10).update("A=i%2==0", "B=true", "C=false");
        final boolean[] target = {true, true, false, false, true, false, true, true, false};
        final boolean[] rst = new boolean[9];
        Java2NumpyCopy.copySlice(t, 2, rst, 3, 3);
        assertEquals(target, rst);
    }

    public void testRandRows() {
        final int nRow = 123;
        final long tSize = 56789;
        final long[] idx = Java2NumpyCopy.randRows(nRow, tSize);
        assertEquals(nRow, idx.length);
        for (long l : idx) {
            assertTrue(l >= 0 && l < tSize);
        }
    }

    public void testRandRowsDistribution() {
        final int nRow = 210;
        final int tSize = 100;
        final int n = 100000;
        final int[] counts = new int[tSize];

        for (int i = 0; i < n; i++) {
            final long[] idx = Java2NumpyCopy.randRows(nRow, tSize);

            for (long l : idx) {
                counts[(int) l]++;
            }
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        for (int count : counts) {
            min = count < min ? count : min;
            max = count > max ? count : max;
        }

        final double expected = (double) (nRow) / (double) (tSize) * n;

        System.out.println(expected + "," + min + "," + max);

        final double tol = 0.1;
        assertTrue((1 - tol) * expected < min && (1 + tol) * expected > max);
    }

    public void testCopyRandDouble() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final double[] target = {1, 1, 2, 3, 1, 2};
        final double[] rst = new double[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new double[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new double[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new double[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new double[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new double[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new double[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (double[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new double[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new double[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandFloat() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final float[] target = {1, 1, 2, 3, 1, 2};
        final float[] rst = new float[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new float[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new float[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new float[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new float[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new float[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new float[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (float[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new float[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new float[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandByte() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final byte[] target = {1, 1, 2, 3, 1, 2};
        final byte[] rst = new byte[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new byte[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new byte[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new byte[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new byte[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new byte[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new byte[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (byte[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new byte[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new byte[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandShort() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final short[] target = {1, 1, 2, 3, 1, 2};
        final short[] rst = new short[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new short[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new short[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new short[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new short[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new short[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new short[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (short[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new short[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new short[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandInt() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final int[] target = {1, 1, 2, 3, 1, 2};
        final int[] rst = new int[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new int[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new int[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new int[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new int[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new int[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new int[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (int[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new int[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new int[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandLong() {
        final Table t = TableTools.emptyTable(10).update("A=i", "B=1.0", "C=(float)2.0");
        final long[] target = {1, 1, 2, 3, 1, 2};
        final long[] rst = new long[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 3});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new long[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new long[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new long[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new long[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new long[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new long[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (long[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new long[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new long[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyRandBoolean() {
        final Table t = TableTools.emptyTable(10).update("A=i%2==0", "B=true", "C=false");
        final boolean[] target = {false, true, false, true, true, false};
        final boolean[] rst = new boolean[6];
        Java2NumpyCopy.copyRand(t, rst, 2, 3, new long[]{1, 2});
        assertEquals(target, rst);

        try {
            Java2NumpyCopy.copyRand(t, new boolean[3], 1, 3, new long[]{-1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new boolean[3], 1, 3, new long[]{10});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new boolean[3], -1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new boolean[3], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new boolean[4], 1, -3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t.view("A", "B"), new boolean[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, (boolean[]) null, 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(null, new boolean[3], 1, 3, new long[]{1});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }

        try {
            Java2NumpyCopy.copyRand(t, new boolean[3], 1, -3, new long[]{1, 2});
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            //pass
        }
    }

    public void testCopyImageSliceShort() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final short[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final short[] rstBW = new short[8];
        Java2NumpyCopy.copyImageSlice(t, 2, rstBW, 2, 2, 2, true, false);
        assertEquals(targetBW, rstBW);

        final short[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final short[] rstC = new short[24];
        Java2NumpyCopy.copyImageSlice(t, 2, rstC, 2, 2, 2, true, true);
        assertEquals(targetC, rstC);
    }

    public void testCopyImageSliceInt() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final int[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final int[] rstBW = new int[8];
        Java2NumpyCopy.copyImageSlice(t, 2, rstBW, 2, 2, 2, true, false);
        assertEquals(targetBW, rstBW);

        final int[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final int[] rstC = new int[24];
        Java2NumpyCopy.copyImageSlice(t, 2, rstC, 2, 2, 2, true, true);
        assertEquals(targetC, rstC);
    }

    public void testCopyImageSliceLong() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final long[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final long[] rstBW = new long[8];
        Java2NumpyCopy.copyImageSlice(t, 2, rstBW, 2, 2, 2, true, false);
        assertEquals(targetBW, rstBW);

        final long[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final long[] rstC = new long[24];
        Java2NumpyCopy.copyImageSlice(t, 2, rstC, 2, 2, 2, true, true);
        assertEquals(targetC, rstC);
    }

    public void testCopyImageSliceFloat() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final float[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final float[] rstBW = new float[8];
        Java2NumpyCopy.copyImageSlice(t, 2, rstBW, 2, 2, 2, true, false);
        assertEquals(targetBW, rstBW);

        final float[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final float[] rstC = new float[24];
        Java2NumpyCopy.copyImageSlice(t, 2, rstC, 2, 2, 2, true, true);
        assertEquals(targetC, rstC);
    }

    public void testCopyImageSliceDouble() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final double[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final double[] rstBW = new double[8];
        Java2NumpyCopy.copyImageSlice(t, 2, rstBW, 2, 2, 2, true, false);
        assertEquals(targetBW, rstBW);

        final double[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final double[] rstC = new double[24];
        Java2NumpyCopy.copyImageSlice(t, 2, rstC, 2, 2, 2, true, true);
        assertEquals(targetC, rstC);
    }

    public void testCopyImageRandShort() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final short[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final short[] rstBW = new short[8];
        Java2NumpyCopy.copyImageRand(t, rstBW, 2, 2, 2, true, false, new long[]{2, 3});
        assertEquals(targetBW, rstBW);

        final short[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final short[] rstC = new short[24];
        Java2NumpyCopy.copyImageRand(t, rstC, 2, 2, 2, true, true, new long[]{2, 3});
        assertEquals(targetC, rstC);
    }

    public void testCopyImageRandInt() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final int[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final int[] rstBW = new int[8];
        Java2NumpyCopy.copyImageRand(t, rstBW, 2, 2, 2, true, false, new long[]{2, 3});
        assertEquals(targetBW, rstBW);

        final int[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final int[] rstC = new int[24];
        Java2NumpyCopy.copyImageRand(t, rstC, 2, 2, 2, true, true, new long[]{2, 3});
        assertEquals(targetC, rstC);
    }

    public void testCopyImageRandLong() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final long[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final long[] rstBW = new long[8];
        Java2NumpyCopy.copyImageRand(t, rstBW, 2, 2, 2, true, false, new long[]{2, 3});
        assertEquals(targetBW, rstBW);

        final long[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final long[] rstC = new long[24];
        Java2NumpyCopy.copyImageRand(t, rstC, 2, 2, 2, true, true, new long[]{2, 3});
        assertEquals(targetC, rstC);
    }

    public void testCopyImageRandFloat() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final float[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final float[] rstBW = new float[8];
        Java2NumpyCopy.copyImageRand(t, rstBW, 2, 2, 2, true, false, new long[]{2, 3});
        assertEquals(targetBW, rstBW);

        final float[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final float[] rstC = new float[24];
        Java2NumpyCopy.copyImageRand(t, rstC, 2, 2, 2, true, true, new long[]{2, 3});
        assertEquals(targetC, rstC);
    }

    public void testCopyImageRandDouble() {
        final Table t = TableTools.emptyTable(10)
                .update("A=io.deephaven.dbtypes.DbImage.newInstance(i%2==0?`" + q1 + "`:`" + q2 + "`)")
                .update("A=i%2==0 ? A : A.resize(2,2)");

        final double[] targetBW = {85, 89, 150, 170, 206, 188, 171, 239};
        final double[] rstBW = new double[8];
        Java2NumpyCopy.copyImageRand(t, rstBW, 2, 2, 2, true, false, new long[]{2, 3});
        assertEquals(targetBW, rstBW);

        final double[] targetC = {0, 255, 1, 255, 5, 7, 3, 193, 255, 255, 255, 0, 185, 205, 229, 195, 214, 155, 217, 150, 148, 254, 254, 210};
        final double[] rstC = new double[24];
        Java2NumpyCopy.copyImageRand(t, rstC, 2, 2, 2, true, true, new long[]{2, 3});
        assertEquals(targetC, rstC);
    }
}
