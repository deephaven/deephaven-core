package io.deephaven.db.v2.utils.rsp.container;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestContainerUtil {

    @Test
    public void testUtilUnsignedIntersection() {
        short[] data1 = {-19, -17, -15, -13, -11, -9, -7, -5, -3, -1};
        short[] data2 = {-18, -16, -14, -12, -10, -8, -1};
        Assert
            .assertTrue(ContainerUtil.unsignedIntersects(data1, data1.length, data2, data2.length));
        short[] data3 = {-19, -17, -15, -13, -11, -9, -7};
        short[] data4 = {-18, -16, -14, -12, -10, -8, -6, -4, -2, 0};
        Assert.assertFalse(
            ContainerUtil.unsignedIntersects(data3, data3.length, data4, data4.length));
        short[] data5 = {};
        short[] data6 = {};
        Assert.assertFalse(
            ContainerUtil.unsignedIntersects(data5, data5.length, data6, data6.length));
    }

    @Test
    public void testBranchyUnsignedBinarySearch() {
        short[] data1 = {-19, -17, -15, -13, -11, -9, -7, -5, -3};
        Assert.assertEquals(8,
            ContainerUtil.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[8]));
        Assert.assertEquals(0,
            ContainerUtil.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[0]));
        Assert.assertEquals(data1.length - 1, ContainerUtil.branchyUnsignedBinarySearch(data1,
            data1.length - 1, data1.length, data1[data1.length - 1]));
        Assert.assertEquals(-1, ContainerUtil.branchyUnsignedBinarySearch(data1, 0, 0, (short) 0));
        Assert.assertEquals(-10,
            ContainerUtil.branchyUnsignedBinarySearch(data1, 0, data1.length, (short) -1));
    }

    @Test
    public void testCompare() {
        Assert.assertTrue(ContainerUtil.compareUnsigned((short) 1, (short) 2) < 0);
        Assert.assertTrue(ContainerUtil.compareUnsigned((short) -32333, (short) 2) > 0);
        Assert.assertTrue(ContainerUtil.compareUnsigned((short) 2, (short) -32333) < 0);
        Assert.assertTrue(ContainerUtil.compareUnsigned((short) 0, (short) 0) == 0);

    }

    @Test
    public void testPartialRadixSortEmpty() {
        int[] data = new int[] {};
        int[] test = Arrays.copyOf(data, data.length);
        ContainerUtil.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortIsStableInSameKey() {
        int[] data = new int[] {25, 1, 0, 10};
        int[] test = Arrays.copyOf(data, data.length);
        ContainerUtil.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectly() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected = new int[] {0, 25, 10, key1 | 1, key1 | 10, key1, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        ContainerUtil.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectlyWithDuplicates() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10,
                key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected =
            new int[] {0, 25, 10, 0, 25, 10, key1 | 1, key1 | 10, key1, key1 | 1, key1 | 10, key1,
                    key2 | 25, key2 | 10, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        ContainerUtil.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }

    @Test
    public void testAdvanceUntil() {
        short[] data = {0, 3, 16, 18, 21, 29, 30, -342};
        Assert.assertEquals(1, ContainerUtil.advanceUntil(data, -1, data.length, (short) 3));
        Assert.assertEquals(5, ContainerUtil.advanceUntil(data, -1, data.length, (short) 28));
        Assert.assertEquals(5, ContainerUtil.advanceUntil(data, -1, data.length, (short) 29));
        Assert.assertEquals(7, ContainerUtil.advanceUntil(data, -1, data.length, (short) -342));
    }

    @Test
    public void testIterateUntil() {
        short[] data = {0, 3, 16, 18, 21, 29, 30, -342};
        Assert.assertEquals(1, ContainerUtil.iterateUntil(data, 0, data.length,
            ContainerUtil.toIntUnsigned((short) 3)));
        Assert.assertEquals(5, ContainerUtil.iterateUntil(data, 0, data.length,
            ContainerUtil.toIntUnsigned((short) 28)));
        Assert.assertEquals(5, ContainerUtil.iterateUntil(data, 0, data.length,
            ContainerUtil.toIntUnsigned((short) 29)));
        Assert.assertEquals(7, ContainerUtil.iterateUntil(data, 0, data.length,
            ContainerUtil.toIntUnsigned((short) -342)));
    }

    @Test
    public void testToUnsigned() {
        Assert.assertEquals(0, ContainerUtil.toIntUnsigned((short) 0));
        Assert.assertEquals(128, ContainerUtil.toIntUnsigned((short) 128));
        Assert.assertEquals(32767, ContainerUtil.toIntUnsigned(Short.MAX_VALUE));
        Assert.assertEquals(32768, ContainerUtil.toIntUnsigned(Short.MIN_VALUE));
        Assert.assertEquals(65535, ContainerUtil.toIntUnsigned((short) -1));
    }

    @Test
    public void testReverseToUnsigned() {
        Assert.assertEquals((short) 0, (short) 0);
        Assert.assertEquals((short) 128, (short) 128);
        Assert.assertEquals(Short.MAX_VALUE, (short) 32767);
        Assert.assertEquals(Short.MIN_VALUE, (short) 32768);
        Assert.assertEquals((short) -1, (short) 65535);
    }

}
