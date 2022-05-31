/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.util.hashing.BooleanChunkEquals;
import io.deephaven.chunk.attributes.Values;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class TestBooleanChunk {
    /**
     * This is less a test and more a comprehensive set of API calls, to make sure all the desired syntax is supported
     */
    @Test
    public void staticEntryPoints() {
        // array
        boolean[] ca = BooleanChunk.makeArray(12);
        BooleanChunk[] cca = BooleanChunkChunk.makeArray(12);

        BooleanChunk<Values> c;
        BooleanChunkChunk<Values> cc;

        // empty chunk
        c = BooleanChunk.getEmptyChunk();
        cc = BooleanChunkChunk.getEmptyChunk();

        // chunk wrap 1
        c = BooleanChunk.chunkWrap(BooleanChunk.makeArray(12), 0, 12);
        cc = BooleanChunkChunk.chunkWrap(BooleanChunkChunk.makeArray(12), 0, 12);

        // chunk wrap 2
        c = BooleanChunk.chunkWrap(BooleanChunk.makeArray(12));
        cc = BooleanChunkChunk.chunkWrap(BooleanChunkChunk.makeArray(12));

        WritableBooleanChunk<Values> wc;
        WritableBooleanChunkChunk<Values> wcc;

        // make writable chunk
        wc = WritableBooleanChunk.makeWritableChunk(12);
        wcc = WritableBooleanChunkChunk.makeWritableChunk(12);

        // writable chunk wrap 1
        wc = WritableBooleanChunk.writableChunkWrap(WritableBooleanChunk.makeArray(12), 0, 12);
        wcc = WritableBooleanChunkChunk.writableChunkWrap(WritableBooleanChunkChunk.makeArray(12), 0, 12);

        // writable chunk wrap 2
        wc = WritableBooleanChunk.writableChunkWrap(WritableBooleanChunk.makeArray(12));
        wcc = WritableBooleanChunkChunk.writableChunkWrap(WritableBooleanChunkChunk.makeArray(12));

        // slice a chunk
        BooleanChunk<Values> sc = c.slice(4, 6);
        BooleanChunkChunk<Values> scc = cc.slice(4, 6);

        // slice a writable chunk
        WritableBooleanChunk<Values> swc = wc.slice(4, 6);
        WritableBooleanChunkChunk<Values> swcc = wcc.slice(4, 6);

        // make a resettable chunk and reset it
        ResettableBooleanChunk<Values> rc = ResettableBooleanChunk.makeResettableChunk();
        rc.resetFromArray(ca, 0, 12);
        ResettableWritableBooleanChunk<Values> rwc = ResettableWritableBooleanChunk.makeResettableChunk();
        rwc.resetFromArray(ca, 0, 12);

        rc.resetFromTypedArray(ca, 3, 5);
        rwc.resetFromTypedArray(ca, 3, 5);

        // A readable chunk can be reset from a readable or writable chunk
        rc.resetFromChunk(c, 3, 5);
        rc.resetFromChunk(wc, 1, 1);

        // A writable chunk can only be reset from another writable chunk
        rwc.resetFromChunk(wc, 1, 1);

        // MUST NOT COMPILE!
        // wc.resetFromChunk(rc, 3, 5);
    }

    /**
     * This is less a test and more a comprehensive set of API calls, to make sure all the desired syntax is supported
     */
    @Test
    public void dynamicEntryPoints() {
        final ChunkType ct = ChunkType.Boolean;

        // array
        boolean[] ca = (boolean[])ct.makeArray(12);
        Chunk[] cca = ct.makeChunkArray(12);

        Chunk<Values> c;
        ChunkChunk<Values> cc;

        // empty chunk
        c = ct.getEmptyChunk();
        cc = ct.getEmptyChunkChunk();

        // chunk wrap 1
        c = ct.chunkWrap(ct.makeArray(12), 0, 12);
        cc = ct.chunkChunkWrap(ct.makeChunkArray(12), 0, 12);

        // chunk wrap 2
        c = ct.chunkWrap(ct.makeArray(12));
        cc = ct.chunkChunkWrap(ct.makeChunkArray(12));

        WritableChunk<Values> wc;
        WritableChunkChunk<Values> wcc;

        // make writable chunk
        wc = ct.makeWritableChunk(12);
        wcc = ct.makeWritableChunkChunk(12);

        // writable chunk wrap 1
        wc = WritableBooleanChunk.writableChunkWrap(WritableBooleanChunk.makeArray(12), 0, 12);
        wcc = WritableBooleanChunkChunk.writableChunkWrap(WritableBooleanChunkChunk.makeArray(12), 0, 12);

        // writable chunk wrap 2
        wc = WritableBooleanChunk.writableChunkWrap(WritableBooleanChunk.makeArray(12));
        wcc = WritableBooleanChunkChunk.writableChunkWrap(WritableBooleanChunkChunk.makeArray(12));

        // slice a chunk
        Chunk<Values> sc = c.slice(4, 6);
        ChunkChunk<Values> scc = cc.slice(4, 6);

        // slice a writable chunk
        WritableChunk<Values> swc = wc.slice(4, 6);
        WritableChunkChunk<Values> swcc = wcc.slice(4, 6);

        // make a resettable chunk and reset it
        ResettableReadOnlyChunk<Values> rc = ct.makeResettableReadOnlyChunk();
        rc.resetFromArray(ca, 0, 12);
        ResettableWritableChunk<Values> rwc = ct.makeResettableWritableChunk();
        rwc.resetFromArray(ca, 0, 12);

        rc.resetFromArray(ca, 3, 5);
        rwc.resetFromArray(ca, 3, 5);

        // A readable chunk can be reset from a readable or writable chunk
        rc.resetFromChunk(c, 3, 5);
        rc.resetFromChunk(wc, 1, 1);

        // A writable chunk can only be reset from another writable chunk
        rwc.resetFromChunk(wc, 1, 1);

        // MUST NOT COMPILE!
        // wc.resetFromChunk(rc, 3, 5);
    }

    @Test
    public void makeArray() {
        final int EMPTY_ARRAY_SIZE = 12;

        // array
        boolean[] ca = BooleanChunk.makeArray(EMPTY_ARRAY_SIZE);
        BooleanChunk[] cca = BooleanChunkChunk.makeArray(EMPTY_ARRAY_SIZE);

        TestCase.assertEquals(EMPTY_ARRAY_SIZE, ca.length);
        TestCase.assertEquals(EMPTY_ARRAY_SIZE, cca.length);
    }

    @Test
    public void emptyChunk() {
        // empty chunk
        final BooleanChunk<Values> c = BooleanChunk.getEmptyChunk();
        final BooleanChunkChunk<Values> cc = BooleanChunkChunk.getEmptyChunk();

        TestCase.assertEquals(0, c.size());
        TestCase.assertEquals(0, cc.size());

        TestCase.assertEquals(0, c.capacity);  // internal state
        TestCase.assertEquals(0, cc.capacity);  // internal state
    }

    @Test
    public void chunkWrap() {
        final Random rng = new Random(500908154);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final boolean[] data1 = ReplicatorHelpers.randomBooleans(rng, 100);

        final BooleanChunk<Values> c0 = BooleanChunk.chunkWrap(data0);
        TestCase.assertEquals(data0.length, c0.size());
        TestCase.assertEquals(data0.length, c0.capacity);  // internal state
        verifyChunkEqualsArray(c0, data0, 0, data0.length);

        final BooleanChunk<Values> c0Slice = BooleanChunk.chunkWrap(data0, 6, 3);
        TestCase.assertEquals(3, c0Slice.size());
        TestCase.assertEquals(3, c0Slice.capacity);  // internal state
        verifyChunkEqualsArray(c0Slice, data0, 6, 3);

        final BooleanChunk<Values> c1 = BooleanChunk.chunkWrap(data1);
        final BooleanChunk[] ccData = {c0, c1, c0, c1};

        final BooleanChunkChunk<Values> cc = BooleanChunkChunk.chunkWrap(ccData);
        TestCase.assertEquals(ccData.length, cc.size());
        TestCase.assertEquals(ccData.length, cc.capacity);  // internal state

        TestCase.assertSame(cc.get(0), c0);
        TestCase.assertSame(cc.get(1), c1);

        final BooleanChunkChunk<Values> ccSlice = BooleanChunkChunk.chunkWrap(ccData, 1, 2);
        TestCase.assertEquals(2, ccSlice.size());
        TestCase.assertEquals(2, ccSlice.capacity);  // internal state

        TestCase.assertSame(ccSlice.get(0), c1);
        TestCase.assertSame(ccSlice.get(1), c0);
    }

    @Test
    public void slice() {
        final Random rng = new Random(230789623);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final boolean[] data1 = ReplicatorHelpers.randomBooleans(rng, 100);

        final BooleanChunk<Values> c0 = BooleanChunk.chunkWrap(data0);
        final BooleanChunk<Values> c0Slice = c0.slice(5, 4);
        TestCase.assertEquals(4, c0Slice.size());
        TestCase.assertEquals(4, c0Slice.capacity);  // internal state
        verifyChunkEqualsArray(c0Slice, data0, 5, 4);

        final BooleanChunk<Values> c0SliceSlice = c0Slice.slice(2, 2);
        TestCase.assertEquals(2, c0SliceSlice.size());
        TestCase.assertEquals(2, c0SliceSlice.capacity);  // internal state
        verifyChunkEqualsArray(c0SliceSlice, data0, 7, 2);

        final BooleanChunk<Values> c1 = BooleanChunk.chunkWrap(data1);
        final BooleanChunk[] ccData = {c0, c1, c0, c1};
        final BooleanChunkChunk<Values> cc = BooleanChunkChunk.chunkWrap(ccData);
        final BooleanChunkChunk<Values> ccSlice = cc.slice(1, 2);

        TestCase.assertEquals(2, ccSlice.size());
        TestCase.assertEquals(2, ccSlice.capacity);  // internal state
        TestCase.assertSame(ccSlice.get(0), c1);
        TestCase.assertSame(ccSlice.get(1), c0);

        final BooleanChunkChunk<Values> ccSliceSlice = ccSlice.slice(1, 1);
        TestCase.assertEquals(1, ccSliceSlice.size());
        TestCase.assertEquals(1, ccSliceSlice.capacity);  // internal state
        TestCase.assertSame(ccSliceSlice.get(0), c0);
    }

    @Test
    public void writableSlice() {
        final Random rng = new Random(513829037);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final boolean[] valueData = ReplicatorHelpers.randomBooleans(rng, 2);
        final boolean value0 = valueData[0];
        final boolean value1 = valueData[1];

        final WritableBooleanChunk<Values> wc0 = WritableBooleanChunk.writableChunkWrap(data0);
        wc0.set(0, value0);
        TestCase.assertEquals(value0, wc0.get(0));
        TestCase.assertEquals(value0, data0[0]);

        final WritableBooleanChunk<Values> wc0Slice = wc0.slice(2, 3);
        wc0Slice.set(0, value1);
        TestCase.assertEquals(value1, wc0Slice.get(0));
        TestCase.assertEquals(value1, wc0.get(2));
        TestCase.assertEquals(value1, data0[2]);

        final boolean[] data1 = ReplicatorHelpers.randomBooleans(rng, 100);
        final WritableBooleanChunk<Values> wc1 = WritableBooleanChunk.writableChunkWrap(data0);


        final WritableBooleanChunk[] wccData = {wc0, wc1, wc0, wc1};
        final WritableBooleanChunkChunk<Values> wcc = WritableBooleanChunkChunk.writableChunkWrap(wccData);
        wcc.set(0, wc1);
        TestCase.assertSame(wc1, wcc.get(0));
        TestCase.assertSame(wc1, wccData[0]);

        final WritableBooleanChunkChunk<Values> wccSlice = wcc.slice(1, 2);
        wccSlice.set(0, wc0);
        TestCase.assertSame(wc0, wccSlice.get(0));
        TestCase.assertSame(wc0, wcc.get(1));
        TestCase.assertSame(wc0, wccData[1]);
    }

    @Test
    public void resettableChunk() {
        final Random rng = new Random(839293077);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final boolean[] data1 = ReplicatorHelpers.randomBooleans(rng, 100);

        final BooleanChunk<Values> c0 = BooleanChunk.chunkWrap(data0);
        final ResettableBooleanChunk<Values> rc1 = ResettableBooleanChunk.makeResettableChunk();
        rc1.resetFromArray(data1, 0, data1.length);
        TestCase.assertEquals(data1[0], rc1.get(0));

        rc1.resetFromChunk(c0, 3, 2);
        TestCase.assertEquals(data0[3], rc1.get(0));

        TestCase.assertEquals(2, rc1.size());
        TestCase.assertEquals(2, rc1.capacity);  // internal state

        rc1.resetFromChunk(rc1, 1, 1);  // Reset from itself
        TestCase.assertEquals(1, rc1.size());
        TestCase.assertEquals(1, rc1.capacity);  // internal state
        TestCase.assertEquals(data0[4], rc1.get(0));

        // WritableChunks are inherently resettable
        final WritableBooleanChunk<Values> wc0 = WritableBooleanChunk.writableChunkWrap(data0);
        final ResettableWritableBooleanChunk<Values> rwc1 = ResettableWritableBooleanChunk.makeResettableChunk();
        rwc1.resetFromArray(data1, 0, data1.length);
        TestCase.assertEquals(data1[0], rwc1.get(0));

        rwc1.resetFromChunk(wc0, 3, 2);
        TestCase.assertEquals(data0[3], rwc1.get(0));

        TestCase.assertEquals(2, rwc1.size());
        TestCase.assertEquals(2, rwc1.capacity);  // internal state

        rwc1.resetFromChunk(wc0, 1, 1);  // Reset from self
        TestCase.assertEquals(1, rwc1.size());
        TestCase.assertEquals(1, rwc1.capacity);  // internal state
        TestCase.assertEquals(data0[1], rwc1.get(0));
    }

    @Test
    public void resettableChunkChunk() {
        final Random rng = new Random(260533548);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final boolean[] data1 = ReplicatorHelpers.randomBooleans(rng, 100);
        final BooleanChunk<Values> c0 = BooleanChunk.chunkWrap(data0);
        final BooleanChunk<Values> c1 = BooleanChunk.chunkWrap(data1);
        BooleanChunk[] ccData = {c0, c1, c0, c1};
        final ResettableBooleanChunkChunk<Values> rcc1 = ResettableBooleanChunkChunk.makeResettableChunk();
        rcc1.resetFromArray(ccData, 0, ccData.length);
        TestCase.assertSame(ccData[0], rcc1.get(0));

        rcc1.resetFromChunk(rcc1, 1, 2);  // Reset from self
        TestCase.assertSame(ccData[1], rcc1.get(0));
        TestCase.assertEquals(2, rcc1.size());
        TestCase.assertEquals(2, rcc1.capacity);  // internal state

        rcc1.resetFromChunk(rcc1, 1, 1);  // Reset from self again
        TestCase.assertEquals(1, rcc1.size());
        TestCase.assertEquals(1, rcc1.capacity);  // internal state
        TestCase.assertSame(ccData[2], rcc1.get(0));

        // WritableChunks are inherently resettable
        final WritableBooleanChunk<Values> wc0 = WritableBooleanChunk.writableChunkWrap(data0);
        final WritableBooleanChunk<Values> wc1 = WritableBooleanChunk.writableChunkWrap(data1);
        WritableBooleanChunk[] wccData = {wc0, wc1, wc0, wc1};
        final ResettableWritableBooleanChunkChunk<Values> rwcc = ResettableWritableBooleanChunkChunk.makeResettableChunk();
        rwcc.resetFromArray(wccData, 0, wccData.length);
        TestCase.assertSame(wccData[0], rwcc.get(0));

        rwcc.resetFromChunk(rwcc, 1, 2);  // Reset from self
        TestCase.assertSame(wccData[1], rwcc.get(0));
        TestCase.assertEquals(2, rwcc.size());
        TestCase.assertEquals(2, rwcc.capacity);  // internal state

        rwcc.resetFromChunk(rwcc, 1, 1);  // Reset from self again
        TestCase.assertEquals(1, rwcc.size());
        TestCase.assertEquals(1, rwcc.capacity);  // internal state
        TestCase.assertSame(wccData[2], rwcc.get(0));
    }

    @Test
    public void chunkWrapBounds() {
        final Random rng = new Random(372402541);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        expectException(IllegalArgumentException.class, () -> BooleanChunk.chunkWrap(data0, -1, 2));
        expectException(IllegalArgumentException.class, () -> BooleanChunk.chunkWrap(data0, 0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> BooleanChunk.chunkWrap(data0, data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> BooleanChunk.chunkWrap(data0, data0.length, 1));
        BooleanChunk.chunkWrap(data0, data0.length, 0);  // Should succeed

        expectException(IllegalArgumentException.class, () -> WritableBooleanChunk.writableChunkWrap(data0, -1, 2));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunk.writableChunkWrap(data0, 0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunk.writableChunkWrap(data0, data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunk.writableChunkWrap(data0, data0.length, 1));
        WritableBooleanChunk.chunkWrap(data0, data0.length, 0);  // Should succeed

        final BooleanChunk[] ccData = new BooleanChunk[4];
        expectException(IllegalArgumentException.class, () -> BooleanChunkChunk.chunkWrap(ccData, -1, 2));
        expectException(IllegalArgumentException.class, () -> BooleanChunkChunk.chunkWrap(ccData, 0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> BooleanChunkChunk.chunkWrap(ccData, ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> BooleanChunkChunk.chunkWrap(ccData, ccData.length, 1));
        BooleanChunkChunk.chunkWrap(ccData, ccData.length, 0);  // Should succeed

        final WritableBooleanChunk[] wccData = new WritableBooleanChunk[4];
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunkChunk.writableChunkWrap(wccData, -1, 2));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunkChunk.writableChunkWrap(wccData, 0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunkChunk.writableChunkWrap(wccData, ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> WritableBooleanChunkChunk.writableChunkWrap(wccData, ccData.length, 1));
        WritableBooleanChunkChunk.writableChunkWrap(wccData, ccData.length, 0);  // Should succeed
    }

    @Test
    public void sliceBounds() {
        final Random rng = new Random(401919534);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final BooleanChunk<Values> c0 = BooleanChunk.chunkWrap(data0);
        expectException(IllegalArgumentException.class, () -> c0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> c0.slice(0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> c0.slice(data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> c0.slice(data0.length, 1));
        c0.slice(data0.length, 0);  // Should succeed

        final WritableBooleanChunk<Values> wc0 = WritableBooleanChunk.writableChunkWrap(data0);
        expectException(IllegalArgumentException.class, () -> wc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> wc0.slice(0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> wc0.slice(data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> wc0.slice(data0.length, 1));
        wc0.slice(data0.length, 0);  // Should succeed

        final BooleanChunk[] ccData = new BooleanChunk[4];
        final BooleanChunkChunk<Values> cc0 = BooleanChunkChunk.chunkWrap(ccData);
        expectException(IllegalArgumentException.class, () -> cc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length, 1));
        cc0.slice(ccData.length, 0);  // Should succeed

        final WritableBooleanChunk[] wccData = new WritableBooleanChunk[4];
        final WritableBooleanChunkChunk<Values> wcc0 = WritableBooleanChunkChunk.writableChunkWrap(wccData);
        expectException(IllegalArgumentException.class, () -> cc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length, 1));
        cc0.slice(ccData.length, 0);  // Should succeed
    }

    @Test
    public void sliceSizeBounds() {
        final Random rng = new Random(625937398);
        final boolean[] data0 = ReplicatorHelpers.randomBooleans(rng, 100);
        final WritableBooleanChunk<Values> c0 = WritableBooleanChunk.writableChunkWrap(data0);
        c0.slice(2, 4);
        c0.setSize(4);
        expectException(IllegalArgumentException.class, () -> c0.slice(2, 4));
    }

    @Test
    public void copyFromChunk() {
        // This is the test we are trying to do (imagine a-z and A-Z being variables holding random values)
        // destChunk is at offset 9, length 10
        // srcChunk is at offset 2 length 5
        // destData:  abcdefghijklmnopqrstuvwxyz
        // destChunk:          ^^^^^^^^^^

        // srcData:   ABCDEFGHIJKLMNOPQRSTUVWXYZ
        // srcChunk:    ^^^^^^

        // Copy 3 elements from srcChunk offset 1 to destChunk offset 3
        //
        // That would be....DEFG goes on top of mnop, so
        // destData:  abcdefghijklDEFGqrstuvwxyz

        final Random rng = new Random(124374349);
        final boolean[] destData = ReplicatorHelpers.randomBooleans(rng, 26);
        final boolean[] manualData = destData.clone();
        final boolean[] srcData = ReplicatorHelpers.randomBooleans(rng, 26);

        final int destChunkOffset = 9;
        final int destChunkCapacity = 10;

        final int srcChunkOffset = 2;
        final int srcChunkCapacity = 5;

        final int destCopyOffset = 4;
        final int srcCopyOffset = 3;
        final int copyLength = 4;

        final WritableBooleanChunk<Values> destChunk = WritableBooleanChunk.writableChunkWrap(destData, destChunkOffset, destChunkCapacity);
        final BooleanChunk<Values> srcChunk = BooleanChunk.chunkWrap(srcData, srcChunkOffset, srcChunkCapacity);
        destChunk.copyFromChunk(srcChunk, srcCopyOffset, destCopyOffset, copyLength);
        System.arraycopy(srcData,srcChunkOffset + srcCopyOffset, manualData,destChunkOffset + destCopyOffset, copyLength);
        final BooleanChunk<Values> manualChunk = BooleanChunk.chunkWrap(manualData, destChunkOffset, destChunkCapacity);
        final boolean same = BooleanChunkEquals.equalReduce(destChunk, manualChunk);
        TestCase.assertTrue("Chunks are not the same", same);

        // Compare the arrays, just for fun
        final boolean arraysSame = Arrays.equals(destData, manualData);
        TestCase.assertTrue("Arrays are not the same", arraysSame);
    }

    @Test
    public void testChunkChunk() {
        final Random rng = new Random(49042646);
        final boolean[] valueData = ReplicatorHelpers.randomBooleans(rng, 2);
        final boolean value0 = valueData[0];
        final boolean value1 = valueData[1];

        final int SIZE = 100;
        boolean[][] data = new boolean[SIZE][SIZE];

        // Visually, "data' is a square matrix.
        // Make a bunch of chunks that are the upper diagonal half of that matrix.

        //noinspection unchecked
        BooleanChunk<Values>[] chunks = new BooleanChunk[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            chunks[ii] = BooleanChunk.chunkWrap(data[ii], ii, SIZE - ii);
        }

        // And this ChunkChunk starts at offset 10 of that diagonal
        BooleanChunkChunk<Values> cc = BooleanChunkChunk.chunkWrap(chunks, 10, SIZE - 10);
        for (int ii = 10; ii < SIZE - 1; ++ii) {
            data[ii][ii] = value0;
            data[ii][ii + 1] = value1;
            final boolean actual0 = cc.get(ii - 10, 0);
            final boolean actual1 = cc.get(ii - 10, 1);
            TestCase.assertEquals(value0, actual0);
            TestCase.assertEquals(value1, actual1);
        }
    }

    @Test
    public void testWritableChunkChunk() {
        final Random rng = new Random(462357030);
        final boolean[] valueData = ReplicatorHelpers.randomBooleans(rng, 2);
        final boolean value0 = valueData[0];
        final boolean value1 = valueData[1];

        final int SIZE = 100;
        final int CHOFF = 10;  // chunk offset
        boolean[][] data = new boolean[SIZE][SIZE];

        // Visually, "data' is a square matrix.
        // Make a bunch of chunks that are the upper diagonal half of that matrix.

        //noinspection unchecked
        WritableBooleanChunk<Values>[] chunks = new WritableBooleanChunk[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            chunks[ii] = WritableBooleanChunk.writableChunkWrap(data[ii], ii, SIZE - ii);
        }

        // And this ChunkChunk starts at offset 10 of that diagonal
        WritableBooleanChunkChunk<Values> cc = WritableBooleanChunkChunk.writableChunkWrap(chunks, CHOFF, SIZE - CHOFF);
        for (int ii = 10; ii < SIZE - 1; ++ii) {
            // set the array, check that the values appear in the chunk, using both the 1D and 2D APIs
            data[ii][ii] = value0;
            data[ii][ii + 1] = value1;
            final boolean actual1D0 = cc.get(ii - CHOFF).get(0);
            final boolean actual1D1 = cc.get(ii - CHOFF).get(1);
            final boolean actual2D0 = cc.get(ii - CHOFF, 0);
            final boolean actual2D1 = cc.get(ii - CHOFF, 1);
            TestCase.assertEquals(value0, actual1D0);
            TestCase.assertEquals(value1, actual1D1);
            TestCase.assertEquals(value0, actual2D0);
            TestCase.assertEquals(value1, actual2D1);

            // set the chunk using the 1D API, check that the values appear in the array
            cc.getWritableChunk(ii - CHOFF).set(0, value1);
            cc.getWritableChunk(ii - CHOFF).set(1, value0);
            final boolean reverseActual1D1 = data[ii][ii];
            final boolean reverseActual1D0 = data[ii][ii + 1];
            TestCase.assertEquals(value0, reverseActual1D0);
            TestCase.assertEquals(value1, reverseActual1D1);

            // set the chunk using the 2D API, check that the values appear in the array
            cc.set(ii - CHOFF, 0, value0);
            cc.set(ii - CHOFF, 1, value1);
            final boolean reverseActual2D0 = data[ii][ii];
            final boolean reverseActual2D1 = data[ii][ii + 1];
            TestCase.assertEquals(value0, reverseActual2D0);
            TestCase.assertEquals(value1, reverseActual2D1);
        }

        // replace a chunk somewhere in the middle
        final int MIDCHOFF = 12;
        boolean[] replacementData = new boolean[SIZE];
        WritableBooleanChunk<Values> replacementChunk = WritableBooleanChunk.writableChunkWrap(replacementData, MIDCHOFF, MIDCHOFF);
        cc.setWritableChunk(3, replacementChunk);

        // set the array, check that the values appear in the chunk, using both the 1D and 2D APIs
        replacementData[17] = value0;
        replacementData[18] = value1;
        final boolean actual1D0 = cc.get(3).get(17 - MIDCHOFF);
        final boolean actual1D1 = cc.get(3).get(18 - MIDCHOFF);
        final boolean actual2D0 = cc.get(3, 17 - MIDCHOFF);
        final boolean actual2D1 = cc.get(3, 18 - MIDCHOFF);
        TestCase.assertEquals(value0, actual1D0);
        TestCase.assertEquals(value1, actual1D1);
        TestCase.assertEquals(value0, actual2D0);
        TestCase.assertEquals(value1, actual2D1);

        // set the chunk using the 1D API, check that the values appear in the array
        cc.getWritableChunk(3).set(17 - MIDCHOFF, value1);
        cc.getWritableChunk(3).set(18 - MIDCHOFF, value0);
        final boolean reverseActual1D1 = replacementData[17];
        final boolean reverseActual1D0 = replacementData[18];
        TestCase.assertEquals(value1, reverseActual1D1);
        TestCase.assertEquals(value0, reverseActual1D0);

        // set the chunk using the 2D API, check that the values appear in the array
        cc.set(3, 17 - MIDCHOFF, value0);
        cc.set(3, 18 - MIDCHOFF, value1);
        final boolean reverseActual2D0 = replacementData[17];
        final boolean reverseActual2D1 = replacementData[18];
        TestCase.assertEquals(value0, reverseActual2D0);
        TestCase.assertEquals(value1, reverseActual2D1);
    }

    private static <ATTR extends Values> void verifyChunkEqualsArray(BooleanChunk<ATTR> chunk, boolean[] data, int offset, int size) {
        for (int ii = 0; ii < size; ++ii) {
            TestCase.assertEquals(String.format("At rowSet %d", ii), data[ii + offset], chunk.get(ii));
        }
    }

    /**
     * Throw an exception if lambda either does not throw, or throws an exception that is not assignable to
     * expectedExceptionType
     */
    private static <T extends Exception> void expectException(Class<T> expectedExceptionType, Runnable lambda) {
        String nameOfCaughtException = "(no exception)";
        try {
            lambda.run();
        } catch (Exception actual) {
            if (expectedExceptionType.isAssignableFrom(actual.getClass())) {
                return;
            }
            nameOfCaughtException = actual.getClass().getSimpleName();
        }
        throw new RuntimeException(String.format("Expected exception %s, got %s",
                expectedExceptionType.getSimpleName(), nameOfCaughtException));
    }
}
