package io.deephaven.chunk;

import io.deephaven.chunk.util.hashing.CharChunkEquals;
import io.deephaven.chunk.attributes.Values;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class TestCharChunk {
    /**
     * This is less a test and more a comprehensive set of API calls, to make sure all the desired syntax is supported
     */
    @Test
    public void staticEntryPoints() {
        // array
        char[] ca = CharChunk.makeArray(12);
        CharChunk[] cca = CharChunkChunk.makeArray(12);

        CharChunk<Values> c;
        CharChunkChunk<Values> cc;

        // empty chunk
        c = CharChunk.getEmptyChunk();
        cc = CharChunkChunk.getEmptyChunk();

        // chunk wrap 1
        c = CharChunk.chunkWrap(CharChunk.makeArray(12), 0, 12);
        cc = CharChunkChunk.chunkWrap(CharChunkChunk.makeArray(12), 0, 12);

        // chunk wrap 2
        c = CharChunk.chunkWrap(CharChunk.makeArray(12));
        cc = CharChunkChunk.chunkWrap(CharChunkChunk.makeArray(12));

        WritableCharChunk<Values> wc;
        WritableCharChunkChunk<Values> wcc;

        // make writable chunk
        wc = WritableCharChunk.makeWritableChunk(12);
        wcc = WritableCharChunkChunk.makeWritableChunk(12);

        // writable chunk wrap 1
        wc = WritableCharChunk.writableChunkWrap(WritableCharChunk.makeArray(12), 0, 12);
        wcc = WritableCharChunkChunk.writableChunkWrap(WritableCharChunkChunk.makeArray(12), 0, 12);

        // writable chunk wrap 2
        wc = WritableCharChunk.writableChunkWrap(WritableCharChunk.makeArray(12));
        wcc = WritableCharChunkChunk.writableChunkWrap(WritableCharChunkChunk.makeArray(12));

        // slice a chunk
        CharChunk<Values> sc = c.slice(4, 6);
        CharChunkChunk<Values> scc = cc.slice(4, 6);

        // slice a writable chunk
        WritableCharChunk<Values> swc = wc.slice(4, 6);
        WritableCharChunkChunk<Values> swcc = wcc.slice(4, 6);

        // make a resettable chunk and reset it
        ResettableCharChunk<Values> rc = ResettableCharChunk.makeResettableChunk();
        rc.resetFromArray(ca, 0, 12);
        ResettableWritableCharChunk<Values> rwc = ResettableWritableCharChunk.makeResettableChunk();
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
        final ChunkType ct = ChunkType.Char;

        // array
        char[] ca = (char[])ct.makeArray(12);
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
        wc = WritableCharChunk.writableChunkWrap(WritableCharChunk.makeArray(12), 0, 12);
        wcc = WritableCharChunkChunk.writableChunkWrap(WritableCharChunkChunk.makeArray(12), 0, 12);

        // writable chunk wrap 2
        wc = WritableCharChunk.writableChunkWrap(WritableCharChunk.makeArray(12));
        wcc = WritableCharChunkChunk.writableChunkWrap(WritableCharChunkChunk.makeArray(12));

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
        char[] ca = CharChunk.makeArray(EMPTY_ARRAY_SIZE);
        CharChunk[] cca = CharChunkChunk.makeArray(EMPTY_ARRAY_SIZE);

        TestCase.assertEquals(EMPTY_ARRAY_SIZE, ca.length);
        TestCase.assertEquals(EMPTY_ARRAY_SIZE, cca.length);
    }

    @Test
    public void emptyChunk() {
        // empty chunk
        final CharChunk<Values> c = CharChunk.getEmptyChunk();
        final CharChunkChunk<Values> cc = CharChunkChunk.getEmptyChunk();

        TestCase.assertEquals(0, c.size());
        TestCase.assertEquals(0, cc.size());

        TestCase.assertEquals(0, c.capacity);  // internal state
        TestCase.assertEquals(0, cc.capacity);  // internal state
    }

    @Test
    public void chunkWrap() {
        final Random rng = new Random(500908154);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final char[] data1 = ReplicatorHelpers.randomChars(rng, 100);

        final CharChunk<Values> c0 = CharChunk.chunkWrap(data0);
        TestCase.assertEquals(data0.length, c0.size());
        TestCase.assertEquals(data0.length, c0.capacity);  // internal state
        verifyChunkEqualsArray(c0, data0, 0, data0.length);

        final CharChunk<Values> c0Slice = CharChunk.chunkWrap(data0, 6, 3);
        TestCase.assertEquals(3, c0Slice.size());
        TestCase.assertEquals(3, c0Slice.capacity);  // internal state
        verifyChunkEqualsArray(c0Slice, data0, 6, 3);

        final CharChunk<Values> c1 = CharChunk.chunkWrap(data1);
        final CharChunk[] ccData = {c0, c1, c0, c1};

        final CharChunkChunk<Values> cc = CharChunkChunk.chunkWrap(ccData);
        TestCase.assertEquals(ccData.length, cc.size());
        TestCase.assertEquals(ccData.length, cc.capacity);  // internal state

        TestCase.assertSame(cc.get(0), c0);
        TestCase.assertSame(cc.get(1), c1);

        final CharChunkChunk<Values> ccSlice = CharChunkChunk.chunkWrap(ccData, 1, 2);
        TestCase.assertEquals(2, ccSlice.size());
        TestCase.assertEquals(2, ccSlice.capacity);  // internal state

        TestCase.assertSame(ccSlice.get(0), c1);
        TestCase.assertSame(ccSlice.get(1), c0);
    }

    @Test
    public void slice() {
        final Random rng = new Random(230789623);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final char[] data1 = ReplicatorHelpers.randomChars(rng, 100);

        final CharChunk<Values> c0 = CharChunk.chunkWrap(data0);
        final CharChunk<Values> c0Slice = c0.slice(5, 4);
        TestCase.assertEquals(4, c0Slice.size());
        TestCase.assertEquals(4, c0Slice.capacity);  // internal state
        verifyChunkEqualsArray(c0Slice, data0, 5, 4);

        final CharChunk<Values> c0SliceSlice = c0Slice.slice(2, 2);
        TestCase.assertEquals(2, c0SliceSlice.size());
        TestCase.assertEquals(2, c0SliceSlice.capacity);  // internal state
        verifyChunkEqualsArray(c0SliceSlice, data0, 7, 2);

        final CharChunk<Values> c1 = CharChunk.chunkWrap(data1);
        final CharChunk[] ccData = {c0, c1, c0, c1};
        final CharChunkChunk<Values> cc = CharChunkChunk.chunkWrap(ccData);
        final CharChunkChunk<Values> ccSlice = cc.slice(1, 2);

        TestCase.assertEquals(2, ccSlice.size());
        TestCase.assertEquals(2, ccSlice.capacity);  // internal state
        TestCase.assertSame(ccSlice.get(0), c1);
        TestCase.assertSame(ccSlice.get(1), c0);

        final CharChunkChunk<Values> ccSliceSlice = ccSlice.slice(1, 1);
        TestCase.assertEquals(1, ccSliceSlice.size());
        TestCase.assertEquals(1, ccSliceSlice.capacity);  // internal state
        TestCase.assertSame(ccSliceSlice.get(0), c0);
    }

    @Test
    public void writableSlice() {
        final Random rng = new Random(513829037);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final char[] valueData = ReplicatorHelpers.randomChars(rng, 2);
        final char value0 = valueData[0];
        final char value1 = valueData[1];

        final WritableCharChunk<Values> wc0 = WritableCharChunk.writableChunkWrap(data0);
        wc0.set(0, value0);
        TestCase.assertEquals(value0, wc0.get(0));
        TestCase.assertEquals(value0, data0[0]);

        final WritableCharChunk<Values> wc0Slice = wc0.slice(2, 3);
        wc0Slice.set(0, value1);
        TestCase.assertEquals(value1, wc0Slice.get(0));
        TestCase.assertEquals(value1, wc0.get(2));
        TestCase.assertEquals(value1, data0[2]);

        final char[] data1 = ReplicatorHelpers.randomChars(rng, 100);
        final WritableCharChunk<Values> wc1 = WritableCharChunk.writableChunkWrap(data0);


        final WritableCharChunk[] wccData = {wc0, wc1, wc0, wc1};
        final WritableCharChunkChunk<Values> wcc = WritableCharChunkChunk.writableChunkWrap(wccData);
        wcc.set(0, wc1);
        TestCase.assertSame(wc1, wcc.get(0));
        TestCase.assertSame(wc1, wccData[0]);

        final WritableCharChunkChunk<Values> wccSlice = wcc.slice(1, 2);
        wccSlice.set(0, wc0);
        TestCase.assertSame(wc0, wccSlice.get(0));
        TestCase.assertSame(wc0, wcc.get(1));
        TestCase.assertSame(wc0, wccData[1]);
    }

    @Test
    public void resettableChunk() {
        final Random rng = new Random(839293077);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final char[] data1 = ReplicatorHelpers.randomChars(rng, 100);

        final CharChunk<Values> c0 = CharChunk.chunkWrap(data0);
        final ResettableCharChunk<Values> rc1 = ResettableCharChunk.makeResettableChunk();
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
        final WritableCharChunk<Values> wc0 = WritableCharChunk.writableChunkWrap(data0);
        final ResettableWritableCharChunk<Values> rwc1 = ResettableWritableCharChunk.makeResettableChunk();
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
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final char[] data1 = ReplicatorHelpers.randomChars(rng, 100);
        final CharChunk<Values> c0 = CharChunk.chunkWrap(data0);
        final CharChunk<Values> c1 = CharChunk.chunkWrap(data1);
        CharChunk[] ccData = {c0, c1, c0, c1};
        final ResettableCharChunkChunk<Values> rcc1 = ResettableCharChunkChunk.makeResettableChunk();
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
        final WritableCharChunk<Values> wc0 = WritableCharChunk.writableChunkWrap(data0);
        final WritableCharChunk<Values> wc1 = WritableCharChunk.writableChunkWrap(data1);
        WritableCharChunk[] wccData = {wc0, wc1, wc0, wc1};
        final ResettableWritableCharChunkChunk<Values> rwcc = ResettableWritableCharChunkChunk.makeResettableChunk();
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
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        expectException(IllegalArgumentException.class, () -> CharChunk.chunkWrap(data0, -1, 2));
        expectException(IllegalArgumentException.class, () -> CharChunk.chunkWrap(data0, 0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> CharChunk.chunkWrap(data0, data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> CharChunk.chunkWrap(data0, data0.length, 1));
        CharChunk.chunkWrap(data0, data0.length, 0);  // Should succeed

        expectException(IllegalArgumentException.class, () -> WritableCharChunk.writableChunkWrap(data0, -1, 2));
        expectException(IllegalArgumentException.class, () -> WritableCharChunk.writableChunkWrap(data0, 0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> WritableCharChunk.writableChunkWrap(data0, data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> WritableCharChunk.writableChunkWrap(data0, data0.length, 1));
        WritableCharChunk.chunkWrap(data0, data0.length, 0);  // Should succeed

        final CharChunk[] ccData = new CharChunk[4];
        expectException(IllegalArgumentException.class, () -> CharChunkChunk.chunkWrap(ccData, -1, 2));
        expectException(IllegalArgumentException.class, () -> CharChunkChunk.chunkWrap(ccData, 0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> CharChunkChunk.chunkWrap(ccData, ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> CharChunkChunk.chunkWrap(ccData, ccData.length, 1));
        CharChunkChunk.chunkWrap(ccData, ccData.length, 0);  // Should succeed

        final WritableCharChunk[] wccData = new WritableCharChunk[4];
        expectException(IllegalArgumentException.class, () -> WritableCharChunkChunk.writableChunkWrap(wccData, -1, 2));
        expectException(IllegalArgumentException.class, () -> WritableCharChunkChunk.writableChunkWrap(wccData, 0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> WritableCharChunkChunk.writableChunkWrap(wccData, ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> WritableCharChunkChunk.writableChunkWrap(wccData, ccData.length, 1));
        WritableCharChunkChunk.writableChunkWrap(wccData, ccData.length, 0);  // Should succeed
    }

    @Test
    public void sliceBounds() {
        final Random rng = new Random(401919534);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final CharChunk<Values> c0 = CharChunk.chunkWrap(data0);
        expectException(IllegalArgumentException.class, () -> c0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> c0.slice(0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> c0.slice(data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> c0.slice(data0.length, 1));
        c0.slice(data0.length, 0);  // Should succeed

        final WritableCharChunk<Values> wc0 = WritableCharChunk.writableChunkWrap(data0);
        expectException(IllegalArgumentException.class, () -> wc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> wc0.slice(0, data0.length + 1));
        expectException(IllegalArgumentException.class, () -> wc0.slice(data0.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> wc0.slice(data0.length, 1));
        wc0.slice(data0.length, 0);  // Should succeed

        final CharChunk[] ccData = new CharChunk[4];
        final CharChunkChunk<Values> cc0 = CharChunkChunk.chunkWrap(ccData);
        expectException(IllegalArgumentException.class, () -> cc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length, 1));
        cc0.slice(ccData.length, 0);  // Should succeed

        final WritableCharChunk[] wccData = new WritableCharChunk[4];
        final WritableCharChunkChunk<Values> wcc0 = WritableCharChunkChunk.writableChunkWrap(wccData);
        expectException(IllegalArgumentException.class, () -> cc0.slice(-1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(0, ccData.length + 1));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length - 1, 2));
        expectException(IllegalArgumentException.class, () -> cc0.slice(ccData.length, 1));
        cc0.slice(ccData.length, 0);  // Should succeed
    }

    @Test
    public void sliceSizeBounds() {
        final Random rng = new Random(625937398);
        final char[] data0 = ReplicatorHelpers.randomChars(rng, 100);
        final WritableCharChunk<Values> c0 = WritableCharChunk.writableChunkWrap(data0);
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
        final char[] destData = ReplicatorHelpers.randomChars(rng, 26);
        final char[] manualData = destData.clone();
        final char[] srcData = ReplicatorHelpers.randomChars(rng, 26);

        final int destChunkOffset = 9;
        final int destChunkCapacity = 10;

        final int srcChunkOffset = 2;
        final int srcChunkCapacity = 5;

        final int destCopyOffset = 4;
        final int srcCopyOffset = 3;
        final int copyLength = 4;

        final WritableCharChunk<Values> destChunk = WritableCharChunk.writableChunkWrap(destData, destChunkOffset, destChunkCapacity);
        final CharChunk<Values> srcChunk = CharChunk.chunkWrap(srcData, srcChunkOffset, srcChunkCapacity);
        destChunk.copyFromChunk(srcChunk, srcCopyOffset, destCopyOffset, copyLength);
        System.arraycopy(srcData,srcChunkOffset + srcCopyOffset, manualData,destChunkOffset + destCopyOffset, copyLength);
        final CharChunk<Values> manualChunk = CharChunk.chunkWrap(manualData, destChunkOffset, destChunkCapacity);
        final boolean same = CharChunkEquals.equalReduce(destChunk, manualChunk);
        TestCase.assertTrue("Chunks are not the same", same);

        // Compare the arrays, just for fun
        final boolean arraysSame = Arrays.equals(destData, manualData);
        TestCase.assertTrue("Arrays are not the same", arraysSame);
    }

    @Test
    public void testChunkChunk() {
        final Random rng = new Random(49042646);
        final char[] valueData = ReplicatorHelpers.randomChars(rng, 2);
        final char value0 = valueData[0];
        final char value1 = valueData[1];

        final int SIZE = 100;
        char[][] data = new char[SIZE][SIZE];

        // Visually, "data' is a square matrix.
        // Make a bunch of chunks that are the upper diagonal half of that matrix.

        //noinspection unchecked
        CharChunk<Values>[] chunks = new CharChunk[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            chunks[ii] = CharChunk.chunkWrap(data[ii], ii, SIZE - ii);
        }

        // And this ChunkChunk starts at offset 10 of that diagonal
        CharChunkChunk<Values> cc = CharChunkChunk.chunkWrap(chunks, 10, SIZE - 10);
        for (int ii = 10; ii < SIZE - 1; ++ii) {
            data[ii][ii] = value0;
            data[ii][ii + 1] = value1;
            final char actual0 = cc.get(ii - 10, 0);
            final char actual1 = cc.get(ii - 10, 1);
            TestCase.assertEquals(value0, actual0);
            TestCase.assertEquals(value1, actual1);
        }
    }

    @Test
    public void testWritableChunkChunk() {
        final Random rng = new Random(462357030);
        final char[] valueData = ReplicatorHelpers.randomChars(rng, 2);
        final char value0 = valueData[0];
        final char value1 = valueData[1];

        final int SIZE = 100;
        final int CHOFF = 10;  // chunk offset
        char[][] data = new char[SIZE][SIZE];

        // Visually, "data' is a square matrix.
        // Make a bunch of chunks that are the upper diagonal half of that matrix.

        //noinspection unchecked
        WritableCharChunk<Values>[] chunks = new WritableCharChunk[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            chunks[ii] = WritableCharChunk.writableChunkWrap(data[ii], ii, SIZE - ii);
        }

        // And this ChunkChunk starts at offset 10 of that diagonal
        WritableCharChunkChunk<Values> cc = WritableCharChunkChunk.writableChunkWrap(chunks, CHOFF, SIZE - CHOFF);
        for (int ii = 10; ii < SIZE - 1; ++ii) {
            // set the array, check that the values appear in the chunk, using both the 1D and 2D APIs
            data[ii][ii] = value0;
            data[ii][ii + 1] = value1;
            final char actual1D0 = cc.get(ii - CHOFF).get(0);
            final char actual1D1 = cc.get(ii - CHOFF).get(1);
            final char actual2D0 = cc.get(ii - CHOFF, 0);
            final char actual2D1 = cc.get(ii - CHOFF, 1);
            TestCase.assertEquals(value0, actual1D0);
            TestCase.assertEquals(value1, actual1D1);
            TestCase.assertEquals(value0, actual2D0);
            TestCase.assertEquals(value1, actual2D1);

            // set the chunk using the 1D API, check that the values appear in the array
            cc.getWritableChunk(ii - CHOFF).set(0, value1);
            cc.getWritableChunk(ii - CHOFF).set(1, value0);
            final char reverseActual1D1 = data[ii][ii];
            final char reverseActual1D0 = data[ii][ii + 1];
            TestCase.assertEquals(value0, reverseActual1D0);
            TestCase.assertEquals(value1, reverseActual1D1);

            // set the chunk using the 2D API, check that the values appear in the array
            cc.set(ii - CHOFF, 0, value0);
            cc.set(ii - CHOFF, 1, value1);
            final char reverseActual2D0 = data[ii][ii];
            final char reverseActual2D1 = data[ii][ii + 1];
            TestCase.assertEquals(value0, reverseActual2D0);
            TestCase.assertEquals(value1, reverseActual2D1);
        }

        // replace a chunk somewhere in the middle
        final int MIDCHOFF = 12;
        char[] replacementData = new char[SIZE];
        WritableCharChunk<Values> replacementChunk = WritableCharChunk.writableChunkWrap(replacementData, MIDCHOFF, MIDCHOFF);
        cc.setWritableChunk(3, replacementChunk);

        // set the array, check that the values appear in the chunk, using both the 1D and 2D APIs
        replacementData[17] = value0;
        replacementData[18] = value1;
        final char actual1D0 = cc.get(3).get(17 - MIDCHOFF);
        final char actual1D1 = cc.get(3).get(18 - MIDCHOFF);
        final char actual2D0 = cc.get(3, 17 - MIDCHOFF);
        final char actual2D1 = cc.get(3, 18 - MIDCHOFF);
        TestCase.assertEquals(value0, actual1D0);
        TestCase.assertEquals(value1, actual1D1);
        TestCase.assertEquals(value0, actual2D0);
        TestCase.assertEquals(value1, actual2D1);

        // set the chunk using the 1D API, check that the values appear in the array
        cc.getWritableChunk(3).set(17 - MIDCHOFF, value1);
        cc.getWritableChunk(3).set(18 - MIDCHOFF, value0);
        final char reverseActual1D1 = replacementData[17];
        final char reverseActual1D0 = replacementData[18];
        TestCase.assertEquals(value1, reverseActual1D1);
        TestCase.assertEquals(value0, reverseActual1D0);

        // set the chunk using the 2D API, check that the values appear in the array
        cc.set(3, 17 - MIDCHOFF, value0);
        cc.set(3, 18 - MIDCHOFF, value1);
        final char reverseActual2D0 = replacementData[17];
        final char reverseActual2D1 = replacementData[18];
        TestCase.assertEquals(value0, reverseActual2D0);
        TestCase.assertEquals(value1, reverseActual2D1);
    }

    private static <ATTR extends Values> void verifyChunkEqualsArray(CharChunk<ATTR> chunk, char[] data, int offset, int size) {
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
