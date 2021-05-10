/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterSparseArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.ObjectChunk;

import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;

import io.deephaven.db.util.BooleanUtils;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.BooleanChunk;
import io.deephaven.db.v2.sources.chunk.WritableBooleanChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;
// endregion boxing imports

import static junit.framework.TestCase.*;

public class TestBooleanSparseArraySource {
    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testFillChunk() {
        final Random random = new Random(0);

        for (int chunkSize = 1024; chunkSize <= 16384; chunkSize *= 2) {
            testFill(random, chunkSize);
        }
    }

    private void testFill(Random random, int chunkSize) {
        final BooleanSparseArraySource source = new BooleanSparseArraySource();

        final ColumnSource.FillContext fillContext = source.makeFillContext(chunkSize);
        final WritableObjectChunk<Boolean, Values> dest = WritableObjectChunk.makeWritableChunk(chunkSize);

        source.fillChunk(fillContext, dest, Index.FACTORY.getIndexByRange(0, 1023));
        for (int ii = 0; ii < 1024; ++ii) {
            checkFromSource("null check: " + ii, NULL_BOOLEAN, dest.get(ii));
        }

        final int expectedBlockSize = 1024;
        final byte [] expectations = new byte[16384];
        // region arrayFill
        Arrays.fill(expectations, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
        // endregion arrayFill
        final byte [] randomBooleans = ArrayGenerator.randomBooleans(random, expectations.length / 2);
        for (int ii = 0; ii < expectations.length; ++ii) {
            final int block = ii / expectedBlockSize;
            if (block % 2 == 0) {
                final byte randomBoolean = randomBooleans[(block / 2 * expectedBlockSize) + (ii % expectedBlockSize)];
                expectations[ii] = randomBoolean;
                source.set(ii, randomBoolean);
            }
        }

        // before we have the previous tracking enabled, prev should just fall through to get
        for (boolean usePrev : new boolean[]{false, true}) {
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 0, expectations.length - 1, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 100, expectations.length - 100, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 200, expectations.length - 1124, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 100, 700, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 100, 1024, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 250, 250, usePrev);
            checkRangeFill(chunkSize, source, fillContext, dest, expectations, 250, 251, usePrev);

            // lets make a few random indices
            for (int seed = 0; seed < 100; ++seed) {
                final Index index = generateIndex(random, expectations.length, 1 + random.nextInt(31));
                checkRandomFill(chunkSize, source, fillContext, dest, expectations, index, usePrev);
            }
        }

        fillContext.close();
    }

    @Test
    public void testGetChunk() {
        final Random random = new Random(0);

        for (int chunkSize = 1024; chunkSize <= 16384; chunkSize *= 2) {
            testGet(random, chunkSize);
        }
    }

    private void testGet(Random random, int chunkSize) {
        final BooleanSparseArraySource source = new BooleanSparseArraySource();

        final ColumnSource.GetContext getContext = source.makeGetContext(chunkSize);

        // the asChunk is not needed here, but it's needed when replicated to Boolean
        final ObjectChunk<Boolean, Values> result = source.getChunk(getContext, Index.FACTORY.getIndexByRange(0, 1023)).asObjectChunk();
        for (int ii = 0; ii < 1024; ++ii) {
            checkFromSource("null check: " + ii, NULL_BOOLEAN, result.get(ii));
        }

        final int expectedBlockSize = 1024;
        final byte [] expectations = new byte[16384];
        // region arrayFill
        Arrays.fill(expectations, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
        // endregion arrayFill
        final byte [] randomBooleans = ArrayGenerator.randomBooleans(random, expectations.length / 2);
        for (int ii = 0; ii < expectations.length; ++ii) {
            final int block = ii / expectedBlockSize;
            if (block % 2 == 0) {
                final byte randomBoolean = randomBooleans[(block / 2 * expectedBlockSize) + (ii % expectedBlockSize)];
                expectations[ii] = randomBoolean;
                source.set(ii, randomBoolean);
            }
        }

        // before we have the previous tracking enabled, prev should just fall through to get
        for (boolean usePrev : new boolean[]{false, true}) {
            checkRangeGet(chunkSize, source, getContext, expectations, 0, expectations.length - 1, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 100, expectations.length - 100, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 200, expectations.length - 1124, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 100, 700, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 100, 1024, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 250, 250, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 250, 251, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 0, 1023, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 1024, 2047, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 1100, 1200, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 1200, 1200, usePrev);
            checkRangeGet(chunkSize, source, getContext, expectations, 1200, 1201, usePrev);
        }

        getContext.close();
    }

    private Index generateIndex(Random random, int maxsize, int runLength) {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        int nextKey = random.nextInt(runLength);
        while (nextKey < maxsize) {
            int lastKey;
            if (random.nextBoolean()) {
                final int length = Math.min(random.nextInt(runLength) + 1, maxsize - nextKey);
                lastKey =  nextKey + length - 1;
                builder.appendRange(nextKey, lastKey);
            } else {
                builder.appendKey(lastKey = nextKey);
            }
            nextKey = lastKey + 1 + random.nextInt(runLength + 1);
        }

        return builder.getIndex();
    }

    private void checkRandomFill(int chunkSize, BooleanSparseArraySource source, ColumnSource.FillContext fillContext,
                                 WritableObjectChunk<Boolean, Values> dest, byte[] expectations, Index index, boolean usePrev) {
        for (final OrderedKeys.Iterator okIt = index.getOrderedKeysIterator(); okIt.hasMore(); ) {
            final OrderedKeys nextOk = okIt.getNextOrderedKeysWithLength(chunkSize);

            if (usePrev) {
                source.fillChunk(fillContext, dest, nextOk);
            } else {
                source.fillPrevChunk(fillContext, dest, nextOk);
            }

            int ii = 0;
            for (final Index.Iterator indexIt = nextOk.asIndex().iterator(); indexIt.hasNext(); ii++) {
                final long next = indexIt.nextLong();
                checkFromValues("expectations[" + next + "] vs. dest[" + ii + "]", expectations[(int)next], dest.get(ii));
            }
        }
    }

    private void checkRangeFill(int chunkSize, BooleanSparseArraySource source, ColumnSource.FillContext fillContext,
                                WritableObjectChunk<Boolean, Values> dest, byte[] expectations, int firstKey, int lastKey, boolean usePrev) {
        int offset;
        final Index index = Index.FACTORY.getIndexByRange(firstKey, lastKey);
        offset = firstKey;
        for (final OrderedKeys.Iterator it = index.getOrderedKeysIterator(); it.hasMore(); ) {
            final OrderedKeys nextOk = it.getNextOrderedKeysWithLength(chunkSize);

            if (usePrev) {
                source.fillPrevChunk(fillContext, dest, nextOk);
            } else {
                source.fillChunk(fillContext, dest, nextOk);
            }
            checkRangeResults(expectations, offset, nextOk, dest);
            offset += nextOk.size();
        }
    }

    private void checkRangeGet(int chunkSize, BooleanSparseArraySource source, ColumnSource.GetContext getContext, byte[] expectations, int firstKey, int lastKey, boolean usePrev) {
        int offset;
        final Index index = Index.FACTORY.getIndexByRange(firstKey, lastKey);
        offset = firstKey;
        for (final OrderedKeys.Iterator it = index.getOrderedKeysIterator(); it.hasMore(); ) {
            final OrderedKeys nextOk = it.getNextOrderedKeysWithLength(chunkSize);

            final ObjectChunk<Boolean, Values> result;
            if (usePrev) {
                result = source.getPrevChunk(getContext, nextOk).asObjectChunk();
            } else {
                result = source.getChunk(getContext, nextOk).asObjectChunk();
            }
            checkRangeResults(expectations, offset, nextOk, result);
            // region samecheck
            // endregion samecheck
            offset += nextOk.size();
        }
    }

    private void checkRangeResults(byte[] expectations, int offset, OrderedKeys nextOk, ObjectChunk<Boolean, Values> result) {
        for (int ii = 0; ii < nextOk.size(); ++ii) {
            checkFromValues("expectations[" + offset + " + " + ii + " = " + (ii + offset) + "] vs. dest[" + ii + "]", expectations[ii + offset], result.get(ii));
        }
    }

    // region fromvalues
    private void checkFromValues(String msg, byte fromValues, Boolean fromChunk) {
        assertEquals(msg, fromValues == BooleanUtils.NULL_BOOLEAN_AS_BYTE ? null : fromValues == BooleanUtils.TRUE_BOOLEAN_AS_BYTE, fromChunk);
    }
    // endregion fromvalues

    // region fromsource
    private void checkFromSource(String msg, Boolean fromSource, Boolean fromChunk) {
        assertEquals(msg, fromSource, fromChunk);
    }
    // endregion fromsource

    @Test
    public void testSourceSink() {
        TestSourceSink.runTests(ChunkType.Boolean, size -> {
            final BooleanSparseArraySource src = new BooleanSparseArraySource();
            src.ensureCapacity(size);
            return src;
        });
    }

    @Test
    public void confirmAliasingForbidden() {
        final Random rng = new Random(438269476);
        final int arraySize = 100;
        final int rangeStart = 20;
        final int rangeEnd = 80;
        final BooleanSparseArraySource source = new BooleanSparseArraySource();
        source.ensureCapacity(arraySize);

        final byte[] data = ArrayGenerator.randomBooleans(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }
        // super hack
        final byte[] peekedBlock = source.ensureBlock(0, 0, 0);

        try (Index srcKeys = Index.FACTORY.getIndexByRange(rangeStart, rangeEnd)) {
            try (Index destKeys = Index.FACTORY.getIndexByRange(rangeStart + 1, rangeEnd + 1)) {
                try (ChunkSource.GetContext srcContext = source.makeGetContext(arraySize)) {
                    try (WritableChunkSink.FillFromContext destContext = source.makeFillFromContext(arraySize)) {
                        Chunk chunk = source.getChunk(srcContext, srcKeys);
                        if (chunk.isAlias(peekedBlock)) {
                            // If the ArraySource gives out aliases of its blocks, then it should throw when we try to
                            // fill from that aliased chunk
                            boolean testFailed;
                            try {
                                source.fillFromChunk(destContext, chunk, destKeys);
                                testFailed = true;
                            } catch (UnsupportedOperationException uoe) {
                                testFailed = false;
                            }
                            assertFalse(testFailed);
                        }
                    }
                }
            }
        }
    }

    // This code tickles a bug where the act of trying to fill a chunk activates the prevFlusher, but the fact that
    // there's no data in the chunk means that the prev arrays were never changed from null. This would trigger a
    // null reference exception at commit time. The fix is to have the chunk methods bail out early if there is nothing
    // to do.
    @Test
    public void testFilllEmptyChunkWithPrev() {
        final BooleanSparseArraySource src = new BooleanSparseArraySource();
        src.startTrackingPrevValues();
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try (final Index keys = Index.FACTORY.getEmptyIndex();
             final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(0)) {
            // Fill from an empty chunk
            src.fillFromChunkByKeys(keys, chunk);
        }
        // NullPointerException in BooleanSparseArraySource.commitUpdates()
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void testSerialization() throws Exception {
        final Random random = new Random(36403335);
        final int numValues = 100_000;
        final long[] randomKeys = new long[numValues];
        for (int ii = 0; ii < numValues; ++ii) {
            // No negative keys please.
            randomKeys[ii] = random.nextLong() & ~(1L << 63);
        }
        final byte[] randomValues = ArrayGenerator.randomBooleans(random, numValues);
        // Make every one out of 10 null
        for (int ii = 0; ii < numValues; ii += 10) {
            // region arrayFill
            randomValues[ii] = BooleanUtils.NULL_BOOLEAN_AS_BYTE;
            // endregion arrayFill
        }

        final BooleanSparseArraySource source = new BooleanSparseArraySource();
        for (int ii = 0; ii < numValues; ++ii) {
            source.set(randomKeys[ii], randomValues[ii]);
        }

        final byte[] bytes;
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(source);
            oos.flush();
            bytes = bos.toByteArray();
        }

        final BooleanSparseArraySource dest;
        try (final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            final ObjectInputStream ois = new ObjectInputStream(bis)) {
            dest = (BooleanSparseArraySource) ois.readObject();
        }

        // region elementGet
        for (long key : randomKeys) {
            final byte srcKey = source.getByte(key);
            final byte destKey = dest.getByte(key);
            assertEquals(srcKey, destKey);
        }
        // endregion elementGet
    }
}
