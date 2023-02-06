/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit AbstractCharacterColumnSourceTest and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.TestSourceSink;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static junit.framework.TestCase.*;
import static junit.framework.TestCase.assertEquals;

public abstract class AbstractIntegerColumnSourceTest {
    @NotNull
    abstract WritableColumnSource<Integer> makeTestSource();

    int getSourceSize() {
        return 16384;
    }

    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testFillChunk() {
        final Random random = new Random(0);

        for (int chunkSize = 1024; chunkSize <= getSourceSize(); chunkSize *= 2) {
            testFill(random, chunkSize);
        }
    }

    private void testFill(Random random, int chunkSize) {
        final WritableColumnSource<Integer> source = makeTestSource();

        final ColumnSource.FillContext fillContext = source.makeFillContext(chunkSize);
        final WritableIntChunk dest = WritableIntChunk.makeWritableChunk(chunkSize);

        source.fillChunk(fillContext, dest, RowSetFactory.fromRange(0, 1023));
        for (int ii = 0; ii < 1024; ++ii) {
            checkFromSource("null check: " + ii, NULL_INT, dest.get(ii));
        }

        final int expectedBlockSize = 1024;
        final int [] expectations = new int[getSourceSize()];
        // region arrayFill
        Arrays.fill(expectations, NULL_INT);
        // endregion arrayFill
        final int [] randomInts = ArrayGenerator.randomInts(random, expectations.length / 2);
        for (int ii = 0; ii < expectations.length; ++ii) {
            final int block = ii / expectedBlockSize;
            if (block % 2 == 0) {
                final int randomInt = randomInts[(block / 2 * expectedBlockSize) + (ii % expectedBlockSize)];
                expectations[ii] = randomInt;
                source.set(ii, randomInt);
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
                final RowSet rowSet = generateIndex(random, expectations.length, 1 + random.nextInt(31));
                checkRandomFill(chunkSize, source, fillContext, dest, expectations, rowSet, usePrev);
            }
        }

        fillContext.close();
    }

    @Test
    public void testGetChunk() {
        final Random random = new Random(0);

        for (int chunkSize = 1024; chunkSize <= getSourceSize(); chunkSize *= 2) {
            testGet(random, chunkSize);
        }
    }

    private void testGet(Random random, int chunkSize) {
        final WritableColumnSource<Integer> source = makeTestSource();

        final ColumnSource.GetContext getContext = source.makeGetContext(chunkSize);

        final Chunk<? extends Values> emptyResult = source.getChunk(getContext, RowSetFactory.empty());
        assertEquals(emptyResult.size(), 0);

        // the asChunk is not needed here, but it's needed when replicated to Boolean
        final IntChunk<? extends Values> result = source.getChunk(getContext, RowSetFactory.fromRange(0, 1023)).asIntChunk();
        for (int ii = 0; ii < 1024; ++ii) {
            checkFromSource("null check: " + ii, NULL_INT, result.get(ii));
        }

        final int expectedBlockSize = 1024;
        final int [] expectations = new int[getSourceSize()];
        // region arrayFill
        Arrays.fill(expectations, NULL_INT);
        // endregion arrayFill
        final int [] randomInts = ArrayGenerator.randomInts(random, expectations.length / 2);
        for (int ii = 0; ii < expectations.length; ++ii) {
            final int block = ii / expectedBlockSize;
            if (block % 2 == 0) {
                final int randomInt = randomInts[(block / 2 * expectedBlockSize) + (ii % expectedBlockSize)];
                expectations[ii] = randomInt;
                source.set(ii, randomInt);
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

    private RowSet generateIndex(Random random, int maxsize, int runLength) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
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

        return builder.build();
    }

    private WritableLongChunk<RowKeys> generateRandomKeys(Random random, int count, int maxsize) {
        final WritableLongChunk<RowKeys> result = WritableLongChunk.makeWritableChunk(count);
        for (int ii = 0; ii < count; ++ii) {
            if (random.nextDouble() < 0.1) {
                result.set(ii, RowSet.NULL_ROW_KEY);
            } else {
                result.set(ii, random.nextInt(maxsize));
            }
        }
        return result;
    }

    private void checkRandomFill(int chunkSize, WritableColumnSource<Integer> source, ColumnSource.FillContext fillContext,
                                 WritableIntChunk dest, int[] expectations, RowSet rowSet, boolean usePrev) {
        for (final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator(); rsIt.hasMore(); ) {
            final RowSequence nextOk = rsIt.getNextRowSequenceWithLength(chunkSize);

            if (usePrev) {
                source.fillChunk(fillContext, dest, nextOk);
            } else {
                source.fillPrevChunk(fillContext, dest, nextOk);
            }

            int ii = 0;
            for (final RowSet.Iterator indexIt = nextOk.asRowSet().iterator(); indexIt.hasNext(); ii++) {
                final long next = indexIt.nextLong();
                checkFromValues("expectations[" + next + "] vs. dest[" + ii + "]", expectations[(int)next], dest.get(ii));
            }
        }
    }

    private void checkRandomFillUnordered(WritableColumnSource<Integer> source, ColumnSource.FillContext fillContext,
                                          WritableIntChunk dest, int[] expectations, LongChunk<RowKeys> keys, boolean usePrev) {
        //noinspection unchecked
        final FillUnordered<Values> fillUnordered = (FillUnordered<Values>)source;
        if (usePrev) {
            fillUnordered.fillChunkUnordered(fillContext, dest, keys);
        } else {
            fillUnordered.fillPrevChunkUnordered(fillContext, dest, keys);
        }

        for (int ii = 0; ii < keys.size(); ii++) {
            final long next = keys.get(ii);
            if (next == RowSet.NULL_ROW_KEY) {
                // region null unordered check
                checkFromValues("null vs. dest[" + ii + "]", NULL_INT, dest.get(ii));
                // endregion null unordered check
            } else {
                checkFromValues("expectations[" + next + "] vs. dest[" + ii + "]", expectations[(int) next], dest.get(ii));
            }
        }
    }

    private void checkRangeFill(int chunkSize, WritableColumnSource<Integer> source, ColumnSource.FillContext fillContext,
                                WritableIntChunk dest, int[] expectations, int firstKey, int lastKey, boolean usePrev) {
        int offset;
        final RowSet rowSet = RowSetFactory.fromRange(firstKey, lastKey);
        offset = firstKey;
        for (final RowSequence.Iterator it = rowSet.getRowSequenceIterator(); it.hasMore(); ) {
            final RowSequence nextOk = it.getNextRowSequenceWithLength(chunkSize);

            if (usePrev) {
                source.fillPrevChunk(fillContext, dest, nextOk);
            } else {
                source.fillChunk(fillContext, dest, nextOk);
            }
            checkRangeResults(expectations, offset, nextOk, dest);
            offset += nextOk.size();
        }
    }

    private void checkRangeGet(int chunkSize, ColumnSource<Integer> source, ColumnSource.GetContext getContext, int[] expectations, int firstKey, int lastKey, boolean usePrev) {
        int offset;
        final RowSet rowSet = RowSetFactory.fromRange(firstKey, lastKey);
        offset = firstKey;
        for (final RowSequence.Iterator it = rowSet.getRowSequenceIterator(); it.hasMore(); ) {
            final RowSequence nextOk = it.getNextRowSequenceWithLength(chunkSize);

            final IntChunk<? extends Values> result;
            if (usePrev) {
                result = source.getPrevChunk(getContext, nextOk).asIntChunk();
            } else {
                result = source.getChunk(getContext, nextOk).asIntChunk();
            }
            checkRangeResults(expectations, offset, nextOk, result);
            // region samecheck
            final int firstBlock = firstKey / 1024;
            final int lastBlock = lastKey / 1024;
            if (!usePrev && (firstBlock == lastBlock) && (firstBlock % 2 == 0)) {
                assertTrue(DefaultGetContext.isMyResettableChunk(getContext, result));
            }
            // endregion samecheck
            offset += nextOk.size();
        }
    }

    private void checkRangeResults(int[] expectations, int offset, RowSequence nextOk, IntChunk<? extends Values> result) {
        for (int ii = 0; ii < nextOk.size(); ++ii) {
            checkFromValues("expectations[" + offset + " + " + ii + " = " + (ii + offset) + "] vs. dest[" + ii + "]", expectations[ii + offset], result.get(ii));
        }
    }

    // region fromvalues
    private void checkFromValues(String msg, int fromValues, int fromChunk) {
        assertEquals(msg, fromValues, fromChunk);
    }
    // endregion fromvalues

    // region fromsource
    private void checkFromSource(String msg, int fromSource, int fromChunk) {
        assertEquals(msg, fromSource, fromChunk);
    }
    // endregion fromsource

    @Test
    public void testSourceSink() {
        TestSourceSink.runTests(ChunkType.Int, size -> {
            final WritableColumnSource<Integer> src = makeTestSource();
            src.ensureCapacity(size);
            return src;
        });
    }

    // This code tickles a bug where the act of trying to fill a chunk activates the prevFlusher, but the fact that
    // there's no data in the chunk means that the prev arrays were never changed from null. This would trigger a
    // null reference exception at commit time. The fix is to have the chunk methods bail out early if there is nothing
    // to do.
    @Test
    public void testFilllEmptyChunkWithPrev() {
        final IntegerSparseArraySource src = new IntegerSparseArraySource();
        src.startTrackingPrevValues();
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try (final RowSet keys = RowSetFactory.empty();
             final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(0)) {
            // Fill from an empty chunk
            src.fillFromChunkByKeys(keys, chunk);
        }
        // NullPointerException in IntegerSparseArraySource.commitUpdates()
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void testFillUnordered() {
        final Random random = new Random(0);
        testFillUnordered(random, 1024);
    }

    private void testFillUnordered(Random random, int chunkSize) {
        final WritableColumnSource<Integer> source = makeTestSource();

        final ColumnSource.FillContext fillContext = source.makeFillContext(chunkSize);
        final WritableIntChunk dest = WritableIntChunk.makeWritableChunk(chunkSize);

        source.fillChunk(fillContext, dest, RowSetFactory.fromRange(0, 1023));
        for (int ii = 0; ii < 1024; ++ii) {
            checkFromSource("null check: " + ii, NULL_INT, dest.get(ii));
        }

        final int expectedBlockSize = 1024;
        final int [] expectations = new int[getSourceSize()];
        // region arrayFill
        Arrays.fill(expectations, NULL_INT);
        // endregion arrayFill
        final int [] randomInts = ArrayGenerator.randomInts(random, expectations.length / 2);
        for (int ii = 0; ii < expectations.length; ++ii) {
            final int block = ii / expectedBlockSize;
            if (block % 2 == 0) {
                final int randomInt = randomInts[(block / 2 * expectedBlockSize) + (ii % expectedBlockSize)];
                expectations[ii] = randomInt;
                source.set(ii, randomInt);
            }
        }

        // before we have the previous tracking enabled, prev should just fall through to get
        for (boolean usePrev : new boolean[]{false, true}) {
            // lets make a few random indices
            for (int seed = 0; seed < 100; ++seed) {
                int count = random.nextInt(chunkSize);
                try (final WritableLongChunk<RowKeys> rowKeys = generateRandomKeys(random, count, expectations.length)) {
                    checkRandomFillUnordered(source, fillContext, dest, expectations, rowKeys, usePrev);
                }
            }
        }

        fillContext.close();
    }
}
