/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.v2.select.FormulaColumn;
import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.Index;
import io.deephaven.engine.v2.utils.OrderedKeys;
import io.deephaven.util.Shuffle;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

import static io.deephaven.engine.v2.sources.ArrayGenerator.indexDataGenerator;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static junit.framework.TestCase.*;

public class TestLongArraySource {
    private LongArraySource forArray(long[] values) {
        final LongArraySource source = new LongArraySource();
        source.ensureCapacity(values.length);
        for (int i = 0; i < values.length; i++) {
            source.set(i, values[i]);
        }
        source.startTrackingPrevValues();
        return source;
    }

    private void updateFromArray(LongArraySource dest, long[] values) {
        dest.ensureCapacity(values.length);
        for (int i = 0; i < values.length; i++) {
            dest.set(i, values[i]);
        }
    }

    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    private void testGetChunkGeneric(long[] values, long[] newValues, int chunkSize, Index index) {
        final LongArraySource source;
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try {
            source = forArray(values);
            validateValues(chunkSize, values, index, source);
        } finally {
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try {
            updateFromArray(source, newValues);
            validateValues(chunkSize, newValues, index, source);
            validatePrevValues(chunkSize, values, index, source);
        } finally {
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }
    }

    private void validateValues(int chunkSize, long[] values, Index index, LongArraySource source) {
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            final LongChunk chunk = source.getChunk(context, okChunk).asLongChunk();
            assertTrue(chunk.size() <= chunkSize);
            if (okIterator.hasMore()) {
                assertEquals(chunkSize, chunk.size());
            }
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource("idx=" + idx + ", i=" + i, source.getLong(idx), chunk.get(i));
                checkFromValues("idx=" + idx + ", i=" + i, values[(int) idx], chunk.get(i));
                pos++;
            }
            // region samecheck
            final LongChunk<OrderedKeyRanges> ranges = okChunk.asKeyRangesChunk();
            if (ranges.size() > 2 || ranges.get(0) / LongArraySource.BLOCK_SIZE != (ranges.get(1) / LongArraySource.BLOCK_SIZE)) {
                assertTrue(DefaultGetContext.isMyWritableChunk(context, chunk));

            } else {
                assertTrue(DefaultGetContext.isMyResettableChunk(context, chunk));
            }
            // endregion samecheck
        }
        assertEquals(pos, index.size());
    }


    private void validatePrevValues(int chunkSize, long[] values, Index index, LongArraySource source) {
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            final LongChunk chunk = source.getPrevChunk(context, okChunk).asLongChunk();
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getPrevLong(idx), chunk.get(i));
                checkFromValues(values[(int) idx], chunk.get(i));
                pos++;
            }
            assertTrue(DefaultGetContext.isMyWritableChunk(context, chunk));
        }
        assertEquals(pos, index.size());
    }

    @Test
    public void testGetChunk() {
        final Random random = new Random(0);
        testGetChunkGeneric(new long[0], new long[0], 1, Index.FACTORY.getIndexByValues());
        testGetChunkGeneric(new long[0], new long[0], 16, Index.FACTORY.getIndexByValues());

        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0, 1));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4,  6));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 3, Index.FACTORY.getIndexByValues(5, 6, 7));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 4, Index.FACTORY.getIndexByValues(4, 5, 6, 7));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 5, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 512), ArrayGenerator.randomLongs(random, 512), 4, Index.FACTORY.getIndexByValues(254, 255, 256, 257));
        testGetChunkGeneric(ArrayGenerator.randomLongs(random, 512), ArrayGenerator.randomLongs(random, 512), 5, Index.FACTORY.getIndexByValues(254, 255, 256, 257, 258));

        for (int sourceSize = 32; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -4; v < 5; v++) {
                testForGivenSourceSize(random, sourceSize + v);
            }
        }

        //References to block test
    }

    // region lazy
    private void testGetChunkGenericLazy(long[] values, int chunkSize, Index index) {
        final LongArraySource sourceOrigin = forArray(values);
        final FormulaColumn formulaColumn = FormulaColumn.createFormulaColumn("Foo", "origin");
        final Index.SequentialBuilder sequentialBuilder = Index.FACTORY.getSequentialBuilder();
        if (values.length > 0) {
            sequentialBuilder.appendRange(0, values.length - 1);
        }
        final Index fullRange = sequentialBuilder.getIndex();
        final Map<String, LongArraySource> oneAndOnly = new HashMap<>();
        oneAndOnly.put("origin", sourceOrigin);
        formulaColumn.initInputs(fullRange, oneAndOnly);
        final ColumnSource<?> source = formulaColumn.getDataView();
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            final LongChunk chunk = source.getChunk(context, okChunk).asLongChunk();
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                assertEquals(chunk.get(i), source.getLong(it.nextLong()));
                pos++;
            }
        }
        assertEquals(pos, index.size());
    }
    // endregion lazy

    @Test
    public void testGetChunkLazy() {
        final Random random = new Random(0);
        testGetChunkGenericLazy(new long[0], 1, Index.FACTORY.getIndexByValues());
        testGetChunkGenericLazy(new long[0], 16, Index.FACTORY.getIndexByValues());

        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0, 1));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4,  6));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 3, Index.FACTORY.getIndexByValues(5, 6, 7));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 4, Index.FACTORY.getIndexByValues(4, 5, 6, 7));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 16), 5, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 512), 4, Index.FACTORY.getIndexByValues(254, 255, 256, 257));
        testGetChunkGenericLazy(ArrayGenerator.randomLongs(random, 512), 5, Index.FACTORY.getIndexByValues(254, 255, 256, 257, 258));

        for (int sourceSize = 512; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -2; v < 3; v += 2) {
                testForGivenSourceLazySize(random, sourceSize + v);
            }
        }

    }

    private void testForGivenSourceLazySize(Random random, int sourceSize) {
        final long[] values = ArrayGenerator.randomLongs(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testIndexSizeVariationsLazy(random, sourceSize, values, indexSize);
        }
    }

    private void testIndexSizeVariationsLazy(Random random, int sourceSize, long[] values, int indexSize) {
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize - 1);
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize);
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize + 1);
    }

    private void testParameterChunkAndIndexLazy(Random random, int sourceSize, long[] values, int indexSize) {
        final Index index = Index.FACTORY.getIndexByValues(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 4) {
            testGetChunkGenericLazy(values, chunkSize, index);
            testGetChunkGenericLazy(values, chunkSize + 1, index);
            testGetChunkGenericLazy(values, chunkSize - 1, index);
        }
    }


    private void testForGivenSourceSize(Random random, int sourceSize) {
        final long[] values = ArrayGenerator.randomLongs(random, sourceSize);
        final long[] newValues = ArrayGenerator.randomLongs(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 2) {
            testIndexSizeVariations(random, sourceSize, values, newValues, indexSize);
        }
    }

    private void testIndexSizeVariations(Random random, int sourceSize, long[] values, long[] newvalues, int indexSize) {
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize - 1);
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize);
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize + 1);
    }

    private void testParameterChunkAndIndex(Random random, int sourceSize, long[] values, long[] newvalues, int indexSize) {
        final Index index = Index.FACTORY.getIndexByValues(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 2) {
            testGetChunkGeneric(values, newvalues, chunkSize, index);
            testGetChunkGeneric(values, newvalues, chunkSize + 1, index);
            testGetChunkGeneric(values, newvalues, chunkSize - 1, index);
        }
    }

    private void testFillChunkGeneric(long[] values, long[] newValues, int chunkSize, Index index) {
        final LongArraySource source;
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try {
            source = forArray(values);
            validateValuesWithFill(chunkSize, values, index, source);
        } finally {
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try {
            updateFromArray(source, newValues);
            validateValuesWithFill(chunkSize, newValues, index, source);
            validatePrevValuesWithFill(chunkSize, values, index, source);
        } finally {
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }
    }

    private void validateValuesWithFill(int chunkSize, long[] values, Index index, LongArraySource source) {
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            source.fillChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getLong(idx), chunk.get(i));
                checkFromValues(values[(int)idx], chunk.get(i));
                pos++;
            }
        }
        assertEquals(pos, index.size());
    }

    private void validatePrevValuesWithFill(int chunkSize, long[] values, Index index, LongArraySource source) {
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            source.fillPrevChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getPrevLong(idx), chunk.get(i));
                checkFromValues(values[(int)idx], chunk.get(i));
                pos++;
            }
        }
        assertEquals(pos, index.size());
    }

    @Test
    public void testFillChunk() {
        final Random random = new Random(0);
        testFillChunkGeneric(new long[0], new long[0], 1, Index.FACTORY.getIndexByValues());
        testFillChunkGeneric(new long[0], new long[0], 16, Index.FACTORY.getIndexByValues());

        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0, 1));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4,  6));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 3, Index.FACTORY.getIndexByValues(5, 6, 7));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 4, Index.FACTORY.getIndexByValues(4, 5, 6, 7));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 16), ArrayGenerator.randomLongs(random, 16), 5, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 512), ArrayGenerator.randomLongs(random, 512), 4, Index.FACTORY.getIndexByValues(254, 255, 256, 257));
        testFillChunkGeneric(ArrayGenerator.randomLongs(random, 512), ArrayGenerator.randomLongs(random, 512), 5, Index.FACTORY.getIndexByValues(254, 255, 256, 257, 258));

        for (int sourceSize = 32; sourceSize < 8192; sourceSize *= 4) {
            for (int v = -4; v < 5; v += 2) {
                testSetForGivenSourceSize(random, sourceSize + v);
            }
        }
    }

    private void testSetForGivenSourceSize(Random random, int sourceSize) {
        final long[] values = ArrayGenerator.randomLongs(random, sourceSize);
        final long[] newValues = ArrayGenerator.randomLongs(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testFillChunkIndexSizeVariations(random, sourceSize, values, newValues, indexSize);
        }
    }

    private void testFillChunkIndexSizeVariations(Random random, int sourceSize, long[] values, long[] newValues, int indexSize) {
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize - 1);
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize);
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize + 1);
    }

    private void testParameterFillChunkAndIndex(Random random, int sourceSize, long[] values, long[] newValues, int indexSize) {
        final Index index = Index.FACTORY.getIndexByValues(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 2) {
            testFillChunkGeneric(values, newValues, chunkSize, index);
            testFillChunkGeneric(values, newValues, chunkSize + 1, index);
            testFillChunkGeneric(values, newValues, chunkSize - 1, index);
        }
    }

    // region lazygeneric
    private void testFillChunkLazyGeneric(long[] values, int chunkSize, Index index) {
        final LongArraySource sourceOrigin = forArray(values);
        final FormulaColumn formulaColumn = FormulaColumn.createFormulaColumn("Foo", "origin");
        final Index.SequentialBuilder sequentialBuilder = Index.FACTORY.getSequentialBuilder();
        if (values.length > 0) {
            sequentialBuilder.appendRange(0, values.length - 1);
        }
        final Index fullRange = sequentialBuilder.getIndex();
        final Map<String, LongArraySource> oneAndOnly = new HashMap<>();
        oneAndOnly.put("origin", sourceOrigin);
        formulaColumn.initInputs(fullRange, oneAndOnly);
        final ColumnSource source = formulaColumn.getDataView();
        final OrderedKeys.Iterator okIterator = index.getOrderedKeysIterator();
        final Index.Iterator it = index.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(okIterator.hasMore());
            final OrderedKeys okChunk = okIterator.getNextOrderedKeysWithLength(chunkSize);
            source.fillChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                assertEquals(chunk.get(i), source.getLong(idx));
                pos++;
            }
        }
        assertEquals(pos, index.size());
    }
    // endregion lazygeneric


    @Test
    public void testFillChunkLazy() {
        final Random random = new Random(0);
        testFillChunkLazyGeneric(new long[0], 1, Index.FACTORY.getIndexByValues());
        testFillChunkLazyGeneric(new long[0], 16, Index.FACTORY.getIndexByValues());

        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(0, 1));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4,  6));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 1, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 3, Index.FACTORY.getIndexByValues(5, 6, 7));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 4, Index.FACTORY.getIndexByValues(4, 5, 6, 7));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 16), 5, Index.FACTORY.getIndexByValues(4, 5, 6, 7, 8));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 512), 4, Index.FACTORY.getIndexByValues(254, 255, 256, 257));
        testFillChunkLazyGeneric(ArrayGenerator.randomLongs(random, 512), 5, Index.FACTORY.getIndexByValues(254, 255, 256, 257, 258));

        for (int sourceSize = 512; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -2; v < 3; v++) {
                testSetForGivenLazySourceSize(random, sourceSize + v);
            }
        }
    }

    private void testSetForGivenLazySourceSize(Random random, int sourceSize) {
        final long[] values = ArrayGenerator.randomLongs(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testFillChunkIndexSizeVariationsLazy(random, sourceSize, values, indexSize);
        }
    }

    private void testFillChunkIndexSizeVariationsLazy(Random random, int sourceSize, long[] values, int indexSize) {
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize - 1);
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize);
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize + 1);
    }

    private void testParameterFillChunkAndIndexLazy(Random random, int sourceSize, long[] values, int indexSize) {
        final Index index = Index.FACTORY.getIndexByValues(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 4) {
            testFillChunkLazyGeneric(values, chunkSize, index);
            testFillChunkLazyGeneric(values, chunkSize + 1, index);
            testFillChunkLazyGeneric(values, chunkSize - 1, index);
        }
    }


    // region fromvalues
    private void checkFromValues(String msg, long fromValues, long fromChunk) {
        assertEquals(msg, fromValues, fromChunk);
    }

    private void checkFromValues(long fromValues, long fromChunk) {
        assertEquals(fromValues, fromChunk);
    }
    // endregion fromvalues

    // region fromsource
    private void checkFromSource(String msg, long fromSource, long fromChunk) {
        assertEquals(msg, fromSource, fromChunk);
    }

    private void checkFromSource(long fromSource, long fromChunk) {
        assertEquals(fromSource, fromChunk);
    }
    // endregion fromsource

    @Test
    public void testSourceSink() {
        TestSourceSink.runTests(ChunkType.Long, size -> {
            final LongArraySource src = new LongArraySource();
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
        final LongArraySource source = new LongArraySource();
        source.ensureCapacity(arraySize);

        final long[] data = ArrayGenerator.randomLongs(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }
        // super hack
        final long[] peekedBlock = (long[])source.getBlock(0);

        try (Index srcKeys = Index.FACTORY.getIndexByRange(rangeStart, rangeEnd)) {
            try (Index destKeys = Index.FACTORY.getIndexByRange(rangeStart + 1, rangeEnd + 1)) {
                try (ChunkSource.GetContext srcContext = source.makeGetContext(arraySize)) {
                    try (WritableChunkSink.FillFromContext destContext = source.makeFillFromContext(arraySize)) {
                        Chunk chunk = source.getChunk(srcContext, srcKeys);
                        if (chunk.isAlias(peekedBlock)) {
                            // If the ArraySource gives out aliases of its blocks, then it should throw when we try to
                            // fill from that aliased chunk
                            try {
                                source.fillFromChunk(destContext, chunk, destKeys);
                                TestCase.fail();
                            } catch (UnsupportedOperationException uoe) {
                                // Expected
                            }
                        }
                    }
                }
            }
        }
    }

    // In the Sparse versions of the code, this tickles a bug causing a null pointer exception. For the non-sparse
    // versions, the bug isn't tickled. But we should probably have this test anyway, in case some future change
    // triggers it.
    @Test
    public void testFillEmptyChunkWithPrev() {
        final LongArraySource src = new LongArraySource();
        src.startTrackingPrevValues();
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        try (final Index keys = Index.FACTORY.getEmptyIndex();
             final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(0)) {
            // Fill from an empty chunk
            src.fillFromChunkByKeys(keys, chunk);
        }
        // NullPointerException in LongSparseArraySource.commitUpdates()
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void testFillUnorderedWithNulls() {
        final LongArraySource source = new LongArraySource();

        final Random rng = new Random(438269476);
        final int arraySize = 100;
        source.ensureCapacity(arraySize);

        final long[] data = ArrayGenerator.randomLongs(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }

        final long [] keys = LongStream.concat(LongStream.of(Index.NULL_KEY), LongStream.range(0, data.length - 1)).toArray();
        Shuffle.shuffleArray(rng, keys);

        try (final ChunkSource.FillContext ctx = source.makeFillContext(keys.length);
             final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(keys.length);
             final ResettableLongChunk<KeyIndices> rlc = ResettableLongChunk.makeResettableChunk()) {
            rlc.resetFromTypedArray(keys, 0, keys.length);
            source.fillChunkUnordered(ctx, dest, rlc);
            assertEquals(keys.length, dest.size());
            for (int ii = 0; ii < keys.length; ++ii) {
                if (keys[ii] == Index.NULL_KEY) {
                    assertEquals(NULL_LONG, dest.get(ii));
                } else {
                    checkFromValues(data[(int)keys[ii]], dest.get(ii));
                }
            }
        }
    }
}
