/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ObjectChunk;

import io.deephaven.chunk.WritableObjectChunk;

import io.deephaven.util.BooleanUtils;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.TestSourceSink;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.testutil.Shuffle;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

import static io.deephaven.chunk.ArrayGenerator.indexDataGenerator;
import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;
import static junit.framework.TestCase.*;

public class TestBooleanArraySource {
    private BooleanArraySource forArray(byte[] values) {
        final BooleanArraySource source = new BooleanArraySource();
        source.ensureCapacity(values.length);
        for (int i = 0; i < values.length; i++) {
            source.set(i, values[i]);
        }
        source.startTrackingPrevValues();
        return source;
    }

    private void updateFromArray(BooleanArraySource dest, byte[] values) {
        dest.ensureCapacity(values.length);
        for (int i = 0; i < values.length; i++) {
            dest.set(i, values[i]);
        }
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

    private void testGetChunkGeneric(byte[] values, byte[] newValues, int chunkSize, RowSet rowSet) {
        final BooleanArraySource source;
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try {
            source = forArray(values);
            validateValues(chunkSize, values, rowSet, source);
        } finally {
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try {
            updateFromArray(source, newValues);
            validateValues(chunkSize, newValues, rowSet, source);
            validatePrevValues(chunkSize, values, rowSet, source);
        } finally {
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }
    }

    private void validateValues(int chunkSize, byte[] values, RowSet rowSet, BooleanArraySource source) {
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            final ObjectChunk<Boolean, ? extends Values> chunk = source.getChunk(context, okChunk).asObjectChunk();
            assertTrue(chunk.size() <= chunkSize);
            if (rsIterator.hasMore()) {
                assertEquals(chunkSize, chunk.size());
            }
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource("idx=" + idx + ", i=" + i, source.getBoolean(idx), chunk.get(i));
                checkFromValues("idx=" + idx + ", i=" + i, values[(int) idx], chunk.get(i));
                pos++;
            }
            // region samecheck
            // endregion samecheck
        }
        assertEquals(pos, rowSet.size());
    }


    private void validatePrevValues(int chunkSize, byte[] values, RowSet rowSet, BooleanArraySource source) {
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            final ObjectChunk<Boolean, ? extends Values> chunk = source.getPrevChunk(context, okChunk).asObjectChunk();
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getPrevBoolean(idx), chunk.get(i));
                checkFromValues(values[(int) idx], chunk.get(i));
                pos++;
            }
            assertTrue(DefaultGetContext.isMyWritableChunk(context, chunk));
        }
        assertEquals(pos, rowSet.size());
    }

    @Test
    public void testGetChunk() {
        final Random random = new Random(0);
        testGetChunkGeneric(new byte[0], new byte[0], 1, RowSetFactory.fromKeys());
        testGetChunkGeneric(new byte[0], new byte[0], 16, RowSetFactory.fromKeys());

        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0, 1));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4,  6));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 3, RowSetFactory.fromKeys(5, 6, 7));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 4, RowSetFactory.fromKeys(4, 5, 6, 7));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 5, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 512), ArrayGenerator.randomBooleans(random, 512), 4, RowSetFactory.fromKeys(254, 255, 256, 257));
        testGetChunkGeneric(ArrayGenerator.randomBooleans(random, 512), ArrayGenerator.randomBooleans(random, 512), 5, RowSetFactory.fromKeys(254, 255, 256, 257, 258));

        for (int sourceSize = 32; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -4; v < 5; v++) {
                testForGivenSourceSize(random, sourceSize + v);
            }
        }

        //References to block test
    }

    // region lazy
    private void testGetChunkGenericLazy(byte[] values, int chunkSize, RowSet rowSet) {
        final BooleanArraySource sourceOrigin = forArray(values);
        final FormulaColumn formulaColumn = FormulaColumn.createFormulaColumn("Foo", "origin");
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        if (values.length > 0) {
            sequentialBuilder.appendRange(0, values.length - 1);
        }
        final TrackingRowSet fullRange = sequentialBuilder.build().toTracking();
        final Map<String, BooleanArraySource> oneAndOnly = new HashMap<>();
        oneAndOnly.put("origin", sourceOrigin);
        formulaColumn.initInputs(fullRange, oneAndOnly);
        final ColumnSource<?> source = formulaColumn.getDataView();
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ChunkSource.GetContext context = source.makeGetContext(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            final ObjectChunk<Boolean, ? extends Values> chunk = source.getChunk(context, okChunk).asObjectChunk();
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                assertEquals(chunk.get(i), source.getBoolean(it.nextLong()));
                pos++;
            }
        }
        assertEquals(pos, rowSet.size());
    }
    // endregion lazy

    @Test
    public void testGetChunkLazy() {
        final Random random = new Random(0);
        testGetChunkGenericLazy(new byte[0], 1, RowSetFactory.fromKeys());
        testGetChunkGenericLazy(new byte[0], 16, RowSetFactory.fromKeys());

        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0, 1));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4,  6));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 3, RowSetFactory.fromKeys(5, 6, 7));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 4, RowSetFactory.fromKeys(4, 5, 6, 7));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 16), 5, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 512), 4, RowSetFactory.fromKeys(254, 255, 256, 257));
        testGetChunkGenericLazy(ArrayGenerator.randomBooleans(random, 512), 5, RowSetFactory.fromKeys(254, 255, 256, 257, 258));

        for (int sourceSize = 512; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -2; v < 3; v += 2) {
                testForGivenSourceLazySize(random, sourceSize + v);
            }
        }

    }

    private void testForGivenSourceLazySize(Random random, int sourceSize) {
        final byte[] values = ArrayGenerator.randomBooleans(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testIndexSizeVariationsLazy(random, sourceSize, values, indexSize);
        }
    }

    private void testIndexSizeVariationsLazy(Random random, int sourceSize, byte[] values, int indexSize) {
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize - 1);
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize);
        testParameterChunkAndIndexLazy(random, sourceSize, values, indexSize + 1);
    }

    private void testParameterChunkAndIndexLazy(Random random, int sourceSize, byte[] values, int indexSize) {
        final RowSet rowSet = RowSetFactory.fromKeys(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 4) {
            testGetChunkGenericLazy(values, chunkSize, rowSet);
            testGetChunkGenericLazy(values, chunkSize + 1, rowSet);
            testGetChunkGenericLazy(values, chunkSize - 1, rowSet);
        }
    }


    private void testForGivenSourceSize(Random random, int sourceSize) {
        final byte[] values = ArrayGenerator.randomBooleans(random, sourceSize);
        final byte[] newValues = ArrayGenerator.randomBooleans(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 2) {
            testIndexSizeVariations(random, sourceSize, values, newValues, indexSize);
        }
    }

    private void testIndexSizeVariations(Random random, int sourceSize, byte[] values, byte[] newvalues, int indexSize) {
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize - 1);
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize);
        testParameterChunkAndIndex(random, sourceSize, values, newvalues, indexSize + 1);
    }

    private void testParameterChunkAndIndex(Random random, int sourceSize, byte[] values, byte[] newvalues, int indexSize) {
        final RowSet rowSet = RowSetFactory.fromKeys(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 2) {
            testGetChunkGeneric(values, newvalues, chunkSize, rowSet);
            testGetChunkGeneric(values, newvalues, chunkSize + 1, rowSet);
            testGetChunkGeneric(values, newvalues, chunkSize - 1, rowSet);
        }
    }

    private void testFillChunkGeneric(byte[] values, byte[] newValues, int chunkSize, RowSet rowSet) {
        final BooleanArraySource source;
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try {
            source = forArray(values);
            validateValuesWithFill(chunkSize, values, rowSet, source);
        } finally {
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try {
            updateFromArray(source, newValues);
            validateValuesWithFill(chunkSize, newValues, rowSet, source);
            validatePrevValuesWithFill(chunkSize, values, rowSet, source);
        } finally {
            UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
        }
    }

    private void validateValuesWithFill(int chunkSize, byte[] values, RowSet rowSet, BooleanArraySource source) {
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            source.fillChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getBoolean(idx), chunk.get(i));
                checkFromValues(values[(int)idx], chunk.get(i));
                pos++;
            }
        }
        assertEquals(pos, rowSet.size());
    }

    private void validatePrevValuesWithFill(int chunkSize, byte[] values, RowSet rowSet, BooleanArraySource source) {
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            source.fillPrevChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                checkFromSource(source.getPrevBoolean(idx), chunk.get(i));
                checkFromValues(values[(int)idx], chunk.get(i));
                pos++;
            }
        }
        assertEquals(pos, rowSet.size());
    }

    @Test
    public void testFillChunk() {
        final Random random = new Random(0);
        testFillChunkGeneric(new byte[0], new byte[0], 1, RowSetFactory.fromKeys());
        testFillChunkGeneric(new byte[0], new byte[0], 16, RowSetFactory.fromKeys());

        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0, 1));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4,  6));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 3, RowSetFactory.fromKeys(5, 6, 7));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 4, RowSetFactory.fromKeys(4, 5, 6, 7));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 16), ArrayGenerator.randomBooleans(random, 16), 5, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 512), ArrayGenerator.randomBooleans(random, 512), 4, RowSetFactory.fromKeys(254, 255, 256, 257));
        testFillChunkGeneric(ArrayGenerator.randomBooleans(random, 512), ArrayGenerator.randomBooleans(random, 512), 5, RowSetFactory.fromKeys(254, 255, 256, 257, 258));

        for (int sourceSize = 32; sourceSize < 8192; sourceSize *= 4) {
            for (int v = -4; v < 5; v += 2) {
                testSetForGivenSourceSize(random, sourceSize + v);
            }
        }
    }

    private void testSetForGivenSourceSize(Random random, int sourceSize) {
        final byte[] values = ArrayGenerator.randomBooleans(random, sourceSize);
        final byte[] newValues = ArrayGenerator.randomBooleans(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testFillChunkIndexSizeVariations(random, sourceSize, values, newValues, indexSize);
        }
    }

    private void testFillChunkIndexSizeVariations(Random random, int sourceSize, byte[] values, byte[] newValues, int indexSize) {
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize - 1);
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize);
        testParameterFillChunkAndIndex(random, sourceSize, values, newValues, indexSize + 1);
    }

    private void testParameterFillChunkAndIndex(Random random, int sourceSize, byte[] values, byte[] newValues, int indexSize) {
        final RowSet rowSet = RowSetFactory.fromKeys(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 2) {
            testFillChunkGeneric(values, newValues, chunkSize, rowSet);
            testFillChunkGeneric(values, newValues, chunkSize + 1, rowSet);
            testFillChunkGeneric(values, newValues, chunkSize - 1, rowSet);
        }
    }

    // region lazygeneric
    private void testFillChunkLazyGeneric(byte[] values, int chunkSize, RowSet rowSet) {
        final BooleanArraySource sourceOrigin = forArray(values);
        final FormulaColumn formulaColumn = FormulaColumn.createFormulaColumn("Foo", "origin");
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        if (values.length > 0) {
            sequentialBuilder.appendRange(0, values.length - 1);
        }
        final TrackingRowSet fullRange = sequentialBuilder.build().toTracking();
        final Map<String, BooleanArraySource> oneAndOnly = new HashMap<>();
        oneAndOnly.put("origin", sourceOrigin);
        formulaColumn.initInputs(fullRange, oneAndOnly);
        final ColumnSource source = formulaColumn.getDataView();
        final RowSequence.Iterator rsIterator = rowSet.getRowSequenceIterator();
        final RowSet.Iterator it = rowSet.iterator();
        final ColumnSource.FillContext context = source.makeFillContext(chunkSize);
        final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(chunkSize);
        long pos = 0;
        while (it.hasNext()) {
            assertTrue(rsIterator.hasMore());
            final RowSequence okChunk = rsIterator.getNextRowSequenceWithLength(chunkSize);
            source.fillChunk(context, chunk, okChunk);
            for (int i = 0; i < chunk.size(); i++) {
                assertTrue(it.hasNext());
                final long idx = it.nextLong();
                assertEquals(chunk.get(i), source.getBoolean(idx));
                pos++;
            }
        }
        assertEquals(pos, rowSet.size());
    }
    // endregion lazygeneric


    @Test
    public void testFillChunkLazy() {
        final Random random = new Random(0);
        testFillChunkLazyGeneric(new byte[0], 1, RowSetFactory.fromKeys());
        testFillChunkLazyGeneric(new byte[0], 16, RowSetFactory.fromKeys());

        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(0, 1));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4,  6));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 1, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 3, RowSetFactory.fromKeys(5, 6, 7));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 4, RowSetFactory.fromKeys(4, 5, 6, 7));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 16), 5, RowSetFactory.fromKeys(4, 5, 6, 7, 8));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 512), 4, RowSetFactory.fromKeys(254, 255, 256, 257));
        testFillChunkLazyGeneric(ArrayGenerator.randomBooleans(random, 512), 5, RowSetFactory.fromKeys(254, 255, 256, 257, 258));

        for (int sourceSize = 512; sourceSize < 4096; sourceSize *= 4) {
            for (int v = -2; v < 3; v++) {
                testSetForGivenLazySourceSize(random, sourceSize + v);
            }
        }
    }

    private void testSetForGivenLazySourceSize(Random random, int sourceSize) {
        final byte[] values = ArrayGenerator.randomBooleans(random, sourceSize);
        for (int indexSize = 2; indexSize < sourceSize; indexSize *= 4) {
            testFillChunkIndexSizeVariationsLazy(random, sourceSize, values, indexSize);
        }
    }

    private void testFillChunkIndexSizeVariationsLazy(Random random, int sourceSize, byte[] values, int indexSize) {
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize - 1);
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize);
        testParameterFillChunkAndIndexLazy(random, sourceSize, values, indexSize + 1);
    }

    private void testParameterFillChunkAndIndexLazy(Random random, int sourceSize, byte[] values, int indexSize) {
        final RowSet rowSet = RowSetFactory.fromKeys(indexDataGenerator(random, indexSize, .1, sourceSize / indexSize, sourceSize));
        for (int chunkSize = 2; chunkSize < sourceSize; chunkSize *= 4) {
            testFillChunkLazyGeneric(values, chunkSize, rowSet);
            testFillChunkLazyGeneric(values, chunkSize + 1, rowSet);
            testFillChunkLazyGeneric(values, chunkSize - 1, rowSet);
        }
    }


    // region fromvalues
    private void checkFromValues(String msg, byte fromValues, Boolean fromChunk) {
        assertEquals(msg, fromValues == BooleanUtils.NULL_BOOLEAN_AS_BYTE ? null : fromValues == BooleanUtils.TRUE_BOOLEAN_AS_BYTE, fromChunk);
    }

    private void checkFromValues(byte fromValues, Boolean fromChunk) {
        assertEquals(fromValues == BooleanUtils.NULL_BOOLEAN_AS_BYTE ? null : fromValues == BooleanUtils.TRUE_BOOLEAN_AS_BYTE, fromChunk);
    }
    // endregion fromvalues

    // region fromsource
    private void checkFromSource(String msg, Boolean fromSource, Boolean fromChunk) {
        assertEquals(msg, fromSource, fromChunk);
    }

    private void checkFromSource(Boolean fromSource, Boolean fromChunk) {
        assertEquals(fromSource, fromChunk);
    }
    // endregion fromsource

    @Test
    public void testSourceSink() {
        TestSourceSink.runTests(ChunkType.Boolean, size -> {
            final BooleanArraySource src = new BooleanArraySource();
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
        final BooleanArraySource source = new BooleanArraySource();
        source.ensureCapacity(arraySize);

        final byte[] data = ArrayGenerator.randomBooleans(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }
        // super hack
        final byte[] peekedBlock = (byte[])source.getBlock(0);

        try (RowSet srcKeys = RowSetFactory.fromRange(rangeStart, rangeEnd)) {
            try (RowSet destKeys = RowSetFactory.fromRange(rangeStart + 1, rangeEnd + 1)) {
                try (ChunkSource.GetContext srcContext = source.makeGetContext(arraySize)) {
                    try (ChunkSink.FillFromContext destContext = source.makeFillFromContext(arraySize)) {
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
        final BooleanArraySource src = new BooleanArraySource();
        src.startTrackingPrevValues();
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        try (final RowSet keys = RowSetFactory.empty();
             final WritableObjectChunk<Boolean, Values> chunk = WritableObjectChunk.makeWritableChunk(0)) {
            // Fill from an empty chunk
            src.fillFromChunkByKeys(keys, chunk);
        }
        // NullPointerException in BooleanSparseArraySource.commitUpdates()
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void testFillUnorderedWithNulls() {
        final BooleanArraySource source = new BooleanArraySource();

        final Random rng = new Random(438269476);
        final int arraySize = 100;
        source.ensureCapacity(arraySize);

        final byte[] data = ArrayGenerator.randomBooleans(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }

        final long [] keys = LongStream.concat(LongStream.of(RowSequence.NULL_ROW_KEY), LongStream.range(0, data.length - 1)).toArray();
        Shuffle.shuffleArray(rng, keys);

        try (final ChunkSource.FillContext ctx = source.makeFillContext(keys.length);
             final WritableObjectChunk<Boolean, Values> dest = WritableObjectChunk.makeWritableChunk(keys.length);
             final ResettableLongChunk<RowKeys> rlc = ResettableLongChunk.makeResettableChunk()) {
            rlc.resetFromTypedArray(keys, 0, keys.length);
            source.fillChunkUnordered(ctx, dest, rlc);
            assertEquals(keys.length, dest.size());
            for (int ii = 0; ii < keys.length; ++ii) {
                if (keys[ii] == RowSequence.NULL_ROW_KEY) {
                    assertEquals(NULL_BOOLEAN, dest.get(ii));
                } else {
                    checkFromValues(data[(int)keys[ii]], dest.get(ii));
                }
            }
        }
    }
}
