 package io.deephaven.db.v2.sources.chunkcolumnsource;

 import gnu.trove.list.array.TLongArrayList;
 import io.deephaven.db.tables.live.LiveTableMonitor;
 import io.deephaven.db.v2.sources.chunk.*;
 import io.deephaven.db.v2.utils.OrderedKeys;
 import io.deephaven.util.QueryConstants;
 import junit.framework.TestCase;
 import org.junit.After;
 import org.junit.Before;
 import org.junit.Test;

 public class TestChunkColumnSource {
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
     public void testSimple() {
         final WritableCharChunk<Attributes.Values> charChunk1 = WritableCharChunk.makeWritableChunk(1024);
         final WritableCharChunk<Attributes.Values> charChunk2 = WritableCharChunk.makeWritableChunk(1024);
         for (int ii = 0; ii < 1024; ++ii) {
             charChunk1.set(ii, (char)(1024 + ii));
             charChunk2.set(ii, (char)(2048 + ii));
         }

         final CharChunkColumnSource columnSource = new CharChunkColumnSource();
         columnSource.addChunk(charChunk1);
         columnSource.addChunk(charChunk2);

         TestCase.assertEquals(QueryConstants.NULL_CHAR, columnSource.getChar(-1));
         TestCase.assertEquals(QueryConstants.NULL_CHAR, columnSource.getChar(2048));

         for (int ii = 0; ii < 1024; ++ii) {
             TestCase.assertEquals(charChunk1.get(ii), columnSource.getChar(ii));
             TestCase.assertEquals(charChunk2.get(ii), columnSource.getChar(ii + 1024));
         }

         final WritableCharChunk<Attributes.Values> destChunk = WritableCharChunk.makeWritableChunk(2048);
         try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(2048)) {
             columnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(0, 2047));
             TestCase.assertEquals(2048, destChunk.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii), destChunk.get(ii));
                 TestCase.assertEquals(charChunk2.get(ii), destChunk.get(ii + 1024));
             }
         }

         try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(2048)) {
             columnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(2047, 2047));
             TestCase.assertEquals(1, destChunk.size());
             TestCase.assertEquals(charChunk2.get(1023), destChunk.get(0));
         }

         try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(2048)) {
             columnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(10, 20));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii + 10), destChunk.get(ii));
             }
         }

         try (final ChunkSource.FillContext fillContext = columnSource.makeFillContext(2048)) {
             columnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(1020, 1030));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii + 1020), destChunk.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(charChunk2.get(ii - 4), destChunk.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(0, 2047)).asCharChunk();
             TestCase.assertEquals(2048, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii), values.get(ii));
                 TestCase.assertEquals(charChunk2.get(ii), values.get(ii + 1024));
             }
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(0, 1023)).asCharChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(1024, 2047)).asCharChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(charChunk2.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(2047, 2047)).asCharChunk();
             TestCase.assertEquals(1, values.size());
             TestCase.assertEquals(charChunk2.get(1023), values.get(0));
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(10, 20)).asCharChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii + 10), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(2048)) {
             final CharChunk<? extends Attributes.Values> values = columnSource.getChunk(getContext, OrderedKeys.forRange(1020, 1030)).asCharChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(charChunk1.get(ii + 1020), values.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(charChunk2.get(ii - 4), values.get(ii));
             }
         }
     }


     @Test
     public void testShared() {
         final WritableLongChunk<Attributes.Values> longChunk1 = WritableLongChunk.makeWritableChunk(1024);
         final WritableLongChunk<Attributes.Values> longChunk2 = WritableLongChunk.makeWritableChunk(1024);

         final WritableDoubleChunk<Attributes.Values> doubleChunk1 = WritableDoubleChunk.makeWritableChunk(1024);
         final WritableDoubleChunk<Attributes.Values> doubleChunk2 = WritableDoubleChunk.makeWritableChunk(1024);

         for (int ii = 0; ii < 1024; ++ii) {
             longChunk1.set(ii, 1024 + ii);
             longChunk2.set(ii, 2048 + ii);
             doubleChunk1.set(ii, 1000.1 * ii);
             doubleChunk2.set(ii, 2000.2 * ii);
         }

         final TLongArrayList offsets = new TLongArrayList();

         final LongChunkColumnSource longColumnSource = new LongChunkColumnSource(offsets);
         longColumnSource.addChunk(longChunk1);
         longColumnSource.addChunk(longChunk2);

         final DoubleChunkColumnSource doubleColumnSource = new DoubleChunkColumnSource(offsets);
         doubleColumnSource.addChunk(doubleChunk1);
         doubleColumnSource.addChunk(doubleChunk2);

         TestCase.assertEquals(QueryConstants.NULL_LONG, longColumnSource.getLong(-1));
         TestCase.assertEquals(QueryConstants.NULL_LONG, longColumnSource.getLong(2048));
         TestCase.assertEquals(QueryConstants.NULL_DOUBLE, doubleColumnSource.getDouble(-1));
         TestCase.assertEquals(QueryConstants.NULL_DOUBLE, doubleColumnSource.getDouble(2048));

         for (int ii = 0; ii < 1024; ++ii) {
             TestCase.assertEquals(longChunk1.get(ii), longColumnSource.getLong(ii));
             TestCase.assertEquals(longChunk2.get(ii), longColumnSource.getLong(ii + 1024));
             TestCase.assertEquals(doubleChunk1.get(ii), doubleColumnSource.getDouble(ii));
             TestCase.assertEquals(doubleChunk2.get(ii), doubleColumnSource.getDouble(ii + 1024));
         }

         checkDoubles(doubleChunk1, doubleChunk2, doubleColumnSource);
         checkLongs(longChunk1, longChunk2, longColumnSource);
     }

     private void checkDoubles(WritableDoubleChunk<Attributes.Values> doubleChunk1, WritableDoubleChunk<Attributes.Values> doubleChunk2, DoubleChunkColumnSource doubleColumnSource) {
         final WritableDoubleChunk<Attributes.Values> destChunk = WritableDoubleChunk.makeWritableChunk(2048);
         try (final ChunkSource.FillContext doubleFillContext = doubleColumnSource.makeFillContext(2048)) {
             doubleColumnSource.fillChunk(doubleFillContext, destChunk, OrderedKeys.forRange(0, 2047));
             TestCase.assertEquals(2048, destChunk.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii), destChunk.get(ii));
                 TestCase.assertEquals(doubleChunk2.get(ii), destChunk.get(ii + 1024));
             }
         }

         try (final ChunkSource.FillContext fillContext = doubleColumnSource.makeFillContext(2048)) {
             doubleColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(2047, 2047));
             TestCase.assertEquals(1, destChunk.size());
             TestCase.assertEquals(doubleChunk2.get(1023), destChunk.get(0));
         }

         try (final ChunkSource.FillContext fillContext = doubleColumnSource.makeFillContext(2048)) {
             doubleColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(10, 20));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii + 10), destChunk.get(ii));
             }
         }

         try (final ChunkSource.FillContext fillContext = doubleColumnSource.makeFillContext(2048)) {
             doubleColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(1020, 1030));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii + 1020), destChunk.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(doubleChunk2.get(ii - 4), destChunk.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(0, 2047)).asDoubleChunk();
             TestCase.assertEquals(2048, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii), values.get(ii));
                 TestCase.assertEquals(doubleChunk2.get(ii), values.get(ii + 1024));
             }
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(0, 1023)).asDoubleChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(1024, 2047)).asDoubleChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(doubleChunk2.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(2047, 2047)).asDoubleChunk();
             TestCase.assertEquals(1, values.size());
             TestCase.assertEquals(doubleChunk2.get(1023), values.get(0));
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(10, 20)).asDoubleChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii + 10), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = doubleColumnSource.makeGetContext(2048)) {
             final DoubleChunk<? extends Attributes.Values> values = doubleColumnSource.getChunk(getContext, OrderedKeys.forRange(1020, 1030)).asDoubleChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(doubleChunk1.get(ii + 1020), values.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(doubleChunk2.get(ii - 4), values.get(ii));
             }
         }
     }

     private void checkLongs(WritableLongChunk<Attributes.Values> longChunk1, WritableLongChunk<Attributes.Values> longChunk2, LongChunkColumnSource longColumnSource) {
         final WritableLongChunk<Attributes.Values> destChunk = WritableLongChunk.makeWritableChunk(2048);
         try (final ChunkSource.FillContext longFillContext = longColumnSource.makeFillContext(2048)) {
             longColumnSource.fillChunk(longFillContext, destChunk, OrderedKeys.forRange(0, 2047));
             TestCase.assertEquals(2048, destChunk.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii), destChunk.get(ii));
                 TestCase.assertEquals(longChunk2.get(ii), destChunk.get(ii + 1024));
             }
         }

         try (final ChunkSource.FillContext fillContext = longColumnSource.makeFillContext(2048)) {
             longColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(2047, 2047));
             TestCase.assertEquals(1, destChunk.size());
             TestCase.assertEquals(longChunk2.get(1023), destChunk.get(0));
         }

         try (final ChunkSource.FillContext fillContext = longColumnSource.makeFillContext(2048)) {
             longColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(10, 20));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii + 10), destChunk.get(ii));
             }
         }

         try (final ChunkSource.FillContext fillContext = longColumnSource.makeFillContext(2048)) {
             longColumnSource.fillChunk(fillContext, destChunk, OrderedKeys.forRange(1020, 1030));
             TestCase.assertEquals(11, destChunk.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii + 1020), destChunk.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(longChunk2.get(ii - 4), destChunk.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(0, 2047)).asLongChunk();
             TestCase.assertEquals(2048, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii), values.get(ii));
                 TestCase.assertEquals(longChunk2.get(ii), values.get(ii + 1024));
             }
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(0, 1023)).asLongChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(1024, 2047)).asLongChunk();
             TestCase.assertEquals(1024, values.size());
             for (int ii = 0; ii < 1024; ++ii) {
                 TestCase.assertEquals(longChunk2.get(ii), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(2047, 2047)).asLongChunk();
             TestCase.assertEquals(1, values.size());
             TestCase.assertEquals(longChunk2.get(1023), values.get(0));
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(10, 20)).asLongChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 10; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii + 10), values.get(ii));
             }
         }

         try (final ChunkSource.GetContext getContext = longColumnSource.makeGetContext(2048)) {
             final LongChunk<? extends Attributes.Values> values = longColumnSource.getChunk(getContext, OrderedKeys.forRange(1020, 1030)).asLongChunk();
             TestCase.assertEquals(11, values.size());
             for (int ii = 0; ii <= 3; ++ii) {
                 TestCase.assertEquals(longChunk1.get(ii + 1020), values.get(ii));
             }
             for (int ii = 4; ii <= 10; ++ii) {
                 TestCase.assertEquals(longChunk2.get(ii - 4), values.get(ii));
             }
         }
     }
 }