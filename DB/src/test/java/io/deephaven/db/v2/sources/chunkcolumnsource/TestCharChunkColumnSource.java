 package io.deephaven.db.v2.sources.chunkcolumnsource;

 import io.deephaven.db.tables.live.LiveTableMonitor;
 import io.deephaven.db.v2.sources.chunk.*;
 import io.deephaven.db.v2.utils.OrderedKeys;
 import io.deephaven.util.QueryConstants;
 import junit.framework.TestCase;
 import org.junit.After;
 import org.junit.Before;
 import org.junit.Test;

 public class TestCharChunkColumnSource {
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
 }