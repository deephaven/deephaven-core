/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionChar}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionChar {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionChar<Values>, Page.WithDefaults<Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public char getChar(long elementIndex) {
            return (char) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            WritableCharChunk<? super Values> charDestination = destination.asWritableCharChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key ->
            {
                for (int i = 0; i < length; ++i) {
                    charDestination.set(size + i, (char) key);
                }
            });

            charDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionChar<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            SUT = ColumnRegionChar.createNull(Long.MAX_VALUE);
        }

        @Override
        public void testGet() {
            TestCase.assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(0));
            TestCase.assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(1));
            TestCase.assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(Integer.MAX_VALUE));
            TestCase.assertEquals(QueryConstants.NULL_CHAR, SUT.getChar((1L << 40) - 2));
            TestCase.assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionChar<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            //noinspection unchecked
            regionSupplier = mock(Supplier.class, "R1");
            checking(new Expectations() {{
                oneOf(regionSupplier).get();
                will(returnValue(new Identity()));
            }});
            SUT = new DeferredColumnRegionChar<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        public void testGet() {
            assertEquals((char) 8, SUT.getChar(8));
            assertIsSatisfied();
            assertEquals((char) 272, SUT.getChar(272));
            assertIsSatisfied();
        }
    }
}

