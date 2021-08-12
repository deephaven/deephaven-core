/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableCharChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionChar}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionChar {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionChar<Attributes.Values>, Page.WithDefaults<Attributes.Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public char getChar(long elementIndex) {
            return (char) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
            WritableCharChunk<? super Attributes.Values> charDestination = destination.asWritableCharChunk();
            int size = destination.size();
            int length = (int) orderedKeys.size();

            orderedKeys.forAllLongs(key ->
            {
                for (int i = 0; i < length; ++i) {
                    charDestination.set(size + i, (char) key);
                }
            });

            charDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionChar<Attributes.Values>> {

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

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionChar<Attributes.Values>> {

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

