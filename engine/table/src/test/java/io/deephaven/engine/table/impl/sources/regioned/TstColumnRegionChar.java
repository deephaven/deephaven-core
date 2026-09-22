//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.JMockRule.Expectations;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

/**
 * Tests for {@link ColumnRegionChar}.
 */

public class TstColumnRegionChar {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionChar<Values>, Page.WithDefaults<Values> {

        @Override
        public void invalidate() {
            // NOTHNG TO DO
        }

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public char getChar(long elementIndex) {
            return (char) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            WritableCharChunk<? super Values> charDestination = destination.asWritableCharChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key -> {
                for (int i = 0; i < length; ++i) {
                    charDestination.set(size + i, (char) key);
                }
            });

            charDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionChar<Values>> {

        @Before
        public void setUp() throws Exception {
            SUT = ColumnRegionChar.createNull(Long.MAX_VALUE);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(0)/* EXTRA */);
            assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(1)/* EXTRA */);
            assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(Integer.MAX_VALUE)/* EXTRA */);
            assertEquals(QueryConstants.NULL_CHAR, SUT.getChar((1L << 40) - 2)/* EXTRA */);
            assertEquals(QueryConstants.NULL_CHAR, SUT.getChar(Long.MAX_VALUE)/* EXTRA */);
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionChar<Values>> {

        @Before
        public void setUp() throws Exception {
            // noinspection unchecked
            regionSupplier = jmock.mock(Supplier.class, "R1");
            jmock.checking(new Expectations() {
                {
                    oneOf(regionSupplier).get();
                    will(returnValue(new Identity()));
                }
            });
            SUT = new DeferredColumnRegionChar<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals((char) 8, SUT.getChar(8)/* EXTRA */);
            jmock.assertIsSatisfied();
            assertEquals((char) 272, SUT.getChar(272)/* EXTRA */);
            jmock.assertIsSatisfied();
        }
    }
}

