//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TstColumnRegionChar and run "./gradlew replicateRegionAndRegionedSourceTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.JMockRule.Expectations;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

/**
 * Tests for {@link ColumnRegionShort}.
 */

public class TstColumnRegionShort {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionShort<Values>, Page.WithDefaults<Values> {

        @Override
        public void invalidate() {
            // NOTHNG TO DO
        }

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public short getShort(long elementIndex) {
            return (short) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            WritableShortChunk<? super Values> shortDestination = destination.asWritableShortChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key -> {
                for (int i = 0; i < length; ++i) {
                    shortDestination.set(size + i, (short) key);
                }
            });

            shortDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionShort<Values>> {

        @Before
        public void setUp() throws Exception {
            SUT = ColumnRegionShort.createNull(Long.MAX_VALUE);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals(QueryConstants.NULL_SHORT, SUT.getShort(0));
            assertEquals(QueryConstants.NULL_SHORT, SUT.getShort(1));
            assertEquals(QueryConstants.NULL_SHORT, SUT.getShort(Integer.MAX_VALUE));
            assertEquals(QueryConstants.NULL_SHORT, SUT.getShort((1L << 40) - 2));
            assertEquals(QueryConstants.NULL_SHORT, SUT.getShort(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionShort<Values>> {

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
            SUT = new DeferredColumnRegionShort<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals((short) 8, SUT.getShort(8));
            jmock.assertIsSatisfied();
            assertEquals((short) 272, SUT.getShort(272));
            jmock.assertIsSatisfied();
        }
    }
}
