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
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

/**
 * Tests for {@link ColumnRegionDouble}.
 */

public class TstColumnRegionDouble {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionDouble<Values>, Page.WithDefaults<Values> {

        @Override
        public void invalidate() {
            // NOTHNG TO DO
        }

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public double getDouble(long elementIndex) {
            return (double) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            WritableDoubleChunk<? super Values> doubleDestination = destination.asWritableDoubleChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key -> {
                for (int i = 0; i < length; ++i) {
                    doubleDestination.set(size + i, (double) key);
                }
            });

            doubleDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionDouble<Values>> {

        @Before
        public void setUp() throws Exception {
            SUT = ColumnRegionDouble.createNull(Long.MAX_VALUE);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(0), .000001f);
            assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(1), .000001f);
            assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(Integer.MAX_VALUE), .000001f);
            assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble((1L << 40) - 2), .000001f);
            assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(Long.MAX_VALUE), .000001f);
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionDouble<Values>> {

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
            SUT = new DeferredColumnRegionDouble<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals((double) 8, SUT.getDouble(8), .000001f);
            jmock.assertIsSatisfied();
            assertEquals((double) 272, SUT.getDouble(272), .000001f);
            jmock.assertIsSatisfied();
        }
    }
}
