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
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

/**
 * Tests for {@link ColumnRegionFloat}.
 */

public class TstColumnRegionFloat {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionFloat<Values>, Page.WithDefaults<Values> {

        @Override
        public void invalidate() {
            // NOTHNG TO DO
        }

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public float getFloat(long elementIndex) {
            return (float) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
                @NotNull RowSequence rowSequence) {
            WritableFloatChunk<? super Values> floatDestination = destination.asWritableFloatChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key -> {
                for (int i = 0; i < length; ++i) {
                    floatDestination.set(size + i, (float) key);
                }
            });

            floatDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionFloat<Values>> {

        @Before
        public void setUp() throws Exception {
            SUT = ColumnRegionFloat.createNull(Long.MAX_VALUE);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(0), .000001f);
            assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(1), .000001f);
            assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(Integer.MAX_VALUE), .000001f);
            assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat((1L << 40) - 2), .000001f);
            assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(Long.MAX_VALUE), .000001f);
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionFloat<Values>> {

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
            SUT = new DeferredColumnRegionFloat<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        @Test
        public void testGet() {
            assertEquals((float) 8, SUT.getFloat(8), .000001f);
            jmock.assertIsSatisfied();
            assertEquals((float) 272, SUT.getFloat(272), .000001f);
            jmock.assertIsSatisfied();
        }
    }
}
