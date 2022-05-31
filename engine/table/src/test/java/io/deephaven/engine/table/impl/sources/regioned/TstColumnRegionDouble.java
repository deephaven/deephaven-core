/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TstColumnRegionChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionDouble}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionDouble {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionDouble<Values>, Page.WithDefaults<Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public double getDouble(long elementIndex) {
            return (double) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            WritableDoubleChunk<? super Values> doubleDestination = destination.asWritableDoubleChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key ->
            {
                for (int i = 0; i < length; ++i) {
                    doubleDestination.set(size + i, (double) key);
                }
            });

            doubleDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionDouble<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            SUT = ColumnRegionDouble.createNull(Long.MAX_VALUE);
        }

        @Override
        public void testGet() {
            TestCase.assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(0));
            TestCase.assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(1));
            TestCase.assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(Integer.MAX_VALUE));
            TestCase.assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble((1L << 40) - 2));
            TestCase.assertEquals(QueryConstants.NULL_DOUBLE, SUT.getDouble(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionDouble<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            //noinspection unchecked
            regionSupplier = mock(Supplier.class, "R1");
            checking(new Expectations() {{
                oneOf(regionSupplier).get();
                will(returnValue(new Identity()));
            }});
            SUT = new DeferredColumnRegionDouble<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        public void testGet() {
            assertEquals((double) 8, SUT.getDouble(8));
            assertIsSatisfied();
            assertEquals((double) 272, SUT.getDouble(272));
            assertIsSatisfied();
        }
    }
}

