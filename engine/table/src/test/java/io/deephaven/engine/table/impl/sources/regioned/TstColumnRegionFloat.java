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
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionFloat}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionFloat {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionFloat<Values>, Page.WithDefaults<Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public float getFloat(long elementIndex) {
            return (float) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            WritableFloatChunk<? super Values> floatDestination = destination.asWritableFloatChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key ->
            {
                for (int i = 0; i < length; ++i) {
                    floatDestination.set(size + i, (float) key);
                }
            });

            floatDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionFloat<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            SUT = ColumnRegionFloat.createNull(Long.MAX_VALUE);
        }

        @Override
        public void testGet() {
            TestCase.assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(0));
            TestCase.assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(1));
            TestCase.assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(Integer.MAX_VALUE));
            TestCase.assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat((1L << 40) - 2));
            TestCase.assertEquals(QueryConstants.NULL_FLOAT, SUT.getFloat(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionFloat<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            //noinspection unchecked
            regionSupplier = mock(Supplier.class, "R1");
            checking(new Expectations() {{
                oneOf(regionSupplier).get();
                will(returnValue(new Identity()));
            }});
            SUT = new DeferredColumnRegionFloat<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        public void testGet() {
            assertEquals((float) 8, SUT.getFloat(8));
            assertIsSatisfied();
            assertEquals((float) 272, SUT.getFloat(272));
            assertIsSatisfied();
        }
    }
}

