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
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionLong}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionLong {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionLong<Values>, Page.WithDefaults<Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getLong(long elementIndex) {
            return (long) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            WritableLongChunk<? super Values> longDestination = destination.asWritableLongChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key ->
            {
                for (int i = 0; i < length; ++i) {
                    longDestination.set(size + i, (long) key);
                }
            });

            longDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionLong<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            SUT = ColumnRegionLong.createNull(Long.MAX_VALUE);
        }

        @Override
        public void testGet() {
            TestCase.assertEquals(QueryConstants.NULL_LONG, SUT.getLong(0));
            TestCase.assertEquals(QueryConstants.NULL_LONG, SUT.getLong(1));
            TestCase.assertEquals(QueryConstants.NULL_LONG, SUT.getLong(Integer.MAX_VALUE));
            TestCase.assertEquals(QueryConstants.NULL_LONG, SUT.getLong((1L << 40) - 2));
            TestCase.assertEquals(QueryConstants.NULL_LONG, SUT.getLong(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionLong<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            //noinspection unchecked
            regionSupplier = mock(Supplier.class, "R1");
            checking(new Expectations() {{
                oneOf(regionSupplier).get();
                will(returnValue(new Identity()));
            }});
            SUT = new DeferredColumnRegionLong<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        public void testGet() {
            assertEquals((long) 8, SUT.getLong(8));
            assertIsSatisfied();
            assertEquals((long) 272, SUT.getLong(272));
            assertIsSatisfied();
        }
    }
}

