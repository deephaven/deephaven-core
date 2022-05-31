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
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.Page;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Tests for {@link ColumnRegionInt}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TstColumnRegionInt {

    @SuppressWarnings("unused")
    static class Identity implements ColumnRegionInt<Values>, Page.WithDefaults<Values> {

        @Override
        public long mask() {
            return Long.MAX_VALUE;
        }

        @Override
        public int getInt(long elementIndex) {
            return (int) elementIndex;
        }

        @Override
        public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            WritableIntChunk<? super Values> intDestination = destination.asWritableIntChunk();
            int size = destination.size();
            int length = (int) rowSequence.size();

            rowSequence.forAllRowKeys(key ->
            {
                for (int i = 0; i < length; ++i) {
                    intDestination.set(size + i, (int) key);
                }
            });

            intDestination.setSize(size + length);
        }
    }

    public static class TestNull extends TstColumnRegionPrimative<ColumnRegionInt<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            SUT = ColumnRegionInt.createNull(Long.MAX_VALUE);
        }

        @Override
        public void testGet() {
            TestCase.assertEquals(QueryConstants.NULL_INT, SUT.getInt(0));
            TestCase.assertEquals(QueryConstants.NULL_INT, SUT.getInt(1));
            TestCase.assertEquals(QueryConstants.NULL_INT, SUT.getInt(Integer.MAX_VALUE));
            TestCase.assertEquals(QueryConstants.NULL_INT, SUT.getInt((1L << 40) - 2));
            TestCase.assertEquals(QueryConstants.NULL_INT, SUT.getInt(Long.MAX_VALUE));
        }
    }

    public static class TestDeferred extends TstColumnRegionPrimative.Deferred<ColumnRegionInt<Values>> {

        @Override
        public void setUp() throws Exception {
            super.setUp();
            //noinspection unchecked
            regionSupplier = mock(Supplier.class, "R1");
            checking(new Expectations() {{
                oneOf(regionSupplier).get();
                will(returnValue(new Identity()));
            }});
            SUT = new DeferredColumnRegionInt<>(Long.MAX_VALUE, regionSupplier);
        }

        @Override
        public void testGet() {
            assertEquals((int) 8, SUT.getInt(8));
            assertIsSatisfied();
            assertEquals((int) 272, SUT.getInt(272));
            assertIsSatisfied();
        }
    }
}

