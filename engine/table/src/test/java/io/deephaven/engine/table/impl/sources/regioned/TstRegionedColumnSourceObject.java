/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

/**
* Base class for tests of {@link RegionedColumnSourceObject} implementations.
*/
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public abstract class TstRegionedColumnSourceObject<DATA_TYPE> extends TstRegionedColumnSourcePrimitive<DATA_TYPE, Values,
        ColumnRegionObject<DATA_TYPE, Values>> {

    TstRegionedColumnSourceObject(Value<?>[] values) {
        super(ColumnRegionObject.class);
        this.values = values;
    }

    static class Value<DATA_TYPE> {

        final DATA_TYPE decoded;
        final byte[] bytes;
        final long lengthAfterPrevious;

        Value(DATA_TYPE decoded, byte[] bytes, long lengthAfterPrevious) {
            this.decoded = decoded;
            this.bytes = bytes;
            this.lengthAfterPrevious = lengthAfterPrevious;
        }
    }

    protected final Value<?>[] values;
    private static final Value<?> NULL_VALUE = new Value<>(null, new byte[0], 0L);

    private void assertLookup(final long elementIndex,
                              final int expectedRegionIndex,
                              final Value<?> output,
                              final boolean prev) {
        checking(new Expectations() {{
            atMost(1).of(cr[expectedRegionIndex]).getObject(with(elementIndex));
            will(returnValue(output.decoded));
            if (elementIndex != 0) {
                atMost(1).of(cr[expectedRegionIndex]).getObject(with(elementIndex - 1));
                will(returnValue(output.decoded));
            }
        }});
        assertEquals(output.decoded, prev ? SUT.getPrev(elementIndex) : SUT.get(elementIndex));
        assertIsSatisfied();
    }

    @Test
    public void testGet() {
        fillRegions();

        assertLookup(0L, 0, values[0], false);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, values[1], false);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, values[2], false);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, values[3], false);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, values[4], false);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, values[5], false);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, values[6], false);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, values[7], false);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, values[8], false);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, values[9], false);

        assertLookup(RegionedColumnSource.getLastRowKey(9) - 3, 9, NULL_VALUE, false);
    }

    @Test
    public void testGetPrev() {
        fillRegions();

        assertLookup(0L, 0, values[0], true);
        assertLookup(RegionedColumnSource.getLastRowKey(0), 0, values[1], true);

        assertLookup(RegionedColumnSource.getFirstRowKey(1) + 1, 1, values[2], true);
        assertLookup(RegionedColumnSource.getLastRowKey(1) - 1, 1, values[3], true);

        assertLookup(RegionedColumnSource.getFirstRowKey(4) + 2, 4, values[4], true);
        assertLookup(RegionedColumnSource.getLastRowKey(4) - 2, 4, values[5], true);

        assertLookup(RegionedColumnSource.getFirstRowKey(8) + 3, 8, values[6], true);
        assertLookup(RegionedColumnSource.getLastRowKey(8) - 3, 8, values[7], true);

        assertLookup(RegionedColumnSource.getFirstRowKey(9) + 4, 9, values[8], true);
        assertLookup(RegionedColumnSource.getLastRowKey(9) - 4, 9, values[9], true);

        assertLookup(RegionedColumnSource.getLastRowKey(9) - 3, 9, NULL_VALUE, true);
    }
}
