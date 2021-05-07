/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Tests for {@link RegionedColumnSourceObjectWithDictionary}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestRegionedColumnSourceObjectReferenced extends TstRegionedColumnSourceReferenced<String, Attributes.DictionaryKeys, ColumnRegionInt<Attributes.DictionaryKeys>, String,
        Attributes.Values,
        ColumnRegionObject<String, Attributes.Values>> {

    private final long TEST_KEY_1 = 4;
    private final long TEST_KEY_2 = 4 * RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS + 100;

    public TestRegionedColumnSourceObjectReferenced() {
        super(ColumnRegionInt.class, ColumnRegionObject.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        SUT_R = new RegionedColumnSourceObject.AsValues<>(String.class, null);
        SUT = new RegionedColumnSourceObjectWithDictionary<>(String.class, SUT_R);

        assertEquals(String.class, SUT.getType());
    }

    @Override
    public void testGet() {
        fillRegions();

        checking(new Expectations() {{
            oneOf(cr[0]).getReferencedRegion(); will(returnValue(cr_n[0]));
            oneOf(cr_n[0]).getInt(TEST_KEY_1); will(returnValue(NULL_INT));
        }});
        assertNull(SUT.get(TEST_KEY_1));

        checking(new Expectations() {{
            oneOf(cr[4]).getReferencedRegion(); will(returnValue(cr_n[4]));
            oneOf(cr_n[4]).getInt(TEST_KEY_2); will(returnValue(1));
            oneOf(cr_r[4]).getObject(1); will(returnValue("HELLO"));
        }});
        assertEquals("HELLO", SUT.get(TEST_KEY_2));
    }

    @Override
    public void testGetPrev() {
        fillRegions();

        checking(new Expectations() {{
            oneOf(cr[0]).getReferencedRegion(); will(returnValue(cr_n[0]));
            oneOf(cr_n[0]).getInt(TEST_KEY_1); will(returnValue(NULL_INT));
        }});
        assertNull(SUT.getPrev(TEST_KEY_1));

        checking(new Expectations() {{
            oneOf(cr[4]).getReferencedRegion(); will(returnValue(cr_n[4]));
            oneOf(cr_n[4]).getInt(TEST_KEY_2); will(returnValue(2));
            oneOf(cr_r[4]).getObject(2); will(returnValue("WORLD"));
        }});
        assertEquals("WORLD", SUT.getPrev(TEST_KEY_2));
    }
}
