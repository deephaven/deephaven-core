/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.util.datastructures.cache.OffsetLookupCache;
import org.junit.Test;

/**
 * Tests for {@link RegionedColumnSourceSymbol}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestOffsetCachedColumnSource extends BaseCachedJMockTestCase {

    private ColumnRegionObject<String, Attributes.Values> source;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        //noinspection unchecked
        source = mock(ColumnRegionObject.class);
    }

    private void doTest(final boolean soft) {
        checking(new Expectations(){{
            oneOf(source).getObject(0L); will(returnValue("A"));
            oneOf(source).getObject(1L); will(returnValue("B"));
            oneOf(source).getObject(2L); will(returnValue("C"));
        }});

        final RegionedColumnSourceSymbol<String, OffsetLookupCache<String, ChunkSource.FillContext>> SUT =
                RegionedColumnSourceSymbol.createWithLookupCache(String.class, soft);

        SUT.addRegionForUnitTests(source);

        assertEquals("A", SUT.get(0));
        assertEquals("B", SUT.get(1));
        assertEquals("C", SUT.get(2));
        assertEquals("A", SUT.get(0));
        assertEquals("B", SUT.get(1));
        assertEquals("C", SUT.get(2));
        assertEquals("A", SUT.getPrev(0));
        assertEquals("B", SUT.getPrev(1));
        assertEquals("C", SUT.getPrev(2));

        assertIsSatisfied();
    }

    @Test
    public void testGet() {
        doTest(true);
        doTest(false);
    }
}
