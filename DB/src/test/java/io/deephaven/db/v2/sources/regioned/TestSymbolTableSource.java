package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.string.cache.*;
import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Unit tests for {@link SymbolTableSource}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestSymbolTableSource extends BaseCachedJMockTestCase {

    private <T extends CharSequence> void doTest(@NotNull final StringCache<T> stringCache, @NotNull final StringCacheTypeAdapter<T> adapter) {
        //noinspection unchecked
        final ColumnRegionObject<String, Attributes.Values> source = mock(ColumnRegionObject.class, stringCache.getType().getName());

        checking(new BaseCachedJMockTestCase.Expectations(){{
            allowing(source).getObject(0L); will(returnValue(adapter.create("A")));
            allowing(source).getObject(1L); will(returnValue(adapter.create("B")));
            allowing(source).getObject(2L); will(returnValue(adapter.create("C")));
        }});

        ColumnDefinition.fromGenericType("TestColumn", stringCache.getType());
        final RegionedColumnSourceSymbol<T, ?> SUT = RegionedColumnSourceSymbol.createWithoutCache(stringCache.getType());

        SUT.addRegionForUnitTests(source);

        assertEquals(adapter.create("A"), SUT.get(0));
        assertEquals(adapter.create("B"), SUT.get(1));
        assertEquals(adapter.create("C"), SUT.get(2));
        assertEquals(adapter.create("A"), SUT.get(0));
        assertEquals(adapter.create("B"), SUT.get(1));
        assertEquals(adapter.create("C"), SUT.get(2));
        assertEquals(adapter.create("A"), SUT.getPrev(0));
        assertEquals(adapter.create("B"), SUT.getPrev(1));
        assertEquals(adapter.create("C"), SUT.getPrev(2));

        assertIsSatisfied();
    }

    @Test
    public void testGet() {
        doTest(AlwaysCreateStringCache.STRING_INSTANCE, StringCacheTypeAdapterStringImpl.INSTANCE);
        doTest(AlwaysCreateStringCache.COMPRESSED_STRING_INSTANCE, StringCacheTypeAdapterCompressedStringImpl.INSTANCE);
        doTest(AlwaysCreateStringCache.MAPPED_COMPRESSED_STRING_INSTANCE, StringCacheTypeAdapterMappedCompressedStringImpl.INSTANCE);
    }
}
