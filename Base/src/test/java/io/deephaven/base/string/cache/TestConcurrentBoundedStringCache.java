/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
public class TestConcurrentBoundedStringCache extends TestCase {

    @SuppressWarnings("unchecked")
    private static StringCacheTypeAdapter<? extends CompressedString>[] COMPRESSED_TYPE_ADAPTERS =
            (StringCacheTypeAdapter<? extends CompressedString>[]) new StringCacheTypeAdapter[] {
                    StringCacheTypeAdapterCompressedStringImpl.INSTANCE,
                    StringCacheTypeAdapterMappedCompressedStringImpl.INSTANCE
            };

    public void testStringPopulation() {
        final StringCache<String> cache =
                new ConcurrentBoundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, 2);
        final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
        final String s2 = "abcd";
        assertSame(s1, cache.getCachedString(s1));
        assertSame(s1, cache.getCachedString(s2));
        assertEquals(s2, cache.getCachedString(s2));
    }

    public void testByteBufferPopulation() {
        final StringCache<String> cache =
                new ConcurrentBoundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, 2);
        final ByteBufferCharSequenceAdapterImpl adapter = new ByteBufferCharSequenceAdapterImpl();

        final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
        assertSame(s1, cache.getCachedString(s1));
        adapter.set(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd'}), 0, 4);
        assertTrue(CharSequenceUtils.contentEquals(adapter, s1));
        assertSame(s1, cache.getCachedString(adapter));
        adapter.clear();

        adapter.set(ByteBuffer.wrap(new byte[] {' ', 'h', 'e', 'l', 'l', 'o', 'w'}), 1, 5);
        assertTrue(CharSequenceUtils.contentEquals(adapter, "hello"));
        final String s2 = cache.getCachedString(adapter);
        assertTrue(CharSequenceUtils.contentEquals(adapter, s2));
        assertSame(s2, cache.getCachedString("hello"));
        adapter.clear();
    }

    public void testByteArrayPopulation() {
        final StringCache<String> cache =
                new ConcurrentBoundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, 2);
        final ByteArrayCharSequenceAdapterImpl adapter = new ByteArrayCharSequenceAdapterImpl();

        final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
        assertSame(s1, cache.getCachedString(s1));
        adapter.set(new byte[] {'a', 'b', 'c', 'd'}, 0, 4);
        assertTrue(CharSequenceUtils.contentEquals(adapter, s1));
        assertSame(s1, cache.getCachedString(adapter));
        adapter.clear();

        adapter.set(new byte[] {' ', 'h', 'e', 'l', 'l', 'o', 'w'}, 1, 5);
        assertTrue(CharSequenceUtils.contentEquals(adapter, "hello"));
        final String s2 = cache.getCachedString(adapter);
        assertTrue(CharSequenceUtils.contentEquals(adapter, s2));
        assertSame(s2, cache.getCachedString("hello"));
        adapter.clear();
    }

    public void testStringPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> cache =
                    new ConcurrentBoundedStringCache<>(typeAdapter, 10, 2);
            final AbstractCompressedString s1 = typeAdapter.create(new String(new char[] {'a', 'b', 'c', 'd'}));
            final AbstractCompressedString s2 = typeAdapter.create("abcd");
            assertSame(s1, cache.getCachedString(s1));
            assertSame(s1, cache.getCachedString(s2));
            assertEquals(s2, cache.getCachedString(s2));
        }
    }

    public void testByteBufferPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> cache =
                    new ConcurrentBoundedStringCache<>(typeAdapter, 10, 2);
            final ByteBufferCharSequenceAdapterImpl adapter = new ByteBufferCharSequenceAdapterImpl();

            final AbstractCompressedString s1 = typeAdapter.create(new String(new char[] {'a', 'b', 'c', 'd'}));
            assertSame(s1, cache.getCachedString(s1));
            adapter.set(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd'}), 0, 4);
            assertTrue(CharSequenceUtils.contentEquals(adapter, s1));
            assertSame(s1, cache.getCachedString(adapter));
            adapter.clear();

            adapter.set(ByteBuffer.wrap(new byte[] {' ', 'h', 'e', 'l', 'l', 'o', 'w'}), 1, 5);
            assertTrue(CharSequenceUtils.contentEquals(adapter, "hello"));
            final AbstractCompressedString s2 = typeAdapter.create(cache.getCachedString(adapter));
            assertTrue(CharSequenceUtils.contentEquals(adapter, s2));
            assertSame(s2, cache.getCachedString("hello"));
            adapter.clear();
        }
    }

    public void testByteArrayPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> cache =
                    new ConcurrentBoundedStringCache<>(typeAdapter, 10, 2);
            final ByteArrayCharSequenceAdapterImpl adapter = new ByteArrayCharSequenceAdapterImpl();

            final AbstractCompressedString s1 = typeAdapter.create(new String(new char[] {'a', 'b', 'c', 'd'}));
            assertSame(s1, cache.getCachedString(s1));
            adapter.set(new byte[] {'a', 'b', 'c', 'd'}, 0, 4);
            assertTrue(CharSequenceUtils.contentEquals(adapter, s1));
            assertSame(s1, cache.getCachedString(adapter));
            adapter.clear();

            adapter.set(new byte[] {' ', 'h', 'e', 'l', 'l', 'o', 'w'}, 1, 5);
            assertTrue(CharSequenceUtils.contentEquals(adapter, "hello"));
            final AbstractCompressedString s2 = cache.getCachedString(adapter);
            assertTrue(CharSequenceUtils.contentEquals(adapter, s2));
            assertSame(s2, cache.getCachedString("hello"));
            adapter.clear();
        }
    }
}
