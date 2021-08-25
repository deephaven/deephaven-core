/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
public class TestConcurrentUnboundedStringCache extends TestCase {

    @SuppressWarnings("unchecked")
    private static StringCacheTypeAdapter<? extends CompressedString>[] COMPRESSED_TYPE_ADAPTERS =
            (StringCacheTypeAdapter<? extends CompressedString>[]) new StringCacheTypeAdapter[] {
                    StringCacheTypeAdapterCompressedStringImpl.INSTANCE,
                    StringCacheTypeAdapterMappedCompressedStringImpl.INSTANCE
            };

    public void testStringPopulation() {
        final StringCache<String> debugCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, true);
        final StringCache<String> regularCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, false);
        for (StringCache cache : new StringCache[] {debugCache, regularCache}) {
            final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
            final String s2 = "abcd";
            assertSame(s1, cache.getCachedString(s1));
            assertSame(s1, cache.getCachedString(s2));
            assertEquals(s2, cache.getCachedString(s2));
        }
    }

    public void testCharSequencePopulation() {
        final StringCache<String> debugCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, true);
        final StringCache<String> regularCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, false);
        // noinspection unchecked
        for (StringCache<String> cache : new StringCache[] {debugCache, regularCache}) {
            final StringBuilder builder = new StringBuilder();

            final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
            assertSame(s1, cache.getCachedString(s1));
            builder.append('a').append('b').append('c').append('d');
            assertTrue(CharSequenceUtils.contentEquals(builder, s1));
            assertSame(s1, cache.getCachedString(builder));
            builder.setLength(0);
            assertSame(s1, cache.getCachedString((CharSequence) s1));

            builder.append("hello");
            assertTrue(CharSequenceUtils.contentEquals(builder, "hello"));
            final String s2 = cache.getCachedString(builder);
            assertTrue(CharSequenceUtils.contentEquals(builder, s2));
            assertSame(s2, cache.getCachedString("hello"));
            builder.setLength(0);
            assertSame(s2, cache.getCachedString((CharSequence) s2));
        }
    }

    public void testByteBufferPopulation() {
        final StringCache<String> debugCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, true);
        final StringCache<String> regularCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, false);
        // noinspection unchecked
        for (StringCache<String> cache : new StringCache[] {debugCache, regularCache}) {
            final ByteBufferCharSequenceAdapterImpl adapter = new ByteBufferCharSequenceAdapterImpl();

            final String s1 = new String(new char[] {'a', 'b', 'c', 'd'});
            assertSame(s1, cache.getCachedString(s1));
            adapter.set(ByteBuffer.wrap(new byte[] {'a', 'b', 'c', 'd'}), 0, 4);
            assertTrue(CharSequenceUtils.contentEquals(adapter, s1));
            assertSame(s1, cache.getCachedString(adapter));
            assertSame(s1, cache.getCachedString((CharSequence) adapter));
            adapter.clear();

            adapter.set(ByteBuffer.wrap(new byte[] {' ', 'h', 'e', 'l', 'l', 'o', 'w'}), 1, 5);
            assertTrue(CharSequenceUtils.contentEquals(adapter, "hello"));
            final String s2 = cache.getCachedString(adapter);
            assertTrue(CharSequenceUtils.contentEquals(adapter, s2));
            assertSame(s2, cache.getCachedString("hello"));
            assertSame(s2, cache.getCachedString((CharSequence) adapter));
            adapter.clear();
        }
    }

    public void testByteArrayPopulation() {
        final StringCache<String> debugCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, true);
        final StringCache<String> regularCache =
                new ConcurrentUnboundedStringCache<>(StringCacheTypeAdapterStringImpl.INSTANCE, 10, false);
        // noinspection unchecked
        for (StringCache<String> cache : new StringCache[] {debugCache, regularCache}) {
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
    }

    public void testStringPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> debugCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, true);
            final StringCache<? extends CompressedString> regularCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, false);
            // noinspection unchecked
            for (StringCache<? extends CompressedString> cache : new StringCache[] {debugCache, regularCache}) {
                final AbstractCompressedString s1 = typeAdapter.create(new String(new char[] {'a', 'b', 'c', 'd'}));
                final AbstractCompressedString s2 = typeAdapter.create("abcd");
                assertSame(s1, cache.getCachedString(s1));
                assertSame(s1, cache.getCachedString(s2));
                assertEquals(s2, cache.getCachedString(s2));
            }
        }
    }

    public void testByteBufferPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> debugCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, true);
            final StringCache<? extends CompressedString> regularCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, false);
            // noinspection unchecked
            for (StringCache<? extends CompressedString> cache : new StringCache[] {debugCache, regularCache}) {
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
    }

    public void testByteArrayPopulationCompressed() {
        for (StringCacheTypeAdapter<? extends CompressedString> typeAdapter : COMPRESSED_TYPE_ADAPTERS) {
            final StringCache<? extends CompressedString> debugCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, true);
            final StringCache<? extends CompressedString> regularCache =
                    new ConcurrentUnboundedStringCache<>(typeAdapter, 10, false);
            // noinspection unchecked
            for (StringCache<? extends CompressedString> cache : new StringCache[] {debugCache, regularCache}) {
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
}
