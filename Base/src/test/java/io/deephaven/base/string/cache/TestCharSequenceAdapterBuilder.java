/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import junit.framework.TestCase;

@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
public class TestCharSequenceAdapterBuilder extends TestCase {

    public void testBuilder() {
        final CharSequenceAdapterBuilder builder = new CharSequenceAdapterBuilder();

        assertTrue(CharSequenceUtils.contentEquals(builder, ""));
        assertEquals(builder.toString(), "");
        assertEquals(builder.hashCode(), "".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("hello"), "hello"));
        assertEquals(builder.toString(), "hello");
        assertEquals(builder.hashCode(), "hello".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("hello world again", 5, 6), "hello world"));
        assertEquals(builder.toString(), "hello world");
        assertEquals(builder.hashCode(), "hello world".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append(' '), "hello world "));
        assertEquals(builder.toString(), "hello world ");
        assertEquals(builder.hashCode(), "hello world ".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("again".toCharArray()), "hello world again"));
        assertEquals(builder.toString(), "hello world again");
        assertEquals(builder.hashCode(), "hello world again".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("!?!?".toCharArray(), 2, 1), "hello world again!"));
        assertEquals(builder.toString(), "hello world again!");
        assertEquals(builder.hashCode(), "hello world again!".hashCode());

        builder.clear();
        assertTrue(CharSequenceUtils.contentEquals(builder.clear(), ""));
        assertEquals(builder.toString(), "");
        assertEquals(builder.hashCode(), "".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("dancing".getBytes()), "dancing"));
        assertEquals(builder.toString(), "dancing");
        assertEquals(builder.hashCode(), "dancing".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append((byte) ' '), "dancing "));
        assertEquals(builder.toString(), "dancing ");
        assertEquals(builder.hashCode(), "dancing ".hashCode());

        assertTrue(CharSequenceUtils.contentEquals(builder.append("dancing with the stars!".getBytes(), 17, 5),
                "dancing stars"));
        assertEquals(builder.toString(), "dancing stars");
        assertEquals(builder.hashCode(), "dancing stars".hashCode());
    }
}
