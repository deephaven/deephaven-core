/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Arrays;

@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
public class TestCompressedString extends TestCase {

    private static CompressedString cs() {
        return new CompressedString();
    }

    private static CompressedString cs(final String string) {
        return new CompressedString(string);
    }

    private static CompressedString cs(final char[] data) {
        return new CompressedString(data);
    }

    private static CompressedString cs(char[] data, int offset, int length) {
        return new CompressedString(data, offset, length);
    }

    private static CompressedString cs(final byte[] data) {
        return new CompressedString(data);
    }

    private static CompressedString cs(byte[] data, int offset, int length) {
        return new CompressedString(data, offset, length);
    }

    private static CompressedString cs(final ByteBuffer data) {
        return new CompressedString(data);
    }

    private static CompressedString cs(ByteBuffer data, int offset, int length) {
        return new CompressedString(data, offset, length);
    }

    public void testContentEquality() {
        CompressedString cs;

        assertTrue(CharSequenceUtils.contentEquals(cs = cs(""), ""));
        assertEquals(cs.toString(), "");
        assertEquals(cs.hashCode(), "".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs("hello"), "hello"));
        assertEquals(cs.toString(), "hello");
        assertEquals(cs.hashCode(), "hello".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "hello".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs("again".toCharArray()), "again"));
        assertEquals(cs.toString(), "again");
        assertEquals(cs.hashCode(), "again".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "again".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs("!?!?".toCharArray(), 2, 1), "!"));
        assertEquals(cs.toString(), "!");
        assertEquals(cs.hashCode(), "!".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "!".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs("dancing".getBytes()), "dancing"));
        assertEquals(cs.toString(), "dancing");
        assertEquals(cs.hashCode(), "dancing".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "dancing".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs("dancing with the stars!".getBytes(), 17, 5), "stars"));
        assertEquals(cs.toString(), "stars");
        assertEquals(cs.hashCode(), "stars".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "stars".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs(ByteBuffer.wrap("happy".getBytes())), "happy"));
        assertEquals(cs.toString(), "happy");
        assertEquals(cs.hashCode(), "happy".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "happy".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(cs = cs(ByteBuffer.wrap("hedgehog!".getBytes()), 5, 3), "hog"));
        assertEquals(cs.toString(), "hog");
        assertEquals(cs.hashCode(), "hog".hashCode());
        assertTrue(Arrays.equals(cs.getData(), "hog".getBytes()));
    }

    public void testEquals() {
        assertTrue(cs().equals(cs("")));
        assertTrue(cs("abc").equals(cs("abc")));
        assertTrue(cs("aBc").equals(cs("aBc")));
        assertTrue(cs("abcD").equals(cs("abcD")));
        assertTrue(cs("ABC").equals(cs("ABC")));
        assertTrue(cs("123").equals(cs("123")));

        assertFalse(cs().equals(cs("0")));
        assertFalse(cs("abc").equals(cs("abD")));
        assertFalse(cs("abc").equals(cs("aBcd")));
        assertFalse(cs("aBcz").equals(cs("abc")));
        assertFalse(cs("abcZ").equals(cs("ABC")));
        assertFalse(cs("0BC").equals(cs("abc")));
    }

    public void testCompareTo() {
        assertTrue(cs().compareTo("") == 0);
        assertTrue(cs("abc").compareTo("abc") == 0);
        assertTrue(cs("abc").compareTo("aBc") > 0);
        assertTrue(cs("aBc").compareTo("abc") < 0);
        assertTrue(cs("abc").compareTo("ABC") > 0);
        assertTrue(cs("ABC").compareTo("abc") < 0);

        assertTrue(cs("ABC").compareTo("abcd") < 0);
        assertTrue(cs("ABCD").compareTo("abc") < 0);
        assertTrue(cs("ABC").compareTo("bbc") < 0);
        assertTrue(cs("BBC").compareTo("abc") < 0);

        assertTrue(cs("abcd").compareTo("ABC") > 0);
        assertTrue(cs("abc").compareTo("ABCD") > 0);
        assertTrue(cs("bbc").compareTo("ABC") > 0);
        assertTrue(cs("abc").compareTo("BBC") > 0);
    }

    public void testIsEmpty() {
        assertTrue(cs().isEmpty());
        assertTrue(cs("").isEmpty());
        assertFalse(cs("a").isEmpty());
        assertFalse(cs("0123456789").isEmpty());
    }

    public void testLength() {
        assertEquals(cs().length(), 0);
        assertEquals(cs("").length(), 0);
        assertEquals(cs("a").length(), 1);
        assertEquals(cs("0123456789").length(), 10);
    }

    public void testCharAt() {
        try {
            cs().charAt(0);
            fail("Expected exception");
        } catch (IndexOutOfBoundsException ignored) {
        }

        assertEquals(cs("a").charAt(0), 'a');
        assertEquals(cs("abc").charAt(0), 'a');
        assertEquals(cs("abc").charAt(1), 'b');
        assertEquals(cs("abc").charAt(2), 'c');

        assertEquals(cs("a").codePointAt(0), "a".codePointAt(0));
        assertEquals(cs("abc").codePointAt(0), "abc".codePointAt(0));
        assertEquals(cs("abc").codePointAt(1), "abc".codePointAt(1));
        assertEquals(cs("abc").codePointAt(2), "abc".codePointAt(2));

        assertEquals(cs("a").codePointBefore(1), "a".codePointBefore(1));
        assertEquals(cs("abc").codePointBefore(1), "abc".codePointBefore(1));
        assertEquals(cs("abc").codePointBefore(2), "abc".codePointBefore(2));
        assertEquals(cs("abc").codePointBefore(3), "abc".codePointBefore(3));

        assertEquals(cs("a").codePointCount(0, 1), "a".codePointCount(0, 1));
        assertEquals(cs("abc").codePointCount(0, 3), "abc".codePointCount(0, 3));
        assertEquals(cs("abc").codePointCount(0, 2), "abc".codePointCount(0, 2));
        assertEquals(cs("abc").codePointCount(1, 2), "abc".codePointCount(1, 2));
    }

    public void testEqualsIgnoreCase() {
        assertTrue(cs().equalsIgnoreCase(""));
        assertTrue(cs("abc").equalsIgnoreCase("abc"));
        assertTrue(cs("abc").equalsIgnoreCase("aBc"));
        assertTrue(cs("aBc").equalsIgnoreCase("abc"));
        assertTrue(cs("abc").equalsIgnoreCase("ABC"));
        assertTrue(cs("ABC").equalsIgnoreCase("abc"));

        assertFalse(cs().equalsIgnoreCase("0"));
        assertFalse(cs("abc").equalsIgnoreCase("abD"));
        assertFalse(cs("abc").equalsIgnoreCase("aBcd"));
        assertFalse(cs("aBcz").equalsIgnoreCase("abc"));
        assertFalse(cs("abcZ").equalsIgnoreCase("ABC"));
        assertFalse(cs("0BC").equalsIgnoreCase("abc"));
    }

    public void testCompareToIgnoreCase() {
        assertTrue(cs().compareToIgnoreCase("") == 0);
        assertTrue(cs("abc").compareToIgnoreCase("abc") == 0);
        assertTrue(cs("abc").compareToIgnoreCase("aBc") == 0);
        assertTrue(cs("aBc").compareToIgnoreCase("abc") == 0);
        assertTrue(cs("abc").compareToIgnoreCase("ABC") == 0);
        assertTrue(cs("ABC").compareToIgnoreCase("abc") == 0);

        assertTrue(cs("ABC").compareToIgnoreCase("abcd") < 0);
        assertTrue(cs("ABCD").compareToIgnoreCase("abc") > 0);
        assertTrue(cs("ABC").compareToIgnoreCase("bbc") < 0);
        assertTrue(cs("BBC").compareToIgnoreCase("abc") > 0);
    }

    public void testRegionMatches() {
        assertTrue(cs("abcdefghij").regionMatches(false, 0, "abc", 0, 3));
        assertTrue(cs("abcdefghij").regionMatches(false, 1, "bcd", 0, 3));
        assertTrue(cs("abcdefghij").regionMatches(false, 2, "0cd3", 1, 2));

        assertTrue(cs("aBcDeFgHiJ").regionMatches(true, 0, "AbC", 0, 3));
        assertTrue(cs("AbCdEfGhIj").regionMatches(true, 1, "BcD", 0, 3));
        assertTrue(cs("abCdeFghIj").regionMatches(true, 2, "0cD3", 1, 2));

        assertFalse(cs("abcdefghij").regionMatches(false, 0, "Abc", 0, 3));
        assertFalse(cs("abcdefghij").regionMatches(false, 1, "bCd", 0, 3));
        assertFalse(cs("abcdefghij").regionMatches(false, 2, "0ce3", 1, 2));

        assertFalse(cs("aBcDeFgHiJ").regionMatches(true, 0, "AbD", 0, 3));
        assertFalse(cs("AbCdEfGhIj").regionMatches(true, 1, "BeD", 0, 3));
        assertFalse(cs("abCdeFghIj").regionMatches(true, 2, "0cE3", 1, 2));
    }

    public void testStartsWith() {
        assertTrue(cs("abc").startsWith("ab"));
        assertTrue(cs("abc").startsWith("abc"));
        assertFalse(cs("abc").startsWith("bc"));
        assertFalse(cs("abc").startsWith("abd"));

        assertTrue(cs("abcdefg").startsWith("defg", 3));
        assertTrue(cs("abcdefg").startsWith("bcd", 1));
        assertFalse(cs("abcdefg").startsWith("cd", 1));
        assertFalse(cs("abcdefg").startsWith("bce", 1));
    }

    public void testEndsWith() {
        assertTrue(cs("0123").endsWith("3"));
        assertTrue(cs("0123").endsWith("23"));
        assertTrue(cs("0123").endsWith("0123"));

        assertFalse(cs("0123").endsWith("012"));
        assertFalse(cs("0123").endsWith("4"));
    }

    public void testIndexOfChar() {
        assertEquals(cs("abcdef").indexOf('a'), "abcdef".indexOf('a'));
        assertEquals(cs("abcdef").indexOf('f'), "abcdef".indexOf('f'));
        assertEquals(cs("abcdef").indexOf('d'), "abcdef".indexOf('d'));

        assertEquals(cs("abcdef").indexOf('a', 1), "abcdef".indexOf('a', 1));
        assertEquals(cs("abcdef").indexOf('b', 1), "abcdef".indexOf('b', 1));
        assertEquals(cs("abcdef").indexOf('d', 2), "abcdef".indexOf('d', 2));
        assertEquals(cs("abcdef").indexOf('f', 3), "abcdef".indexOf('f', 3));

        assertEquals(cs("abcdef").lastIndexOf('a'), "abcdef".lastIndexOf('a'));
        assertEquals(cs("abcdef").lastIndexOf('f'), "abcdef".lastIndexOf('f'));
        assertEquals(cs("abcdef").lastIndexOf('d'), "abcdef".lastIndexOf('d'));
        assertEquals(cs("aaabbb").lastIndexOf('a'), "aaabbb".lastIndexOf('a'));
        assertEquals(cs("aaabbb").lastIndexOf('f'), "aaabbb".lastIndexOf('f'));
        assertEquals(cs("aaabbb").lastIndexOf('b'), "aaabbb".lastIndexOf('b'));

        assertEquals(cs("abcdef").lastIndexOf('a', 1), "abcdef".lastIndexOf('a', 1));
        assertEquals(cs("abcdef").lastIndexOf('f', 2), "abcdef".lastIndexOf('f', 2));
        assertEquals(cs("abcdef").lastIndexOf('d', 3), "abcdef".lastIndexOf('d', 3));
        assertEquals(cs("aaabbb").lastIndexOf('a', 1), "aaabbb".lastIndexOf('a', 1));
        assertEquals(cs("aaabbb").lastIndexOf('f', 2), "aaabbb".lastIndexOf('f', 2));
        assertEquals(cs("aaabbb").lastIndexOf('b', 4), "aaabbb".lastIndexOf('b', 4));
    }
}
