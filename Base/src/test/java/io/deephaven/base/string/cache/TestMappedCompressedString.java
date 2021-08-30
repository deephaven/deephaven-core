/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import io.deephaven.base.reference.HardSimpleReference;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.verify.RequirementFailure;
import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Arrays;

@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
public class TestMappedCompressedString extends TestCase {

    public void testEquality() {
        MappedCompressedString mcs;

        assertTrue(CharSequenceUtils.contentEquals(mcs = new MappedCompressedString(""), ""));
        assertEquals(mcs.toString(), "");
        assertEquals(mcs.hashCode(), "".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "".getBytes()));

        assertTrue(
            CharSequenceUtils.contentEquals(mcs = new MappedCompressedString("hello"), "hello"));
        assertEquals(mcs.toString(), "hello");
        assertEquals(mcs.hashCode(), "hello".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "hello".getBytes()));

        assertTrue(CharSequenceUtils
            .contentEquals(mcs = new MappedCompressedString("again".toCharArray()), "again"));
        assertEquals(mcs.toString(), "again");
        assertEquals(mcs.hashCode(), "again".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "again".getBytes()));

        assertTrue(CharSequenceUtils
            .contentEquals(mcs = new MappedCompressedString("!?!?".toCharArray(), 2, 1), "!"));
        assertEquals(mcs.toString(), "!");
        assertEquals(mcs.hashCode(), "!".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "!".getBytes()));

        assertTrue(CharSequenceUtils
            .contentEquals(mcs = new MappedCompressedString("dancing".getBytes()), "dancing"));
        assertEquals(mcs.toString(), "dancing");
        assertEquals(mcs.hashCode(), "dancing".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "dancing".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(
            mcs = new MappedCompressedString("dancing with the stars!".getBytes(), 17, 5),
            "stars"));
        assertEquals(mcs.toString(), "stars");
        assertEquals(mcs.hashCode(), "stars".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "stars".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(
            mcs = new MappedCompressedString(ByteBuffer.wrap("happy".getBytes())), "happy"));
        assertEquals(mcs.toString(), "happy");
        assertEquals(mcs.hashCode(), "happy".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "happy".getBytes()));

        assertTrue(CharSequenceUtils.contentEquals(
            mcs = new MappedCompressedString(ByteBuffer.wrap("hedgehog!".getBytes()), 5, 3),
            "hog"));
        assertEquals(mcs.toString(), "hog");
        assertEquals(mcs.hashCode(), "hog".hashCode());
        assertTrue(Arrays.equals(mcs.getData(), "hog".getBytes()));
    }

    public void testMappingInvariants() {
        final MappedCompressedString mcs = new MappedCompressedString("");
        final SimpleReference<Object> key1 = new HardSimpleReference<>(new Object());
        try {
            mcs.putIfAbsent(null, 0);
            fail();
        } catch (RequirementFailure ignored) {
        }
        try {
            mcs.putIfAbsent(key1, MappedCompressedString.NULL_MAPPING_VALUE);
            fail();
        } catch (RequirementFailure ignored) {
        }
    }

    public void testMapping() {
        final MappedCompressedString mcs = new MappedCompressedString("");
        final SimpleReference<Object> key1 = new HardSimpleReference<>(new Object());
        final SimpleReference<Object> key2 = new HardSimpleReference<>(new Object());
        final SimpleReference<Object> key3 = new HardSimpleReference<>(new Object());
        final SimpleReference<Object> key4 = new HardSimpleReference<>(new Object());
        final SimpleReference<Object> key5 = new HardSimpleReference<>(new Object());

        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key1, 1));
        assertEquals(1, mcs.capacity());
        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key2, 2));
        assertEquals(2, mcs.capacity());
        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key3, 3));
        assertEquals(4, mcs.capacity());

        assertEquals(1, mcs.putIfAbsent(key1, 2));
        assertEquals(4, mcs.capacity());
        assertEquals(2, mcs.putIfAbsent(key2, 4));
        assertEquals(4, mcs.capacity());
        assertEquals(3, mcs.putIfAbsent(key3, 6));
        assertEquals(4, mcs.capacity());

        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key4, 4));
        assertEquals(4, mcs.capacity());
        assertEquals(4, mcs.putIfAbsent(key4, 8));
        assertEquals(4, mcs.capacity());

        assertEquals(3, mcs.putIfAbsent(key3, 9));
        assertEquals(4, mcs.capacity());
        key3.clear();
        assertEquals(3, mcs.putIfAbsent(key3, 12));
        assertEquals(4, mcs.capacity());

        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key5, 5));
        assertEquals(4, mcs.capacity());
        assertEquals(5, mcs.putIfAbsent(key5, 10));
        assertEquals(4, mcs.capacity());

        assertEquals(MappedCompressedString.NULL_MAPPING_VALUE, mcs.putIfAbsent(key3, 15));
        assertEquals(8, mcs.capacity());
        assertEquals(15, mcs.putIfAbsent(key3, 18));
        assertEquals(8, mcs.capacity());

        assertEquals(1, mcs.putIfAbsent(key1, 3));
        assertEquals(8, mcs.capacity());
        assertEquals(2, mcs.putIfAbsent(key2, 6));
        assertEquals(8, mcs.capacity());
        assertEquals(15, mcs.putIfAbsent(key3, 21));
        assertEquals(8, mcs.capacity());
        assertEquals(4, mcs.putIfAbsent(key4, 12));
        assertEquals(8, mcs.capacity());
        assertEquals(5, mcs.putIfAbsent(key5, 15));
        assertEquals(8, mcs.capacity());
    }
}
