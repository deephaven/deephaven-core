/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;

import io.deephaven.base.Predicate;
import io.deephaven.base.verify.Require;
import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import junit.framework.ComparisonFailure;

public class SimpleTestSupport {

    public static void assertListEmpty(List l) {
        Assert.assertTrue(l.isEmpty());
    }

    public static <T> void assertOrderedListEquals(List<T> l, T... elements) {
        Assert.assertEquals(elements.length, l.size());
        for (int i = 0; i < elements.length; ++i) {
            Assert.assertEquals(elements[i], l.get(i));
        }
    }

    public static <T> void assertOrderedListEquals(List<T> l, Predicate.Binary<T, T> equals, T... elements) {
        Assert.assertEquals(elements.length, l.size());
        for (int i = 0; i < elements.length; ++i) {
            Assert.assertTrue(equals.call(elements[i], l.get(i)));
        }
    }

    public static <T extends Comparable<? super T>> void assertUnorderedListEquals(List<T> l, T... elements) {
        ArrayList<T> sorted_l = new ArrayList<>(l);
        Collections.sort(sorted_l);
        Arrays.sort(elements);
        Assert.assertEquals(elements.length, sorted_l.size());
        for (int i = 0; i < elements.length; ++i) {
            Assert.assertEquals(elements[i], sorted_l.get(i));
        }
    }

    public static <T extends Comparable<? super T>> void assertUnorderedListEquals(List<T> l,
            Predicate.Binary<T, T> equals, T... elements) {
        ArrayList<T> sorted_l = new ArrayList<>(l);
        Collections.sort(sorted_l);
        Arrays.sort(elements);
        Assert.assertEquals(elements.length, sorted_l.size());
        for (int i = 0; i < elements.length; ++i) {
            Assert.assertTrue(equals.call(elements[i], l.get(i)));
        }
    }

    // ################################################################

    // ----------------------------------------------------------------
    /**
     * Asserts that the given collection contains exactly the given elements (in any order).
     */
    public static <E> void assertCollectionContainsExactly(Collection<E> collectionToSearch, E... itemsToFind) {
        assertCollectionContainsExactly(null, collectionToSearch, itemsToFind);
    }

    // ----------------------------------------------------------------
    /**
     * Asserts that the given collection contains exactly the given elements (in any order).
     */
    public static <E> void assertCollectionContainsExactly(String sMessage, Collection<E> collectionToSearch,
            E... itemsToFind) {
        try {
            String sPrefix = null == sMessage ? "" : sMessage + " ";
            if (null == itemsToFind) {
                Assert.assertNull(sPrefix + "Expected collectionToSearch to be null.", collectionToSearch);
            } else {
                Assert.assertNotNull(sPrefix + "Expected collectionToSearch to be non-null.", collectionToSearch);
                Assert.assertEquals(sPrefix + "Expected collectionToSearch and itemsToFind to be the same size.",
                        itemsToFind.length, collectionToSearch.size());
                for (E item : itemsToFind) {
                    Assert.assertTrue(sPrefix + "Expected collectionToSearch to contain \"" + item + "\".",
                            collectionToSearch.contains(item));
                }
            }
        } catch (AssertionFailedError e) {
            System.err.println("Expected (" + itemsToFind.length + " items):");
            for (E item : itemsToFind) {
                System.err.println("    " + item);
            }
            System.err.println("Actual (" + collectionToSearch.size() + " items):");
            for (E item : collectionToSearch) {
                System.err.println("    " + item);
            }
            throw e;
        }
    }

    // ----------------------------------------------------------------
    /** Asserts that the given string contains the given substring. */
    public static void assertStringContains(String sWhole, String sFragment) {
        if (!sWhole.contains(sFragment)) {
            throw new ComparisonFailure("Could not find fragment(expected) in whole(actual).", sFragment, sWhole);
        }
    }

    // ----------------------------------------------------------------
    /** Asserts that the given string contains the given substring. */
    public static void assertStringContains(String sTestDescription, String sWhole, String sFragment) {
        if (!sWhole.contains(sFragment)) {
            throw new ComparisonFailure(sTestDescription + " Could not find fragment(expected) in whole(actual).",
                    sFragment, sWhole);
        }
    }

    // ----------------------------------------------------------------
    /** Serializes then deserializes an object. */
    @SuppressWarnings("unchecked")
    public static <T> T serializeDeserialize(T t) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(t);
        objectOutputStream.writeObject(t);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        // noinspection unchecked
        objectInputStream.readObject();
        T deserializedT = (T) objectInputStream.readObject();
        objectInputStream.close();
        return deserializedT;
    }


    // ----------------------------------------------------------------
    /** Serializes then deserializes an object. */
    @SuppressWarnings("unchecked")
    public static <T extends Externalizable> T readWriteExternal(T t, T u, T v)
            throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        t.writeExternal(objectOutputStream);
        t.writeExternal(objectOutputStream);
        objectOutputStream.close();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        // noinspection unchecked
        u.readExternal(objectInputStream);
        v.readExternal(objectInputStream);
        objectInputStream.close();
        return v;
    }

    // ----------------------------------------------------------------
    /** Reports whether a {@link TimerTask} is cancelled. */
    public static boolean isTimerTaskCancelled(TimerTask timerTask) {
        Require.neqNull(timerTask, "timerTask");
        try {
            Field field = TimerTask.class.getDeclaredField("state");
            field.setAccessible(true);
            int state = field.getInt(timerTask);
            return 3 == state;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw io.deephaven.base.verify.Assert.exceptionNeverCaught(e);
        }
    }
}
