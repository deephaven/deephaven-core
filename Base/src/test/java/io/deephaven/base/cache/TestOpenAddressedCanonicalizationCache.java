/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase", "UnnecessaryBoxing"})
public class TestOpenAddressedCanonicalizationCache extends TestCase {

    @Test
    public void testDefaultAdapter() {
        final OpenAddressedCanonicalizationCache SUT =
            new OpenAddressedCanonicalizationCache(1, 0.9f);

        final Integer[] cachedIntegers = new Integer[SUT.getOccupancyThreshold() * 100];
        final Double[] cachedDoubles = new Double[cachedIntegers.length];
        final String[] cachedStrings = new String[cachedIntegers.length];

        for (int ii = 0; ii < cachedIntegers.length; ++ii) {
            final Integer newInteger = new Integer(ii);
            cachedIntegers[ii] = newInteger;
            assertSame(newInteger, SUT.getCachedItem(newInteger));

            final Double newDouble = new Double(ii);
            cachedDoubles[ii] = newDouble;
            assertSame(newDouble, SUT.getCachedItem(newDouble));

            final String newString = newInteger.toString();
            cachedStrings[ii] = newString;
            assertSame(newString, SUT.getCachedItem(newString));
        }

        final int savedOccupancyThreshold = SUT.getOccupancyThreshold();
        assertEquals(3 * cachedIntegers.length, SUT.getOccupiedSlots());
        System.gc();
        assertSame(cachedIntegers[0], SUT.getCachedItem(cachedIntegers[0])); // Force cleanup
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(3 * cachedIntegers.length, SUT.getOccupiedSlots());

        for (int ii = 0; ii < cachedIntegers.length; ++ii) {
            final Integer equalInteger = new Integer(ii);
            final Integer cachedInteger = SUT.getCachedItem(equalInteger);
            assertEquals(equalInteger, cachedInteger);
            assertNotSame(equalInteger, cachedInteger);
            assertSame(cachedIntegers[ii], cachedInteger);

            final Double equalDouble = new Double(ii);
            final Double cachedDouble = SUT.getCachedItem(equalDouble);
            assertEquals(equalDouble, cachedDouble);
            assertNotSame(equalDouble, cachedDouble);
            assertSame(cachedDoubles[ii], cachedDouble);

            final String equalString = equalInteger.toString();
            final String cachedString = SUT.getCachedItem(equalString);
            assertEquals(equalString, cachedString);
            assertNotSame(equalString, cachedString);
            assertSame(cachedStrings[ii], cachedString);

            if ((ii & 1) == 1) {
                cachedIntegers[ii] = null;
                cachedDoubles[ii] = null;
                cachedStrings[ii] = null;
            }
        }

        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertTrue(3 * cachedIntegers.length / 2 <= SUT.getOccupiedSlots());
        assertTrue(3 * cachedIntegers.length >= SUT.getOccupiedSlots());
        System.gc();
        while (SUT.getOccupiedSlots() != 3 * cachedIntegers.length / 2) {
            assertSame(cachedIntegers[0], SUT.getCachedItem(cachedIntegers[0])); // Force cleanup
        }
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(3 * cachedIntegers.length / 2, SUT.getOccupiedSlots());

        for (int ii = 0; ii < cachedIntegers.length; ++ii) {
            if ((ii & 1) == 1) {
                final Integer newInteger = new Integer(ii);
                cachedIntegers[ii] = newInteger;
                assertSame(newInteger, SUT.getCachedItem(newInteger));

                final Double newDouble = new Double(ii);
                cachedDoubles[ii] = newDouble;
                assertSame(newDouble, SUT.getCachedItem(newDouble));

                final String newString = newInteger.toString();
                cachedStrings[ii] = newString;
                assertSame(newString, SUT.getCachedItem(newString));
            } else {
                final Integer equalInteger = new Integer(ii);
                final Integer cachedInteger = SUT.getCachedItem(equalInteger);
                assertEquals(equalInteger, cachedInteger);
                assertNotSame(equalInteger, cachedInteger);
                assertSame(cachedIntegers[ii], cachedInteger);

                final Double equalDouble = new Double(ii);
                final Double cachedDouble = SUT.getCachedItem(equalDouble);
                assertEquals(equalDouble, cachedDouble);
                assertNotSame(equalDouble, cachedDouble);
                assertSame(cachedDoubles[ii], cachedDouble);

                final String equalString = equalInteger.toString();
                final String cachedString = SUT.getCachedItem(equalString);
                assertEquals(equalString, cachedString);
                assertNotSame(equalString, cachedString);
                assertSame(cachedStrings[ii], cachedString);
            }
        }

        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(3 * cachedIntegers.length, SUT.getOccupiedSlots());
        System.gc();
        assertSame(cachedIntegers[0], SUT.getCachedItem(cachedIntegers[0])); // Force cleanup
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(3 * cachedIntegers.length, SUT.getOccupiedSlots());
    }

    /**
     * This is a really crappy example... but it's fine for a unit test.
     */
    private static OpenAddressedCanonicalizationCache.Adapter<Object, String> OSA =
        new OpenAddressedCanonicalizationCache.Adapter<Object, String>() {

            @Override
            public boolean equals(@NotNull Object inputItem, @NotNull Object cachedItem) {
                if (cachedItem instanceof String) {
                    return inputItem.toString().equals(cachedItem);
                }
                return false;
            }

            @Override
            public int hashCode(@NotNull Object inputItem) {
                return inputItem.toString().hashCode();
            }

            @Override
            public String makeCacheableItem(@NotNull Object inputItem) {
                return inputItem.toString();
            }
        };

    @Test
    public void testSpecialAdapters() {
        final OpenAddressedCanonicalizationCache SUT =
            new OpenAddressedCanonicalizationCache(1, 0.9f);

        final String[] cachedStrings = new String[SUT.getOccupancyThreshold() * 100 * 2];

        for (int ii = 0; ii < cachedStrings.length; ii += 2) {
            final String intString = SUT.getCachedItem(ii, OSA);
            assertEquals(ii, Integer.parseInt(intString));
            assertSame(intString, SUT.getCachedItem(Integer.toString(ii)));
            cachedStrings[ii] = intString;

            final String doubleString = SUT.getCachedItem((double) ii, OSA);
            assertEquals((double) ii, Double.parseDouble(doubleString));
            assertSame(doubleString, SUT.getCachedItem(Double.toString(ii)));
            cachedStrings[ii + 1] = doubleString;
        }

        final int savedOccupancyThreshold = SUT.getOccupancyThreshold();
        assertEquals(cachedStrings.length, SUT.getOccupiedSlots());
        System.gc();
        assertSame(cachedStrings[0], SUT.getCachedItem(cachedStrings[0])); // Force cleanup
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(cachedStrings.length, SUT.getOccupiedSlots());

        for (int ii = 0; ii < cachedStrings.length; ii += 2) {
            final String intString = SUT.getCachedItem(ii, OSA);
            assertSame(intString, cachedStrings[ii]);
            assertSame(cachedStrings[ii], SUT.getCachedItem(Integer.toString(ii)));

            final String doubleString = SUT.getCachedItem((double) ii, OSA);
            assertSame(doubleString, cachedStrings[ii + 1]);
            assertSame(cachedStrings[ii + 1], SUT.getCachedItem(Double.toString(ii)));

            cachedStrings[ii + 1] = null;
        }

        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertTrue(cachedStrings.length / 2 <= SUT.getOccupiedSlots());
        assertTrue(cachedStrings.length >= SUT.getOccupiedSlots());
        System.gc();
        while (SUT.getOccupiedSlots() != cachedStrings.length / 2) {
            assertSame(cachedStrings[0], SUT.getCachedItem(cachedStrings[0])); // Force cleanup
        }
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(cachedStrings.length / 2, SUT.getOccupiedSlots());

        for (int ii = 0; ii < cachedStrings.length; ii += 2) {
            final String intString = SUT.getCachedItem(ii, OSA);
            assertSame(intString, cachedStrings[ii]);
            assertSame(cachedStrings[ii], SUT.getCachedItem(Integer.toString(ii)));

            final String doubleString = SUT.getCachedItem((double) ii, OSA);
            assertEquals((double) ii, Double.parseDouble(doubleString));
            assertSame(doubleString, SUT.getCachedItem(Double.toString(ii)));
            cachedStrings[ii + 1] = doubleString;
        }

        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(cachedStrings.length, SUT.getOccupiedSlots());
        System.gc();
        assertSame(cachedStrings[0], SUT.getCachedItem(cachedStrings[0])); // Force cleanup
        assertEquals(savedOccupancyThreshold, SUT.getOccupancyThreshold());
        assertEquals(cachedStrings.length, SUT.getOccupiedSlots());
    }
}
