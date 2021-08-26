/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import junit.framework.TestCase;

import java.util.*;

public class ShiftDataTest extends TestCase {

    public void testSystematic() {
        Index index = getSortedIndex();
        Index removed = getSortedIndex();
        Index added = getSortedIndex();
        ShiftData shiftData = new ShiftData(index, removed, added);
        shiftData.applyDataShift(new ShiftData.ShiftCallback() {
            @Override
            public void shift(long start, long end, long offset) {
                assertTrue("Should not call it", false);
            }
        });
        index = getSortedIndex(1L);
        testNoNotification(index, removed, added);
        added = getSortedIndex(1L);
        testNoNotification(index, removed, added);
        index = getSortedIndex(1L, 2);
        added = getSortedIndex(1L, 2);
        testNoNotification(index, removed, added);

        index = getSortedIndex(1L, 2, 3);
        added = getSortedIndex(2, 3);
        testNoNotification(index, removed, added);

        removed = getSortedIndex(4, 5);
        testNoNotification(index, removed, added);

        index = getSortedIndex();
        added = getSortedIndex();
        removed = getSortedIndex(4, 5);
        testNoNotification(index, removed, added);

        index = getSortedIndex(1L, 2, 4);
        added = getSortedIndex(2);
        removed = getSortedIndex(3);
        testNoNotification(index, removed, added);

        index = getSortedIndex(1L, 3, 4);
        added = getSortedIndex(3);
        removed = getSortedIndex(2);
        testNoNotification(index, removed, added);

        index = getSortedIndex();
        added = getSortedIndex();
        removed = getSortedIndex(1, 2, 4);
        testNoNotification(index, removed, added);


        index = getSortedIndex(4);
        added = getSortedIndex();
        removed = getSortedIndex(1);
        checkExpectations(index, removed, added, new long[][] {{1, 1, -1}});

        index = getSortedIndex(4, 5);
        added = getSortedIndex(4);
        removed = getSortedIndex();
        checkExpectations(index, removed, added, new long[][] {{0, 0, 1}});

        index = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(4);
        removed = getSortedIndex();
        checkExpectations(index, removed, added, new long[][] {{0, 2, 1}});

        index = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(4, 5);
        removed = getSortedIndex();
        checkExpectations(index, removed, added, new long[][] {{0, 1, 2}});

        index = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5, 6);
        removed = getSortedIndex();
        checkExpectations(index, removed, added, new long[][] {{1, 1, 2}});
        // was 1,4,7
        index = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5, 6);
        removed = getSortedIndex(1);
        checkExpectations(index, removed, added, new long[][] {{1, 1, -1}, {2, 2, 1}});

        // was 1,2,4,6,7
        index = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5);
        removed = getSortedIndex(1, 2);
        checkExpectations(index, removed, added, new long[][] {{2, 2, -2}, {3, 4, -1}});

        // was 6,7,9,11
        index = getSortedIndex(4, 5, 9, 10, 11);
        added = getSortedIndex(4, 5, 10);
        removed = getSortedIndex(6, 7);
        checkExpectations(index, removed, added, new long[][] {{3, 3, 1}});

        // was 6,7,9,11
        index = getSortedIndex(4, 9, 10, 11);
        added = getSortedIndex(4, 10);
        removed = getSortedIndex(6, 7);
        checkExpectations(index, removed, added, new long[][] {{2, 2, -1}});

        // was 2,4,6,8
        index = getSortedIndex(1, 2, 3, 4, 5, 6, 7, 8);
        added = getSortedIndex(1, 3, 5, 7);
        removed = getSortedIndex();
        checkExpectations(index, removed, added, new long[][] {{3, 3, 4}, {2, 2, 3}, {1, 1, 2}, {0, 0, 1}});

        // was 2,4,6,8,10,12,16
        index = getSortedIndex(1, 2, 3, 4, 8, 16);
        added = getSortedIndex(1, 3);
        removed = getSortedIndex(6, 10, 12);
        checkExpectations(index, removed, added, new long[][] {{3, 3, 1}, {1, 1, 2}, {0, 0, 1}, {6, 6, -1}});

        // was 100,200,300,400,500,600,700
        index = getSortedIndex(100, 200, 230, 240, 250, 260, 270, 500, 550, 700);
        added = getSortedIndex(230, 240, 250, 260, 270, 550);
        removed = getSortedIndex(300, 400, 600);
        checkExpectations(index, removed, added, new long[][] {{6, 6, 3}, {4, 4, 3}});
    }

    private void checkExpectations(Index index, Index removed, Index added, long[][] expected) {
        ShiftData shiftData;
        class Expectations implements ShiftData.ShiftCallback {

            private final long[][] expected;
            private int i = 0;

            Expectations(long[][] expected) {
                this.expected = expected;
            }

            @Override
            public void shift(long start, long end, long offset) {
                long[] current = expected[i++];
                assertEquals(current[0], start);
                assertEquals(current[1], end);
                assertEquals(current[2], offset);
            }

            public void allMet() {
                assertEquals(i, expected.length);
            }
        }
        shiftData = new ShiftData(index, removed, added);
        final Expectations expectations = new Expectations(expected);
        shiftData.applyDataShift(expectations);
        expectations.allMet();
    }

    private void testNoNotification(Index index, Index removed, Index added) {
        ShiftData shiftData;
        shiftData = new ShiftData(index, removed, added);
        shiftData.applyDataShift(new ShiftData.ShiftCallback() {
            @Override
            public void shift(long start, long end, long offset) {
                assertTrue("Should not call it", false);
            }
        });
    }

    Random random = new Random(123);

    public void testRandom() {
        for (int k = 0; k < 100; k++) {
            Index initialIndex = getBaseIndex(100, 10);
            Index added = getRandomIndex(20, 1, 10);
            Index removed = getRandomRemoves(initialIndex, 2);
            Index finalIndex = getFinalIndex(initialIndex, added, removed);
            final long resultKeys[] = new long[(int) Math.max(initialIndex.size(), finalIndex.size())];
            int pos = 0;
            for (Index.Iterator it = initialIndex.iterator(); it.hasNext();) {
                resultKeys[pos++] = it.nextLong();
            }
            ShiftData shiftData = new ShiftData(finalIndex, removed, added);
            shiftData.applyDataShift(new ShiftData.ShiftCallback() {
                @Override
                public void shift(long start, long end, long offset) {
                    if (offset > 0) {
                        for (int i = (int) end; i >= start; i--) {
                            resultKeys[((int) (i + offset))] = resultKeys[i];
                        }
                    } else {
                        for (int i = (int) start; i <= end; i++) {
                            resultKeys[((int) (i + offset))] = resultKeys[i];
                        }
                    }
                }
            });
            Index addedPos = shiftData.getAddedPos();

            for (Index.Iterator iterator = addedPos.iterator(), valueIt = added.iterator(); iterator.hasNext();) {
                resultKeys[((int) iterator.nextLong())] = valueIt.nextLong();
            }

            pos = 0;
            for (Index.Iterator iterator = finalIndex.iterator(); iterator.hasNext();) {
                assertEquals(iterator.nextLong(), resultKeys[pos++]);
            }
        }
    }

    private Index getFinalIndex(Index initialIndex, Index added, Index removed) {
        TreeSet<Long> finalKeys = new TreeSet<Long>();
        for (Index.Iterator iterator = initialIndex.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.add(next);
        }
        for (Index.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.remove(next);
        }
        for (Index.Iterator iterator = added.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.add(next);
        }
        Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        for (Long finalKey : finalKeys) {
            builder.addKey(finalKey);
        }
        return builder.getIndex();
    }

    private Index getRandomRemoves(Index index, int prob) {
        Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        for (Index.Iterator iterator = index.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            if (random.nextInt(prob) == 0) {
                builder.addKey(next);
            }
        }
        return builder.getIndex();
    }


    private Index getBaseIndex(int base, int size) {
        Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        for (int i = 0; i < size; i++) {
            builder.appendKey(i * size);
        }
        return builder.getIndex();
    }

    private Index getRandomIndex(int base, int offset, int size) {
        Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        for (int i = 0; i < size; i++) {
            if (random.nextInt(2) == 0) {
                builder.addKey(i * base + offset);
            }
        }
        return builder.getIndex();
    }


    protected SortedIndex getSortedIndex(long... keys) {
        Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
        for (long key : keys) {
            builder.addKey(key);
        }
        return (SortedIndex) builder.getIndex();
    }

}
