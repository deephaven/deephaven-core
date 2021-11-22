/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import junit.framework.TestCase;

import java.util.*;

public class ShiftDataTest extends TestCase {

    public void testSystematic() {
        RowSet rowSet = getSortedIndex();
        RowSet removed = getSortedIndex();
        RowSet added = getSortedIndex();
        ShiftData shiftData = new ShiftData(rowSet, removed, added);
        shiftData.applyDataShift(new ShiftData.ShiftCallback() {
            @Override
            public void shift(long start, long end, long offset) {
                assertTrue("Should not call it", false);
            }
        });
        rowSet = getSortedIndex(1L);
        testNoNotification(rowSet, removed, added);
        added = getSortedIndex(1L);
        testNoNotification(rowSet, removed, added);
        rowSet = getSortedIndex(1L, 2);
        added = getSortedIndex(1L, 2);
        testNoNotification(rowSet, removed, added);

        rowSet = getSortedIndex(1L, 2, 3);
        added = getSortedIndex(2, 3);
        testNoNotification(rowSet, removed, added);

        removed = getSortedIndex(4, 5);
        testNoNotification(rowSet, removed, added);

        rowSet = getSortedIndex();
        added = getSortedIndex();
        removed = getSortedIndex(4, 5);
        testNoNotification(rowSet, removed, added);

        rowSet = getSortedIndex(1L, 2, 4);
        added = getSortedIndex(2);
        removed = getSortedIndex(3);
        testNoNotification(rowSet, removed, added);

        rowSet = getSortedIndex(1L, 3, 4);
        added = getSortedIndex(3);
        removed = getSortedIndex(2);
        testNoNotification(rowSet, removed, added);

        rowSet = getSortedIndex();
        added = getSortedIndex();
        removed = getSortedIndex(1, 2, 4);
        testNoNotification(rowSet, removed, added);


        rowSet = getSortedIndex(4);
        added = getSortedIndex();
        removed = getSortedIndex(1);
        checkExpectations(rowSet, removed, added, new long[][] {{1, 1, -1}});

        rowSet = getSortedIndex(4, 5);
        added = getSortedIndex(4);
        removed = getSortedIndex();
        checkExpectations(rowSet, removed, added, new long[][] {{0, 0, 1}});

        rowSet = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(4);
        removed = getSortedIndex();
        checkExpectations(rowSet, removed, added, new long[][] {{0, 2, 1}});

        rowSet = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(4, 5);
        removed = getSortedIndex();
        checkExpectations(rowSet, removed, added, new long[][] {{0, 1, 2}});

        rowSet = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5, 6);
        removed = getSortedIndex();
        checkExpectations(rowSet, removed, added, new long[][] {{1, 1, 2}});
        // was 1,4,7
        rowSet = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5, 6);
        removed = getSortedIndex(1);
        checkExpectations(rowSet, removed, added, new long[][] {{1, 1, -1}, {2, 2, 1}});

        // was 1,2,4,6,7
        rowSet = getSortedIndex(4, 5, 6, 7);
        added = getSortedIndex(5);
        removed = getSortedIndex(1, 2);
        checkExpectations(rowSet, removed, added, new long[][] {{2, 2, -2}, {3, 4, -1}});

        // was 6,7,9,11
        rowSet = getSortedIndex(4, 5, 9, 10, 11);
        added = getSortedIndex(4, 5, 10);
        removed = getSortedIndex(6, 7);
        checkExpectations(rowSet, removed, added, new long[][] {{3, 3, 1}});

        // was 6,7,9,11
        rowSet = getSortedIndex(4, 9, 10, 11);
        added = getSortedIndex(4, 10);
        removed = getSortedIndex(6, 7);
        checkExpectations(rowSet, removed, added, new long[][] {{2, 2, -1}});

        // was 2,4,6,8
        rowSet = getSortedIndex(1, 2, 3, 4, 5, 6, 7, 8);
        added = getSortedIndex(1, 3, 5, 7);
        removed = getSortedIndex();
        checkExpectations(rowSet, removed, added, new long[][] {{3, 3, 4}, {2, 2, 3}, {1, 1, 2}, {0, 0, 1}});

        // was 2,4,6,8,10,12,16
        rowSet = getSortedIndex(1, 2, 3, 4, 8, 16);
        added = getSortedIndex(1, 3);
        removed = getSortedIndex(6, 10, 12);
        checkExpectations(rowSet, removed, added, new long[][] {{3, 3, 1}, {1, 1, 2}, {0, 0, 1}, {6, 6, -1}});

        // was 100,200,300,400,500,600,700
        rowSet = getSortedIndex(100, 200, 230, 240, 250, 260, 270, 500, 550, 700);
        added = getSortedIndex(230, 240, 250, 260, 270, 550);
        removed = getSortedIndex(300, 400, 600);
        checkExpectations(rowSet, removed, added, new long[][] {{6, 6, 3}, {4, 4, 3}});
    }

    private void checkExpectations(RowSet rowSet, RowSet removed, RowSet added, long[][] expected) {
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
        shiftData = new ShiftData(rowSet, removed, added);
        final Expectations expectations = new Expectations(expected);
        shiftData.applyDataShift(expectations);
        expectations.allMet();
    }

    private void testNoNotification(RowSet rowSet, RowSet removed, RowSet added) {
        ShiftData shiftData;
        shiftData = new ShiftData(rowSet, removed, added);
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
            RowSet initialRowSet = getBaseIndex(100, 10);
            RowSet added = getRandomIndex(20, 1, 10);
            RowSet removed = getRandomRemoves(initialRowSet, 2);
            RowSet finalRowSet = getFinalIndex(initialRowSet, added, removed);
            final long resultKeys[] = new long[(int) Math.max(initialRowSet.size(), finalRowSet.size())];
            int pos = 0;
            for (RowSet.Iterator it = initialRowSet.iterator(); it.hasNext();) {
                resultKeys[pos++] = it.nextLong();
            }
            ShiftData shiftData = new ShiftData(finalRowSet, removed, added);
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
            RowSet addedPos = shiftData.getAddedPos();

            for (RowSet.Iterator iterator = addedPos.iterator(), valueIt = added.iterator(); iterator.hasNext();) {
                resultKeys[((int) iterator.nextLong())] = valueIt.nextLong();
            }

            pos = 0;
            for (RowSet.Iterator iterator = finalRowSet.iterator(); iterator.hasNext();) {
                assertEquals(iterator.nextLong(), resultKeys[pos++]);
            }
        }
    }

    private RowSet getFinalIndex(RowSet initialRowSet, RowSet added, RowSet removed) {
        TreeSet<Long> finalKeys = new TreeSet<Long>();
        for (RowSet.Iterator iterator = initialRowSet.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.add(next);
        }
        for (RowSet.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.remove(next);
        }
        for (RowSet.Iterator iterator = added.iterator(); iterator.hasNext();) {
            Long next = iterator.nextLong();
            finalKeys.add(next);
        }
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (Long finalKey : finalKeys) {
            builder.addKey(finalKey);
        }
        return builder.build();
    }

    private RowSet getRandomRemoves(RowSet rowSet, int prob) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            if (random.nextInt(prob) == 0) {
                builder.addKey(next);
            }
        }
        return builder.build();
    }


    private RowSet getBaseIndex(int base, int size) {
        RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int i = 0; i < size; i++) {
            builder.appendKey(i * size);
        }
        return builder.build();
    }

    private RowSet getRandomIndex(int base, int offset, int size) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (int i = 0; i < size; i++) {
            if (random.nextInt(2) == 0) {
                builder.addKey(i * base + offset);
            }
        }
        return builder.build();
    }


    protected RowSet getSortedIndex(long... keys) {
        RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        for (long key : keys) {
            builder.addKey(key);
        }
        return builder.build();
    }

}
