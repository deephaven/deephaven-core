//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.clientsupport.gotorow;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SeekRowTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * Helper to verify that a given value can not be found no matter which row is started from
     *
     * @param t the table to search
     * @param impossibleValue a value that isn't present in the table
     */
    public static void assertNotFound(int impossibleValue, Table t) {
        // ensure we can't find values that don't exist
        for (int i = 0; i < t.size(); i++) {
            assertSeekPosition(t, impossibleValue, true, i, -1);
            assertSeekPosition(t, impossibleValue, false, i, -1);
        }
    }

    /**
     * Helper to run SeekRow and validate that the discovered row is what was expected. Validates the
     * {@code expectedPosition} before running, to ensure test data makes sense.
     *
     * @param t the table to search
     * @param seekValue the value to search for
     * @param seekForward true to seek forward, false to seek backward
     * @param currentPosition the position to start searching
     * @param expectedPosition the next expected position of the seek value
     */
    private static void assertSeekPosition(Table t, int seekValue, boolean seekForward, int currentPosition,
            int expectedPosition) {
        if (expectedPosition != -1) {
            // Confirm that the expected position matches
            assertEquals(seekValue, t.flatten().getColumnSource("num").getInt(expectedPosition));
        } else {
            // Confirm that the value actually doesn't exist
            assertTrue(t.where("num" + "=" + seekValue).isEmpty());
        }
        // Actually perform the requested assertion
        SeekRow seek = new SeekRow(currentPosition, "num", seekValue, false, false, !seekForward);
        assertEquals(expectedPosition, seek.seek(t));
    }

    /**
     * Helper to seek from every row in a table, and assert that a valid value can be found in a valid position from
     * each.
     *
     * @param t the table to search
     * @param seekValue the value to search for
     * @param forwardPositions expected positions when searching forward, indexed on starting row
     * @param backwardPositions expected positions when searching backwards, indexed on starting row
     */
    private static void assertSeekPositionAllRows(Table t, int seekValue, int[] forwardPositions,
            int[] backwardPositions) {
        assertEquals(t.size(), forwardPositions.length);
        assertEquals(t.size(), backwardPositions.length);
        for (int i = 0; i < t.size(); i++) {
            // seek from the current position, confirm we get the expected position
            assertSeekPosition(t, seekValue, true, i, forwardPositions[i]);
            assertSeekPosition(t, seekValue, false, i, backwardPositions[i]);
        }
    }

    /**
     * Helper to run asserts for int rows that are already sorted at initialization
     *
     * @param data the data, must be sorted in ascending order already
     * @param seekValue the value to search for
     * @param ascForwardPositions expected positions when searching forwards for ascending data
     * @param ascBackwardPositions expected positions when searching backwards for ascending data
     * @param descForwardPositions expected positions when searching forwards for descending data
     * @param descBackwardPositions expected positions when searching backwards for descending data
     */
    private static void assertNaturallySorted(int[] data, int seekValue,
            int[] ascForwardPositions, int[] ascBackwardPositions,
            int[] descForwardPositions, int[] descBackwardPositions) {
        // ascending tables
        Table ascUnsorted = TableTools.newTable(intCol("num", data));
        Table ascSorted = ascUnsorted.sort("num");
        // reverse data to be in descending
        for (int i = 0; i < data.length / 2; i++) {
            int tmp = data[i];
            data[i] = data[data.length - i - 1];
            data[data.length - i - 1] = tmp;
        }
        // descending tables
        Table descUnsorted = TableTools.newTable(intCol("num", data));
        Table descSorted = descUnsorted.sortDescending("num");

        assertSeekPositionAllRows(ascUnsorted, seekValue, ascForwardPositions, ascBackwardPositions);
        assertSeekPositionAllRows(ascSorted, seekValue, ascForwardPositions, ascBackwardPositions);
        assertSeekPositionAllRows(descUnsorted, seekValue, descForwardPositions, descBackwardPositions);
        assertSeekPositionAllRows(descSorted, seekValue, descForwardPositions, descBackwardPositions);
    }

    @Test
    public void emptyTable() {
        Table t = TableTools.newTable(intCol("num"));

        assertSeekPosition(t, 1, true, 0, -1);
        assertSeekPosition(t, 1, false, 0, -1);

        // repeat with sorted
        t = t.sort("num");
        assertSeekPosition(t, 1, true, 0, -1);
        assertSeekPosition(t, 1, false, 0, -1);
    }

    @Test
    public void singleRow() {
        Table t = TableTools.newTable(intCol("num", 1));
        assertSeekPosition(t, 1, true, 0, 0);
        assertSeekPosition(t, 1, false, 0, 0);

        assertSeekPosition(t, 100, false, 0, -1);
        assertSeekPosition(t, 100, true, 0, -1);

        // repeat with sorted
        t = t.sort("num");
        assertSeekPosition(t, 1, true, 0, 0);
        assertSeekPosition(t, 1, false, 0, 0);

        assertSeekPosition(t, 100, false, 0, -1);
        assertSeekPosition(t, 100, true, 0, -1);

        // repeat with sorted descending
        t = t.sortDescending("num");
        assertSeekPosition(t, 1, true, 0, 0);
        assertSeekPosition(t, 1, false, 0, 0);

        assertSeekPosition(t, 100, false, 0, -1);
        assertSeekPosition(t, 100, true, 0, -1);
    }

    @Test
    public void mono1() {
        assertNaturallySorted(
                new int[] {1}, 1,
                new int[] {0},
                new int[] {0},
                new int[] {0},
                new int[] {0});
    }

    @Test
    public void mono2() {
        assertNaturallySorted(
                new int[] {1, 1}, 1,
                new int[] {1, 0},
                new int[] {1, 0},
                new int[] {1, 0},
                new int[] {1, 0});
    }

    @Test
    public void mono3() {
        assertNaturallySorted(
                new int[] {1, 1, 1}, 1,
                new int[] {1, 2, 0},
                new int[] {2, 0, 1},
                new int[] {1, 2, 0},
                new int[] {2, 0, 1});
    }

    @Test
    public void start1() {
        assertNaturallySorted(
                new int[] {1, 2}, 1,
                new int[] {0, 0},
                new int[] {0, 0},
                new int[] {1, 1},
                new int[] {1, 1});
    }

    @Test
    public void start2() {
        assertNaturallySorted(
                new int[] {1, 1, 2}, 1,
                new int[] {1, 0, 0},
                new int[] {1, 0, 1},
                new int[] {1, 2, 1},
                new int[] {2, 2, 1});
    }

    @Test
    public void start3() {
        assertNaturallySorted(
                new int[] {1, 1, 1, 2}, 1,
                new int[] {1, 2, 0, 0},
                new int[] {2, 0, 1, 2},
                new int[] {1, 2, 3, 1},
                new int[] {3, 3, 1, 2});
    }

    @Test
    public void middle1() {
        assertNaturallySorted(
                new int[] {1, 2, 3}, 2,
                new int[] {1, 1, 1},
                new int[] {1, 1, 1},
                new int[] {1, 1, 1},
                new int[] {1, 1, 1});
    }

    @Test
    public void middle2() {
        assertNaturallySorted(
                new int[] {1, 2, 2, 3}, 2,
                new int[] {1, 2, 1, 1},
                new int[] {2, 2, 1, 2},
                new int[] {1, 2, 1, 1},
                new int[] {2, 2, 1, 2});
    }

    @Test
    public void middle3() {
        assertNaturallySorted(
                new int[] {1, 2, 2, 2, 3}, 2,
                new int[] {1, 2, 3, 1, 1},
                new int[] {3, 3, 1, 2, 3},
                new int[] {1, 2, 3, 1, 1},
                new int[] {3, 3, 1, 2, 3});
    }

    @Test
    public void end1() {
        assertNaturallySorted(
                new int[] {1, 2}, 2,
                new int[] {1, 1},
                new int[] {1, 1},
                new int[] {0, 0},
                new int[] {0, 0});
    }

    @Test
    public void end2() {
        assertNaturallySorted(
                new int[] {1, 2, 2}, 2,
                new int[] {1, 2, 1},
                new int[] {2, 2, 1},
                new int[] {1, 0, 0},
                new int[] {1, 0, 1});
    }

    @Test
    public void end3() {
        assertNaturallySorted(
                new int[] {1, 2, 2, 2}, 2,
                new int[] {1, 2, 3, 1},
                new int[] {3, 3, 1, 2},
                new int[] {1, 2, 0, 0},
                new int[] {2, 0, 1, 2});
    }

    @Test
    public void notFound() {
        assertNaturallySorted(
                new int[] {2, 4, 6}, 1,
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1});
        assertNaturallySorted(
                new int[] {2, 4, 6}, 3,
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1});
        assertNaturallySorted(
                new int[] {2, 4, 6}, 5,
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1});
        assertNaturallySorted(
                new int[] {2, 4, 6}, 7,
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1},
                new int[] {-1, -1, -1});
    }

    @Test
    public void unsorted() {
        final Table t = newTable(intCol("num", 3, 1, 1, 2, 3, 1, 1, 2));
        assertSeekPositionAllRows(t, 1,
                new int[] {1, 2, 5, 5, 5, 6, 1, 1},
                new int[] {6, 6, 1, 2, 2, 2, 5, 6});
        assertSeekPositionAllRows(t, 2,
                new int[] {3, 3, 3, 7, 7, 7, 7, 3},
                new int[] {7, 7, 7, 7, 3, 3, 3, 3});
        assertSeekPositionAllRows(t, 3,
                new int[] {4, 4, 4, 4, 0, 0, 0, 0},
                new int[] {4, 0, 0, 0, 0, 4, 4, 4});
    }
}
