//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.LongStream;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK1_SHIFT;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.ir;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A sparse array source whose row keys are those of a row set releases, at the end of a cycle, the blocks that no
 * longer hold a row of that row set, so that its memory is bounded by its live rows rather than by every row key it
 * ever held.
 */
public class TestSparseArraySourceNullBlocks {
    private static final long LONG_BLOCK_BYTES = (long) BLOCK_SIZE * Long.BYTES;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    /**
     * A redirection over a sparse source whose outer row keys are the rows of a tracking row set.
     */
    private static final class Fixture {
        final LongSparseArraySource source = new LongSparseArraySource();
        final LongColumnSourceWritableRowRedirection redirection = new LongColumnSourceWritableRowRedirection(source);
        final TrackingWritableRowSet rowSet;

        /**
         * @param liveRows the initial outer row keys
         * @param writtenRows the outer row keys given a redirection, {@code key -> key * 10}
         */
        Fixture(final RowSet liveRows, final RowSet writtenRows) {
            rowSet = liveRows.copy().toTracking();
            writtenRows.forAllRowKeys(key -> redirection.put(key, key * 10));
            redirection.startTrackingPrevValues();
        }

        /**
         * Apply an update that removes {@code removed} and then shifts by {@code shifted}, as an owner does.
         */
        void update(final RowSet removed, final RowSetShiftData shifted) {
            try (final RowSet prevRowSet = rowSet.copyPrev()) {
                redirection.removeAll(removed);
                rowSet.remove(removed);
                try (final RowSet prevLessRemoved = prevRowSet.minus(removed)) {
                    redirection.applyShift(prevLessRemoved, shifted);
                }
                shifted.apply(rowSet);
                redirection.releaseVacatedStorage(removed, shifted, rowSet);
            }
        }
    }

    private static void assertRedirections(final Fixture fixture, final long firstKey, final long lastKey,
            final boolean prev, final long delta) {
        for (long key = firstKey; key <= lastKey; ++key) {
            final long expected = delta == NULL_LONG ? RowSet.NULL_ROW_KEY : (key - delta) * 10;
            assertEquals(expected, prev ? fixture.redirection.getPrev(key) : fixture.redirection.get(key));
        }
    }

    @Test
    public void testIntRedirectionReleasesRemovedAndShiftedBlocks() {
        final IntegerSparseArraySource source = new IntegerSparseArraySource();
        final IntColumnSourceWritableRowRedirection redirection = new IntColumnSourceWritableRowRedirection(source);
        final TrackingWritableRowSet rowSet = ir(0, 3 * BLOCK_SIZE - 1).toTracking();
        rowSet.forAllRowKeys(key -> redirection.put(key, key * 10));
        redirection.startTrackingPrevValues();
        final long intBlockBytes = (long) BLOCK_SIZE * Integer.BYTES;
        final long sizeBefore = source.estimateSize();

        // remove the first block, and shift the third block up by one block, vacating it
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        shiftBuilder.shiftRange(2L * BLOCK_SIZE, 3L * BLOCK_SIZE - 1, BLOCK_SIZE);
        final RowSetShiftData shifted = shiftBuilder.build();
        updateGraph().runWithinUnitTestCycle(() -> {
            try (final RowSet removed = ir(0, BLOCK_SIZE - 1);
                    final RowSet prevRowSet = rowSet.copyPrev()) {
                redirection.removeAll(removed);
                rowSet.remove(removed);
                try (final RowSet prevLessRemoved = prevRowSet.minus(removed)) {
                    redirection.applyShift(prevLessRemoved, shifted);
                }
                shifted.apply(rowSet);
                redirection.releaseVacatedStorage(removed, shifted, rowSet);
            }
            for (long key = 0; key < 3L * BLOCK_SIZE; ++key) {
                assertEquals(key * 10, redirection.getPrev(key));
            }
        });

        // the removed and vacated blocks are released, and the shifted block is allocated at its new position
        assertEquals(sizeBefore - intBlockBytes, source.estimateSize());
        for (long key = 0; key < BLOCK_SIZE; ++key) {
            assertEquals(RowSet.NULL_ROW_KEY, redirection.get(key));
            assertEquals(RowSet.NULL_ROW_KEY, redirection.get(key + 2L * BLOCK_SIZE));
            assertEquals((key + BLOCK_SIZE) * 10, redirection.get(key + BLOCK_SIZE));
            assertEquals((key + 2L * BLOCK_SIZE) * 10, redirection.get(key + 3L * BLOCK_SIZE));
        }
    }

    @Test
    public void testTopBlockReleased() throws InterruptedException {
        final long topKey = Long.MAX_VALUE;
        try (final RowSet rows = i(topKey - 1, topKey)) {
            final Fixture fixture = new Fixture(rows, rows);
            final long sizeBefore = fixture.source.estimateSize();
            final ExecutionContext context = ExecutionContext.getContext();
            final MutableObject<Throwable> failure = new MutableObject<>();

            // the update runs on its own thread so that a release that fails to terminate fails this test
            final Thread updateThread = new Thread(() -> {
                try (final SafeCloseable ignored = context.open()) {
                    updateGraph().runWithinUnitTestCycle(() -> {
                        try (final RowSet removed = i(topKey)) {
                            fixture.update(removed, RowSetShiftData.EMPTY);
                        }
                    });
                    assertEquals(sizeBefore, fixture.source.estimateSize());
                    updateGraph().runWithinUnitTestCycle(() -> {
                        try (final RowSet removed = i(topKey - 1)) {
                            fixture.update(removed, RowSetShiftData.EMPTY);
                        }
                    });
                } catch (final Throwable err) {
                    failure.setValue(err);
                }
            }, "TestSparseArraySourceNullBlocks-top-block");
            updateThread.setDaemon(true);
            updateThread.start();
            updateThread.join(60_000);
            assertFalse("releasing the top block terminates", updateThread.isAlive());
            if (failure.getValue() != null) {
                throw new AssertionError(failure.getValue());
            }

            assertTrue(fixture.source.estimateSize() < sizeBefore);
            assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.get(topKey - 1));
            assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.get(topKey));
        }
    }

    @Test
    public void testRemovedBlockReleasedWithPrevValues() {
        try (final RowSet rows = ir(0, 2 * BLOCK_SIZE - 1)) {
            final Fixture fixture = new Fixture(rows, rows);
            final long sizeBefore = fixture.source.estimateSize();

            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = ir(0, BLOCK_SIZE - 1)) {
                    fixture.update(removed, RowSetShiftData.EMPTY);
                }
                assertRedirections(fixture, 0, BLOCK_SIZE - 1, false, NULL_LONG);
                assertRedirections(fixture, 0, 2 * BLOCK_SIZE - 1, true, 0);
                assertEquals(sizeBefore, fixture.source.estimateSize());
            });

            assertEquals(sizeBefore - LONG_BLOCK_BYTES, fixture.source.estimateSize());
            assertRedirections(fixture, 0, BLOCK_SIZE - 1, false, NULL_LONG);
            assertRedirections(fixture, 0, BLOCK_SIZE - 1, true, NULL_LONG);
            assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, false, 0);
            assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, true, 0);
        }
    }

    @Test
    public void testPartiallyLiveBlockRetained() {
        try (final RowSet rows = ir(0, 2 * BLOCK_SIZE - 1)) {
            final Fixture fixture = new Fixture(rows, rows);
            final long sizeBefore = fixture.source.estimateSize();

            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = ir(0, BLOCK_SIZE - 2)) {
                    fixture.update(removed, RowSetShiftData.EMPTY);
                }
            });

            assertEquals(sizeBefore, fixture.source.estimateSize());
            assertRedirections(fixture, BLOCK_SIZE - 1, 2 * BLOCK_SIZE - 1, false, 0);
        }
    }

    @Test
    public void testRewriteReleasedBlock() {
        try (final RowSet rows = ir(0, 2 * BLOCK_SIZE - 1)) {
            final Fixture fixture = new Fixture(rows, rows);
            final long sizeBefore = fixture.source.estimateSize();

            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = ir(0, BLOCK_SIZE - 1)) {
                    fixture.update(removed, RowSetShiftData.EMPTY);
                }
            });
            assertEquals(sizeBefore - LONG_BLOCK_BYTES, fixture.source.estimateSize());

            updateGraph().runWithinUnitTestCycle(() -> {
                fixture.rowSet.insert(5);
                fixture.redirection.put(5, 50);
                assertEquals(50, fixture.redirection.get(5));
                assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.getPrev(5));
                assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.get(6));
            });

            assertEquals(sizeBefore, fixture.source.estimateSize());
            assertEquals(50, fixture.redirection.get(5));
            assertEquals(50, fixture.redirection.getPrev(5));
            assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.get(6));
        }
    }

    @Test
    public void testShiftReleasesVacatedBlock() {
        try (final RowSet rows = ir(0, 2 * BLOCK_SIZE - 1)) {
            final Fixture fixture = new Fixture(rows, rows);
            final long sizeBefore = fixture.source.estimateSize();
            final long shiftDelta = 4L * BLOCK_SIZE;

            updateGraph().runWithinUnitTestCycle(() -> {
                final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
                builder.shiftRange(BLOCK_SIZE, 2 * BLOCK_SIZE - 1, shiftDelta);
                try (final RowSet removed = RowSetFactory.empty()) {
                    fixture.update(removed, builder.build());
                }
                assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, false, NULL_LONG);
                assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, true, 0);
                assertRedirections(fixture, BLOCK_SIZE + shiftDelta, 2 * BLOCK_SIZE - 1 + shiftDelta, false,
                        shiftDelta);
                assertRedirections(fixture, BLOCK_SIZE + shiftDelta, 2 * BLOCK_SIZE - 1 + shiftDelta, true,
                        NULL_LONG);
            });

            // the vacated block is released, and the destination holds as many blocks as the source did
            assertEquals(sizeBefore, fixture.source.estimateSize());
            assertRedirections(fixture, 0, BLOCK_SIZE - 1, false, 0);
            assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, false, NULL_LONG);
            assertRedirections(fixture, BLOCK_SIZE + shiftDelta, 2 * BLOCK_SIZE - 1 + shiftDelta, false,
                    shiftDelta);
        }
    }

    @Test
    public void testUnwrittenBlocksTolerated() {
        // live rows in two block1 structures; nothing is written in the second, nor in one block of the first
        final long secondBlock1 = 1L << BLOCK1_SHIFT;
        try (final RowSet rows = ir(0, 2 * BLOCK_SIZE - 1);
                final RowSet written = ir(BLOCK_SIZE, 2 * BLOCK_SIZE - 1)) {
            rows.writableCast().insertRange(secondBlock1, secondBlock1 + BLOCK_SIZE - 1);
            final Fixture fixture = new Fixture(rows, written);
            final long sizeBefore = fixture.source.estimateSize();

            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = ir(0, BLOCK_SIZE - 1)) {
                    removed.writableCast().insertRange(secondBlock1, secondBlock1 + BLOCK_SIZE - 1);
                    fixture.update(removed, RowSetShiftData.EMPTY);
                }
            });

            assertEquals(sizeBefore, fixture.source.estimateSize());
            assertRedirections(fixture, BLOCK_SIZE, 2 * BLOCK_SIZE - 1, false, 0);
        }
    }

    @Test
    public void testEmptyReleasesEveryLevel() {
        // rows in two block1 structures and two blocks of each, so that every level holds an array
        final long secondBlock1 = 1L << BLOCK1_SHIFT;
        try (final RowSet rows = ir(0, 0)) {
            for (final long firstKey : new long[] {3L * BLOCK_SIZE, secondBlock1, secondBlock1 + BLOCK_SIZE}) {
                rows.writableCast().insertRange(firstKey, firstKey + 1);
            }
            final Fixture fixture = new Fixture(rows, rows);
            assertTrue(fixture.source.estimateSize() > 4 * LONG_BLOCK_BYTES);

            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = fixture.rowSet.copy()) {
                    fixture.update(removed, RowSetShiftData.EMPTY);
                }
            });

            assertEquals(0, fixture.source.estimateSize());
            rows.forAllRowKeys(key -> assertEquals(RowSet.NULL_ROW_KEY, fixture.redirection.get(key)));

            updateGraph().runWithinUnitTestCycle(() -> {
                fixture.rowSet.insert(secondBlock1);
                fixture.redirection.put(secondBlock1, 7);
            });
            assertEquals(LONG_BLOCK_BYTES, fixture.source.estimateSize());
            assertEquals(7, fixture.redirection.get(secondBlock1));
        }
    }

    // region end to end

    private static final int ROWS_PER_CYCLE = 500;
    private static final int LIVE_ROWS = 4 * ROWS_PER_CYCLE;
    private static final int RIGHT_KEYS = 5;

    /**
     * Keys {@code 0..RIGHT_KEYS-1} in even blocks of row keys and {@code RIGHT_KEYS..2*RIGHT_KEYS-1} in odd blocks, so
     * that a join against a right table holding only the first range leaves every row of an odd block unmatched.
     */
    private static int keyFor(final long rowKey) {
        return (int) ((rowKey / BLOCK_SIZE) % 2 * RIGHT_KEYS + rowKey % RIGHT_KEYS);
    }

    private static QueryTable marchingLeft() {
        return TstUtils.testRefreshingTable(RowSetFactory.fromRange(0, LIVE_ROWS - 1).toTracking(),
                intCol("Key", LongStream.range(0, LIVE_ROWS).mapToInt(TestSparseArraySourceNullBlocks::keyFor)
                        .toArray()),
                longCol("Stamp", LongStream.range(0, LIVE_ROWS).toArray()),
                instantCol("Ts", LongStream.range(0, LIVE_ROWS).mapToObj(Instant::ofEpochSecond)
                        .toArray(Instant[]::new)));
    }

    private static Table right(final boolean refreshing) {
        final ColumnHolder<?>[] columns = new ColumnHolder[] {
                intCol("Key", LongStream.range(0, RIGHT_KEYS).mapToInt(key -> (int) key).toArray()),
                longCol("RightStamp", new long[RIGHT_KEYS]),
                longCol("RightValue", LongStream.range(0, RIGHT_KEYS).map(key -> key * 100).toArray())};
        return refreshing
                ? TstUtils.testRefreshingTable(RowSetFactory.flat(RIGHT_KEYS).toTracking(), columns)
                : TstUtils.testTable(RowSetFactory.flat(RIGHT_KEYS).toTracking(), columns);
    }

    private static long redirectionSize(final Table result, final String column) {
        final ColumnSource<?> source = result.getColumnSource(column);
        assertTrue(source.getClass().getName(), source instanceof RedirectedColumnSource);
        final Object redirection = ((RedirectedColumnSource<?>) source).getRowRedirection();
        assertTrue(redirection.getClass().getName(), redirection instanceof LongColumnSourceRowRedirection);
        return ((SparseArrayColumnSource<?>) ((LongColumnSourceRowRedirection<?>) redirection).getColumnSource())
                .estimateSize();
    }

    private static long sparseSize(final Table result, final String column) {
        final ColumnSource<?> source = ReinterpretUtils.maybeConvertToPrimitive(result.getColumnSource(column));
        assertTrue(source.getClass().getName(), source instanceof SparseArrayColumnSource);
        return ((SparseArrayColumnSource<?>) source).estimateSize();
    }

    /**
     * Add rows with ever higher row keys to {@code left} and remove its oldest rows, many times, and assert that the
     * memory {@code size} reports does not grow with the row keys used.
     *
     * @return the result of the last cycle
     */
    private static Table checkMarchingBounded(final QueryTable left, final Function<QueryTable, Table> operation,
            final ToLongFunction<Table> size) {
        final Table result = operation.apply(left);
        long nextKey = LIVE_ROWS;
        long firstSize = -1;
        long maxSize = 0;
        for (int cycle = 0; cycle < 200; ++cycle) {
            final long firstAdded = nextKey;
            nextKey += ROWS_PER_CYCLE;
            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet removed = ir(firstAdded - LIVE_ROWS, firstAdded - LIVE_ROWS + ROWS_PER_CYCLE - 1);
                        final RowSet added = ir(firstAdded, firstAdded + ROWS_PER_CYCLE - 1)) {
                    removeRows(left, removed);
                    final LongStream addedKeys = LongStream.rangeClosed(added.firstRowKey(), added.lastRowKey());
                    final long[] keys = addedKeys.toArray();
                    addToTable(left, added,
                            intCol("Key", LongStream.of(keys).mapToInt(TestSparseArraySourceNullBlocks::keyFor)
                                    .toArray()),
                            longCol("Stamp", keys),
                            instantCol("Ts", LongStream.of(keys).mapToObj(Instant::ofEpochSecond)
                                    .toArray(Instant[]::new)));
                    left.notifyListeners(added.copy(), removed.copy(), i());
                }
            });
            final long currentSize = size.applyAsLong(result);
            if (firstSize < 0) {
                firstSize = currentSize;
            }
            maxSize = Math.max(maxSize, currentSize);
        }
        assertEquals(LIVE_ROWS, result.size());
        // the live rows are a contiguous range, so they span the same number of blocks, give or take one, each cycle
        assertTrue("firstSize=" + firstSize + ", maxSize=" + maxSize, maxSize <= firstSize + 2 * LONG_BLOCK_BYTES);
        return result;
    }

    private static void assertJoined(final Table result) {
        final ColumnSource<?> keys = result.getColumnSource("Key");
        final ColumnSource<?> values = result.getColumnSource("RightValue");
        result.getRowSet().forAllRowKeys(rowKey -> {
            final int key = keys.getInt(rowKey);
            assertEquals(key < RIGHT_KEYS ? key * 100L : NULL_LONG, values.getLong(rowKey));
        });
    }

    @Test
    public void testNaturalJoinMarchingLeftBounded() {
        assertJoined(checkMarchingBounded(marchingLeft(), left -> left.naturalJoin(right(false), "Key", "RightValue"),
                result -> redirectionSize(result, "RightValue")));
    }

    @Test
    public void testNaturalJoinBothTickingMarchingLeftBounded() {
        assertJoined(checkMarchingBounded(marchingLeft(), left -> left.naturalJoin(right(true), "Key", "RightValue"),
                result -> redirectionSize(result, "RightValue")));
    }

    @Test
    public void testAjMarchingLeftBounded() {
        assertJoined(checkMarchingBounded(marchingLeft(),
                left -> left.aj(right(false), "Key,Stamp>=RightStamp", "RightValue"),
                result -> redirectionSize(result, "RightValue")));
    }

    @Test
    public void testAjBothTickingMarchingLeftBounded() {
        assertJoined(checkMarchingBounded(marchingLeft(),
                left -> left.aj(right(true), "Key,Stamp>=RightStamp", "RightValue"),
                result -> redirectionSize(result, "RightValue")));
    }

    @Test
    public void testZeroKeyAjMarchingLeftBounded() {
        for (final boolean rightRefreshing : new boolean[] {false, true}) {
            final Table result = checkMarchingBounded(marchingLeft(),
                    left -> left.aj(right(rightRefreshing).where("Key = 1"), "Stamp>=RightStamp", "RightValue"),
                    table -> redirectionSize(table, "RightValue"));
            final ColumnSource<?> values = result.getColumnSource("RightValue");
            result.getRowSet().forAllRowKeys(rowKey -> assertEquals(100L, values.getLong(rowKey)));
        }
    }

    @Test
    public void testSortMarchingBounded() {
        final Table result = checkMarchingBounded(marchingLeft(), left -> left.sort("Stamp"),
                table -> redirectionSize(table, "Stamp"));
        final ColumnSource<?> stamps = result.getColumnSource("Stamp");
        final long[] expected = new long[] {stamps.getLong(result.getRowSet().firstRowKey())};
        result.getRowSet().forAllRowKeys(rowKey -> assertEquals(expected[0]++, stamps.getLong(rowKey)));
    }

    @Test
    public void testUpdateMarchingBounded() {
        checkMarchingBounded(marchingLeft(), left -> left.update("Doubled = Stamp * 2"),
                table -> sparseSize(table, "Doubled"));
    }

    @Test
    public void testUpdateByMarchingBounded() {
        for (final String[] byColumns : List.of(new String[0], new String[] {"Key"})) {
            checkMarchingBounded(marchingLeft(), left -> left.updateBy(UpdateByOperation.CumSum("Summed=Stamp"),
                    byColumns), table -> sparseSize(table, "Summed"));
        }
    }

    @Test
    public void testUpdateByInstantOutputMarchingBounded() {
        for (final String[] byColumns : List.of(new String[0], new String[] {"Key"})) {
            checkMarchingBounded(marchingLeft(), left -> left.updateBy(UpdateByOperation.CumMax("MaxTs=Ts"),
                    byColumns), table -> sparseSize(table, "MaxTs"));
        }
    }

    @Test
    public void testUpdateByRedirectedMarchingBounded() {
        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();
        for (final String[] byColumns : List.of(new String[0], new String[] {"Key"})) {
            checkMarchingBounded(marchingLeft(),
                    left -> left.updateBy(control, UpdateByOperation.CumSum("Summed=Stamp"), byColumns),
                    table -> redirectionSize(table, "Summed"));
        }
    }

    @Test
    public void testUpdateByInternalSparseSources() {
        // these operators also write sparse sources that are not output columns; each source is released at most once
        // per cycle, and the output stays correct
        final List<UpdateByOperation> operations = List.of(
                UpdateByOperation.EmStd(10, "Std=Stamp"),
                UpdateByOperation.RollingGroup("Ts", Duration.ofSeconds(3), "Group=Stamp"),
                UpdateByOperation.CumSum("Summed=Stamp"));
        for (final String[] byColumns : List.of(new String[0], new String[] {"Key"})) {
            final QueryTable left = marchingLeft();
            final Table result = checkMarchingBounded(left, table -> table.updateBy(operations, byColumns),
                    table -> sparseSize(table, "Summed"));
            TstUtils.assertTableEquals(left.snapshot().updateBy(operations, byColumns), result);
        }
    }

    // endregion end to end
}
