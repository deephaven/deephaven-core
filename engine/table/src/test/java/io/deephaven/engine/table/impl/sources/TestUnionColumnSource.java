//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UnionColumnSource}, focused on the state its contexts carry between accesses: the slot search
 * hint, and the context held for the constituent {@link ColumnSource} occupying the current slot.
 * <p>
 * Neither is observable through results. {@link UnionRedirection#currSlotForRowKey(long, int)} and its siblings return
 * the same slot for any hint, resetting to a full search when the hint cannot be used, so a stale hint costs a binary
 * search over the slot key space instead of two array reads -- which matters for unions with very many constituents.
 * Constituent contexts are re-usable across accesses that resolve to the same constituent source, and must be replaced
 * when they do not; holding one too long is a correctness risk rather than a cost. These tests observe both via
 * {@link UnionColumnSource.ContextInternals}.
 */
public class TestUnionColumnSource {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int NUM_CONSTITUENTS = 8;
    private static final int CONSTITUENT_SIZE = 10;
    private static final int CHUNK_CAPACITY = NUM_CONSTITUENTS * CONSTITUENT_SIZE;

    private Table merged;
    private ColumnSource<?> source;

    /**
     * The row keys occupied by each constituent in {@code merged}'s key space. Constituents are assigned slots in merge
     * order, so index is slot.
     */
    private RowSet[] slotRows;

    @Before
    public void setUp() {
        final Table[] constituents = IntStream.range(0, NUM_CONSTITUENTS)
                .mapToObj(ci -> constituent(ci * 100L))
                .toArray(Table[]::new);
        merged = TableTools.merge(constituents);
        source = merged.getColumnSource("Sentinel");
        assertThat(source).isInstanceOf(UnionColumnSource.class);
        slotRows = IntStream.range(0, NUM_CONSTITUENTS)
                .mapToObj(slot -> rowsForSlots(merged.getRowSet(), slot, slot))
                .toArray(RowSet[]::new);
    }

    @After
    public void tearDown() {
        for (final RowSet slotRowSet : slotRows) {
            slotRowSet.close();
        }
    }

    @Test
    public void freshContextsHintAtSlotZero() {
        try (final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY);
                final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY)) {
            assertThat(lastSlot(fillContext)).isEqualTo(0);
            assertThat(lastSlot(getContext)).isEqualTo(0);
            assertThat(constituentContext(fillContext)).isNull();
            assertThat(constituentContext(getContext)).isNull();
        }
    }

    @Test
    public void fillChunkHintsAtTheSlotAccessed() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY)) {
            for (int slot = 0; slot < NUM_CONSTITUENTS; ++slot) {
                source.fillChunk(fillContext, destination, slotRows[slot]);
                assertThat(lastSlot(fillContext)).isEqualTo(slot);
            }
        }
    }

    @Test
    public void fillPrevChunkHintsAtTheSlotAccessed() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY)) {
            for (int slot = 0; slot < NUM_CONSTITUENTS; ++slot) {
                source.fillPrevChunk(fillContext, destination, slotRows[slot]);
                assertThat(lastSlot(fillContext)).isEqualTo(slot);
            }
        }
    }

    @Test
    public void fillChunkHintsAtTheLastSlotAccessedWhenSpanningSlots() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY);
                final RowSet spanning = rowsForSlots(merged.getRowSet(), 2, 5)) {
            source.fillChunk(fillContext, destination, spanning);
            assertThat(lastSlot(fillContext)).isEqualTo(5);
        }
    }

    @Test
    public void getChunkHintsAtTheSlotAccessed() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY)) {
            for (int slot = 0; slot < NUM_CONSTITUENTS; ++slot) {
                source.getChunk(getContext, slotRows[slot]);
                assertThat(lastSlot(getContext)).isEqualTo(slot);
            }
        }
    }

    @Test
    public void getChunkByKeyRangeHintsAtTheSlotAccessed() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY)) {
            for (int slot = 0; slot < NUM_CONSTITUENTS; ++slot) {
                source.getChunk(getContext, slotRows[slot].firstRowKey(), slotRows[slot].lastRowKey());
                assertThat(lastSlot(getContext)).isEqualTo(slot);
            }
        }
    }

    /**
     * A get context delegates to its embedded fill context for row sequences that span multiple slots. Its hint must
     * reflect that access; tracking the hint per slot state instead of per context left it stale here.
     */
    @Test
    public void getChunkHintsAtTheLastSlotAccessedWhenSpanningSlots() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY);
                final RowSet spanning = rowsForSlots(merged.getRowSet(), 2, 5)) {
            source.getChunk(getContext, spanning);
            assertThat(lastSlot(getContext)).isEqualTo(5);
        }
    }

    @Test
    public void getPrevChunkHintsAtTheLastSlotAccessedWhenSpanningSlots() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY);
                final RowSet spanning = rowsForSlots(merged.getRowSet(), 2, 5)) {
            source.getPrevChunk(getContext, spanning);
            assertThat(lastSlot(getContext)).isEqualTo(5);
        }
    }

    /**
     * The hint records the most recent access, not the highest slot reached. Taking the maximum over a get context's
     * slot states would satisfy the spanning cases above while failing this one, and would seed a full search whenever
     * access moves backwards.
     */
    @Test
    public void getChunkHintFollowsAccessBackwards() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY);
                final RowSet spanning = rowsForSlots(merged.getRowSet(), 2, 5)) {
            source.getChunk(getContext, spanning);
            assertThat(lastSlot(getContext)).isEqualTo(5);
            source.getChunk(getContext, slotRows[1]);
            assertThat(lastSlot(getContext)).isEqualTo(1);
        }
    }

    @Test
    public void fillChunkHintFollowsAccessBackwards() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY)) {
            source.fillChunk(fillContext, destination, slotRows[5]);
            assertThat(lastSlot(fillContext)).isEqualTo(5);
            source.fillChunk(fillContext, destination, slotRows[2]);
            assertThat(lastSlot(fillContext)).isEqualTo(2);
        }
    }

    /**
     * Alternating between current and previous data for one slot must not disturb the hint, and must not rebuild the
     * constituent context either -- both versions resolve to the same constituent {@link ColumnSource} while the
     * constituent is unchanged.
     */
    @Test
    public void alternatingDataVersionsReuseTheSameSlotAndFillContext() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY)) {
            source.fillChunk(fillContext, destination, slotRows[3]);
            assertThat(lastSlot(fillContext)).isEqualTo(3);
            final Context constituentContext = constituentContext(fillContext);
            assertThat(constituentContext).isNotNull();

            source.fillPrevChunk(fillContext, destination, slotRows[3]);
            assertThat(lastSlot(fillContext)).isEqualTo(3);
            assertThat(constituentContext(fillContext)).isSameAs(constituentContext);

            source.fillChunk(fillContext, destination, slotRows[3]);
            assertThat(lastSlot(fillContext)).isEqualTo(3);
            assertThat(constituentContext(fillContext)).isSameAs(constituentContext);
        }
    }

    /**
     * The same, for a get context: {@link ColumnSource#getChunk} and {@link ColumnSource#getPrevChunk} for one slot
     * resolve to the same constituent {@link ColumnSource}, and so must share one constituent context.
     */
    @Test
    public void alternatingDataVersionsReuseTheSameSlotAndGetContext() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY)) {
            source.getChunk(getContext, slotRows[3]);
            assertThat(lastSlot(getContext)).isEqualTo(3);
            final Context constituentContext = constituentContext(getContext);
            assertThat(constituentContext).isNotNull();

            source.getPrevChunk(getContext, slotRows[3]);
            assertThat(lastSlot(getContext)).isEqualTo(3);
            assertThat(constituentContext(getContext)).isSameAs(constituentContext);

            source.getChunk(getContext, slotRows[3]);
            assertThat(lastSlot(getContext)).isEqualTo(3);
            assertThat(constituentContext(getContext)).isSameAs(constituentContext);
        }
    }

    /**
     * Negative control for the reuse asserted above: our constituents are distinct {@link ColumnSource sources} that
     * hand out a fresh context per request, so moving to another slot must produce a different constituent context.
     * Without this, the assertions above would hold for a source that shared one context across all slots.
     */
    @Test
    public void changingSlotsMakesANewConstituentContext() {
        try (final WritableChunk<Values> destination = makeChunk();
                final ChunkSource.FillContext fillContext = source.makeFillContext(CHUNK_CAPACITY)) {
            source.fillChunk(fillContext, destination, slotRows[3]);
            final Context slotThreeContext = constituentContext(fillContext);
            source.fillChunk(fillContext, destination, slotRows[4]);
            assertThat(constituentContext(fillContext)).isNotSameAs(slotThreeContext);
            source.fillChunk(fillContext, destination, slotRows[3]);
            assertThat(constituentContext(fillContext)).isNotSameAs(slotThreeContext);
        }
    }

    /**
     * When a constituent is replaced mid-cycle, one slot resolves to different constituent {@link ColumnSource sources}
     * for current and previous data. A context that alternates versions at that slot must therefore drop its
     * constituent context and make a new one for the source it actually needs, rather than reusing the context it
     * happens to hold for the same slot.
     */
    @Test
    public void modifiedConstituentAtASlotMakesANewConstituentContext() {
        final Table original = constituent(0);
        final Table other = constituent(100);
        final Table replacement = constituent(200);

        final QueryTable underlying = testRefreshingTable(i(0, 1).toTracking(),
                col("Constituent", original, other));
        final Table ticking = PartitionedTableFactory.of(underlying).merge();
        final ColumnSource<?> tickingSource = ticking.getColumnSource("Sentinel");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(underlying, i(1), col("Constituent", replacement));
            underlying.notifyListeners(i(), i(), i(1));
            updateGraph.flushAllNormalNotificationsForUnitTests();

            // Slot 1 held "other" as of the previous cycle, and holds "replacement" now.
            assertSlotSwitchesConstituentContext(ticking, tickingSource, 1,
                    constituentValues(200), constituentValues(100));
        });
    }

    /**
     * Inserting a constituent ahead of the existing ones changes which constituent a slot resolves to without any
     * constituent itself changing: every following slot then holds the constituent that used to occupy the slot before
     * it, so that slot's current and previous sources differ. A context that alternates data versions there must switch
     * constituent contexts, just as it must when a constituent is replaced in place.
     */
    @Test
    public void insertedConstituentShiftingASlotMakesANewConstituentContext() {
        final Table first = constituent(0);
        final Table second = constituent(100);
        final Table inserted = constituent(200);

        final QueryTable underlying = testRefreshingTable(i(1, 2).toTracking(),
                col("Constituent", first, second));
        final Table ticking = PartitionedTableFactory.of(underlying).merge();
        final ColumnSource<?> tickingSource = ticking.getColumnSource("Sentinel");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(underlying, i(0), col("Constituent", inserted));
            underlying.notifyListeners(i(0), i(), i());
            updateGraph.flushAllNormalNotificationsForUnitTests();

            // Inserting at the front pushes "first" from slot 0 to slot 1, where "second" used to be.
            assertSlotSwitchesConstituentContext(ticking, tickingSource, 1,
                    constituentValues(0), constituentValues(100));
        });
    }

    /**
     * Assert that alternating data versions at {@code slot} of {@code ticking} switches constituent contexts, and reads
     * the expected data for each version. Must be called during the updating phase of a cycle, while {@code ticking}'s
     * previous values are still those of the cycle in which {@code slot} held a different constituent.
     *
     * @param ticking The merged table under test
     * @param tickingSource {@code ticking}'s {@code Sentinel} source
     * @param slot The slot whose constituent changed this cycle
     * @param expectedCurr The values the constituent now in {@code slot} holds
     * @param expectedPrev The values the constituent previously in {@code slot} held
     */
    private static void assertSlotSwitchesConstituentContext(
            @NotNull final Table ticking,
            @NotNull final ColumnSource<?> tickingSource,
            final int slot,
            final long[] expectedCurr,
            final long[] expectedPrev) {
        // Fill directly into the array we assert on; a wrapped chunk owns nothing and needs no closing.
        final long[] filled = new long[CONSTITUENT_SIZE];
        final WritableLongChunk<Values> destination = WritableLongChunk.writableChunkWrap(filled);
        try (final ChunkSource.FillContext fillContext = tickingSource.makeFillContext(CONSTITUENT_SIZE);
                final RowSet currRows = rowsForSlots(ticking.getRowSet(), slot, slot);
                final RowSet prevRows = rowsForSlots(ticking.getRowSet().prev(), slot, slot)) {

            tickingSource.fillChunk(fillContext, destination, currRows);
            assertThat(lastSlot(fillContext)).isEqualTo(slot);
            assertThat(filled).containsExactly(expectedCurr);
            final Context currContext = constituentContext(fillContext);
            assertThat(currContext).isNotNull();

            // The previous data for this slot lives in a different constituent, so it needs a different context.
            tickingSource.fillPrevChunk(fillContext, destination, prevRows);
            assertThat(lastSlot(fillContext)).isEqualTo(slot);
            assertThat(filled).containsExactly(expectedPrev);
            final Context prevContext = constituentContext(fillContext);
            assertThat(prevContext).isNotNull();
            assertThat(prevContext).isNotSameAs(currContext);

            // ...and switching back must not keep using the context made for the previous constituent.
            tickingSource.fillChunk(fillContext, destination, currRows);
            assertThat(lastSlot(fillContext)).isEqualTo(slot);
            assertThat(filled).containsExactly(expectedCurr);
            assertThat(constituentContext(fillContext)).isNotSameAs(prevContext);
        }
    }

    /**
     * @param firstValue The first value
     * @return A constituent table holding {@link #CONSTITUENT_SIZE} values beginning at {@code firstValue}
     */
    private static Table constituent(final long firstValue) {
        return TableTools.newTable(TableTools.longCol("Sentinel", constituentValues(firstValue)));
    }

    /**
     * @param firstValue The first value
     * @return The values held by the constituent {@link #constituent(long) built from} {@code firstValue}
     */
    private static long[] constituentValues(final long firstValue) {
        return LongStream.range(firstValue, firstValue + CONSTITUENT_SIZE).toArray();
    }

    private WritableChunk<Values> makeChunk() {
        return source.getChunkType().makeWritableChunk(CHUNK_CAPACITY);
    }

    /**
     * All constituents in these tests have {@link #CONSTITUENT_SIZE} rows, so a slot's keys are found by position.
     *
     * @param rowSet A merged table's row set, current or previous
     * @param firstSlot The first slot to include
     * @param lastSlot The last slot to include
     * @return The keys {@code rowSet} holds for the constituents in slots {@code [firstSlot, lastSlot]}
     */
    private static WritableRowSet rowsForSlots(
            @NotNull final RowSet rowSet, final int firstSlot, final int lastSlot) {
        return rowSet.subSetByPositionRange(
                (long) firstSlot * CONSTITUENT_SIZE, (long) (lastSlot + 1) * CONSTITUENT_SIZE);
    }

    private static int lastSlot(@NotNull final Context context) {
        return internals(context).lastSlot();
    }

    private static Context constituentContext(@NotNull final Context context) {
        return internals(context).constituentContext();
    }

    private static UnionColumnSource.ContextInternals internals(@NotNull final Context context) {
        assertThat(context).isInstanceOf(UnionColumnSource.ContextInternals.class);
        return (UnionColumnSource.ContextInternals) context;
    }
}
