//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UnionColumnSource}, focused on the slot search hint its contexts maintain.
 * <p>
 * The hint is not observable through results: {@link UnionRedirection#currSlotForRowKey(long, int)} and its siblings
 * return the same slot for any hint, resetting to a full search when the hint cannot be used. A stale hint therefore
 * costs a binary search over the slot key space instead of two array reads, which matters for unions with very many
 * constituents. These tests observe it, along with the constituent contexts our contexts hold, via
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
                .mapToObj(ci -> TableTools.newTable(TableTools.longCol("Sentinel",
                        LongStream.range(ci * 100L, ci * 100L + CONSTITUENT_SIZE).toArray())))
                .toArray(Table[]::new);
        merged = TableTools.merge(constituents);
        source = merged.getColumnSource("Sentinel");
        assertThat(source).isInstanceOf(UnionColumnSource.class);
        slotRows = IntStream.range(0, NUM_CONSTITUENTS)
                .mapToObj(slot -> rowsForSlots(slot, slot))
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
                final RowSet spanning = rowsForSlots(2, 5)) {
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
                final RowSet spanning = rowsForSlots(2, 5)) {
            source.getChunk(getContext, spanning);
            assertThat(lastSlot(getContext)).isEqualTo(5);
        }
    }

    @Test
    public void getPrevChunkHintsAtTheLastSlotAccessedWhenSpanningSlots() {
        try (final ChunkSource.GetContext getContext = source.makeGetContext(CHUNK_CAPACITY);
                final RowSet spanning = rowsForSlots(2, 5)) {
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
                final RowSet spanning = rowsForSlots(2, 5)) {
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

    private WritableChunk<Values> makeChunk() {
        return source.getChunkType().makeWritableChunk(CHUNK_CAPACITY);
    }

    /**
     * @param firstSlot The first slot to include
     * @param lastSlot The last slot to include
     * @return The row keys occupied by the constituents in slots {@code [firstSlot, lastSlot]}
     */
    private WritableRowSet rowsForSlots(final int firstSlot, final int lastSlot) {
        return merged.getRowSet().subSetByPositionRange(
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
