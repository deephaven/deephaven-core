//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import java.util.BitSet;
import java.util.List;

/**
 * Direct coverage of {@link BarrageMessageDelta#coalesce} for the one run shape the round-trip tests cannot arrange
 * deterministically: deltas of two subscription generations whose column sets differ. That happens when a removal-only
 * subscription change promotes a narrower column set at once and the update graph records a delta under it before the
 * propagation job coalesces the queue. The result must carry exactly the columns every delta recorded, sourcing each
 * surviving row from the latest delta that holds it, and must not ask the narrower delta for a column it never stored.
 */
public class BarrageMessageDeltaCoalesceTest extends RefreshingTableTestCase {

    private static final int INT_COL = 0;
    private static final int DOUBLE_COL = 1;
    private static final int STR_COL = 2;

    private QueryTable table;
    private ChunkSource.WithPrev<Values>[] chunkSources;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = TstUtils.testRefreshingTable(RowSetFactory.flat(10).toTracking(),
                TableTools.intCol("intCol", new int[10]),
                TableTools.doubleCol("doubleCol", new double[10]),
                TableTools.stringCol("strCol", new String[10]));
        // noinspection unchecked
        chunkSources = table.getColumnSources().toArray(ChunkSource.WithPrev[]::new);
    }

    private static BitSet columns(final int... indices) {
        final BitSet result = new BitSet();
        for (final int index : indices) {
            result.set(index);
        }
        return result;
    }

    /** One chunk per set column, each holding {@code values.length} rows whose value encodes {@code tag} and row. */
    private WritableChunk<Values>[][] chunks(final BitSet forColumns, final int numRows, final int tag) {
        // noinspection unchecked
        final WritableChunk<Values>[][] result = new WritableChunk[chunkSources.length][];
        for (int ci = forColumns.nextSetBit(0); ci >= 0; ci = forColumns.nextSetBit(ci + 1)) {
            final ChunkType chunkType = chunkSources[ci].getChunkType();
            final WritableChunk<Values> chunk = chunkType.makeWritableChunk(numRows);
            for (int ii = 0; ii < numRows; ++ii) {
                switch (ci) {
                    case INT_COL:
                        chunk.asWritableIntChunk().set(ii, tag * 100 + ii);
                        break;
                    case DOUBLE_COL:
                        chunk.asWritableDoubleChunk().set(ii, tag + ii / 10.0);
                        break;
                    default:
                        chunk.<String>asWritableObjectChunk().set(ii, tag + ":" + ii);
                        break;
                }
            }
            // noinspection unchecked
            result[ci] = new WritableChunk[] {chunk};
        }
        return result;
    }

    private static TableUpdate update(final RowSet added, final RowSet modified, final ModifiedColumnSet mcs) {
        return new TableUpdateImpl(added, RowSetFactory.empty(), modified, RowSetShiftData.EMPTY, mcs);
    }

    /**
     * Delta 1, generation 1, all three columns: adds rows 10 and 11, modifies row 5. Delta 2, generation 2, only the
     * first two columns: adds row 20, modifies row 11 (an add of delta 1). Coalesced from the base row set 0-9.
     */
    public void testNarrowerLaterGeneration() {
        final BitSet wide = columns(INT_COL, DOUBLE_COL, STR_COL);
        final BitSet narrow = columns(INT_COL, DOUBLE_COL);

        final WritableRowSet adds1 = RowSetFactory.fromRange(10, 11);
        final RowSet mods1 = RowSetFactory.fromKeys(5);
        final BarrageMessageDelta delta1 = new BarrageMessageDelta(1, 1, 1,
                update(adds1.copy(), mods1.copy(), ModifiedColumnSet.ALL),
                adds1, mods1, null, (BitSet) wide.clone(), (BitSet) wide.clone(),
                chunks(wide, 2, 1), chunks(wide, 1, 11));

        final WritableRowSet adds2 = RowSetFactory.fromKeys(20);
        final RowSet mods2 = RowSetFactory.fromKeys(11);
        final BarrageMessageDelta delta2 = new BarrageMessageDelta(2, 2, 2,
                update(adds2.copy(), mods2.copy(), table.newModifiedColumnSet("intCol", "doubleCol")),
                adds2, mods2, null, (BitSet) narrow.clone(), (BitSet) narrow.clone(),
                chunks(narrow, 1, 2), chunks(narrow, 1, 22));

        try (final RowSet base = RowSetFactory.flat(10);
                final BarrageMessageDelta result =
                        BarrageMessageDelta.coalesce(List.of(delta1, delta2), base, chunkSources)) {
            assertEquals("generation of the run's head", 1, result.generation);
            assertEquals("steps spanned", 1, result.firstStep);
            assertEquals("steps spanned", 2, result.lastStep);

            assertEquals("added rows", RowSetFactory.fromKeys(10, 11, 20), result.update.added());
            assertEquals("modified rows", RowSetFactory.fromKeys(5), result.update.modified());
            assertEquals("recorded adds", RowSetFactory.fromKeys(10, 11, 20), result.recordedAdds);
            assertEquals("recorded mods", RowSetFactory.fromKeys(5), result.recordedMods);

            // only the columns both deltas recorded survive, for adds and mods alike
            assertEquals("column set", narrow, result.subscribedColumns);
            assertEquals("modified columns", narrow, result.modifiedColumns);
            assertNull("no add data for the dropped column", result.addChunks[STR_COL]);
            assertNull("no mod data for the dropped column", result.modChunks[STR_COL]);

            // row 10 from delta 1's adds, row 11 from delta 2's mods (the latest data), row 20 from delta 2's adds
            assertEquals(1, result.addChunks[INT_COL].length);
            assertEquals(3, result.addChunks[INT_COL][0].size());
            assertEquals(100, result.addChunks[INT_COL][0].asIntChunk().get(0));
            assertEquals(2200, result.addChunks[INT_COL][0].asIntChunk().get(1));
            assertEquals(200, result.addChunks[INT_COL][0].asIntChunk().get(2));
            assertEquals(1.0, result.addChunks[DOUBLE_COL][0].asDoubleChunk().get(0));
            assertEquals(22.0, result.addChunks[DOUBLE_COL][0].asDoubleChunk().get(1));
            assertEquals(2.0, result.addChunks[DOUBLE_COL][0].asDoubleChunk().get(2));

            // row 5 from delta 1's mods
            assertEquals(1, result.modChunks[INT_COL][0].size());
            assertEquals(1100, result.modChunks[INT_COL][0].asIntChunk().get(0));
            assertEquals(11.0, result.modChunks[DOUBLE_COL][0].asDoubleChunk().get(0));
            assertEquals(RowSetFactory.fromKeys(5), result.getRecordedMods(INT_COL));
        } finally {
            delta1.close();
            delta2.close();
        }
    }

    /** The same run with equal column sets, as compaction always sees, keeps every column. */
    public void testEqualColumnSetsKeepEverything() {
        final BitSet wide = columns(INT_COL, DOUBLE_COL, STR_COL);

        final WritableRowSet adds1 = RowSetFactory.fromRange(10, 11);
        final BarrageMessageDelta delta1 = new BarrageMessageDelta(1, 1, 1,
                update(adds1.copy(), RowSetFactory.empty(), ModifiedColumnSet.EMPTY),
                adds1, RowSetFactory.empty(), null, (BitSet) wide.clone(), new BitSet(),
                chunks(wide, 2, 1), new WritableChunk[chunkSources.length][]);

        final RowSet mods2 = RowSetFactory.fromKeys(11);
        final BarrageMessageDelta delta2 = new BarrageMessageDelta(1, 2, 2,
                update(RowSetFactory.empty(), mods2.copy(), ModifiedColumnSet.ALL),
                RowSetFactory.empty(), mods2, null, (BitSet) wide.clone(), (BitSet) wide.clone(),
                new WritableChunk[chunkSources.length][], chunks(wide, 1, 22));

        try (final RowSet base = RowSetFactory.flat(10);
                final BarrageMessageDelta result =
                        BarrageMessageDelta.coalesce(List.of(delta1, delta2), base, chunkSources)) {
            assertEquals("column set", wide, result.subscribedColumns);
            assertEquals("added rows", RowSetFactory.fromKeys(10, 11), result.update.added());
            // a row added and then modified within the run is an add carrying the modified value
            assertTrue("no modified rows", result.update.modified().isEmpty());
            assertTrue("no modified columns", result.modifiedColumns.isEmpty());
            assertEquals("22:0", result.addChunks[STR_COL][0].<String>asObjectChunk().get(1));
            assertEquals("1:0", result.addChunks[STR_COL][0].<String>asObjectChunk().get(0));
        } finally {
            delta1.close();
            delta2.close();
        }
    }
}
