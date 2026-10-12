//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

public class ChangedKeyRowsTest extends RefreshingTableTestCase {

    @Test
    public void testFindChanged() {
        final QueryTable table = testRefreshingTable(i(0, 1, 2, 3).toTracking(), intCol("I", 1, 2, 3, 4),
                col("S", "a", "b", "c", "d"));
        final ColumnSource<?>[] sources = {table.getColumnSource("I"), table.getColumnSource("S")};
        final ChangedKeyRows changedKeyRows = new ChangedKeyRows(
                Arrays.stream(sources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            // row 0 changes I, row 1 changes S, row 2 is rewritten with the same values, row 3 changes both
            addToTable(table, i(0, 1, 2, 3), intCol("I", 10, 2, 3, 40), col("S", "a", "bb", "c", "dd"));

            final RowSetBuilderSequential preBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential postBuilder = RowSetFactory.builderSequential();
            final List<Long> probedRows = new ArrayList<>();
            final List<Integer> probedI = new ArrayList<>();
            final List<String> probedS = new ArrayList<>();
            changedKeyRows.findChanged(sources, i(0, 1, 2, 3), i(0, 1, 2, 3), preBuilder, postBuilder,
                    (rows, previousKeys) -> {
                        rows.forAllRowKeys(probedRows::add);
                        final IntChunk<?> previousI = previousKeys[0].asIntChunk();
                        final ObjectChunk<?, ?> previousS = previousKeys[1].asObjectChunk();
                        assertEquals(rows.intSize(), previousI.size());
                        assertEquals(rows.intSize(), previousS.size());
                        for (int ii = 0; ii < rows.intSize(); ++ii) {
                            probedI.add(previousI.get(ii));
                            probedS.add((String) previousS.get(ii));
                        }
                    });
            // the changed rows are reported in order, and the probe sees only their previous key values
            assertEquals(i(0, 1, 3), preBuilder.build());
            assertEquals(i(0, 1, 3), postBuilder.build());
            assertEquals(Arrays.asList(0L, 1L, 3L), probedRows);
            assertEquals(Arrays.asList(1, 2, 4), probedI);
            assertEquals(Arrays.asList("a", "b", "d"), probedS);

            // without a probe or a pre-shift output, the changed post-shift keys are still reported
            final RowSetBuilderSequential postOnly = RowSetFactory.builderSequential();
            changedKeyRows.findChanged(sources, i(0, 1, 2, 3), i(0, 1, 2, 3), null, postOnly, null);
            assertEquals(i(0, 1, 3), postOnly.build());

            // an empty input reports nothing and never calls the probe
            final RowSetBuilderSequential preEmpty = RowSetFactory.builderSequential();
            final RowSetBuilderSequential postEmpty = RowSetFactory.builderSequential();
            changedKeyRows.findChanged(sources, i(), i(), preEmpty, postEmpty,
                    (rows, previousKeys) -> fail("the probe must not be called for an empty input"));
            assertTrue(preEmpty.build().isEmpty());
            assertTrue(postEmpty.build().isEmpty());

            table.notifyListeners(i(), i(), i(0, 1, 2, 3));
        });
    }
}
