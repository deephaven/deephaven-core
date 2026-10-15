//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.preview.ColumnPreviewManager;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.table.Table.BARRAGE_COMPRESSION_ATTRIBUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

/**
 * {@link Table#BARRAGE_COMPRESSION_ATTRIBUTE} must follow operations whose result holds only the opted-in table's
 * values, and must not follow operations that compute new values or bring in another table's data.
 */
public class BarrageCompressionAttributeCopyTest {

    private static final EnumSet<BaseTable.CopyAttributeOperation> KEPT_BY = EnumSet.of(
            BaseTable.CopyAttributeOperation.Coalesce,
            BaseTable.CopyAttributeOperation.DropColumns,
            BaseTable.CopyAttributeOperation.Filter,
            BaseTable.CopyAttributeOperation.FirstBy,
            BaseTable.CopyAttributeOperation.Flatten,
            BaseTable.CopyAttributeOperation.LastBy,
            BaseTable.CopyAttributeOperation.PartitionBy,
            BaseTable.CopyAttributeOperation.Preview,
            BaseTable.CopyAttributeOperation.RenameColumns,
            BaseTable.CopyAttributeOperation.Reverse,
            BaseTable.CopyAttributeOperation.Sort);

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static Table source() {
        return TableTools.emptyTable(10).update("A = ii", "B = ii % 3", "Arr = new int[] {i, i + 1}");
    }

    private static Table compressed(final Table table) {
        return table.withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,gzip"));
    }

    private static void assertKept(final Table result) {
        assertEquals("zstd,gzip", result.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    private static void assertDropped(final Table result) {
        assertNull(result.getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void attributeFollowsOperationsThatKeepTheTablesValues() {
        final Table compressed = compressed(source());
        assertKept(compressed);

        assertKept(compressed.where("A > 2"));
        assertKept(compressed.sort("B"));
        assertKept(compressed.reverse());
        // flatten() of an already-flat table returns the same table, so flatten a filtered one
        assertKept(compressed(source().where("A > 2")).flatten());
        assertKept(compressed.dropColumns("Arr"));
        assertKept(compressed.renameColumns("C = A"));
        assertKept(compressed.firstBy("B"));
        assertKept(compressed.lastBy("B"));
        for (final Table constituent : compressed.partitionBy("B").constituents()) {
            assertKept(constituent);
        }

        // the int[] column makes the preview a new table
        final Table preview = ColumnPreviewManager.applyPreview(compressed);
        assertNotSame(compressed, preview);
        assertKept(preview);

        // a filter, then a sort, as a client would apply before subscribing
        assertKept(compressed.where("A > 2").sort("B"));
        assertKept(compressed.withAttributes(Map.of("Other", "value")));
        assertKept(compressed.coalesce());
    }

    @Test
    public void attributeDoesNotFollowOperationsThatComputeOrJoinValues() {
        final Table source = source();
        final Table compressed = compressed(source);

        assertDropped(compressed.view("A", "C = A * 2"));
        assertDropped(compressed.updateView("C = A * 2"));
        assertDropped(compressed.update("C = A * 2"));
        assertDropped(compressed.select("A"));
        assertDropped(compressed.wouldMatch("C = A > 5"));
        assertDropped(compressed.naturalJoin(source, "A", "B2 = B"));
        assertDropped(compressed.aj(source, "A>=A", "B2 = B"));
        assertDropped(compressed.head(5));

        // a rollup or tree must allow compression itself
        assertNull(compressed.rollup(List.of(Aggregation.AggSum("A")), "B")
                .getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        final Table treeSource = compressed(TableTools.emptyTable(10)
                .update("ID = ii", "Parent = ii == 0 ? NULL_LONG : 0L"));
        assertNull(treeSource.tree("ID", "Parent").getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
    }

    @Test
    public void copyOperationsAreExactlyTheKeptSet() {
        for (final BaseTable.CopyAttributeOperation op : BaseTable.CopyAttributeOperation.values()) {
            assertEquals(op.name(), KEPT_BY.contains(op),
                    BaseTable.shouldCopyAttribute(BARRAGE_COMPRESSION_ATTRIBUTE, op));
        }
    }
}
