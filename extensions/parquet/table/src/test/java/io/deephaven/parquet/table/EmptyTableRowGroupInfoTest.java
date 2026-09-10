//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * DH-23557 finding 1: writing a <em>zero-row</em> table with an explicit {@link RowGroupInfo#maxRows(long)} failed with
 * {@code IllegalArgumentException: Number of groups must be at least 1, got: 0}.
 *
 * <p>
 * {@code RowGroupTableIteratorVisitor.splitByMaxRows} computed {@code (size / maxRows) + (size % maxRows > 0 ? 1 : 0)},
 * which is {@code 0} for an empty table, and then handed that zero to {@code splitByMaxGroups}, which rejects anything
 * below one. Neither value came from the caller -- {@code RowGroupInfo.MaxRows} already rejects a non-positive
 * {@code maxRows} -- so this was an internal invariant violated by the empty case alone.
 *
 * <p>
 * The pushdown fuzzer reached it whenever an empty table, or an empty slice beside populated ones in a multi-file
 * layout, was written with an explicit row-group spec: 40 of its first 100 failures were this one defect. Fuzzer case
 * seed {@code -5347797226962475569L}.
 *
 * @see RowGroupTableIteratorVisitor
 */
public class EmptyTableRowGroupInfoTest {

    private static final String ROOT_FILENAME = EmptyTableRowGroupInfoTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    private static Table emptyTable() {
        return TableTools.newTable(TableTools.intCol("Value"), TableTools.stringCol("Key"));
    }

    private static List<Table> rowGroupsOf(final Table table, final RowGroupInfo rowGroupInfo) {
        final List<Table> groups = new ArrayList<>();
        for (final Iterator<Table> it = RowGroupTableIteratorVisitor.of(rowGroupInfo, table); it.hasNext();) {
            groups.add(it.next());
        }
        return groups;
    }

    private String path(final String name) {
        return Path.of(rootFile.getPath(), name).toString();
    }

    /** The reduced defect: the split itself, with no parquet involved. */
    @Test
    public void emptyTableSplitsByMaxRows() {
        final List<Table> groups = rowGroupsOf(emptyTable(), RowGroupInfo.maxRows(3));
        assertEquals("one group for an empty table, as SingleGroup yields", 1, groups.size());
        assertTrue("the single group is empty", groups.get(0).isEmpty());
    }

    /** {@code maxGroups} already tolerated an empty table by yielding no groups; pin that down. */
    @Test
    public void emptyTableSplitsByMaxGroups() {
        assertEquals(0, rowGroupsOf(emptyTable(), RowGroupInfo.maxGroups(4)).size());
    }

    /** The user-visible symptom: a zero-row write with an explicit row-group spec. */
    @Test
    public void writeEmptyTableWithMaxRows() {
        final String dest = path("empty_max_rows.parquet");
        ParquetTools.writeTable(emptyTable(), dest,
                new ParquetInstructions.Builder().setRowGroupInfo(RowGroupInfo.maxRows(3)).build());
        assertEquals(0, ParquetTools.readTable(dest).size());
    }

    @Test
    public void writeEmptyTableWithMaxGroups() {
        final String dest = path("empty_max_groups.parquet");
        ParquetTools.writeTable(emptyTable(), dest,
                new ParquetInstructions.Builder().setRowGroupInfo(RowGroupInfo.maxGroups(4)).build());
        assertEquals(0, ParquetTools.readTable(dest).size());
    }

    @Test
    public void writeEmptyTableWithByGroups() {
        final String dest = path("empty_by_groups.parquet");
        ParquetTools.writeTable(emptyTable(), dest,
                new ParquetInstructions.Builder().setRowGroupInfo(RowGroupInfo.byGroups(3, "Key")).build());
        assertEquals(0, ParquetTools.readTable(dest).size());
    }

    /**
     * The fuzzer's actual shape: an empty slice beside populated ones in a flat multi-file layout, all written with one
     * set of instructions.
     */
    @Test
    public void writeEmptySliceBesidePopulatedSlices() {
        final ParquetInstructions instructions =
                new ParquetInstructions.Builder().setRowGroupInfo(RowGroupInfo.maxRows(3)).build();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("Value", 1, 2, 3, 4, 5),
                TableTools.stringCol("Key", "a", "a", "b", "b", "c")),
                path("table_00000.parquet"), instructions);
        ParquetTools.writeTable(emptyTable(), path("table_00001.parquet"), instructions);
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("Value", 6, 7),
                TableTools.stringCol("Key", "d", "d")),
                path("table_00002.parquet"), instructions);

        final Table disk = ParquetTools.readTable(rootFile.getPath());
        assertEquals(7, disk.size());
        assertEquals(3, disk.where("Value <= 3").size());
    }

    /** Non-empty splits must be unchanged by the fix. */
    @Test
    public void nonEmptySplitIsUnchanged() {
        final Table table = TableTools.emptyTable(7).update("Value = (int) ii");
        final List<Table> groups = rowGroupsOf(table, RowGroupInfo.maxRows(3));
        assertEquals(3, groups.size());
        assertEquals(3, groups.get(0).size());
        assertEquals(2, groups.get(1).size());
        assertEquals(2, groups.get(2).size());
    }
}
