//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 13: a filter that references an indexed column <em>and</em> a duplicate alias of it threw on the
 * disk table while working in memory:
 *
 * <pre>
 * disk table threw but memory table did not: IllegalArgumentException: Duplicate source column(s): Col1
 *     at RenameColumnHelper.createLookupAndValidate
 *     at ParquetTableLocation.pushdownDataIndex
 * </pre>
 *
 * <p>
 * {@code pushdownDataIndex} has to present the data-index table's columns under the filter's names. It did that by
 * inverting {@code filterColumnToManagerColumnName} into rename pairs and calling {@code renameColumns}. When a filter
 * names a column and an {@code updateView} alias of it, both filter names map to the same manager column, so the
 * inverted map has two pairs with the same <em>source</em> — which {@code renameColumns} rejects, since a rename cannot
 * produce two columns from one.
 *
 * <p>
 * Fuzzer seeds {@code 428667830982598836L} and {@code 6656699729815370963L}; the first is part of the cluster
 * {@code OLD_FINDINGS.md} recorded as its finding 3.
 *
 * <p>
 * <strong>What pins the defect.</strong> Those two seeds, replayed by {@code PushdownFuzzerTest.testInterestingSeeds} —
 * both fail without the fix and pass with it. The cases <em>here</em> exercise the surrounding shapes and pass either
 * way: reaching the defect needs the filter to arrive at the location still carrying two distinct names for one manager
 * column, and {@code DeferredViewTable.applyFilterRenamings} normally renames it into manager space on the way down,
 * which collapses them ({@code AbstractConditionFilter.getColumns()} is {@code distinct()}). Reconstructing the exact
 * combination of rename, alias and index that avoids that collapse did not succeed; the seeds do it deterministically,
 * so they are the regression test and these are coverage.
 */
public class DuplicateAliasDataIndexTest {

    private static final String ROOT_FILENAME = DuplicateAliasDataIndexTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;
    private boolean savedMemoize;
    private double savedThreshold;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        savedMemoize = QueryTable.setMemoizeResults(false);
        // pushdownDataIndex returns early unless the maybe-set exceeds indexSize / threshold, so the default hides
        // the data-index path on a small table.
        savedThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = Double.MAX_VALUE;
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedThreshold;
        QueryTable.setMemoizeResults(savedMemoize);
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    /** A table with an index on {@code Col1}, read back uncoalesced. */
    private Table indexedOnCol1(final String name) {
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("Col0", 1, 2, 3, 4, 5, 6),
                TableTools.intCol("Col1", 10, 20, 20, 30, 30, 30)), dest,
                new ParquetInstructions.Builder().addIndexColumns("Col1").build());
        return ParquetTools.readTable(dest);
    }

    /**
     * The reported case. The rename matters: it makes the filter's names differ from the index table's, so
     * {@code pushdownDataIndex} takes its renaming path instead of using the index table as-is.
     */
    @Test
    public void filterOverARenamedIndexedColumnAndItsAlias() {
        final Table disk = indexedOnCol1("alias").coalesce().updateView("Dup = Col1");
        final Filter filter = RawString.of("Col1 != null && Dup != null && Col1 == Dup");
        assertEquals(6, disk.select().where(filter).size());
        assertEquals(6, disk.where(filter).size());
    }

    /** The same shape without a rename, which declined the index rather than failing. */
    @Test
    public void filterOverAnIndexedColumnAndItsAlias() {
        final Table disk = indexedOnCol1("alias_plain").updateView("Dup = Col1");
        final Filter filter = RawString.of("Col1 != null && Dup != null && Col1 == Dup");
        assertEquals(6, disk.select().where(filter).size());
        assertEquals(6, disk.where(filter).size());
    }

    /** The alias compared against a value, so the index is genuinely useful. */
    @Test
    public void filterOverTheAliasWithASelectiveComparison() {
        final Table disk = indexedOnCol1("alias_value").coalesce().updateView("Dup = Col1");
        final Filter filter = RawString.of("Col1 == 30 && Dup == 30");
        assertEquals(3, disk.select().where(filter).size());
        assertEquals(3, disk.where(filter).size());
    }

    /** Conjunction of a plain match on another column with the aliased pair, as the fuzz case had. */
    @Test
    public void conjunctionWithAnUnindexedColumn() {
        final Table disk = indexedOnCol1("alias_and").coalesce().updateView("Dup = Col1");
        final Filter filter = Filter.and(
                RawString.of("Col0 != null"),
                RawString.of("Dup != null && Col1 != null && Dup == Col1"));
        assertEquals(6, disk.select().where(filter).size());
        assertEquals(6, disk.where(filter).size());
    }

    /** Two aliases of the same indexed column. */
    @Test
    public void twoAliasesOfTheSameColumn() {
        final Table disk = indexedOnCol1("two_alias").coalesce().updateView("DupA = Col1", "DupB = Col1");
        final Filter filter = RawString.of("DupA == DupB && DupA >= 20");
        assertEquals(5, disk.select().where(filter).size());
        assertEquals(5, disk.where(filter).size());
    }

    /** The alias alone, never mentioning the original name. */
    @Test
    public void filterOverTheAliasAlone() {
        final Table disk = indexedOnCol1("alias_only").coalesce().updateView("Dup = Col1");
        assertEquals(3, disk.select().where("Dup == 30").size());
        assertEquals(3, disk.where("Dup == 30").size());
    }

    /** No alias: the ordinary rename path through the index must keep working. */
    @Test
    public void renamedIndexedColumnIsUnchanged() {
        final Table disk = indexedOnCol1("renamed").renameColumns("Renamed = Col1");
        assertEquals(3, disk.select().where("Renamed == 30").size());
        assertEquals(3, disk.where("Renamed == 30").size());
    }

    /** And with no rename at all. */
    @Test
    public void plainIndexedFilterIsUnchanged() {
        final Table disk = indexedOnCol1("plain");
        assertEquals(3, disk.select().where("Col1 == 30").size());
        assertEquals(3, disk.where("Col1 == 30").size());
        assertEquals(5, disk.where("Col1 >= 20").size());
    }
}
