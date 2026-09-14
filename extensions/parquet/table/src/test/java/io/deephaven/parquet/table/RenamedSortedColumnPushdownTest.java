//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 9: a parquet file's recorded sorting column was never translated out of parquet name space, so under
 * a read-time rename that <em>permutes</em> names, the sortedness claim landed on a different, unsorted column and rows
 * were silently dropped.
 *
 * <p>
 * {@code ParquetTableLocation.initialize} set {@code sortingColumns} straight from the file's Deephaven
 * {@link io.deephaven.parquet.table.metadata.TableInfo}, whose names are the ones the writer saw — parquet-space names.
 * Both consumers of {@code TableLocation.getSortedColumns()} read them as table-space:
 *
 * <ol>
 * <li>{@code SourceTable.doCoalesce} publishes them as the coalesced table's {@link SortedColumnsAttribute}, a
 * user-visible claim that also drives {@code AbstractRangeFilter}'s binary search — with no pushdown setting gating
 * it.</li>
 * <li>Every {@code ParquetColumnRegion*.estimatePushdownAction} compares them against
 * {@code filterColumnToManagerColumnName}'s output, which is table-space.</li>
 * </ol>
 *
 * <p>
 * A <em>uniform</em> rename hides the defect: the untranslated parquet name then matches no table column, the claim is
 * declined, and the answer is right by accident. Only a permutation leaves the stale name valid while pointing
 * elsewhere. Both pre-existing rename tests ({@code ParquetTableFilterTest.flatPartitionsColumnRenameTest} and
 * {@code flatPartitionsInstructionColumnRenameTest}) rename every column with a uniform suffix, which is precisely the
 * shape under which the wrong code returns the right answer.
 *
 * <p>
 * Fuzzer case seed {@code -5347797226962475569L}, which dropped 11 of 28 rows.
 */
public class RenamedSortedColumnPushdownTest {

    private static final String ROOT_FILENAME = RenamedSortedColumnPushdownTest.class.getName() + "_root";

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

    /**
     * Storage holds {@code A} sorted ascending and {@code B} unsorted; the file records sorting column {@code A}.
     *
     * @return the path written
     */
    private String writeSortedOnA(final String name) {
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3, 4),
                TableTools.doubleCol("B", -0.0, -1.0, -2.0, -3.0)).sort("A"), dest);
        return dest;
    }

    /** Rotate the names, so table {@code A} is storage {@code B} (the unsorted doubles) and table {@code B} is A. */
    private static ParquetInstructions permutingRename() {
        return new ParquetInstructions.Builder()
                .addColumnNameMapping("A", "B")
                .addColumnNameMapping("B", "A")
                .build();
    }

    /** The defect at its most direct: a range filter over the falsely-sorted column drops rows. */
    @Test
    public void rangeFilterUnderPermutedRename() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("range"), permutingRename());
        // Table A is storage B = [-0.0, -1.0, -2.0, -3.0], which is descending, not ascending.
        assertEquals(3, disk.where("A <= -1.0").size());
        assertEquals(3, disk.select().where("A <= -1.0").size());
    }

    /** A match filter takes the other branch of the same claim. */
    @Test
    public void matchFilterUnderPermutedRename() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("match"), permutingRename());
        assertEquals(1, disk.where("A == -1.0").size());
        assertEquals(1, disk.select().where("A == -1.0").size());
    }

    /**
     * The user-visible half of the defect: the coalesced table must not advertise a sort order for a column that is not
     * sorted. This is what made the range filter wrong even with every pushdown switch off.
     */
    @Test
    public void coalescedTableMakesNoFalseSortClaim() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("attribute"), permutingRename()).coalesce();
        // Table A is storage B, which is not sorted ascending; no claim may be made about it.
        assertEquals(Optional.empty(), SortedColumnsAttribute.getOrderForColumn(disk, "A"));
    }

    /** The claim must land on the column that really is sorted, so the optimization is not simply abandoned. */
    @Test
    public void sortClaimFollowsTheRenameToTheRightColumn() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("followed"), permutingRename()).coalesce();
        // Storage A (sorted ascending) is read as table B.
        assertEquals(List.of(SortColumn.asc(ColumnName.of("B"))),
                SortedColumnsAttribute.getSortedColumns(disk));
        assertEquals(2, disk.where("B <= 2").size());
    }

    /** With no rename at all, the claim and the filters are unchanged. */
    @Test
    public void noRenameIsUnchanged() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("plain")).coalesce();
        assertEquals(List.of(SortColumn.asc(ColumnName.of("A"))),
                SortedColumnsAttribute.getSortedColumns(disk));
        assertEquals(2, disk.where("A <= 2").size());
        assertEquals(2, disk.select().where("A <= 2").size());
    }

    /** A uniform rename — the shape that accidentally worked — must keep working, and now keeps the claim. */
    @Test
    public void uniformRenameKeepsTheClaim() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("uniform"), new ParquetInstructions.Builder()
                .addColumnNameMapping("A", "A_renamed")
                .addColumnNameMapping("B", "B_renamed")
                .build()).coalesce();
        assertEquals(List.of(SortColumn.asc(ColumnName.of("A_renamed"))),
                SortedColumnsAttribute.getSortedColumns(disk));
        assertEquals(2, disk.where("A_renamed <= 2").size());
        assertEquals(2, disk.select().where("A_renamed <= 2").size());
    }

    /**
     * The sorted parquet column left unmapped while another column takes its name. Table {@code A} reads storage
     * {@code B}, and storage {@code A} -- the sorted one -- is not read at all. Naming {@code A} as sorted would be
     * false, and there is no table-space name for the column that really is sorted, so the claim must be dropped rather
     * than passed through untranslated.
     */
    @Test
    public void unmappedSortedColumnWhoseNameIsTakenDropsTheClaim() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("shadowed"), new ParquetInstructions.Builder()
                .addColumnNameMapping("B", "A")
                .setTableDefinition(TableDefinition.of(ColumnDefinition.ofDouble("A")))
                .build()).coalesce();
        assertEquals(Optional.empty(), SortedColumnsAttribute.getOrderForColumn(disk, "A"));
        assertEquals(3, disk.where("A <= -1.0").size());
        assertEquals(3, disk.select().where("A <= -1.0").size());
    }

    /**
     * The second site: {@code hasDictionaryPage} looked a column location up by its <em>parquet</em> name, but
     * {@code getColumnLocation} takes a table-space name and translates it itself, so the double translation fetched a
     * different physical column. It is reached from {@code estimatePushdownAction}, so a query fails merely from the
     * dictionary action being <em>considered</em>. With the permuted types here the wrong column cannot convert:
     * {@code IllegalArgumentException: Cannot convert parquet int column to double}. This is what
     * {@link #rangeFilterUnderPermutedRename} and {@link #matchFilterUnderPermutedRename} above actually exercise --
     * reverting only that site makes exactly those two fail.
     */
    @Test
    public void bothPermutedColumnsInOneFilter() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("conjunction"), permutingRename());
        // Table A is storage B = [-0.0, -1.0, -2.0, -3.0]; table B is storage A = [1, 2, 3, 4].
        // A <= -1.0 keeps rows 1..3, B >= 2 keeps rows 1..3, so the conjunction is all three.
        assertEquals(3, disk.select().where("A <= -1.0", "B >= 2").size());
        assertEquals(3, disk.where("A <= -1.0", "B >= 2").size());
    }

    /** A multi-value match, another route into the same estimate. */
    @Test
    public void multiValueMatchUnderPermutedRename() {
        final Table disk = ParquetTools.readTable(writeSortedOnA("multi"), permutingRename());
        assertEquals(2, disk.select().where("B in 1, 3").size());
        assertEquals(2, disk.where("B in 1, 3").size());
    }

    /** String columns, where a dictionary really is written and used. */
    @Test
    public void stringColumnsUnderPermutedRename() {
        final String dest = Path.of(rootFile.getPath(), "strings.parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.stringCol("SA", "aa", "aa", "bb", "bb", "cc", "cc"),
                TableTools.stringCol("SB", "xx", "yy", "xx", "yy", "xx", "yy")).sort("SA"), dest);
        final Table disk = ParquetTools.readTable(dest, new ParquetInstructions.Builder()
                .addColumnNameMapping("SA", "SB")
                .addColumnNameMapping("SB", "SA")
                .build());
        // Table SA holds storage SB's values.
        assertEquals(3, disk.select().where("SA == `xx`").size());
        assertEquals(3, disk.where("SA == `xx`").size());
        // A value belonging to the other column must borrow none of its rows.
        assertEquals(0, disk.select().where("SA == `aa`").size());
        assertEquals(0, disk.where("SA == `aa`").size());
    }

    /** Descending order must be carried through the translation too, not silently flipped. */
    @Test
    public void descendingOrderSurvivesTranslation() {
        final String dest = Path.of(rootFile.getPath(), "descending.parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3, 4),
                TableTools.doubleCol("B", -0.0, -1.0, -2.0, -3.0)).sortDescending("A"), dest);
        final Table disk = ParquetTools.readTable(dest, permutingRename()).coalesce();
        assertEquals(List.of(SortColumn.desc(ColumnName.of("B"))),
                SortedColumnsAttribute.getSortedColumns(disk));
        assertEquals(2, disk.where("B <= 2").size());
        assertEquals(2, disk.select().where("B <= 2").size());
    }
}
