//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 10: a {@code renameColumns} that reassigns a name another column reads from — a swap, a chain, a
 * rotation — silently produced the <strong>wrong data</strong> on an uncoalesced (parquet-backed) table.
 *
 * <pre>
 * ParquetTools.readTable(dir).renameColumns("A = B", "B = A").coalesce();
 * // B | A expected B | A
 * // 1 | 1 1 | x
 * // 2 | 2 2 | y
 * // 3 | 3 3 | z
 * </pre>
 *
 * <p>
 * {@code RedefinableTable.renameColumns} lowers a rename into {@code SourceColumn}s and
 * {@code DeferredViewTable.applyDeferredViews} applied them with {@code view}. But {@code view} evaluates its columns
 * in order, each against a name space that already holds the ones before it, whereas a rename is simultaneous. So
 * {@code A = B} resolved against the {@code B} that {@code B = A} had just defined, and both columns ended up with the
 * first column's values. The in-memory {@code QueryTable.renameColumns} is simultaneous and was always correct, so the
 * same three lines gave different answers depending on whether the table had been coalesced.
 *
 * <p>
 * The fuzzer saw this as a {@code ClassCastException} rather than as wrong data, because it follows the rename with a
 * {@code merge}: {@code UnionSourceManager} types its columns from {@code constituentDefinition()} — correct, taken
 * from the uncoalesced table — and fetches sources from each coalesced constituent, which had the wrong types.
 *
 * <pre>
 * ClassCastException: ResettableWritableObjectChunk cannot be cast to WritableIntChunk
 *     at IntChunkPage.fillChunkAppend
 *     at UnionColumnSource.fillChunkFromMultipleSources
 * </pre>
 *
 * <p>
 * Fuzzer seeds {@code 5201278404043255708L} and {@code 2423783905725303439L}, the latter under
 * {@code ALL_PUSHDOWN_DISABLED} — so this is not a pushdown defect.
 */
public class DeferredRenameSimultaneityTest {

    private static final String ROOT_FILENAME = DeferredRenameSimultaneityTest.class.getName() + "_root";

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

    private static Table inMemory() {
        return TableTools.newTable(
                TableTools.intCol("A", 1, 2, 3),
                TableTools.stringCol("B", "x", "y", "z"),
                TableTools.doubleCol("C", 1.5, 2.5, 3.5));
    }

    /** The same data read back from parquet, so the rename applies to an uncoalesced table. */
    private Table onDisk(final String name) {
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        ParquetTools.writeTable(inMemory(), dest);
        return ParquetTools.readTable(dest);
    }

    private static List<String> rowStrings(final Table table) {
        final Table selected = table.select();
        final List<String> rows = new ArrayList<>();
        final List<String> names = selected.getDefinition().getColumnNames();
        selected.getRowSet().forAllRowKeys(rowKey -> {
            final StringBuilder sb = new StringBuilder();
            for (final String columnName : names) {
                sb.append(columnName).append('=')
                        .append(selected.getColumnSource(columnName).get(rowKey)).append(' ');
            }
            rows.add(sb.toString().trim());
        });
        return rows;
    }

    /**
     * The property under test: a deferred rename must produce exactly what the same rename produces in memory, where
     * {@code QueryTable.renameColumns} has always been simultaneous.
     */
    private void assertMatchesInMemory(final String name, final String... renames) {
        final List<String> expected = rowStrings(inMemory().renameColumns(renames));
        assertEquals(String.join(", ", renames) + " definition",
                inMemory().renameColumns(renames).getDefinition().getColumnNames(),
                onDisk(name).renameColumns(renames).getDefinition().getColumnNames());
        assertEquals(String.join(", ", renames), expected, rowStrings(onDisk(name).renameColumns(renames)));
    }

    /** The reported case. */
    @Test
    public void swap() {
        assertMatchesInMemory("swap", "A = B", "B = A");
    }

    /** A chain, where the second rename reads the name the first reassigned. */
    @Test
    public void chain() {
        assertMatchesInMemory("chain", "B = A", "C = B");
    }

    /** A three-way rotation. */
    @Test
    public void rotation() {
        assertMatchesInMemory("rotation", "A = B", "B = C", "C = A");
    }

    /** A swap that leaves a third column untouched. */
    @Test
    public void swapLeavingAColumnAlone() {
        assertMatchesInMemory("swap_partial", "A = B", "B = A");
    }

    /** A plain rename to fresh names, which was never affected and must stay correct. */
    @Test
    public void uniformRenameUnaffected() {
        assertMatchesInMemory("uniform", "A2 = A", "B2 = B", "C2 = C");
    }

    /** A single masking rename, which drops the shadowed column. */
    @Test
    public void singleMaskingRename() {
        assertMatchesInMemory("masking", "B = A");
    }

    /** The loud symptom: rename then merge, which is how the fuzzer met it. */
    @Test
    public void swapThenMerge() {
        final Table renamed = onDisk("swap_merge").renameColumns("A = B", "B = A");
        final Table merged = TableTools.merge(renamed, renamed);
        assertEquals(6, merged.size());

        final List<String> expectedOnce = rowStrings(inMemory().renameColumns("A = B", "B = A"));
        final List<String> expected = new ArrayList<>(expectedOnce);
        expected.addAll(expectedOnce);
        assertEquals(expected, rowStrings(merged));
    }

    /** A rotation then merge, the same shape one level harder. */
    @Test
    public void rotationThenMerge() {
        final Table renamed = onDisk("rotate_merge").renameColumns("A = B", "B = C", "C = A");
        final Table merged = TableTools.merge(renamed, renamed);
        assertEquals(6, merged.size());
        // A holds old B = [x, y, z], once per constituent.
        assertEquals(2, merged.where("A == `x`").size());
    }

    /** A filter over a swapped column must see the swapped values. */
    @Test
    public void filterAfterSwap() {
        final Table swapped = onDisk("swap_filter").renameColumns("A = B", "B = A");
        assertEquals(1, swapped.where("A == `x`").size());
        assertEquals(1, swapped.where("B == 1").size());
        assertEquals(1, swapped.select().where("A == `x`").size());
    }

    /**
     * A duplicating view on top of a rename, which reads one source column twice. {@code view}'s sequential evaluation
     * is correct there — nothing is reassigned — and the fix must leave it alone; this is finding 7's shape.
     */
    @Test
    public void duplicatingViewIsUnaffected() {
        final Table duplicated = onDisk("dup").renameColumns("Renamed = A").updateView("Dup = Renamed");
        assertEquals(3, duplicated.size());
        assertEquals(1, duplicated.where("Renamed == 1 && Dup == 1").size());
        assertEquals(0, duplicated.where("Renamed == 1 && Dup == 2").size());
    }
}
