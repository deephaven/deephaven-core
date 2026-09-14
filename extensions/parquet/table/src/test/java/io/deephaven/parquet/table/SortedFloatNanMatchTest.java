//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

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
 * DH-23557 finding 17: sorted-column pushdown answered a <em>match</em> filter with an ordering binary search, and for
 * {@code float}/{@code double} Deephaven's ordering and equality deliberately disagree.
 *
 * <p>
 * {@code QueryLanguageFunctionUtils.compareTo(float, float)} delegates to {@code Double.compare} — a total order, in
 * which {@code NaN} equals itself and {@code -0.0} sorts below {@code 0.0}. {@code eq(float, float)} is plain
 * {@code a == b} — IEEE, in which {@code NaN} equals nothing and {@code -0.0 == 0.0}. A binary search finds the run
 * where {@code compareTo == 0}, which is the wrong question for a match filter at exactly those values.
 *
 * <p>
 * Fuzzer seed {@code -5472033891179623763L}: a sorted {@code float} column ending in four {@code NaN}s, filtered
 * {@code Col0_r != NaN}. Every row should match, since {@code NaN != NaN} is true; the search found the {@code NaN} run
 * and the inverted filter subtracted it, returning 21 rows of 25.
 */
public class SortedFloatNanMatchTest {

    private static final String ROOT_FILENAME = SortedFloatNanMatchTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;
    private boolean savedMemoize;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        savedMemoize = QueryTable.setMemoizeResults(false);
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        QueryTable.setMemoizeResults(savedMemoize);
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    /** A float column sorted ascending: -0.0, 0.0, ordinary values, Infinity, then four NaNs. */
    private Table sortedFloats(final String name) {
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(TableTools.floatCol("F",
                -0.0f, 0.0f, 1.0f, 1.0f, 3.14f, Float.POSITIVE_INFINITY,
                Float.NaN, Float.NaN, Float.NaN, Float.NaN)).sort("F"), dest);
        return ParquetTools.readTable(dest);
    }

    /** The same values with no sortedness claim anywhere: the unambiguous oracle. */
    private static Table unsortedOracle() {
        return TableTools.newTable(TableTools.floatCol("F",
                -0.0f, 0.0f, 1.0f, 1.0f, 3.14f, Float.POSITIVE_INFINITY,
                Float.NaN, Float.NaN, Float.NaN, Float.NaN));
    }

    /**
     * Assert that the sorted, parquet-backed table agrees with the same filter over the same values held plainly in
     * memory. {@code disk.select()} is deliberately not the oracle here: it inherits the sortedness claim, and the
     * in-memory sorted path had the very same defect, so it would have agreed with the wrong answer.
     */
    private void assertAgrees(final Table disk, final String filter) {
        assertEquals(filter, unsortedOracle().where(filter).size(), disk.where(filter).size());
    }

    /** The reported case: every row satisfies {@code != NaN}, including the NaN rows. */
    @Test
    public void notEqualsNaN() {
        final Table disk = sortedFloats("ne_nan");
        // NaN != NaN is true, so every row matches.
        assertEquals(10, unsortedOracle().where("F != NaN").size());
        assertEquals(10, disk.where("F != NaN").size());
    }

    /** The other direction: {@code == NaN} matches nothing, since NaN equals nothing. */
    @Test
    public void equalsNaN() {
        final Table disk = sortedFloats("eq_nan");
        // NaN equals nothing, so nothing matches.
        assertEquals(0, unsortedOracle().where("F == NaN").size());
        assertEquals(0, disk.where("F == NaN").size());
    }

    /** {@code == 0.0} must match {@code -0.0} too, which the total order separates. */
    @Test
    public void equalsZeroMatchesNegativeZero() {
        final Table disk = sortedFloats("eq_zero");
        assertEquals(2, unsortedOracle().where("F == 0.0").size());
        assertEquals(2, disk.where("F == 0.0").size());
    }

    /** And the inverse. */
    @Test
    public void notEqualsZeroExcludesBothZeros() {
        final Table disk = sortedFloats("ne_zero");
        assertEquals(8, unsortedOracle().where("F != 0.0").size());
        assertEquals(8, disk.where("F != 0.0").size());
    }

    /** A multi-value match including NaN. */
    @Test
    public void matchInWithNaN() {
        final Table disk = sortedFloats("in_nan");
        assertAgrees(disk, "F in 1.0, NaN");
        assertAgrees(disk, "F not in 1.0, NaN");
    }

    /** Ordinary float matches must keep working, and keep using the optimization. */
    @Test
    public void ordinaryFloatMatchesStillWork() {
        final Table disk = sortedFloats("ordinary");
        assertEquals(2, disk.where("F == 1.0").size());
        assertEquals(8, disk.where("F != 1.0").size());
        assertEquals(1, disk.where("F == 3.14").size());
        assertAgrees(disk, "F == 1.0");
        assertAgrees(disk, "F != 1.0");
    }

    /** Range filters over the same column, including at the NaN boundary. */
    @Test
    public void rangeFiltersAgree() {
        final Table disk = sortedFloats("ranges");
        assertAgrees(disk, "F < 1.0");
        assertAgrees(disk, "F >= 1.0");
        assertAgrees(disk, "F < NaN");
        assertAgrees(disk, "F >= NaN");
        assertAgrees(disk, "F <= 0.0");
    }

    /**
     * The same defect with no parquet at all: {@code sort} publishes the sortedness claim, and the in-memory sorted
     * match optimization used the ordering's equality. Three ordinary operations.
     */
    @Test
    public void inMemorySortThenMatch() {
        final Table sorted = unsortedOracle().sort("F");
        assertEquals(10, sorted.where("F != NaN").size());
        assertEquals(0, sorted.where("F == NaN").size());
        assertEquals(2, sorted.where("F == 0.0").size());
        assertEquals(8, sorted.where("F != 0.0").size());
    }

    /** {@code in}/{@code not in} compare boxed values, where NaN already equals itself; they must not change. */
    @Test
    public void inAndNotInAreUnchanged() {
        final Table sorted = unsortedOracle().sort("F");
        assertEquals(4, unsortedOracle().where("F in NaN").size());
        assertEquals(4, sorted.where("F in NaN").size());
        assertEquals(6, unsortedOracle().where("F not in NaN").size());
        assertEquals(6, sorted.where("F not in NaN").size());
    }

    /** A double column takes the replicated copy of the same code. */
    @Test
    public void doubleColumnBehavesTheSame() {
        final String dest = Path.of(rootFile.getPath(), "doubles.parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(TableTools.doubleCol("D",
                -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN, Double.NaN)).sort("D"), dest);
        final Table disk = ParquetTools.readTable(dest);
        final Table oracle = TableTools.newTable(TableTools.doubleCol("D",
                -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN, Double.NaN));
        assertEquals(6, oracle.where("D != NaN").size());
        assertEquals(6, disk.where("D != NaN").size());
        assertEquals(2, disk.where("D == 0.0").size());
    }
}
