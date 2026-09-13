//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 11: a table location reporting a <em>multi-column</em> sort had every one of its columns published
 * as independently sorted, which is false for all but the first — and the false claim silently returned wrong rows.
 *
 * <p>
 * {@code SourceTable.doCoalesce} looped over {@code TableLocation.getSortedColumns()} and called
 * {@code SortedColumnsAttribute.setOrderForColumn} for each. But that list is, in its own javadoc, "ordered by
 * precedence, representing a multi-column sort", and a multi-column sort orders each later column only <em>within
 * ties</em> of the ones before it. Only the leading column is sorted on its own.
 *
 * <p>
 * The reachable producer is the parquet data-index writer: a composite index on {@code (A, B)} is written
 * {@code sort(A, B)} and records both columns as ascending sorting columns. Reading it back published
 * {@code SortedColumns=A=Ascending,B=Ascending}, and {@code AbstractRangeFilter} then binary-searched {@code B} as if
 * ascending — with no pushdown switch gating it, since it consults the attribute directly.
 *
 * <p>
 * The fuzzer met it through {@code ParquetTableLocation.pushdownDataIndex}, which applies the filter to the index table
 * and trusts the answer as exact: a disjunction over both index columns returned 7 of 7 index rows where 6 was correct,
 * so a non-matching row was reported as a definite match. Fuzzer case seed {@code 1681357320861610709L}, recorded as
 * finding 7a in {@code OLD_FINDINGS.md} and open since that round.
 *
 * @see io.deephaven.engine.table.impl.locations.TableLocation#getSortedColumns()
 */
public class MultiColumnSortedAttributeTest {

    private static final String ROOT_FILENAME = MultiColumnSortedAttributeTest.class.getName() + "_root";

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

    /**
     * A table sorted by {@code A} whose {@code B} is deliberately <em>not</em> monotonic within the whole column, only
     * within ties of {@code A}. That is what a multi-column sort produces, and what makes the false claim about
     * {@code B} observable.
     */
    private Table writeWithCompositeIndex(final String name) {
        final String dest = Path.of(rootFile.getPath(), name + ".parquet").toString();
        final Table source = TableTools.newTable(
                TableTools.col("A",
                        LocalDate.parse("1900-01-01"), LocalDate.parse("1900-03-01"),
                        LocalDate.parse("1970-01-01"), LocalDate.parse("1970-01-01"),
                        LocalDate.parse("2000-01-01"), LocalDate.parse("2024-02-29"),
                        LocalDate.parse("2024-02-29")),
                TableTools.col("B",
                        new BigDecimal("9.19012"), new BigDecimal("3004.23000"), new BigDecimal("8.09598"),
                        new BigDecimal("116031.00000"), new BigDecimal("55.56580"), new BigDecimal("89.85440"),
                        new BigDecimal("689.79600")));
        ParquetTools.writeTable(source, dest,
                new ParquetInstructions.Builder().addIndexColumns("A", "B").build());
        return ParquetTools.readTable(dest);
    }

    /**
     * The fuzzer's route: a composite data index makes {@code pushdownDataIndex} apply the filter to the index table
     * and trust it as exact, so a false sortedness claim on the index table's trailing column becomes a wrong answer
     * for the data.
     */
    @Test
    public void disjunctionOverACompositeDataIndex() {
        // pushdownDataIndex returns early unless the maybe-set is larger than
        // indexSize / DATA_INDEX_FOR_WHERE_THRESHOLD, so the default threshold hides this on a small table. Raising
        // it makes the data-index action engage, which is what the fuzz case's toggle profile did.
        final double savedThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
        try {
            QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = Double.MAX_VALUE;
            disjunctionOverACompositeDataIndexImpl();
        } finally {
            QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedThreshold;
        }
    }

    private void disjunctionOverACompositeDataIndexImpl() {
        final Table disk = writeWithCompositeIndex("composite");
        ExecutionContext.getContext().getQueryScope().putParam("bound", new BigDecimal("55.5658"));
        ExecutionContext.getContext().getQueryScope().putParam("key", LocalDate.parse("1970-01-01"));
        final Filter filter = Filter.or(RawString.of("B >= bound"), RawString.of("A != key"));

        // Row 2 (A = 1970-01-01, B = 8.09598) satisfies neither disjunct; the other six satisfy at least one.
        assertEquals(6, disk.select().where(filter).size());
        assertEquals(6, disk.where(filter).size());
    }

    /** Each disjunct alone was always correct; keep them covered so a regression is localized. */
    @Test
    public void eachDisjunctAloneOverACompositeDataIndex() {
        final double savedThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = Double.MAX_VALUE;
        try {
            eachDisjunctAloneImpl();
        } finally {
            QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedThreshold;
        }
    }

    private void eachDisjunctAloneImpl() {
        final Table disk = writeWithCompositeIndex("composite_parts");
        ExecutionContext.getContext().getQueryScope().putParam("bound", new BigDecimal("55.5658"));
        ExecutionContext.getContext().getQueryScope().putParam("key", LocalDate.parse("1970-01-01"));
        assertEquals(5, disk.where(RawString.of("B >= bound")).size());
        assertEquals(5, disk.where(RawString.of("A != key")).size());
    }

    /** A single-column data index is genuinely sorted, so its claim must survive. */
    @Test
    public void singleColumnIndexKeepsItsClaim() {
        final String dest = Path.of(rootFile.getPath(), "single.parquet").toString();
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("A", 3, 1, 2),
                TableTools.intCol("B", 30, 10, 20)).sort("A"), dest,
                new ParquetInstructions.Builder().addIndexColumns("A").build());
        final Table disk = ParquetTools.readTable(dest);
        assertEquals(List.of(SortColumn.asc(ColumnName.of("A"))),
                SortedColumnsAttribute.getSortedColumns(disk.coalesce()));
        assertEquals(2, disk.where("A >= 2").size());
        assertEquals(2, disk.select().where("A >= 2").size());
    }
}
