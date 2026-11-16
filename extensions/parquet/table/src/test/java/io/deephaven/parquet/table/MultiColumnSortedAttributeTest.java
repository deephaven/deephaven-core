//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * A table location reporting a <em>multi-column</em> sort must have only its leading column published as sorted.
 *
 * <p>
 * {@code TableLocation.getSortedColumns()} is "ordered by precedence, representing a multi-column sort", and a
 * multi-column sort orders each later column only <em>within ties</em> of the ones before it. {@code SourceTable}'s
 * coalesce used to publish every column of that list through {@code SortedColumnsAttribute.setOrderForColumn}, and
 * {@code AbstractRangeFilter} then binary-searched the trailing ones as if they were sorted, with no pushdown switch
 * gating it.
 *
 * <p>
 * The parquet data-index writer produces such a location: a composite index on {@code (A, B)} is written
 * {@code sort(A, B)} and records both columns as ascending sorting columns. {@code ParquetTableLocation}'s data-index
 * pushdown applies a filter to the index table and trusts the answer as exact, so a false claim about {@code B} became
 * a wrong answer for the data: a disjunction over both index columns matched 7 of 7 rows where 6 was correct.
 *
 * @see io.deephaven.engine.table.impl.locations.TableLocation#getSortedColumns()
 */
public class MultiColumnSortedAttributeTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private String path(final String name) {
        return new File(tempDir.getRoot(), name + ".parquet").getPath();
    }

    /**
     * Runs {@code test} with the data-index action engaged however small the table. {@code pushdownDataIndex} returns
     * early unless the maybe-set is larger than {@code indexSize / DATA_INDEX_FOR_WHERE_THRESHOLD}, so the default
     * threshold hides the bug on a small table; raising it is what the fuzz case's toggle profile did.
     */
    private static void withDataIndexPushdown(final Runnable test) {
        final double savedThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = Double.MAX_VALUE;
        try {
            test.run();
        } finally {
            QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedThreshold;
        }
    }

    /**
     * Returns the sorted columns the engine publishes for the index table that {@code dest}'s data index on
     * {@code keyColumnNames} reads, from a file whose footer records the sort of each of its key columns.
     */
    private static List<SortColumn> publishedSortOfIndexTable(final String dest, final String... keyColumnNames) {
        final ParquetTableLocation location = new ParquetTableLocation(
                StandaloneTableKey.getInstance(),
                new ParquetTableLocationKey(new File(dest).toURI(), 0, null, ParquetInstructions.EMPTY),
                ParquetInstructions.EMPTY);
        return SortedColumnsAttribute.getSortedColumns(location.loadDataIndex(keyColumnNames).table().coalesce());
    }

    /**
     * A table sorted by {@code A} whose {@code B} is deliberately <em>not</em> monotonic within the whole column, only
     * within ties of {@code A}. That is what a multi-column sort produces, and what makes the false claim about
     * {@code B} observable.
     */
    private String writeWithCompositeIndex(final String name) {
        final String dest = path(name);
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
        ParquetTools.writeTable(source, dest, ParquetInstructions.builder().addIndexColumns("A", "B").build());
        return dest;
    }

    /**
     * The composite index table is written {@code sort(A, B)} and records both columns as sorted, but only {@code A},
     * its leading column, is published as sorted.
     */
    @Test
    public void compositeIndexTablePublishesOnlyItsLeadingColumn() {
        assertEquals(List.of(SortColumn.asc(ColumnName.of("A"))),
                publishedSortOfIndexTable(writeWithCompositeIndex("composite_sort"), "A", "B"));
    }

    /**
     * The fuzzer's route: a composite data index makes {@code pushdownDataIndex} apply the filter to the index table
     * and trust it as exact, so a false sortedness claim on the index table's trailing column becomes a wrong answer
     * for the data.
     */
    @Test
    public void disjunctionOverACompositeDataIndex() {
        withDataIndexPushdown(this::disjunctionOverACompositeDataIndexImpl);
    }

    private void disjunctionOverACompositeDataIndexImpl() {
        final Table disk = ParquetTools.readTable(writeWithCompositeIndex("composite"));
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
        withDataIndexPushdown(this::eachDisjunctAloneImpl);
    }

    private void eachDisjunctAloneImpl() {
        final Table disk = ParquetTools.readTable(writeWithCompositeIndex("composite_parts"));
        ExecutionContext.getContext().getQueryScope().putParam("bound", new BigDecimal("55.5658"));
        ExecutionContext.getContext().getQueryScope().putParam("key", LocalDate.parse("1970-01-01"));
        assertEquals(5, disk.where(RawString.of("B >= bound")).size());
        assertEquals(5, disk.where(RawString.of("A != key")).size());
    }

    /** A single-column data index table is genuinely sorted by its key, so its claim must survive. */
    @Test
    public void singleColumnIndexKeepsItsClaim() {
        final String dest = path("single");
        ParquetTools.writeTable(TableTools.newTable(
                TableTools.intCol("A", 3, 1, 2, 1),
                TableTools.intCol("B", 30, 10, 20, 15)), dest,
                ParquetInstructions.builder().addIndexColumns("A").build());
        assertEquals(List.of(SortColumn.asc(ColumnName.of("A"))), publishedSortOfIndexTable(dest, "A"));
    }
}
