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
import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 18: a location's data index can read back with a <em>different column type</em> than the data it
 * indexes, and matching against it then silently finds nothing.
 *
 * <p>
 * {@code ParquetTableLocation.readDataIndexTable} reads the index file with no
 * {@link io.deephaven.engine.table.TableDefinition}, so its column types are inferred from that file alone. A
 * {@code BigDecimal} column whose values happen to have scale 0 is written as {@code DECIMAL(p, 0)}, which infers back
 * as {@code BigInteger}. Matching a {@code BigDecimal} value against a {@code BigInteger} column compares
 * {@code BigDecimal.equals(BigInteger)}, which is always {@code false}.
 *
 * <p>
 * Nothing throws. The index reports that nothing matches, {@code pushdownDataIndex} returns that with an empty
 * maybe-set — an <em>exact</em> claim — and every consumer believes it. Under a negation the rows then all appear to
 * match:
 *
 * <pre>
 * disk.where(Filter.not(RawString.of("Col0 in p1")))   // 5 rows; the correct answer is 4
 * disk.where(Filter.or(a, b))                          // 1 row;  the correct answer is 2
 * </pre>
 *
 * <p>
 * The un-negated {@code Col0 in p1} was right, because a cheaper action resolved that region before the index was
 * consulted — which is what made this hard to see.
 *
 * <p>
 * This is the silent form of the mismatch {@code DH-19443} covers; the {@code catch} in {@code pushdownDataIndex}
 * cannot see it because there is no exception. Fuzzer case seed {@code -7982720036514329702L}.
 */
public class DataIndexTypeMismatchTest {

    private static final String ROOT_FILENAME = DataIndexTypeMismatchTest.class.getName() + "_root";

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private File rootFile;
    private boolean savedMemoize;
    private double savedIndexThreshold;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        savedMemoize = QueryTable.setMemoizeResults(false);
        savedIndexThreshold = QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD;
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
        QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD = savedIndexThreshold;
        QueryTable.setMemoizeResults(savedMemoize);
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
        executionContextCloseable.close();
    }

    /**
     * Mixed scales, one value per file so each file computes its own decimal scale. The {@code -1} file gets scale 0
     * and its index therefore infers back as {@code BigInteger}; the others keep scale 21 and stay {@code BigDecimal}.
     */
    private static final BigDecimal[] VALUES = {
            new BigDecimal("-1"), new BigDecimal("1E-21"),
            new BigDecimal("1.000000000000000000000"), new BigDecimal("1E-21"), null};

    private Table onDisk() {
        final ParquetInstructions instructions =
                new ParquetInstructions.Builder().addIndexColumns("Col0").build();
        for (int ii = 0; ii < VALUES.length; ++ii) {
            ParquetTools.writeTable(TableTools.newTable(TableTools.col("Col0", VALUES[ii])),
                    Path.of(rootFile.getPath(), String.format("t_%02d.parquet", ii)).toString(), instructions);
        }
        return ParquetTools.readTable(rootFile.getPath());
    }

    private static Table oracle() {
        return TableTools.newTable(TableTools.col("Col0", VALUES));
    }

    private void assertAgrees(final String label, final Filter filter) {
        assertEquals(label, oracle().where(filter).size(), onDisk().where(filter).size());
    }

    private static Filter matchesMinusOne() {
        ExecutionContext.getContext().getQueryScope().putParam("p1", new BigDecimal("-1"));
        return RawString.of("Col0 in p1");
    }

    /** The reduced case: the negation returned every row. */
    @Test
    public void negatedMatch() {
        final Filter a = matchesMinusOne();
        assertEquals(4, oracle().where(Filter.not(a)).size());
        assertEquals(4, onDisk().where(Filter.not(a)).size());
    }

    /** The un-negated form, which was right by luck and must stay right. */
    @Test
    public void plainMatch() {
        final Filter a = matchesMinusOne();
        assertEquals(1, oracle().where(a).size());
        assertEquals(1, onDisk().where(a).size());
    }

    /** A disjunction lost a row for the same reason. */
    @Test
    public void disjunction() {
        final Filter a = matchesMinusOne();
        final Filter b = RawString.of("Col0 == null");
        assertEquals(2, oracle().where(Filter.or(a, b)).size());
        assertEquals(2, onDisk().where(Filter.or(a, b)).size());
    }

    /** The full nested shape the fuzzer generated. */
    @Test
    public void nestedNegation() {
        final Filter a = matchesMinusOne();
        ExecutionContext.getContext().getQueryScope().putParam("p2", BigDecimal.ZERO);
        final Filter filter = Filter.not(Filter.or(
                a, RawString.of("Col0 == null"), Filter.not(RawString.of("Col0 < p2"))));
        assertEquals(0, oracle().where(filter).size());
        assertEquals(0, onDisk().where(filter).size());
    }

    /** A conjunction of negations, which trusted the same exact claim. */
    @Test
    public void conjunctionOfNegations() {
        final Filter a = matchesMinusOne();
        assertAgrees("and(not(a), not(null))",
                Filter.and(Filter.not(a), Filter.not(RawString.of("Col0 == null"))));
    }

    /** A uniform-scale column indexes and matches normally; the optimization must not be lost generally. */
    @Test
    public void uniformScaleColumnIsUnaffected() {
        final File dir = Path.of(rootFile.getPath(), "uniform").toFile();
        // noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        ParquetTools.writeTable(TableTools.newTable(TableTools.col("Col0",
                new BigDecimal("1.50"), new BigDecimal("2.50"), new BigDecimal("1.50"))),
                Path.of(dir.getPath(), "u.parquet").toString(),
                new ParquetInstructions.Builder().addIndexColumns("Col0").build());
        final Table disk = ParquetTools.readTable(dir.getPath());
        ExecutionContext.getContext().getQueryScope().putParam("q", new BigDecimal("1.50"));
        assertEquals(2, disk.where(RawString.of("Col0 in q")).size());
        assertEquals(1, disk.where(Filter.not(RawString.of("Col0 in q"))).size());
    }

    /** An int column, where the index type always agrees, must be untouched. */
    @Test
    public void intColumnIsUnaffected() {
        final File dir = Path.of(rootFile.getPath(), "ints").toFile();
        // noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        ParquetTools.writeTable(TableTools.newTable(TableTools.intCol("Col0", 7, 8, 9)),
                Path.of(dir.getPath(), "i.parquet").toString(),
                new ParquetInstructions.Builder().addIndexColumns("Col0").build());
        final Table disk = ParquetTools.readTable(dir.getPath());
        assertEquals(1, disk.where("Col0 in 7").size());
        assertEquals(2, disk.where(Filter.not(RawString.of("Col0 in 7"))).size());
    }
}
