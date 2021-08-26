package io.deephaven.db.v2;

import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.DbDoubleArrayDirect;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.libs.primitives.*;
import io.deephaven.util.QueryConstants;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

public class TestTotalsTable extends LiveTableTestCase {

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
        .getBooleanForClassWithDefault(TestTotalsTable.class, "CompilerTools.logEnabled", false);

    private boolean oldCompilerToolsLogEnabled;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        oldCompilerToolsLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        CompilerTools.setLogEnabled(oldCompilerToolsLogEnabled);
        super.tearDown();
    }

    private long shortSum(short[] values) {
        long sum = 0;
        for (short value : values) {
            if (value == QueryConstants.NULL_SHORT) {
                continue;
            }
            sum += value;
        }
        return sum;
    }

    public void testTotalsTable() {
        final int size = 1000;
        final Random random = new Random(0);

        final QueryTable queryTable = getTable(size, random,
            initColumnInfos(
                new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol",
                        "doubleCol2", "floatCol", "charCol", "byteCol", "shortCol"},
                new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                new TstUtils.IntGenerator(10, 100),
                new TstUtils.IntGenerator(1, 1000),
                new TstUtils.DoubleGenerator(0, 100),
                new TstUtils.DoubleGenerator(0, 100, 0.1, 0.001),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                new TstUtils.FloatGenerator(0, 100, 0.1, 0.001),
                new TstUtils.CharGenerator('a', 'z'),
                new TstUtils.ByteGenerator(),
                new TstUtils.ShortGenerator()));

        final TotalsTableBuilder builder = new TotalsTableBuilder();
        final Table totals = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(
            () -> TotalsTableBuilder.makeTotalsTable(queryTable.setTotalsTable(builder)));
        final Map<String, ? extends ColumnSource> resultColumns = totals.getColumnSourceMap();
        assertEquals(1, totals.size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("intCol", "intCol2", "doubleCol",
            "doubleNullCol", "doubleCol2", "floatCol", "byteCol", "shortCol")),
            resultColumns.keySet());

        assertEquals(
            (long) IntegerNumericPrimitives.sum((int[]) queryTable.getColumn("intCol").getDirect()),
            totals.getColumn("intCol").get(0));
        assertEquals(
            DoubleNumericPrimitives.sum((double[]) queryTable.getColumn("doubleCol").getDirect()),
            totals.getColumn("doubleCol").get(0));
        assertEquals(
            DoubleNumericPrimitives
                .sum((double[]) queryTable.getColumn("doubleNullCol").getDirect()),
            totals.getColumn("doubleNullCol").get(0));
        assertEquals("floatCol",
            FloatNumericPrimitives.sum((float[]) queryTable.getColumn("floatCol").getDirect()),
            (float) totals.getColumn("floatCol").get(0), 0.02);
        assertEquals(shortSum((short[]) queryTable.getColumn("shortCol").getDirect()),
            totals.getColumn("shortCol").get(0));

        builder.setDefaultOperation("skip");
        builder.setOperation("byteCol", "min");
        builder.setOperation("Sym", "first");
        builder.setOperation("intCol2", "last");

        final Table totals2 = LiveTableMonitor.DEFAULT.exclusiveLock()
            .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "byteCol")),
            totals2.getColumnSourceMap().keySet());
        assertEquals(
            ByteNumericPrimitives.min((byte[]) queryTable.getColumn("byteCol").getDirect()),
            totals2.getColumn("byteCol").get(0));
        assertEquals(queryTable.getColumn("Sym").get(0), totals2.getColumn("Sym").get(0));
        assertEquals(queryTable.getColumn("intCol2").get(queryTable.size() - 1),
            totals2.getColumn("intCol2").get(0));

        builder.setOperation("byteCol", "max");
        builder.setOperation("doubleCol", "var");
        builder.setOperation("doubleNullCol", "std");
        builder.addOperation("doubleNullCol", "count");
        builder.setOperation("doubleCol2", "avg");
        builder.setOperation("shortCol", "count");

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            final Table totals3 = LiveTableMonitor.DEFAULT.exclusiveLock()
                .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
            assertEquals(
                new LinkedHashSet<>(
                    Arrays.asList("Sym", "intCol2", "doubleCol", "doubleNullCol__Std",
                        "doubleNullCol__Count", "doubleCol2", "byteCol", "shortCol")),
                totals3.getColumnSourceMap().keySet());
            assertEquals(
                ByteNumericPrimitives.max((byte[]) queryTable.getColumn("byteCol").getDirect()),
                totals3.getColumn("byteCol").get(0));
            assertEquals(
                DoubleNumericPrimitives.var(new DbDoubleArrayDirect(
                    (double[]) queryTable.getColumn("doubleCol").getDirect())),
                totals3.getColumn("doubleCol").get(0));
            assertEquals(
                DoubleNumericPrimitives.std(new DbDoubleArrayDirect(
                    (double[]) queryTable.getColumn("doubleNullCol").getDirect())),
                totals3.getColumn("doubleNullCol__Std").get(0));
            assertEquals(queryTable.size(), totals3.getColumn("doubleNullCol__Count").get(0));
            assertEquals(
                DoubleNumericPrimitives.avg(new DbDoubleArrayDirect(
                    (double[]) queryTable.getColumn("doubleCol2").getDirect())),
                totals3.getColumn("doubleCol2").get(0));
            assertEquals(queryTable.size(), (long) totals3.getColumn("shortCol").get(0));

            final Table totals4 = LiveTableMonitor.DEFAULT.exclusiveLock()
                .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
            assertTrue(totals3 == totals4);
        } finally {
            QueryTable.setMemoizeResults(old);
        }
    }

    public void testTotalsTableIncremental() throws IOException {
        final int size = 1000;
        final Random random = new Random(0);
        final TstUtils.ColumnInfo columnInfo[];

        final QueryTable queryTable = getTable(size, random,
            columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol",
                        "doubleCol2", "shortCol"},
                new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                new TstUtils.IntGenerator(10, 100),
                new TstUtils.IntGenerator(1, 1000),
                new TstUtils.DoubleGenerator(0, 100),
                new TstUtils.DoubleGenerator(0, 100, 0.1, 0.001),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                new TstUtils.ShortGenerator()));

        final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                            .computeLocked(() -> queryTable.setTotalsTable(totalsTableBuilder));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                            .computeLocked(() -> TotalsTableBuilder
                                .makeTotalsTable(queryTable.setTotalsTable(totalsTableBuilder)));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                            .computeLocked(() -> TotalsTableBuilder
                                .makeTotalsTable(queryTable.setTotalsTable(totalsTableBuilder)));
                    }
                },
        };
        for (int i = 0; i < 50; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}
