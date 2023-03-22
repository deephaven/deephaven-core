/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.util.TotalsTableBuilder;
import io.deephaven.function.Numeric;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

public class TestTotalsTable extends RefreshingTableTestCase {

    private static final double EPSILON = 0.000000001;

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
                        new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2",
                                "floatCol", "charCol", "byteCol", "shortCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new IntGenerator(1, 1000),
                        new DoubleGenerator(0, 100),
                        new DoubleGenerator(0, 100, 0.1, 0.001),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 100, 0.1, 0.001),
                        new CharGenerator('a', 'z'),
                        new ByteGenerator(),
                        new ShortGenerator()));

        final TotalsTableBuilder builder = new TotalsTableBuilder();
        final Table totals = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(builder.applyToTable(queryTable)));
        final Map<String, ? extends ColumnSource<?>> resultColumns = totals.getColumnSourceMap();
        assertEquals(1, totals.size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2",
                "floatCol", "byteCol", "shortCol")), resultColumns.keySet());

        assertEquals((long) Numeric.sum((int[]) queryTable.getColumn("intCol").getDirect()),
                totals.getColumn("intCol").get(0));
        assertEquals(Numeric.sum((double[]) queryTable.getColumn("doubleCol").getDirect()),
                totals.getColumn("doubleCol").get(0));
        assertEquals(Numeric.sum((double[]) queryTable.getColumn("doubleNullCol").getDirect()),
                totals.getColumn("doubleNullCol").get(0));
        assertEquals("floatCol", Numeric.sum((float[]) queryTable.getColumn("floatCol").getDirect()),
                (float) totals.getColumn("floatCol").get(0), 0.02);
        assertEquals(shortSum((short[]) queryTable.getColumn("shortCol").getDirect()),
                totals.getColumn("shortCol").get(0));

        builder.setDefaultOperation("skip");
        builder.setOperation("byteCol", "min");
        builder.setOperation("Sym", "first");
        builder.setOperation("intCol2", "last");

        final Table totals2 = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "byteCol")),
                totals2.getColumnSourceMap().keySet());
        assertEquals(Numeric.min((byte[]) queryTable.getColumn("byteCol").getDirect()),
                totals2.getColumn("byteCol").get(0));
        assertEquals(queryTable.getColumn("Sym").get(0), totals2.getColumn("Sym").get(0));
        assertEquals(queryTable.getColumn("intCol2").get(queryTable.size() - 1), totals2.getColumn("intCol2").get(0));

        builder.setOperation("byteCol", "max");
        builder.setOperation("doubleCol", "var");
        builder.setOperation("doubleNullCol", "std");
        builder.addOperation("doubleNullCol", "count");
        builder.setOperation("doubleCol2", "avg");
        builder.setOperation("shortCol", "count");

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            final Table totals3 = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                    .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
            assertEquals(
                    new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "doubleCol", "doubleNullCol__Std",
                            "doubleNullCol__Count", "doubleCol2", "byteCol", "shortCol")),
                    totals3.getColumnSourceMap().keySet());
            assertEquals(
                    Numeric.max((byte[]) queryTable.getColumn("byteCol").getDirect()),
                    totals3.getColumn("byteCol").getByte(0));
            assertEquals(
                    Numeric.var(new DoubleVectorDirect((double[]) queryTable.getColumn("doubleCol").getDirect())),
                    totals3.getColumn("doubleCol").getDouble(0),
                    EPSILON);
            assertEquals(
                    Numeric.std(new DoubleVectorDirect((double[]) queryTable.getColumn("doubleNullCol").getDirect())),
                    totals3.getColumn("doubleNullCol__Std").getDouble(0),
                    EPSILON);
            assertEquals(queryTable.size(), totals3.getColumn("doubleNullCol__Count").getLong(0));
            assertEquals(
                    Numeric.avg(new DoubleVectorDirect((double[]) queryTable.getColumn("doubleCol2").getDirect())),
                    totals3.getColumn("doubleCol2").getDouble(0),
                    EPSILON);
            assertEquals(queryTable.size(), (long) totals3.getColumn("shortCol").get(0));

            final Table totals4 = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                    .computeLocked(() -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
            assertSame(totals3, totals4);
        } finally {
            QueryTable.setMemoizeResults(old);
        }
    }

    public void testTotalsTableIncremental() {
        final int size = 1000;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;

        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2", "shortCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 100),
                new IntGenerator(1, 1000),
                new DoubleGenerator(0, 100),
                new DoubleGenerator(0, 100, 0.1, 0.001),
                new SetGenerator<>(10.1, 20.1, 30.1),
                new ShortGenerator()));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> totalsTableBuilder.applyToTable(queryTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> TotalsTableBuilder
                                .makeTotalsTable(totalsTableBuilder.applyToTable(queryTable)));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> TotalsTableBuilder
                                .makeTotalsTable(totalsTableBuilder.applyToTable(queryTable)));
                    }
                },
        };
        for (int i = 0; i < 50; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}
