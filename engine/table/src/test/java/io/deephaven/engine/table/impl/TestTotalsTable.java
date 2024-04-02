//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.util.TotalsTableBuilder;
import io.deephaven.function.Numeric;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
        final Table totals = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> TotalsTableBuilder.makeTotalsTable(builder.applyToTable(queryTable)));
        final Set<String> resultColumns = totals.getDefinition().getColumnNameSet();
        assertEquals(1, totals.size());
        assertEquals(new LinkedHashSet<>(Arrays.asList("intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2",
                "floatCol", "byteCol", "shortCol")), resultColumns);

        assertEquals((long) Numeric.sum((int[]) DataAccessHelpers.getColumn(queryTable, "intCol").getDirect()),
                DataAccessHelpers.getColumn(totals, "intCol").get(0));
        assertEquals(Numeric.sum((double[]) DataAccessHelpers.getColumn(queryTable, "doubleCol").getDirect()),
                DataAccessHelpers.getColumn(totals, "doubleCol").get(0));
        assertEquals(Numeric.sum((double[]) DataAccessHelpers.getColumn(queryTable, "doubleNullCol").getDirect()),
                DataAccessHelpers.getColumn(totals, "doubleNullCol").get(0));
        assertEquals("floatCol", Numeric.sum((float[]) DataAccessHelpers.getColumn(queryTable, "floatCol").getDirect()),
                (float) DataAccessHelpers.getColumn(totals, "floatCol").get(0), 0.02);
        assertEquals(shortSum((short[]) DataAccessHelpers.getColumn(queryTable, "shortCol").getDirect()),
                DataAccessHelpers.getColumn(totals, "shortCol").get(0));

        builder.setDefaultOperation("skip");
        builder.setOperation("byteCol", "min");
        builder.setOperation("Sym", "first");
        builder.setOperation("intCol2", "last");

        final Table totals2 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "byteCol")),
                totals2.getDefinition().getColumnNameSet());
        assertEquals(Numeric.min((byte[]) DataAccessHelpers.getColumn(queryTable, "byteCol").getDirect()),
                DataAccessHelpers.getColumn(totals2, "byteCol").get(0));
        assertEquals(DataAccessHelpers.getColumn(queryTable, "Sym").get(0),
                DataAccessHelpers.getColumn(totals2, "Sym").get(0));
        assertEquals(DataAccessHelpers.getColumn(queryTable, "intCol2").get(queryTable.size() - 1),
                DataAccessHelpers.getColumn(totals2, "intCol2").get(0));

        builder.setOperation("byteCol", "max");
        builder.setOperation("doubleCol", "var");
        builder.setOperation("doubleNullCol", "std");
        builder.addOperation("doubleNullCol", "count");
        builder.setOperation("doubleCol2", "avg");
        builder.setOperation("shortCol", "count");

        final boolean old = QueryTable.setMemoizeResults(true);
        try {
            final Table totals3 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                    () -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
            assertEquals(
                    new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "doubleCol", "doubleNullCol__Std",
                            "doubleNullCol__Count", "doubleCol2", "byteCol", "shortCol")),
                    totals3.getDefinition().getColumnNameSet());
            assertEquals(
                    Numeric.max((byte[]) DataAccessHelpers.getColumn(queryTable, "byteCol").getDirect()),
                    DataAccessHelpers.getColumn(totals3, "byteCol").getByte(0));
            assertEquals(
                    Numeric.var(new DoubleVectorDirect(
                            (double[]) DataAccessHelpers.getColumn(queryTable, "doubleCol").getDirect())),
                    DataAccessHelpers.getColumn(totals3, "doubleCol").getDouble(0),
                    EPSILON);
            assertEquals(
                    Numeric.std(new DoubleVectorDirect(
                            (double[]) DataAccessHelpers.getColumn(queryTable, "doubleNullCol").getDirect())),
                    DataAccessHelpers.getColumn(totals3, "doubleNullCol__Std").getDouble(0),
                    EPSILON);
            assertEquals(queryTable.size(), DataAccessHelpers.getColumn(totals3, "doubleNullCol__Count").getLong(0));
            assertEquals(
                    Numeric.avg(new DoubleVectorDirect(
                            (double[]) DataAccessHelpers.getColumn(queryTable, "doubleCol2").getDirect())),
                    DataAccessHelpers.getColumn(totals3, "doubleCol2").getDouble(0),
                    EPSILON);
            assertEquals(queryTable.size(), (long) DataAccessHelpers.getColumn(totals3, "shortCol").get(0));

            final Table totals4 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                    () -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
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
                        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                                () -> totalsTableBuilder.applyToTable(queryTable));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                                () -> TotalsTableBuilder.makeTotalsTable(totalsTableBuilder.applyToTable(queryTable)));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        final TotalsTableBuilder totalsTableBuilder = new TotalsTableBuilder();
                        return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                                () -> TotalsTableBuilder.makeTotalsTable(totalsTableBuilder.applyToTable(queryTable)));
                    }
                },
        };
        for (int i = 0; i < 50; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}
