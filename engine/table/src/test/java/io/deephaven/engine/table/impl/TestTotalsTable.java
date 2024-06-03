//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.util.TotalsTableBuilder;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.function.Numeric.*;

public class TestTotalsTable extends RefreshingTableTestCase {

    private static final double EPSILON = 0.000000001;

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

        assertEquals(sum(ColumnVectors.ofInt(queryTable, "intCol")),
                totals.getColumnSource("intCol").getLong(totals.getRowSet().firstRowKey()));
        assertEquals(sum(ColumnVectors.ofDouble(queryTable, "doubleCol")),
                totals.getColumnSource("doubleCol").getDouble(totals.getRowSet().firstRowKey()));
        assertEquals(sum(ColumnVectors.ofDouble(queryTable, "doubleNullCol")),
                totals.getColumnSource("doubleNullCol").getDouble(totals.getRowSet().firstRowKey()));
        assertEquals(sum(ColumnVectors.ofFloat(queryTable, "floatCol")),
                totals.getColumnSource("floatCol").getFloat(totals.getRowSet().firstRowKey()), 0.02);
        assertEquals(sum(ColumnVectors.ofShort(queryTable, "shortCol")),
                totals.getColumnSource("shortCol").getLong(totals.getRowSet().firstRowKey()));

        builder.setDefaultOperation("skip");
        builder.setOperation("byteCol", "min");
        builder.setOperation("Sym", "first");
        builder.setOperation("intCol2", "last");

        final Table totals2 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> TotalsTableBuilder.makeTotalsTable(queryTable, builder));
        assertEquals(new LinkedHashSet<>(Arrays.asList("Sym", "intCol2", "byteCol")),
                totals2.getDefinition().getColumnNameSet());
        assertEquals(min(ColumnVectors.ofByte(queryTable, "byteCol")),
                totals2.getColumnSource("byteCol").getByte(totals2.getRowSet().firstRowKey()));
        assertEquals(queryTable.getColumnSource("Sym").get(queryTable.getRowSet().firstRowKey()),
                totals2.getColumnSource("Sym").get(totals2.getRowSet().firstRowKey()));
        assertEquals(queryTable.getColumnSource("intCol2").getInt(queryTable.getRowSet().lastRowKey()),
                totals2.getColumnSource("intCol2").getInt(totals2.getRowSet().get(totals2.getRowSet().firstRowKey())));

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
            assertEquals(max(ColumnVectors.ofByte(queryTable, "byteCol")),
                    totals3.getColumnSource("byteCol").getByte(totals3.getRowSet().firstRowKey()));
            assertEquals(var(ColumnVectors.ofDouble(queryTable, "doubleCol")),
                    totals3.getColumnSource("doubleCol").getDouble(totals3.getRowSet().firstRowKey()), EPSILON);
            assertEquals(std(ColumnVectors.ofDouble(queryTable, "doubleNullCol")),
                    totals3.getColumnSource("doubleNullCol__Std").getDouble(totals3.getRowSet().firstRowKey()),
                    EPSILON);
            assertEquals(queryTable.size(),
                    totals3.getColumnSource("doubleNullCol__Count").getLong(totals3.getRowSet().firstRowKey()));
            assertEquals(avg(ColumnVectors.ofDouble(queryTable, "doubleCol2")),
                    totals3.getColumnSource("doubleCol2").getDouble(totals3.getRowSet().firstRowKey()), EPSILON);
            assertEquals(queryTable.size(),
                    totals3.getColumnSource("shortCol").getLong(totals3.getRowSet().firstRowKey()));

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
