package io.deephaven.engine.table.impl.updateby;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BadDataBehavior;
import io.deephaven.engine.table.EmaControl;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.UpdateByClause;
import io.deephaven.engine.table.impl.EvalNugget;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableWithDefaults;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.numerics.movingaverages.AbstractMa;
import io.deephaven.numerics.movingaverages.ByEma;
import io.deephaven.numerics.movingaverages.ByEmaSimple;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.table.impl.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.table.impl.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.time.DateTimeUtils.MINUTE;
import static io.deephaven.time.DateTimeUtils.convertDateTime;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Category(OutOfBandTest.class)
public class TestEma extends BaseUpdateByTest {

    // region Zero Key Tests
    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(100000, false, false, false, 0xFFFABBBC,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final EmaControl skipControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Skip)
                .onNanValue(BadDataBehavior.Skip).build();

        final EmaControl resetControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Reset)
                .onNanValue(BadDataBehavior.Reset).build();

        computeEma((TableWithDefaults) t.dropColumns("ts"), null, 100, skipControl);
        computeEma((TableWithDefaults) t.dropColumns("ts"), null, 100, resetControl);

        computeEma(t, "ts", 10 * MINUTE, skipControl);
        computeEma(t, "ts", 10 * MINUTE, resetControl);
    }


    // endregion
    // region Bucketed Tests
    @Test
    public void testStaticBucketed() {
        doTestStaticBucketed(false);
    }

    @Test
    public void testStaticGroupedBucketed() {
        doTestStaticBucketed(true);
    }

    private void doTestStaticBucketed(boolean grouped) {
        final TableWithDefaults t = createTestTable(100000, true, grouped, false, 0x31313131,
                new String[] {"ts"}, new TstUtils.Generator[] {new TstUtils.SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))}).t;

        final EmaControl skipControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Skip)
                .onNanValue(BadDataBehavior.Skip).build();

        final EmaControl resetControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Reset)
                .onNanValue(BadDataBehavior.Reset).build();
        computeEma((TableWithDefaults) t.dropColumns("ts"), null, 100, skipControl, "Sym");
        computeEma((TableWithDefaults) t.dropColumns("ts"), null, 100, resetControl, "Sym");

        computeEma(t, "ts", 10 * MINUTE, skipControl, "Sym");
        computeEma(t, "ts", 10 * MINUTE, resetControl, "Sym");
    }

    @Test
    public void testThrowBehaviors() {
        final EmaControl.Builder throwControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Throw);

        final TableWithDefaults bytes = testTable(RowSetFactory.flat(4).toTracking(),
                byteCol("col", (byte) 0, (byte) 1, NULL_BYTE, (byte) 3));

        assertThrows(TableDataException.class,
                () -> bytes.updateBy(UpdateByClause.ema(10, throwControl)));


        assertThrows(TableDataException.class,
                () -> bytes.updateBy(UpdateByClause.ema(10, throwControl)));

        TableWithDefaults shorts = testTable(RowSetFactory.flat(4).toTracking(),
                shortCol("col", (short) 0, (short) 1, NULL_SHORT, (short) 3));

        assertThrows(TableDataException.class,
                () -> shorts.updateBy(UpdateByClause.ema(10, throwControl)));

        TableWithDefaults ints = testTable(RowSetFactory.flat(4).toTracking(),
                intCol("col", 0, 1, NULL_INT, 3));

        assertThrows(TableDataException.class,
                () -> ints.updateBy(UpdateByClause.ema(10, throwControl)));

        TableWithDefaults longs = testTable(RowSetFactory.flat(4).toTracking(),
                longCol("col", 0, 1, NULL_LONG, 3));

        assertThrows(TableDataException.class,
                () -> longs.updateBy(UpdateByClause.ema(10, throwControl)));

        TableWithDefaults floats = testTable(RowSetFactory.flat(4).toTracking(),
                floatCol("col", 0, 1, NULL_FLOAT, Float.NaN));

        assertThrows(TableDataException.class,
                () -> floats.updateBy(
                        UpdateByClause.ema(10, EmaControl.builder().onNullValue(BadDataBehavior.Throw).build())),
                "Encountered null value during EMA processing");

        assertThrows(TableDataException.class,
                () -> floats.updateBy(
                        UpdateByClause.ema(10, EmaControl.builder().onNanValue(BadDataBehavior.Throw).build())),
                "Encountered NaN value during EMA processing");

        TableWithDefaults doubles = testTable(RowSetFactory.flat(4).toTracking(),
                doubleCol("col", 0, 1, NULL_DOUBLE, Double.NaN));

        assertThrows(TableDataException.class,
                () -> doubles.updateBy(
                        UpdateByClause.ema(10, EmaControl.builder().onNullValue(BadDataBehavior.Throw).build())),
                "Encountered null value during EMA processing");

        assertThrows(TableDataException.class,
                () -> doubles.updateBy(
                        UpdateByClause.ema(10, EmaControl.builder().onNanValue(BadDataBehavior.Throw).build())),
                "Encountered NaN value during EMA processing");


        TableWithDefaults bi = testTable(RowSetFactory.flat(4).toTracking(),
                col("col", BigInteger.valueOf(0), BigInteger.valueOf(1), null, BigInteger.valueOf(3)));

        assertThrows(TableDataException.class,
                () -> bi.updateBy(UpdateByClause.ema(10, throwControl)));

        TableWithDefaults bd = testTable(RowSetFactory.flat(4).toTracking(),
                col("col", BigDecimal.valueOf(0), BigDecimal.valueOf(1), null, BigDecimal.valueOf(3)));

        assertThrows(TableDataException.class,
                () -> bd.updateBy(UpdateByClause.ema(10, throwControl)));
    }

    @Test
    public void testTimeThrowBehaviors() {
        final ColumnHolder ts = col("ts",
                convertDateTime("2022-03-11T09:30:00.000 NY"),
                convertDateTime("2022-03-11T09:29:00.000 NY"),
                convertDateTime("2022-03-11T09:30:00.000 NY"),
                convertDateTime("2022-03-11T09:32:00.000 NY"),
                null);

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        byteCol("col", (byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        shortCol("col", (short) 0, (short) 1, (short) 2, (short) 3, (short) 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        intCol("col", 0, 1, 2, 3, 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        longCol("col", 0, 1, 2, 3, 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        floatCol("col", 0, 1, 2, 3, 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        doubleCol("col", 0, 1, 2, 3, 4)));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        col("col", BigInteger.valueOf(0),
                                BigInteger.valueOf(1),
                                BigInteger.valueOf(2),
                                BigInteger.valueOf(3),
                                BigInteger.valueOf(4))));

        testThrowsInternal(
                testTable(RowSetFactory.flat(5).toTracking(), ts,
                        col("col", BigDecimal.valueOf(0),
                                BigDecimal.valueOf(1),
                                BigDecimal.valueOf(2),
                                BigDecimal.valueOf(3),
                                BigDecimal.valueOf(4))));
    }

    private void testThrowsInternal(TableWithDefaults table) {
        assertThrows(TableDataException.class,
                () -> table.updateBy(UpdateByClause.ema("ts", 100,
                        EmaControl.builder())),
                "Encountered negative delta time during EMA processing");

        assertThrows(TableDataException.class,
                () -> table.updateBy(UpdateByClause.ema("ts", 100,
                        EmaControl.builder()
                                .onNegativeDeltaTime(BadDataBehavior.Skip)
                                .onZeroDeltaTime(BadDataBehavior.Throw))),
                "Encountered zero delta time during EMA processing");

        assertThrows(TableDataException.class,
                () -> table.updateBy(UpdateByClause.ema("ts", 100,
                        EmaControl.builder()
                                .onNegativeDeltaTime(BadDataBehavior.Skip)
                                .onNullTime(BadDataBehavior.Throw))),
                "Encountered null timestamp during EMA processing");
    }

    @Test
    public void testResetBehavior() {
        final ColumnHolder ts = col("ts",
                convertDateTime("2022-03-11T09:30:00.000 NY"),
                convertDateTime("2022-03-11T09:29:00.000 NY"),
                convertDateTime("2022-03-11T09:31:00.000 NY"),
                convertDateTime("2022-03-11T09:31:00.000 NY"),
                convertDateTime("2022-03-11T09:32:00.000 NY"),
                null);

        Table expected = testTable(RowSetFactory.flat(6).toTracking(), ts,
                doubleCol("col", 0, NULL_DOUBLE, 2, NULL_DOUBLE, 4, NULL_DOUBLE));

        testResetBehaviorInternal(expected, ts,
                byteCol("col", (byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5));
        testResetBehaviorInternal(expected, ts,
                shortCol("col", (short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5));
        testResetBehaviorInternal(expected, ts, intCol("col", 0, 1, 2, 3, 4, 5));
        testResetBehaviorInternal(expected, ts, longCol("col", 0, 1, 2, 3, 4, 5));
        testResetBehaviorInternal(expected, ts, floatCol("col", 0, 1, 2, 3, 4, 5));
        testResetBehaviorInternal(expected, ts, doubleCol("col", 0, 1, 2, 3, 4, 5));

        expected = testTable(RowSetFactory.flat(6).toTracking(), ts,
                col("col", BigDecimal.valueOf(0), null, BigDecimal.valueOf(2), null, BigDecimal.valueOf(4), null));

        testResetBehaviorInternal(expected, ts, col("col", BigInteger.valueOf(0),
                BigInteger.valueOf(1),
                BigInteger.valueOf(2),
                BigInteger.valueOf(3),
                BigInteger.valueOf(4),
                BigInteger.valueOf(5)));

        testResetBehaviorInternal(expected, ts,
                col("col", BigDecimal.valueOf(0),
                        BigDecimal.valueOf(1),
                        BigDecimal.valueOf(2),
                        BigDecimal.valueOf(3),
                        BigDecimal.valueOf(4),
                        BigDecimal.valueOf(5)));

        // Test reset for NaN values
        final EmaControl resetControl = EmaControl.builder()
                .onNanValue(BadDataBehavior.Reset)
                .build();

        TableWithDefaults input = testTable(RowSetFactory.flat(3).toTracking(), doubleCol("col", 0, Double.NaN, 1));
        Table result = input.updateBy(UpdateByClause.ema(100, resetControl));
        expected = testTable(RowSetFactory.flat(3).toTracking(), doubleCol("col", 0, NULL_DOUBLE, 1));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(3).toTracking(), floatCol("col", 0, Float.NaN, 1));
        result = input.updateBy(UpdateByClause.ema(100, resetControl));
        expected = testTable(RowSetFactory.flat(3).toTracking(), doubleCol("col", 0, NULL_DOUBLE, 1));
        assertTableEquals(expected, result);
    }

    private void testResetBehaviorInternal(Table expected, final ColumnHolder ts, final ColumnHolder col) {
        final EmaControl resetControl = EmaControl.builder().onNegativeDeltaTime(BadDataBehavior.Reset)
                .onNullTime(BadDataBehavior.Reset)
                .onZeroDeltaTime(BadDataBehavior.Reset)
                .build();

        TableWithDefaults input = testTable(RowSetFactory.flat(6).toTracking(), ts, col);
        final Table result = input.updateBy(UpdateByClause.ema("ts", 1_000_000_000, resetControl));
        assertTableEquals(expected, result);
    }

    @Test
    public void testPoison() {
        final EmaControl nanCtl = EmaControl.builder().onNanValue(BadDataBehavior.Poison)
                .onNullValue(BadDataBehavior.Reset)
                .onNullTime(BadDataBehavior.Reset)
                .onNegativeDeltaTime(BadDataBehavior.Reset)
                .build();

        Table expected = testTable(RowSetFactory.flat(5).toTracking(),
                doubleCol("col", 0, Double.NaN, NULL_DOUBLE, Double.NaN, Double.NaN));
        TableWithDefaults input = testTable(RowSetFactory.flat(5).toTracking(),
                doubleCol("col", 0, Double.NaN, NULL_DOUBLE, Double.NaN, 4));
        assertTableEquals(expected, input.updateBy(UpdateByClause.ema(10, nanCtl)));
        input = testTable(RowSetFactory.flat(5).toTracking(), floatCol("col", 0, Float.NaN, NULL_FLOAT, Float.NaN, 4));
        assertTableEquals(expected, input.updateBy(UpdateByClause.ema(10, nanCtl)));

        final ColumnHolder ts = col("ts",
                convertDateTime("2022-03-11T09:30:00.000 NY"),
                convertDateTime("2022-03-11T09:31:00.000 NY"),
                null,
                convertDateTime("2022-03-11T09:33:00.000 NY"),
                convertDateTime("2022-03-11T09:34:00.000 NY"),
                convertDateTime("2022-03-11T09:33:00.000 NY"));

        expected = testTable(RowSetFactory.flat(6).toTracking(), ts,
                doubleCol("col", 0, Double.NaN, NULL_DOUBLE, Double.NaN, Double.NaN, NULL_DOUBLE));
        input = testTable(RowSetFactory.flat(6).toTracking(), ts,
                doubleCol("col", 0, Double.NaN, 2, Double.NaN, 4, 5));
        Table result = input.updateBy(UpdateByClause.ema("ts", 10, nanCtl));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(), ts,
                floatCol("col", 0, Float.NaN, 2, Float.NaN, 4, 5));
        assertTableEquals(expected, input.updateBy(UpdateByClause.ema("ts", 10, nanCtl)));
    }

    // endregion

    // region Live Tests

    @Test
    public void testZeroKeyAppendOnly() {
        doTestTicking(false, true);
    }

    @Test
    public void testZeroKeyGeneral() {
        doTestTicking(false, false);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestTicking(true, true);
    }

    @Test
    public void testBucketedGeneral() {
        doTestTicking(true, false);
    }

    private void doTestTicking(boolean bucketed, boolean appendOnly) {
        final CreateResult tickResult = createTestTable(10000, bucketed, false, true, 0x31313131);
        final CreateResult timeResult = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"ts"}, new Generator[] {new SortedDateTimeGenerator(
                        convertDateTime("2022-03-09T09:00:00.000 NY"),
                        convertDateTime("2022-03-09T16:30:00.000 NY"))});

        if (appendOnly) {
            tickResult.t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            timeResult.t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final EmaControl skipControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Skip)
                .onNanValue(BadDataBehavior.Skip).build();

        final EmaControl resetControl = EmaControl.builder()
                .onNullValue(BadDataBehavior.Reset)
                .onNanValue(BadDataBehavior.Reset).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? tickResult.t.updateBy(UpdateByClause.ema(100, skipControl), "Sym")
                                : tickResult.t.updateBy(UpdateByClause.ema(100, skipControl));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? tickResult.t.updateBy(UpdateByClause.ema(100, resetControl), "Sym")
                                : tickResult.t.updateBy(UpdateByClause.ema(100, resetControl));
                    }
                }
        };

        final EvalNugget[] timeNuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableWithDefaults base = timeResult.t;
                        if (!appendOnly) {
                            base = (TableWithDefaults) base.sort("ts");
                        }
                        return bucketed
                                ? base.updateBy(UpdateByClause.ema("ts", 10 * MINUTE, skipControl), "Sym")
                                : base.updateBy(UpdateByClause.ema("ts", 10 * MINUTE, skipControl));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableWithDefaults base = timeResult.t;
                        if (!appendOnly) {
                            base = (TableWithDefaults) base.sort("ts");
                        }
                        return bucketed
                                ? base.updateBy(UpdateByClause.ema("ts", 10 * MINUTE, resetControl), "Sym")
                                : base.updateBy(UpdateByClause.ema("ts", 10 * MINUTE, resetControl));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            try {
                if (appendOnly) {
                    UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                        generateAppends(100, billy, tickResult.t, tickResult.infos);
                        generateAppends(100, billy, timeResult.t, timeResult.infos);
                    });
                    validate("Table", nuggets);
                    validate("Table", timeNuggets);
                } else {
                    simulateShiftAwareStep(100, billy, tickResult.t, tickResult.infos, nuggets);
                    simulateShiftAwareStep(100, billy, timeResult.t, timeResult.infos, timeNuggets);
                }
            } catch (Throwable t) {
                System.out.println("Crapped out on step " + ii);
                throw t;
            }
        }
    }

    // endregion

    private void computeEma(TableWithDefaults source,
            final String tsCol,
            long scale,
            EmaControl control,
            String... groups) {
        final boolean useTicks = StringUtils.isNullOrEmpty(tsCol);
        final ByEmaSimple bes = new ByEmaSimple(
                control.onNullValue() == BadDataBehavior.Reset
                        ? ByEma.BadDataBehavior.BD_RESET
                        : ByEma.BadDataBehavior.BD_SKIP,
                control.onNanValue() == BadDataBehavior.Reset
                        ? ByEma.BadDataBehavior.BD_RESET
                        : ByEma.BadDataBehavior.BD_SKIP,
                useTicks ? AbstractMa.Mode.TICK : AbstractMa.Mode.TIME,
                scale,
                TimeUnit.NANOSECONDS);
        QueryScope.addParam("bes", bes);

        final String clausePrefix = useTicks ? "bes.update(" : "bes.update(ts, ";
        final String groupSuffix = groups == null || groups.length == 0 ? ""
                : "," + String.join(", ", groups);

        final Table expected = source.update(
                "byteCol   = " + clausePrefix + "(double)byteCol   , `b`" + groupSuffix + ")",
                "shortCol  = " + clausePrefix + "(double)shortCol  , 's'" + groupSuffix + ")",
                "intCol    = " + clausePrefix + "(double)intCol    , `i`" + groupSuffix + ")",
                "longCol   = " + clausePrefix + "(double)longCol   , `l`" + groupSuffix + ")",
                "floatCol  = " + clausePrefix + "(double)floatCol  , `f`" + groupSuffix + ")",
                "doubleCol = " + clausePrefix + "(double)doubleCol , `d`" + groupSuffix + ")",
                "bigIntCol = " + clausePrefix + "(bigIntCol == null ? NULL_DOUBLE : bigIntCol.doubleValue()) , `bi`"
                        + groupSuffix + ")",
                "bigDecimalCol = " + clausePrefix
                        + "(bigDecimalCol == null ? NULL_DOUBLE : bigDecimalCol.doubleValue()) , `bd`" + groupSuffix
                        + ")")
                .updateView(
                        "byteCol= Double.isNaN(byteCol) ? NULL_DOUBLE : byteCol",
                        "shortCol= Double.isNaN(shortCol) ? NULL_DOUBLE : shortCol",
                        "intCol= Double.isNaN(intCol) ? NULL_DOUBLE : intCol",
                        "longCol= Double.isNaN(longCol) ? NULL_DOUBLE : longCol",
                        "floatCol= Double.isNaN(floatCol) ? NULL_DOUBLE : floatCol",
                        "doubleCol= Double.isNaN(doubleCol) ? NULL_DOUBLE : doubleCol",
                        "bigIntCol= Double.isNaN(bigIntCol) ? NULL_DOUBLE : bigIntCol",
                        "bigDecimalCol= Double.isNaN(bigDecimalCol) ? NULL_DOUBLE : bigDecimalCol");

        final UpdateByClause emaClause;
        if (useTicks) {
            emaClause = UpdateByClause.ema(scale, control);
        } else {
            emaClause = UpdateByClause.ema(tsCol, scale, control);
        }

        final Table result = source.updateBy(emaClause, groups)
                .updateView("bigIntCol=bigIntCol == null ? NULL_DOUBLE : bigIntCol.doubleValue()",
                        "bigDecimalCol=bigDecimalCol == null ? NULL_DOUBLE : bigDecimalCol.doubleValue()");
        assertTableEquals(expected, result, TableDiff.DiffItems.DoublesExact);
        QueryScope.addParam("bes", null);
    }
}
