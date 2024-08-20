//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.stats;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.testutil.generator.BigDecimalGenerator;
import io.deephaven.engine.testutil.generator.BigIntegerGenerator;
import io.deephaven.engine.testutil.generator.ByteGenerator;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.FloatGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.LongGenerator;
import io.deephaven.engine.testutil.generator.ShortGenerator;
import io.deephaven.engine.testutil.generator.StringGenerator;
import io.deephaven.engine.testutil.generator.UnsortedInstantGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ChunkedStatsKernelTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testNonNumericColumns() {
        final int size = 1000;
        final Random random = new Random();

        Instant now = Instant.now();
        // This sample deliberately limits the universe of possible values to ensure that the count of each item is
        // high enough to ensure consistent sorts. The column stats code is not concerned about stable sorts, so we
        // need to make sure our "expected" values are close enough to pass these tests.
        final QueryTable queryTable = getTable(size, random,
                initColumnInfos(
                        new String[] {"charCol", "instantCol", "strCol"},
                        new CharGenerator('@', '~', 0.01),
                        new UnsortedInstantGenerator(now, now.plus(Duration.ofHours(1)), 0.01),
                        new StringGenerator(100, 0.01)));

        Table expectedDistinctCounts = queryTable.aggAllBy(AggSpec.countDistinct()).update("charCol = (int) charCol",
                "strCol = (int) strCol", "instantCol = (int) instantCol");
        Table expectedCharValues = queryTable.where("charCol != null")
                .updateView("charCol=`` + charCol")
                .countBy("Count", "charCol")
                .sortDescending("Count", "charCol")
                .head(100)
                .groupBy()
                .updateView("charCol = charCol.toArray()", "Count = Count.toArray()");
        Table expectedStrValues = queryTable.where("strCol != null")
                .countBy("Count", "strCol")
                .sortDescending("Count", "strCol")
                .head(100)
                .groupBy()
                .updateView("strCol = strCol.toArray()", "Count = Count.toArray()");
        Table expectedMax = queryTable.maxBy();
        Table expectedMin = queryTable.minBy();
        Table nonNullCount = queryTable.update("charCol = charCol == null ? 0 : 1", "strCol = strCol == null ? 0 : 1",
                "instantCol = instantCol == null ? 0 : 1").sumBy();

        Table charStats = new CharacterChunkedStats(1000, 100)
                .processChunks(queryTable.getRowSet(), queryTable.getColumnSource("charCol"), false);
        Table instantStats = new DateTimeChunkedStats()
                .processChunks(queryTable.getRowSet(),
                        ReinterpretUtils.instantToLongSource(queryTable.getColumnSource("instantCol")), false);
        Table strStats = new ObjectChunkedStats(1000, 100)
                .processChunks(queryTable.getRowSet(), queryTable.getColumnSource("strCol"), false);

        assertEquals(expectedDistinctCounts.getColumnSource("charCol").get(0),
                charStats.getColumnSource("UNIQUE_VALUES").get(0));
        assertEquals(expectedDistinctCounts.getColumnSource("strCol").get(0),
                strStats.getColumnSource("UNIQUE_VALUES").get(0));

        // first validate that the counts are the same - the column stats order isn't actually stable, so check keys
        // separately
        assertArrayEquals((long[]) expectedCharValues.getColumnSource("Count").get(0),
                (long[]) charStats.getColumnSource("UNIQUE_COUNTS").get(0));
        // now that we know the counts were correctly descending, force keys to be in the same order and compare those
        Table sortedCharStats = charStats.ungroup("UNIQUE_COUNTS", "UNIQUE_KEYS")
                .sortDescending("UNIQUE_COUNTS", "UNIQUE_KEYS")
                .groupBy()
                .updateView("UNIQUE_KEYS = UNIQUE_KEYS.toArray()");
        assertArrayEquals((String[]) expectedCharValues.getColumnSource("charCol").get(0),
                (String[]) sortedCharStats.getColumnSource("UNIQUE_KEYS").get(0));

        assertArrayEquals((long[]) expectedStrValues.getColumnSource("Count").get(0),
                (long[]) strStats.getColumnSource("UNIQUE_COUNTS").get(0));
        Table sortedStrStats = strStats.ungroup("UNIQUE_COUNTS", "UNIQUE_KEYS")
                .sortDescending("UNIQUE_COUNTS", "UNIQUE_KEYS")
                .groupBy()
                .updateView("UNIQUE_KEYS = UNIQUE_KEYS.toArray()");
        assertArrayEquals((String[]) expectedStrValues.getColumnSource("strCol").get(0),
                (String[]) sortedStrStats.getColumnSource("UNIQUE_KEYS").get(0));

        assertEquals(nonNullCount.getColumnSource("charCol").get(0), charStats.getColumnSource("COUNT").get(0));
        assertEquals(nonNullCount.getColumnSource("strCol").get(0), strStats.getColumnSource("COUNT").get(0));
        assertEquals(nonNullCount.getColumnSource("instantCol").get(0), instantStats.getColumnSource("COUNT").get(0));

        assertEquals(expectedMin.getColumnSource("instantCol").get(0), instantStats.getColumnSource("MIN").get(0));
        assertEquals(expectedMax.getColumnSource("instantCol").get(0), instantStats.getColumnSource("MAX").get(0));


        // retest with smaller thresholds for items to collect
        charStats = new CharacterChunkedStats(20, 10)
                .processChunks(queryTable.getRowSet(), queryTable.getColumnSource("charCol"), false);
        strStats = new ObjectChunkedStats(20, 10)
                .processChunks(queryTable.getRowSet(), queryTable.getColumnSource("strCol"), false);

        assertEquals(expectedDistinctCounts.getColumnSource("charCol").get(0),
                charStats.getColumnSource("UNIQUE_VALUES").get(0));
        assertEquals(expectedDistinctCounts.getColumnSource("strCol").get(0),
                strStats.getColumnSource("UNIQUE_VALUES").get(0));

        assertEquals(nonNullCount.getColumnSource("charCol").get(0), charStats.getColumnSource("COUNT").get(0));
        assertEquals(nonNullCount.getColumnSource("strCol").get(0), strStats.getColumnSource("COUNT").get(0));

        assertFalse(charStats.hasColumns("UNIQUE_KEYS", "UNIQUE_COUNTS"));
        assertFalse(strStats.hasColumns("UNIQUE_KEYS", "UNIQUE_COUNTS"));
    }

    @Test
    public void testNumericPrecision() {
        final Table queryTable = TableTools.newTable(
                TableTools.shortCol("shortCol", (short) (Short.MAX_VALUE - 1), (short) 1),
                TableTools.intCol("intCol", Integer.MAX_VALUE - 1, 1),
                TableTools.longCol("longCol", Long.MAX_VALUE - 1, 1),
                TableTools.longCol("rollingLongCol", Long.MIN_VALUE + 1, 2));

        final Table shortStats = getChunkedNumericalStats(queryTable, "shortCol");
        assertSame("shortStats Sum Precision", long.class, shortStats.getColumnSource("SUM").getType());
        assertSame("shortStats AbsSum Precision", long.class, shortStats.getColumnSource("SUM_ABS").getType());

        final Table intStats = getChunkedNumericalStats(queryTable, "intCol");
        assertSame("intStats Sum Precision", long.class, intStats.getColumnSource("SUM").getType());
        assertSame("intStats AbsSum Precision", long.class, intStats.getColumnSource("SUM_ABS").getType());

        final Table longStats = getChunkedNumericalStats(queryTable, "longCol");
        assertSame("longStats Sum Precision", long.class, longStats.getColumnSource("SUM").getType());
        assertSame("longStats AbsSum Precision", long.class, longStats.getColumnSource("SUM_ABS").getType());

        // Sum can still be Long, but AbsSum should have rolled to Double
        final Table rollingLongStats = getChunkedNumericalStats(queryTable, "rollingLongCol");
        assertSame("rollingLongStats Sum Precision", long.class, rollingLongStats.getColumnSource("SUM").getType());
        assertSame("rollingLongStats AbsSum Precision", double.class,
                rollingLongStats.getColumnSource("SUM_ABS").getType());
    }

    @Test
    public void testAccuracy() {
        final int size = 1000;
        final Random random = new Random();

        final QueryTable queryTable = getTable(size, random,
                initColumnInfos(
                        new String[] {"shortCol", "byteCol", "intCol", "longCol", "floatCol", "doubleCol", "bigIntCol",
                                "bigDecimalCol"},
                        new ShortGenerator(QueryConstants.MIN_SHORT, QueryConstants.MAX_SHORT, 0.01),
                        new ByteGenerator(QueryConstants.MIN_BYTE, QueryConstants.MAX_BYTE, 0.01),
                        new IntGenerator(QueryConstants.MIN_INT / 2, QueryConstants.MAX_INT / 2, 0.01),
                        new LongGenerator(QueryConstants.MIN_LONG / 2, QueryConstants.MAX_LONG / 2, 0.01),
                        new FloatGenerator(QueryConstants.MIN_FINITE_FLOAT, Float.MAX_VALUE, 0.01),
                        new DoubleGenerator(QueryConstants.MIN_FINITE_DOUBLE, Double.MAX_VALUE, 0.01),
                        new BigIntegerGenerator(0.01),
                        new BigDecimalGenerator(0.01)));

        final Table stdDev = queryTable.aggAllBy(AggSpec.std());

        final Table shortStats = getChunkedNumericalStats(queryTable, "shortCol");
        final Table byteStats = getChunkedNumericalStats(queryTable, "byteCol");
        final Table intStats = getChunkedNumericalStats(queryTable, "intCol");
        final Table longStats = getChunkedNumericalStats(queryTable, "longCol");
        final Table floatStats = getChunkedNumericalStats(queryTable, "floatCol");
        final Table doubleStats = getChunkedNumericalStats(queryTable, "doubleCol");
        final Table bigIntStats = getChunkedNumericalStats(queryTable, "bigIntCol");
        final Table bigDecimalStats = getChunkedNumericalStats(queryTable, "bigDecimalCol");

        assertTrue("shortCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("shortCol").get(0),
                (Number) shortStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("byteCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("byteCol").get(0),
                (Number) byteStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("intCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("intCol").get(0),
                (Number) intStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("longCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("longCol").get(0),
                (Number) longStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("floatCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("floatCol").get(0),
                (Number) floatStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("doubleCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("doubleCol").get(0),
                (Number) doubleStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("bigIntCol StdDev Accuracy", withinTolerance((Number) stdDev.getColumnSource("bigIntCol").get(0),
                (Number) bigIntStats.getColumnSource("STD_DEV").get(0)));
        assertTrue("bigDecimalCol StdDev Accuracy",
                withinTolerance((Number) stdDev.getColumnSource("bigDecimalCol").get(0),
                        (Number) bigDecimalStats.getColumnSource("STD_DEV").get(0)));
    }

    private Table getChunkedNumericalStats(Table queryTable, String colName) {
        ColumnSource<Object> colSource = queryTable.getColumnSource(colName);
        return ChunkedNumericalStatsKernel.makeChunkedNumericalStats(colSource.getType())
                .processChunks(queryTable.getRowSet(), colSource, false);
    }

    private static boolean withinTolerance(final Number expected, final Number actual) {
        if (expected.equals(actual)) {
            return true;
        }

        final double expectedDouble = expected.doubleValue();
        final double actualDouble = actual.doubleValue();

        final double epsilon = (Math.abs(expectedDouble - actualDouble)) / Math.min(expectedDouble, actualDouble);
        double expectedTolerance = 1.0E-14;
        return epsilon < expectedTolerance;
    }
}
