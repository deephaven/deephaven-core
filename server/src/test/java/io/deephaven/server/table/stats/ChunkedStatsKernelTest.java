package io.deephaven.server.table.stats;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.generator.BigDecimalGenerator;
import io.deephaven.engine.testutil.generator.BigIntegerGenerator;
import io.deephaven.engine.testutil.generator.ByteGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.FloatGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.LongGenerator;
import io.deephaven.engine.testutil.generator.ShortGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ChunkedStatsKernelTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testPrecision() {
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
                        new FloatGenerator(QueryConstants.NULL_FLOAT + 1, Float.MAX_VALUE, 0.01),
                        new DoubleGenerator(QueryConstants.NULL_DOUBLE + 1, Double.MAX_VALUE, 0.01),
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
        assertEquals(actualDouble, expectedDouble, .000001);
        return epsilon < .000001;
    }
}
