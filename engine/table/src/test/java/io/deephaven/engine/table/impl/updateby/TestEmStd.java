//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableDefaults;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.function.Basic.isNull;
import static io.deephaven.time.DateTimeUtils.*;
import static io.deephaven.util.QueryConstants.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;

@Category(OutOfBandTest.class)
public class TestEmStd extends BaseUpdateByTest {
    private final BigDecimal allowableDelta = BigDecimal.valueOf(0.000000001);
    private final BigDecimal allowableFraction = BigDecimal.valueOf(0.000001);
    final static MathContext mathContextDefault = UpdateByControl.mathContextDefault();

    /**
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
     */
    final String[] primitiveColumns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
    };

    /**
     * These are used in the static table evaluations where we verify static tables against external computations.
     */
    final String[] columns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "bigIntCol",
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 50_000;
    final int DYNAMIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    // compare with external computations
    private BigDecimal[] convert(double[] input) {
        final BigDecimal[] output = new BigDecimal[input.length];
        for (int ii = 0; ii < input.length; ii++) {
            output[ii] = Double.isNaN(input[ii]) ? BigDecimal.ZERO : BigDecimal.valueOf(input[ii]);
        }
        return output;
    }

    @Test
    public void testVerifyVsPandas() {
        final double[] input = new double[] {
                3044.036874267936, 9277.06357303164, 2756.633937780162, 7743.843415779234, 9467.848642932411,
                4968.554887857865, 4656.419031975637, 7138.715217224906, 6830.617965299797, 2619.34137833728,
                2129.3865352345897, 3851.3197212890527, 9224.941816799124, 7612.305201070732, 4570.605755977481,
                1035.5494377268192, 2693.9200786766783, 9970.63287529204, 4550.053843483862, 7671.542538001727,
                3732.9509228843103, 2271.390635991, 1386.3407240397264, 5987.1358436555465, 7605.857032048177,
                8145.939848640695, 5758.429314671887, 5562.706552433403, 1308.1130377187455, 6215.138681169624,
                38.93719235443238, 2438.2257138008977, 7076.164759895435, 6493.979314581544, 1604.0937263881972,
                2969.907279707977, 2557.652276375833, 1508.6245871505132, 480.97220090168815, 7439.432841631966,
                5437.939391944992, 7401.016761283339, 7595.256199407152, 4804.574509653461, 7159.136733918436,
                7529.447306478534, 7872.06363905515, 3360.83213635778, 5402.200645239415, 5147.34047239619,
                9261.1629698534, 6531.98506480781, 7745.237308691785, 5528.398677894106, 5746.685634874996,
                5195.739318781431, 5522.486685980084, 8563.906431896508, 3718.717081618237, 2551.576950500187,
                1980.3173765530205, 9743.130751210314, 8149.239872101157, 7843.5016046112, 5748.55692717864,
                269.56081296702905, 9211.475181390406, 8185.105089347185, 284.50010707247395, 5168.543074804966,
                8920.359378541387, 336.156717815711, 9219.578845865371, 5807.697429290317, 6805.781790099588,
                6068.042709776192, 547.0520519482914, 2144.870356542036, 4469.860029364697, 8031.929942805711,
                3796.8985296424185, 1241.699513875738, 1479.886685187709, 3021.183947330874, 4031.813777799328,
                3721.900936795862, 9641.86378969102, 6883.040473215315, 1414.0952551716734, 1245.2943630760917,
                9411.5625872464, 476.4446971932623, 4588.484027634333, 6507.598675723354, 1939.1295931603004,
                3379.203299153102, 4963.994763036741, 3538.2106035979355, 8451.558000867379, 7046.344718937799,
        };

        // generated from python pandas:
        // one_minus_alpha = 1.0 - math.exp(-1.0 / 10)
        // data["input"].ewm(alpha=one_minus_alpha, adjust=False).std(bias=True)
        // NOTE: manually converted the first value to NaN to match Deephaven (from 0.0 in pandas)
        final double[] externalStd10 = new double[] {
                Double.NaN, 1829.0154392759346, 1758.8960442013629, 2076.3748022882937, 2553.5576361856415,
                2433.2971958076046, 2314.9508862196717, 2330.690949555806, 2296.9168425011217, 2291.9729309301392,
                2312.255890924336, 2207.8214715377408, 2525.894583487406, 2531.3289503882397, 2413.990404017413,
                2587.5388769864812, 2531.6706405602604, 2890.7631052604515, 2753.5309285046505, 2734.616641196746,
                2638.9666870089936, 2644.2841777010944, 2711.183547399474, 2615.3147605681256, 2634.9116923462207,
                2678.308658165963, 2552.314511458238, 2429.207130967664, 2592.1560774480554, 2494.3976370808587,
                2791.8186528838073, 2728.7016093315524, 2714.2049310075963, 2639.1741766827986, 2680.6007021664377,
                2589.1961179237524, 2518.8307916766107, 2521.418444274615, 2603.279802063857, 2720.307749560414,
                2623.3759660190804, 2676.0719223294273, 2710.65505306357, 2578.5665802378544, 2554.237038842129,
                2543.8197143812963, 2543.078230871345, 2496.2248929622288, 2374.8492168999196, 2259.3298950477774,
                2448.7281649008, 2343.892348635581, 2306.9081582762524, 2197.3886026655937, 2090.596313198161,
                1998.4225502984302, 1902.7628557689422, 1986.0233140144787, 2008.5175354281926, 2138.0236128319266,
                2282.3945098309828, 2551.602521361136, 2539.002550194218, 2484.927173905604, 2365.2903525978977,
                2811.14776128965, 2891.0604295164812, 2836.0827828843867, 3183.9927200149073, 3030.260917239198,
                3055.343326310255, 3318.7105301905876, 3362.201866339264, 3198.5521757227402, 3060.8619138294707,
                2912.8505585383973, 3171.111825935619, 3155.583493276383, 3005.747183386171, 2998.6282518327253,
                2883.850884852967, 2968.439132483307, 2981.2386854225483, 2865.7729929680518, 2727.1014749511946,
                2599.0796514317017, 2940.349915523336, 2867.1579883693703, 2916.824216416535, 2944.329012780149,
                3179.370447665765, 3276.423240667965, 3117.3335736493514, 3030.0503309661476, 2985.115825828421,
                2853.331525725566, 2722.3940145613783, 2599.5591313621235, 2764.605382587145, 2722.970379404129,
        };

        // generated from python pandas:
        // one_minus_alpha = 1.0 - math.exp(-1.0 / 50)
        // data["input"].ewm(alpha=one_minus_alpha, adjust=False).std(bias=True)
        // NOTE: manually converted the first value to NaN to match Deephaven (from 0.0 in pandas)
        final double[] externalStd50 = new double[] {
                Double.NaN, 868.3667520669262, 861.6294154053817, 1065.6661593432248, 1365.1098539430147,
                1369.6797998341608, 1367.2161208721454, 1448.8924859183394, 1507.5259703546187, 1498.3774979527218,
                1496.6149141177552, 1482.433499352465, 1668.504887524693, 1742.0801310749232, 1728.8129551576424,
                1752.4940689267764, 1740.522951793199, 1934.2905671973408, 1917.9800735018696, 1973.8732379079147,
                1954.340632684716, 1947.787301590286, 1958.6047602962583, 1962.9786087769285, 2013.0097235344442,
                2078.3277308826296, 2072.203653988832, 2062.618124184041, 2077.866682356043, 2080.0219727492604,
                2133.8995910235953, 2123.46429247787, 2147.1742042402825, 2153.90538320899, 2159.6213021942212,
                2142.997190540604, 2130.975509652542, 2137.167340005217, 2169.1148346119676, 2205.2824352344105,
                2193.666496356612, 2224.6571814035365, 2258.430038161702, 2238.2259296605826, 2256.6754210596964,
                2283.1015631257233, 2316.837747042132, 2297.4785691178095, 2280.0128759269514, 2260.4157215600476,
                2341.564252727817, 2336.967293913151, 2358.6570660203606, 2339.4006916540234, 2322.20547116764,
                2300.7780765760913, 2281.6867814230286, 2325.632844056922, 2306.341288354808, 2302.1340256171525,
                2308.5748868885926, 2396.8763909821846, 2422.177388026586, 2436.9018320808154, 2416.307809302607,
                2474.626200421032, 2528.5097587219147, 2547.0536987593027, 2601.703086960408, 2576.35930363248,
                2614.6355468320025, 2664.8277978298124, 2709.5902584878468, 2685.769945648109, 2672.350036113764,
                2650.4894212811923, 2694.9994322436596, 2695.0045324751645, 2668.6177837145683, 2680.0048998572497,
                2657.5498112837868, 2678.6758690042775, 2691.525378490025, 2675.1402976576246, 2650.056018913352,
                2626.982918608684, 2692.31283634888, 2682.095183009885, 2696.7033900013766, 2713.445888213874,
                2767.0395703864137, 2803.373890218947, 2775.499061119734, 2759.8971294300572, 2759.3301615428513,
                2737.5429056912308, 2710.7323988816947, 2688.0269092034773, 2714.744160327266, 2707.8821785721707,
        };

        final QueryTable table = TstUtils.testTable(
                doubleCol("value", input));

        final Table result = table.updateBy(List.of(
                UpdateByOperation.EmStd(10, "emstd10=value"),
                UpdateByOperation.EmStd(50, "emstd50=value")));

        final QueryTable externalTable = TstUtils.testTable(
                doubleCol("value", input),
                doubleCol("emstd10", externalStd10),
                doubleCol("emstd50", externalStd50));

        TstUtils.assertTableEquals("Tables match", externalTable, result,
                EnumSet.of(TableDiff.DiffItems.DoublesExact));

        // Repeat the test but convert to BigDecimal and make sure we are still correct.
        final BigDecimal[] bdInput = convert(input);
        final BigDecimal[] bdStd10 = convert(externalStd10);
        final BigDecimal[] bdStd50 = convert(externalStd50);

        // set the first output values to zero to match the null output from Deephaven
        bdStd10[0] = bdStd50[0] = null;

        final QueryTable bdTable = TstUtils.testTable(
                col("value", bdInput));

        final Table bdResult = bdTable.updateBy(List.of(
                UpdateByOperation.EmStd(10, "emstd10=value"),
                UpdateByOperation.EmStd(50, "emstd50=value")));

        assertBDArrayEquals(bdStd10, ColumnVectors.ofObject(bdResult, "emstd10", BigDecimal.class).toArray());
        assertBDArrayEquals(bdStd50, ColumnVectors.ofObject(bdResult, "emstd50", BigDecimal.class).toArray());
    }

    // region Zero Key Tests
    @Test
    public void testStaticZeroKey() {
        final long ticks = 10;

        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                parseInstant("2022-03-09T09:00:00.000 NY"),
                                parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        final Table actualSkip = t.updateBy(UpdateByOperation.EmStd(skipControl, ticks, columns));
        final Table actualReset = t.updateBy(UpdateByOperation.EmStd(resetControl, ticks, columns));

        for (String col : columns) {
            final Class<?> colType = t.getDefinition().getColumn(col).getDataType();
            assertWithEmStdTicks(skipControl, ticks, ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(actualSkip, col).toArray(),
                    colType);
            assertWithEmStdTicks(resetControl, ticks, ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(actualReset, col).toArray(),
                    colType);
        }

        final Table actualSkipTime = t.updateBy(UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, columns));
        final Table actualResetTime = t.updateBy(UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, columns));

        final Instant[] ts = ColumnVectors.ofObject(t, "ts", Instant.class).toArray();
        final long[] timestamps = new long[t.intSize()];
        for (int i = 0; i < t.intSize(); i++) {
            timestamps[i] = epochNanos(ts[i]);
        }

        for (String col : columns) {
            final Class<?> colType = t.getDefinition().getColumn(col).getDataType();
            assertWithEmStdTime(skipControl, 10 * MINUTE, timestamps, ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(actualSkipTime, col).toArray(),
                    colType);
            assertWithEmStdTime(resetControl, 10 * MINUTE, timestamps, ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(actualResetTime, col).toArray(),
                    colType);
        }
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
        final TableDefaults t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                parseInstant("2022-03-09T09:00:00.000 NY"),
                                parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        final Table actualSkip = t.updateBy(UpdateByOperation.EmStd(skipControl, 100, columns), "Sym");
        final Table actualReset = t.updateBy(UpdateByOperation.EmStd(resetControl, 100, columns), "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOpSkip = actualSkip.partitionBy("Sym");
        final PartitionedTable postOpReset = actualReset.partitionBy("Sym");

        preOp.partitionedTransform(postOpSkip, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                final Class<?> colType = source.getDefinition().getColumn(col).getDataType();
                assertWithEmStdTicks(skipControl, 100, ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        colType);
            });
            return source;
        });

        preOp.partitionedTransform(postOpReset, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                final Class<?> colType = source.getDefinition().getColumn(col).getDataType();
                assertWithEmStdTicks(resetControl, 100, ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        colType);
            });
            return source;
        });

        final Table actualSkipTime =
                t.updateBy(UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, columns), "Sym");
        final Table actualResetTime =
                t.updateBy(UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, columns), "Sym");

        final PartitionedTable postOpSkipTime = actualSkipTime.partitionBy("Sym");
        final PartitionedTable postOpResetTime = actualResetTime.partitionBy("Sym");

        preOp.partitionedTransform(postOpSkipTime, (source, actual) -> {
            final int sourceSize = source.intSize();
            final Instant[] ts = ColumnVectors.ofObject(source, "ts", Instant.class).toArray();
            final long[] timestamps = new long[sourceSize];
            for (int i = 0; i < sourceSize; i++) {
                timestamps[i] = epochNanos(ts[i]);
            }
            Arrays.stream(columns).forEach(col -> {
                final Class<?> colType = source.getDefinition().getColumn(col).getDataType();
                assertWithEmStdTime(skipControl, 10 * MINUTE, timestamps,
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        colType);
            });
            return source;
        });

        preOp.partitionedTransform(postOpResetTime, (source, actual) -> {
            final int sourceSize = source.intSize();
            final Instant[] ts = ColumnVectors.ofObject(source, "ts", Instant.class).toArray();
            final long[] timestamps = new long[sourceSize];
            for (int i = 0; i < sourceSize; i++) {
                timestamps[i] = epochNanos(ts[i]);
            }
            Arrays.stream(columns).forEach(col -> {
                final Class<?> colType = source.getDefinition().getColumn(col).getDataType();
                assertWithEmStdTime(resetControl, 10 * MINUTE, timestamps,
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col).toArray(),
                        colType);
            });
            return source;
        });

    }

    @Test
    public void testThrowBehaviors() {
        final OperationControl throwControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.THROW).build();

        final TableDefaults bytes = testTable(RowSetFactory.flat(4).toTracking(),
                byteCol("col", (byte) 0, (byte) 1, NULL_BYTE, (byte) 3));

        Throwable err = assertThrows(TableInitializationException.class,
                () -> bytes.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");


        err = assertThrows(TableInitializationException.class,
                () -> bytes.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");

        TableDefaults shorts = testTable(RowSetFactory.flat(4).toTracking(),
                shortCol("col", (short) 0, (short) 1, NULL_SHORT, (short) 3));

        err = assertThrows(TableInitializationException.class,
                () -> shorts.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");

        TableDefaults ints = testTable(RowSetFactory.flat(4).toTracking(),
                intCol("col", 0, 1, NULL_INT, 3));

        err = assertThrows(TableInitializationException.class,
                () -> ints.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");

        TableDefaults longs = testTable(RowSetFactory.flat(4).toTracking(),
                longCol("col", 0, 1, NULL_LONG, 3));

        err = assertThrows(TableInitializationException.class,
                () -> longs.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");

        TableDefaults floats = testTable(RowSetFactory.flat(4).toTracking(),
                floatCol("col", 0, 1, NULL_FLOAT, Float.NaN));

        err = assertThrows(TableInitializationException.class,
                () -> floats.updateBy(
                        UpdateByOperation.EmStd(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(),
                                10)));
        err = err.getCause();
        Assert.eqTrue(err.getClass() == TableDataException.class,
                "err.getClass() == TableDataException.class");
        Assert.eqTrue(err.getMessage().contains("Encountered null value during Exponential Moving output processing"),
                "err.getMessage().contains(\"Encountered null value during Exponential Moving output processing\")");

        err = assertThrows(TableInitializationException.class,
                () -> floats.updateBy(
                        UpdateByOperation.EmStd(OperationControl.builder().onNanValue(BadDataBehavior.THROW).build(),
                                10)));
        err = err.getCause();
        Assert.eqTrue(err.getClass() == TableDataException.class,
                "err.getClass() == TableDataException.class");
        Assert.eqTrue(err.getMessage().contains("Encountered NaN value during Exponential Moving output processing"),
                "err.getMessage().contains(\"Encountered NaN value during Exponential Moving output processing\")");

        TableDefaults doubles = testTable(RowSetFactory.flat(4).toTracking(),
                doubleCol("col", 0, 1, NULL_DOUBLE, Double.NaN));

        err = assertThrows(TableInitializationException.class,
                () -> doubles.updateBy(
                        UpdateByOperation.EmStd(OperationControl.builder().onNullValue(BadDataBehavior.THROW).build(),
                                10)));
        err = err.getCause();
        Assert.eqTrue(err.getClass() == TableDataException.class,
                "err.getClass() == TableDataException.class");
        Assert.eqTrue(err.getMessage().contains("Encountered null value during Exponential Moving output processing"),
                "err.getMessage().contains(\"Encountered null value during Exponential Moving output processing\")");

        err = assertThrows(TableInitializationException.class,
                () -> doubles.updateBy(
                        UpdateByOperation.EmStd(OperationControl.builder().onNanValue(BadDataBehavior.THROW).build(),
                                10)));
        err = err.getCause();
        Assert.eqTrue(err.getClass() == TableDataException.class,
                "err.getClass() == TableDataException.class");
        Assert.eqTrue(err.getMessage().contains("Encountered NaN value during Exponential Moving output processing"),
                "err.getMessage().contains(\"Encountered NaN value during Exponential Moving output processing\")");


        TableDefaults bi = testTable(RowSetFactory.flat(4).toTracking(),
                col("col", BigInteger.valueOf(0), BigInteger.valueOf(1), null, BigInteger.valueOf(3)));

        err = assertThrows(TableInitializationException.class,
                () -> bi.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");

        TableDefaults bd = testTable(RowSetFactory.flat(4).toTracking(),
                col("col", BigDecimal.valueOf(0), BigDecimal.valueOf(1), null, BigDecimal.valueOf(3)));

        err = assertThrows(TableInitializationException.class,
                () -> bd.updateBy(UpdateByOperation.EmStd(throwControl, 10)));
        Assert.eqTrue(err.getCause().getClass() == TableDataException.class,
                "err.getCause().getClass() == TableDataException.class");
    }

    @Test
    public void testTimeThrowBehaviors() {
        final ColumnHolder ts = col("ts",
                parseInstant("2022-03-11T09:30:00.000 NY"),
                parseInstant("2022-03-11T09:29:00.000 NY"),
                parseInstant("2022-03-11T09:30:00.000 NY"),
                parseInstant("2022-03-11T09:32:00.000 NY"),
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

    private void testThrowsInternal(TableDefaults table) {
        assertThrows(
                "Encountered negative delta time during EMS processing",
                TableDataException.class,
                () -> table.updateBy(UpdateByOperation.EmStd(
                        OperationControl.builder().build(), "ts", 100)));
    }

    @Test
    public void testResetBehavior() {
        // Value reset
        final OperationControl dataResetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .build();

        Table expected = testTable(RowSetFactory.flat(6).toTracking(),
                doubleCol("col",
                        Double.NaN,
                        0.2934393718606584,
                        Double.NaN,
                        Double.NaN,
                        Double.NaN,
                        0.2934393718606584));

        TableDefaults input = testTable(RowSetFactory.flat(6).toTracking(),
                byteCol("col", (byte) 0, (byte) 1, NULL_BYTE, NULL_BYTE, (byte) 4, (byte) 5));
        Table result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                shortCol("col", (short) 0, (short) 1, NULL_SHORT, NULL_SHORT, (short) 4, (short) 5));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                intCol("col", 0, 1, NULL_INT, NULL_INT, 4, 5));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                longCol("col", 0, 1, NULL_LONG, NULL_LONG, 4, 5));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                floatCol("col", 0, 1, NULL_FLOAT, NULL_FLOAT, 4, 5));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                doubleCol("col", 0, 1, NULL_DOUBLE, NULL_DOUBLE, 4, 5));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        assertTableEquals(expected, result);

        // BigInteger/BigDecimal
        BigInteger[] inputBI = new BigInteger[] {
                BigInteger.valueOf(0),
                BigInteger.valueOf(1),
                null,
                null,
                BigInteger.valueOf(4),
                BigInteger.valueOf(5)
        };

        BigDecimal[] inputBD = new BigDecimal[] {
                BigDecimal.valueOf(0),
                BigDecimal.valueOf(1),
                null,
                null,
                BigDecimal.valueOf(4),
                BigDecimal.valueOf(5)
        };

        BigDecimal[] expectedBD = new BigDecimal[] {
                null,
                new BigDecimal("0.2934393718606584219961949056365202"),
                null,
                null,
                null,
                new BigDecimal("0.2934393718606584219961949056365202")
        };

        input = testTable(RowSetFactory.flat(6).toTracking(),
                col("col", inputBI));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));

        // Extract the BD result and fuzzy-validate against expectations
        BigDecimal[] actualBD = ColumnVectors.ofObject(result, "col", BigDecimal.class).toArray();
        assertBDArrayEquals(actualBD, expectedBD);

        input = testTable(RowSetFactory.flat(6).toTracking(),
                col("col", inputBD));
        result = input.updateBy(UpdateByOperation.EmStd(dataResetControl, 10));
        actualBD = ColumnVectors.ofObject(result, "col", BigDecimal.class).toArray();
        assertBDArrayEquals(actualBD, expectedBD);

        // Test reset for NaN values
        expected = testTable(RowSetFactory.flat(5).toTracking(),
                doubleCol("col",
                        Double.NaN,
                        0.2934393718606584,
                        Double.NaN,
                        Double.NaN,
                        0.2934393718606584));

        final OperationControl resetControl = OperationControl.builder()
                .onNanValue(BadDataBehavior.RESET)
                .build();

        input = testTable(RowSetFactory.flat(5).toTracking(), doubleCol("col",
                0,
                1,
                Double.NaN,
                3,
                4));
        result = input.updateBy(UpdateByOperation.EmStd(resetControl, 10));
        assertTableEquals(expected, result);

        input = testTable(RowSetFactory.flat(5).toTracking(), floatCol("col",
                0,
                1,
                Float.NaN,
                3,
                4));
        result = input.updateBy(UpdateByOperation.EmStd(resetControl, 10));
        assertTableEquals(expected, result);
    }

    @Test
    public void testPoison() {
        final OperationControl nanCtl = OperationControl.builder().onNanValue(BadDataBehavior.POISON)
                .onNullValue(BadDataBehavior.RESET)
                .build();

        Table expected = testTable(RowSetFactory.flat(11).toTracking(),
                doubleCol("col",
                        Double.NaN,
                        0.2934393718606584,
                        Double.NaN,
                        Double.NaN,
                        Double.NaN,
                        Double.NaN, // reset
                        Double.NaN,
                        0.2934393718606584,
                        Double.NaN,
                        Double.NaN,
                        Double.NaN));

        TableDefaults input = testTable(RowSetFactory.flat(11).toTracking(),
                doubleCol("col",
                        0,
                        1,
                        Double.NaN,
                        2,
                        3,
                        NULL_DOUBLE, // reset
                        0,
                        1,
                        Double.NaN,
                        2,
                        3));
        assertTableEquals(expected, input.updateBy(UpdateByOperation.EmStd(nanCtl, 10)));

        input = testTable(RowSetFactory.flat(11).toTracking(),
                floatCol("col",
                        0,
                        1,
                        Float.NaN,
                        2,
                        3,
                        NULL_FLOAT, // reset
                        0,
                        1,
                        Float.NaN,
                        2,
                        3));
        assertTableEquals(expected, input.updateBy(UpdateByOperation.EmStd(nanCtl, 10)));
    }

    /**
     * This is a hacky, inefficient way to force nulls into the timestamps while maintaining sorted-ness otherwise
     */
    private class SortedIntGeneratorWithNulls extends SortedInstantGenerator {
        final double nullFrac;

        public SortedIntGeneratorWithNulls(Instant minTime, Instant maxTime, double nullFrac) {
            super(minTime, maxTime);
            this.nullFrac = nullFrac;
        }

        @Override
        public Chunk<Values> populateChunk(RowSet toAdd, Random random) {
            Chunk<Values> retChunk = super.populateChunk(toAdd, random);
            if (nullFrac == 0.0) {
                return retChunk;
            }
            ObjectChunk<Instant, Values> srcChunk = retChunk.asObjectChunk();
            Object[] dateArr = new Object[srcChunk.size()];
            srcChunk.copyToArray(0, dateArr, 0, dateArr.length);

            // force some entries to null
            for (int ii = 0; ii < srcChunk.size(); ii++) {
                if (random.nextDouble() < nullFrac) {
                    dateArr[ii] = null;
                }
            }
            return ObjectChunk.chunkWrap(dateArr);
        }
    }

    @Test
    public void testNullTimestamps() {
        final CreateResult timeResult = createTestTable(100, true, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                parseInstant("2022-03-09T09:00:00.000 NY"),
                                parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final EvalNugget[] timeNuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        // short timescale to make sure we trigger all the transition behavior
                        return base.updateBy(UpdateByOperation.EmStd(skipControl, "ts", 2 * MINUTE, primitiveColumns));
                    }
                }
        };
        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            try {
                simulateShiftAwareStep(10, billy, timeResult.t, timeResult.infos, timeNuggets);
            } catch (Throwable t) {
                System.out.println("Crapped out on step " + ii);
                throw t;
            }
        }
    }
    // endregion


    // region Live Tests
    @Test
    public void testZeroKeyAppendOnly() {
        doTestTicking(false, true, false);
    }

    @Test
    public void testZeroKeyAppendOnlyRedirected() {
        doTestTicking(false, true, true);
    }

    @Test
    public void testZeroKeyGeneral() {
        doTestTicking(false, false, false);
    }

    @Test
    public void testZeroKeyGeneralRedirected() {
        doTestTicking(false, false, true);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestTicking(true, true, false);
    }

    @Test
    public void testBucketedAppendOnlyRedirected() {
        doTestTicking(true, true, true);
    }

    @Test
    public void testBucketedGeneral() {
        doTestTicking(true, false, false);
    }

    @Test
    public void testBucketedGeneralRedirected() {
        doTestTicking(true, false, true);
    }

    private void doTestTicking(boolean bucketed, boolean appendOnly, boolean redirected) {
        final CreateResult tickResult = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"}, new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1)});
        final CreateResult timeResult = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                parseInstant("2022-03-09T09:00:00.000 NY"),
                                parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        if (appendOnly) {
            tickResult.t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
            timeResult.t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final UpdateByControl control = UpdateByControl.builder().useRedirection(redirected).build();

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? tickResult.t.updateBy(control,
                                        UpdateByOperation.EmStd(skipControl, 100, primitiveColumns),
                                        "Sym")
                                : tickResult.t.updateBy(control,
                                        UpdateByOperation.EmStd(skipControl, 100, primitiveColumns));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? tickResult.t.updateBy(control,
                                        UpdateByOperation.EmStd(resetControl, 100, primitiveColumns),
                                        "Sym")
                                : tickResult.t.updateBy(control,
                                        UpdateByOperation.EmStd(resetControl, 100, primitiveColumns));
                    }
                }
        };

        final EvalNugget[] timeNuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        if (!appendOnly) {
                            base = (TableDefaults) base.sort("ts");
                        }
                        return bucketed
                                ? base.updateBy(control,
                                        UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, primitiveColumns),
                                        "Sym")
                                : base.updateBy(control,
                                        UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, primitiveColumns));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        if (!appendOnly) {
                            base = (TableDefaults) base.sort("ts");
                        }
                        return bucketed
                                ? base.updateBy(control,
                                        UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, primitiveColumns),
                                        "Sym")
                                : base.updateBy(control,
                                        UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, primitiveColumns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                if (appendOnly) {
                    final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                    updateGraph.runWithinUnitTestCycle(() -> {
                        generateAppends(DYNAMIC_UPDATE_SIZE, billy, tickResult.t, tickResult.infos);
                        generateAppends(DYNAMIC_UPDATE_SIZE, billy, timeResult.t, timeResult.infos);
                    });
                    validate("Table", nuggets);
                    validate("Table", timeNuggets);
                } else {
                    simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, tickResult.t, tickResult.infos, nuggets);
                    simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, timeResult.t, timeResult.infos, timeNuggets);
                }
            } catch (Throwable t) {
                System.out.println("Crapped out on step " + ii);
                throw t;
            }
        }
    }
    // endregion

    // region Special Tests
    @Test
    public void testInitialEmptySingleRowIncrement() {
        final CreateResult tickResult = createTestTable(0, true, false, true, 0x31313131,
                new String[] {"charCol"}, new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1)});
        final CreateResult timeResult = createTestTable(0, true, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                parseInstant("2022-03-09T09:00:00.000 NY"),
                                parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        tickResult.t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        timeResult.t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final UpdateByControl control = UpdateByControl.builder().useRedirection(false).build();

        final OperationControl skipControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.SKIP)
                .onNanValue(BadDataBehavior.SKIP).build();

        final OperationControl resetControl = OperationControl.builder()
                .onNullValue(BadDataBehavior.RESET)
                .onNanValue(BadDataBehavior.RESET).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tickResult.t.updateBy(control,
                                UpdateByOperation.EmStd(skipControl, 100, primitiveColumns),
                                "Sym");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tickResult.t.updateBy(control,
                                UpdateByOperation.EmStd(skipControl, 100, primitiveColumns));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tickResult.t.updateBy(control,
                                UpdateByOperation.EmStd(resetControl, 100, primitiveColumns),
                                "Sym");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return tickResult.t.updateBy(control,
                                UpdateByOperation.EmStd(resetControl, 100, primitiveColumns));
                    }
                }
        };

        final EvalNugget[] timeNuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        return base.updateBy(control,
                                UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, primitiveColumns),
                                "Sym");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        return base.updateBy(control,
                                UpdateByOperation.EmStd(skipControl, "ts", 10 * MINUTE, primitiveColumns));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        return base.updateBy(control,
                                UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, primitiveColumns),
                                "Sym");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        TableDefaults base = timeResult.t;
                        return base.updateBy(control,
                                UpdateByOperation.EmStd(resetControl, "ts", 10 * MINUTE, primitiveColumns));
                    }
                }

        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 500; ii++) {
            if (ii % 100 == 0) {
                // Force nulls into the table for all the primitive columns

            } else {
                try {
                    final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                    updateGraph.runWithinUnitTestCycle(() -> {
                        generateAppends(1, billy, tickResult.t, tickResult.infos);
                        generateAppends(1, billy, timeResult.t, timeResult.infos);
                    });
                    validate("Table", nuggets);
                    validate("Table", timeNuggets);
                } catch (Throwable t) {
                    System.out.println("Crapped out on step " + ii);
                    throw t;
                }
            }
        }
    }

    // endregion

    // region Manual Verification functions
    public static double[] compute_emstd_ticks(OperationControl control, long ticks, double[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        final double alpha = Math.exp(-1.0 / (double) ticks);
        final double oneMinusAlpha = 1.0 - alpha;

        final double[] result = new double[values.length];

        double runningEma = NULL_DOUBLE;
        double runningVariance = NULL_DOUBLE;
        double outputVal = Double.NaN;

        for (int i = 0; i < values.length; i++) {
            if (values[i] == NULL_DOUBLE) {
                if (control.onNullValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = NULL_DOUBLE;
                    runningVariance = NULL_DOUBLE;
                    outputVal = Double.NaN;
                }
                // if SKIP, keep prev values
            } else if (values[i] == Double.NaN) {
                if (control.onNanValueOrDefault() == BadDataBehavior.POISON) {
                    runningEma = Double.NaN;
                    runningVariance = Double.NaN;
                } else if (control.onNanValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = NULL_DOUBLE;
                    runningVariance = NULL_DOUBLE;
                }
                outputVal = Double.NaN;
            } else if (runningEma == NULL_DOUBLE) {
                runningEma = values[i];
                runningVariance = 0.0;
                outputVal = Double.NaN;
            } else {
                // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                runningVariance = alpha * (runningVariance + oneMinusAlpha * Math.pow(values[i] - runningEma, 2.0));

                final double decayedEmaVal = runningEma * alpha;
                runningEma = decayedEmaVal + oneMinusAlpha * values[i];
                outputVal = Math.sqrt(runningVariance);
            }
            result[i] = outputVal;
        }

        return result;
    }

    public static BigDecimal[] compute_emstd_ticks(OperationControl control, long ticks, BigDecimal[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new BigDecimal[0];
        }

        final BigDecimal alpha = BigDecimal.valueOf(Math.exp(-1.0 / (double) ticks));
        final BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha, mathContextDefault);

        final BigDecimal[] result = new BigDecimal[values.length];

        BigDecimal runningEma = null;
        BigDecimal runningVariance = null;
        BigDecimal outputVal = null;

        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                if (control.onNullValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = null;
                    runningVariance = null;
                    outputVal = null;
                }
                // if SKIP, keep prev runningVal
            } else if (runningEma == null) {
                runningEma = values[i];
                runningVariance = BigDecimal.ZERO;
                outputVal = null;
            } else {
                // incremental variance = alpha * (prevVariance + (1 - alpha) * (x - prevEma)^2)
                runningVariance = alpha.multiply(
                        runningVariance.add(
                                oneMinusAlpha.multiply(values[i].subtract(runningEma).pow(2, mathContextDefault)),
                                mathContextDefault),
                        mathContextDefault);

                final BigDecimal decayedEmaVal = runningEma.multiply(alpha, mathContextDefault);
                runningEma = decayedEmaVal.add(oneMinusAlpha.multiply(values[i], mathContextDefault));
                outputVal = runningVariance.sqrt(mathContextDefault);
            }
            result[i] = outputVal;
        }

        return result;
    }

    public static double[] compute_emstd_time(OperationControl control, long nanos, long[] timestamps,
            double[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        final double[] result = new double[values.length];
        double runningEma = NULL_DOUBLE;
        double runningVariance = NULL_DOUBLE;
        double outputVal = Double.NaN;
        long lastStamp = NULL_LONG;

        for (int i = 0; i < values.length; i++) {
            final long timestamp = timestamps[i];

            if (values[i] == NULL_DOUBLE) {
                if (control.onNullValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = NULL_DOUBLE;
                    runningVariance = NULL_DOUBLE;
                    outputVal = Double.NaN;
                }
                // if SKIP, keep prev values
            } else if (values[i] == Double.NaN) {
                if (control.onNanValueOrDefault() == BadDataBehavior.POISON) {
                    runningEma = Double.NaN;
                    runningVariance = Double.NaN;
                } else if (control.onNanValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = NULL_DOUBLE;
                    runningVariance = NULL_DOUBLE;
                }
                outputVal = Double.NaN;
            } else if (timestamp == NULL_LONG) {
                // no change to curVal and lastStamp
            } else if (runningEma == NULL_DOUBLE) {
                runningEma = values[i];
                runningVariance = 0.0;
                outputVal = Double.NaN;
                lastStamp = timestamp;
            } else {
                final long dt = timestamp - lastStamp;

                // alpha is dynamic, based on time
                final double alpha = Math.exp(-dt / (double) nanos);
                final double oneMinusAlpha = 1.0 - alpha;

                // incremental variance = (1 - alpha)(prevVariance + alpha * (x - prevEma)^2)
                runningVariance = alpha * (runningVariance + oneMinusAlpha * Math.pow(values[i] - runningEma, 2.0));

                final double decayedEmaVal = runningEma * alpha;
                runningEma = decayedEmaVal + oneMinusAlpha * values[i];
                outputVal = Math.sqrt(runningVariance);

                lastStamp = timestamp;
            }
            result[i] = outputVal;
        }

        return result;
    }

    public static BigDecimal[] compute_emstd_time(OperationControl control, long nanos, long[] timestamps,
            BigDecimal[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new BigDecimal[0];
        }

        final BigDecimal[] result = new BigDecimal[values.length];
        BigDecimal runningEma = null;
        BigDecimal runningVariance = null;
        BigDecimal outputVal = null;
        long lastStamp = NULL_LONG;

        for (int i = 0; i < values.length; i++) {
            final long timestamp = timestamps[i];

            if (values[i] == null) {
                if (control.onNullValueOrDefault() == BadDataBehavior.RESET) {
                    runningEma = null;
                    runningVariance = null;
                    outputVal = null;
                }
                // if SKIP, keep prev runningVal
            } else if (runningEma == null) {
                runningEma = values[i];
                runningVariance = BigDecimal.ZERO;
                outputVal = null;
                lastStamp = timestamp;
            } else if (timestamp == NULL_LONG) {
                // no change to curVal and lastStamp
            } else {
                final long dt = timestamp - lastStamp;

                // alpha is dynamic, based on time
                final BigDecimal alpha = BigDecimal.valueOf(Math.exp(-dt / (double) nanos));
                final BigDecimal oneMinusAlpha = BigDecimal.ONE.subtract(alpha, mathContextDefault);

                runningVariance = alpha.multiply(
                        runningVariance.add(
                                oneMinusAlpha.multiply(values[i].subtract(runningEma).pow(2, mathContextDefault)),
                                mathContextDefault),
                        mathContextDefault);

                final BigDecimal decayedEmaVal = runningEma.multiply(alpha, mathContextDefault);
                runningEma = decayedEmaVal.add(oneMinusAlpha.multiply(values[i], mathContextDefault));
                outputVal = runningVariance.sqrt(mathContextDefault);

                lastStamp = timestamp;
            }
            result[i] = outputVal;
        }
        return result;
    }


    final double[] convertArray(byte[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final double[] convertArray(char[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final double[] convertArray(short[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final double[] convertArray(int[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final double[] convertArray(long[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final double[] convertArray(float[] array) {
        final double[] result = new double[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? NULL_DOUBLE : (double) array[ii];
        }
        return result;
    }

    final BigDecimal[] convertArray(BigInteger[] array) {
        final BigDecimal[] result = new BigDecimal[array.length];
        for (int ii = 0; ii < array.length; ii++) {
            result[ii] = isNull(array[ii]) ? null : new BigDecimal(array[ii]);
        }
        return result;
    }

    final boolean fuzzyEquals(BigDecimal actualVal, BigDecimal expectedVal) {
        if (actualVal == null && expectedVal == null) {
            return true;
        } else if (actualVal == null) {
            return false;
        } else if (expectedVal == null) {
            return false;
        }

        BigDecimal diff = actualVal.subtract(expectedVal, mathContextDefault).abs();
        // Equal if the difference is zero or smaller than the allowed delta
        if (diff.equals(BigDecimal.ZERO) || allowableDelta.compareTo(diff) == 1) {
            return true;
        }

        // Equal if the difference is smaller than the allowable fraction.
        BigDecimal diffFraction = diff.divide(actualVal, mathContextDefault).abs();
        return allowableFraction.compareTo(diffFraction) == 1;
    }

    void assertBDArrayEquals(final BigDecimal[] expected,
            final BigDecimal[] actual) {
        if (expected.length != actual.length) {
            fail("Array lengths do not mach");
        }
        for (int ii = 0; ii < expected.length; ii++) {
            if (!fuzzyEquals(expected[ii], actual[ii])) {
                fail("Values do not match, expected: " + expected[ii] + " vs. actual: " + actual[ii]);
            }
        }
    }

    final void assertWithEmStdTicks(final OperationControl control,
            final long ticks,
            @NotNull final Object expected,
            @NotNull final Object actual,
            final Class<?> type) {
        if (expected instanceof double[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, (double[]) expected), (double[]) actual, .001d);
        }
        if (expected instanceof byte[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((byte[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof char[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((char[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof short[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((short[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof int[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((int[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof long[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((long[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof float[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, convertArray((float[]) expected)), (double[]) actual,
                    .001d);
        } else if (expected instanceof double[]) {
            assertArrayEquals(compute_emstd_ticks(control, ticks, (double[]) expected), (double[]) actual, .001d);
            // } else if (expected instanceof Boolean[]) {
            // assertArrayEquals(cumsum((Boolean[]) expected), (long[]) actual);
        } else {
            if (type == BigInteger.class) {
                assertBDArrayEquals(compute_emstd_ticks(control, ticks, convertArray((BigInteger[]) expected)),
                        (BigDecimal[]) actual);
            } else if (type == BigDecimal.class) {
                assertBDArrayEquals(compute_emstd_ticks(control, ticks, (BigDecimal[]) expected),
                        (BigDecimal[]) actual);
            }
        }
    }

    final void assertWithEmStdTime(final OperationControl control,
            final long nanos,
            @NotNull final long[] timestamps,
            @NotNull final Object expected,
            @NotNull final Object actual,
            final Class<?> type) {
        if (expected instanceof double[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, (double[]) expected), (double[]) actual,
                    .001d);
        }
        if (expected instanceof byte[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((byte[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof short[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((short[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof char[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((char[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof int[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((int[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof long[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((long[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof float[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, convertArray((float[]) expected)),
                    (double[]) actual, .001d);
        } else if (expected instanceof double[]) {
            assertArrayEquals(compute_emstd_time(control, nanos, timestamps, (double[]) expected), (double[]) actual,
                    .001d);
            // } else if (expected instanceof Boolean[]) {
            // assertArrayEquals(cumsum((Boolean[]) expected), (long[]) actual);
        } else {
            if (type == BigInteger.class) {
                assertBDArrayEquals(
                        compute_emstd_time(control, nanos, timestamps, convertArray((BigInteger[]) expected)),
                        (BigDecimal[]) actual);
            } else if (type == BigDecimal.class) {
                assertBDArrayEquals(compute_emstd_time(control, nanos, timestamps, (BigDecimal[]) expected),
                        (BigDecimal[]) actual);
            }
        }
    }
    // endregion
}
