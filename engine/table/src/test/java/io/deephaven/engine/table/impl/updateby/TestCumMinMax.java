//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Numeric;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static org.junit.Assert.assertArrayEquals;

@Category(OutOfBandTest.class)
public class TestCumMinMax extends BaseUpdateByTest {
    private final int STATIC_TABLE_SIZE = 10000;

    private final String[] cumMin = new String[] {
            "byteColMin=byteCol",
            "charColMin=charCol",
            "shortColMin=shortCol",
            "intColMin=intCol",
            "longColMin=longCol",
            "floatColMin=floatCol",
            "doubleColMin=doubleCol",
            "bigIntColMin=bigIntCol",
            "bigDecimalColMin=bigDecimalCol"
    };

    private final String[] cumMax = new String[] {
            "byteColMax=byteCol",
            "charColMax=charCol",
            "shortColMax=shortCol",
            "intColMax=intCol",
            "longColMax=longCol",
            "floatColMax=floatCol",
            "doubleColMax=doubleCol",
            "bigIntColMax=bigIntCol",
            "bigDecimalColMax=bigDecimalCol"
    };

    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x2134BCFA,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin(cumMin),
                UpdateByOperation.CumMax(cumMax)));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumMin(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(result, col + "Min").toArray());
            assertWithCumMax(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(result, col + "Max").toArray());
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x2134BCFA,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin(cumMin),
                UpdateByOperation.CumMax(cumMax)));
        for (String col : t.getDefinition().getColumnNamesArray()) {
            if ("boolCol".equals(col)) {
                continue;
            }
            assertWithCumMin(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col + "Min").toArray());
            assertWithCumMax(
                    ColumnVectors.of(t, col).toArray(),
                    ColumnVectors.of(result, col + "Max").toArray());
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
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xACDB4321,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table result = t.updateBy(List.of(
                UpdateByOperation.CumMin(cumMin),
                UpdateByOperation.CumMax(cumMax)),
                "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = result.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithCumMin(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col + "Min").toArray());
                assertWithCumMax(
                        ColumnVectors.of(source, col).toArray(),
                        ColumnVectors.of(actual, col + "Max").toArray());
            });
            return source;
        });
    }

    // endregion

    // region Live Tests

    @Test
    public void testZeroKeyAppendOnly() {
        doTestTicking(false, true);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestTicking(true, true);
    }

    @Test
    public void testZeroKeyGeneral() {
        doTestTicking(false, false);
    }

    @Test
    public void testBucketedGeneral() {
        doTestTicking(true, false);
    }

    private void doTestTicking(boolean bucketed, boolean appendOnly) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        if (appendOnly) {
            t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumMin(), "Sym")
                                : t.updateBy(UpdateByOperation.CumMin());
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.CumMax(), "Sym")
                                : t.updateBy(UpdateByOperation.CumMax());
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            if (appendOnly) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                        () -> generateAppends(100, billy, t, result.infos));
                TstUtils.validate("Table", nuggets);
            } else {
                simulateShiftAwareStep(100, billy, t, result.infos, nuggets);
            }
        }
    }
    // endregion

    @Test
    public void testResultDataTypes() {
        final Instant baseInstant = DateTimeUtils.parseInstant("2023-01-01T00:00:00 NY");
        final ZoneId zone = ZoneId.of("America/Los_Angeles");

        QueryScope.addParam("baseInstant", baseInstant);
        QueryScope.addParam("baseLDT", LocalDateTime.ofInstant(baseInstant, zone));
        QueryScope.addParam("baseZDT", baseInstant.atZone(zone));

        final TableDefinition expectedDefinition = TableDefinition.of(
                ColumnDefinition.ofByte("byteCol"),
                ColumnDefinition.ofChar("charCol"),
                ColumnDefinition.ofShort("shortCol"),
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofLong("longCol"),
                ColumnDefinition.ofFloat("floatCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("stringCol"),
                ColumnDefinition.fromGenericType("instantCol", Instant.class),
                ColumnDefinition.fromGenericType("ldtCol", LocalDateTime.class),
                ColumnDefinition.fromGenericType("zdtCol", ZonedDateTime.class));

        final String[] columnNames = expectedDefinition.getColumnNamesArray();

        final String[] updateStrings = new String[] {
                "byteCol=(byte)i",
                "charCol=(char)(i + 64)",
                "shortCol=(short)i",
                "intCol=i",
                "longCol=ii",
                "floatCol=(float)ii",
                "doubleCol=(double)ii",
                "stringCol=String.valueOf(i)",
                "instantCol=baseInstant.plusSeconds(i)",
                "ldtCol=baseLDT.plusSeconds(i)",
                "zdtCol=baseZDT.plusSeconds(i)",
        };

        // NOTE: boolean is not supported by CumMinMaxSpec.applicableTo()
        final Table source = TableTools.emptyTable(20).update(updateStrings);

        // Verify all the source columns are the expected types.
        source.getDefinition().checkCompatibility(expectedDefinition);

        final Table expected = source.updateBy(UpdateByOperation.CumMin(columnNames));

        // Verify all the result columns are the expected types.
        expected.getDefinition().checkCompatibility(expectedDefinition);
    }

    @Test
    public void testProxy() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;

        PartitionedTable pt = t.partitionBy("Sym");
        actual = pt.proxy()
                .updateBy(UpdateByOperation.CumMin())
                .target().merge().sort("Sym");
        expected = t.sort("Sym").updateBy(UpdateByOperation.CumMin(), "Sym");
        TstUtils.assertTableEquals(expected, actual);

        actual = pt.proxy()
                .updateBy(UpdateByOperation.CumMax())
                .target().merge().sort("Sym");
        expected = t.sort("Sym").updateBy(UpdateByOperation.CumMax(), "Sym");
        TstUtils.assertTableEquals(expected, actual);
    }

    public static char[] cumMin(char... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new char[0];
        }

        char[] result = new char[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == NULL_CHAR) {
                result[i] = values[i];
            } else if (values[i] == NULL_CHAR) {
                result[i] = result[i - 1];
            } else {
                result[i] = values[i] < result[i - 1] ? values[i] : result[i - 1];
            }
        }

        return result;
    }

    public static char[] cumMax(char... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new char[0];
        }

        char[] result = new char[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == NULL_CHAR) {
                result[i] = values[i];
            } else if (values[i] == NULL_CHAR) {
                result[i] = result[i - 1];
            } else {
                result[i] = values[i] > result[i - 1] ? values[i] : result[i - 1];
            }
        }

        return result;
    }

    public static Object[] cumMin(Object... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == null) {
                result[i] = values[i];
            } else if (values[i] == null) {
                result[i] = result[i - 1];
            } else {
                result[i] = ((Comparable) result[i - 1]).compareTo(values[i]) < 0 ? result[i - 1] : values[i];
            }
        }

        return result;
    }

    public static Object[] cumMax(Object... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new Object[0];
        }

        Object[] result = new Object[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i - 1] == null) {
                result[i] = values[i];
            } else if (values[i] == null) {
                result[i] = result[i - 1];
            } else {
                result[i] = ((Comparable) result[i - 1]).compareTo(values[i]) > 0 ? result[i - 1] : values[i];
            }
        }

        return result;
    }

    final void assertWithCumMin(@NotNull final Object expected, @NotNull final Object actual) {
        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cummin((byte[]) expected), (byte[]) actual);
        } else if (expected instanceof char[]) {
            assertArrayEquals(cumMin((char[]) expected), (char[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cummin((short[]) expected), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cummin((int[]) expected), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cummin((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cummin((float[]) expected), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cummin((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(cumMin((Object[]) expected), (Object[]) actual);
        }
    }

    final void assertWithCumMax(@NotNull final Object expected, @NotNull final Object actual) {
        if (expected instanceof byte[]) {
            assertArrayEquals(Numeric.cummax((byte[]) expected), (byte[]) actual);
        } else if (expected instanceof char[]) {
            assertArrayEquals(cumMax((char[]) expected), (char[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Numeric.cummax((short[]) expected), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Numeric.cummax((int[]) expected), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Numeric.cummax((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Numeric.cummax((float[]) expected), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Numeric.cummax((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(cumMax((Object[]) expected), (Object[]) actual);
        }
    }
}
