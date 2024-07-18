//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.function.Basic.isNull;

@Category(OutOfBandTest.class)
public class TestRollingProduct extends BaseUpdateByTest {
    private final BigDecimal allowableFraction = BigDecimal.valueOf(0.000001);
    private final MathContext mathContextDefault = UpdateByControl.mathContextDefault();

    /**
     * Because of the pairwise functions performing re-ordering of the values during summation, we can get very small
     * comparison errors in floating point values. This class tolerates larger differences in the float/double operator
     * results.
     */
    private abstract static class FuzzyEvalNugget extends EvalNugget {
        @Override
        @NotNull
        public EnumSet<TableDiff.DiffItems> diffItems() {
            return EnumSet.of(TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);
        }
    }

    /**
     * These are used in the static tests and leverage the Numeric class functions for verification. Additional tests
     * are performed on BigInteger/BigDecimal columns as well.
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
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
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
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    private String[] getFormulas(String[] columns) {
        return Arrays.stream(columns)
                .map(c -> String.format("%s=product(%s)", c, c, c))
                .toArray(String[]::new);
    }

    private String[] getCastingFormulas(String[] columns) {
        return Arrays.stream(columns)
                .map(c -> String.format("%s=(double)%s", c, c))
                .toArray(String[]::new);
    }

    /**
     * Create a custom test table where the values are small enough they won't overflow Double.MAX_VALUE when
     * multiplied. This will allow the results to be verified with the Numeric#product() function.
     */
    @SuppressWarnings({"rawtypes"})
    static CreateResult createSmallTestTable(int tableSize,
            boolean includeSym,
            boolean includeGroups,
            boolean isRefreshing,
            int seed,
            String[] extraNames,
            TestDataGenerator[] extraGenerators) {
        if (includeGroups && !includeSym) {
            throw new IllegalArgumentException();
        }

        final List<String> colsList = new ArrayList<>();
        final List<TestDataGenerator> generators = new ArrayList<>();
        if (includeSym) {
            colsList.add("Sym");
            generators.add(new SetGenerator<>("a", "b", "c", "d", null));
        }

        if (extraNames.length > 0) {
            colsList.addAll(Arrays.asList(extraNames));
            generators.addAll(Arrays.asList(extraGenerators));
        }

        colsList.addAll(Arrays.asList("byteCol", "shortCol", "intCol", "longCol", "floatCol", "doubleCol", "boolCol",
                "bigIntCol", "bigDecimalCol"));
        generators.addAll(Arrays.asList(new ByteGenerator((byte) -1, (byte) 5, .1),
                new ShortGenerator((short) -1, (short) 5, .1),
                new IntGenerator(-1, 5, .1),
                new LongGenerator(-1, 5, .1),
                new FloatGenerator(-1, 5, .1),
                new DoubleGenerator(-1, 5, .1),
                new BooleanGenerator(.5, .1),
                new BigIntegerGenerator(new BigInteger("-1"), new BigInteger("5"), .1),
                new BigDecimalGenerator(new BigInteger("1"), new BigInteger("2"), 5, .1)));

        final Random random = new Random(seed);
        final ColumnInfo[] columnInfos = initColumnInfos(colsList.toArray(String[]::new),
                generators.toArray(new TestDataGenerator[0]));
        final QueryTable t = getTable(tableSize, random, columnInfos);

        if (!isRefreshing && includeGroups) {
            DataIndexer.getOrCreateDataIndex(t, "Sym");
        }

        t.setRefreshing(isRefreshing);

        return new CreateResult(t, columnInfos, random);
    }

    // region Object Helper functions

    @SuppressWarnings("unused") // Functions used via QueryLibrary
    @VisibleForTesting
    @TestUseOnly
    public static class Helpers {

        public static BigInteger prodBigInt(ObjectVector<BigInteger> bigIntegerObjectVector) {
            if (bigIntegerObjectVector == null || bigIntegerObjectVector.isEmpty()) {
                return null;
            }

            BigInteger product = BigInteger.ONE;

            final long n = bigIntegerObjectVector.size();
            long nullCount = 0;

            for (long i = 0; i < n; i++) {
                BigInteger val = bigIntegerObjectVector.get(i);
                if (!isNull(val)) {
                    product = product.multiply(val);
                } else {
                    nullCount++;
                }
            }
            return nullCount == n ? null : product;
        }

        public static BigDecimal prodBigDec(ObjectVector<BigDecimal> bigDecimalObjectVector) {
            MathContext mathContextDefault = UpdateByControl.mathContextDefault();

            if (bigDecimalObjectVector == null || bigDecimalObjectVector.isEmpty()) {
                return null;
            }

            BigDecimal product = BigDecimal.ONE;

            final long n = bigDecimalObjectVector.size();
            long nullCount = 0;

            for (long i = 0; i < n; i++) {
                BigDecimal val = bigDecimalObjectVector.get(i);
                if (!isNull(val)) {
                    product = product.multiply(val, mathContextDefault);
                } else {
                    nullCount++;
                }
            }
            return nullCount == n ? null : product;
        }
    }

    private void doTestStaticZeroKeyBigNumbers(final QueryTable t, final int prevTicks, final int postTicks) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        Table actual = t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"));
        Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"))
                .update("bigIntCol=prodBigInt(bigIntCol)", "bigDecimalCol=prodBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                // Do a fuzzy compare.
                BigDecimal diff = actualVal.subtract(expectedVal, mathContextDefault).abs();
                if (!diff.equals(BigDecimal.ZERO)) {
                    BigDecimal diffFraction = diff.divide(actualVal, mathContextDefault).abs();
                    Assert.eqTrue(allowableFraction.compareTo(diffFraction) == 1, "values match");
                }
            }
        }
    }

    private void doTestStaticZeroKeyTimedBigNumbers(final QueryTable t, final Duration prevTime,
            final Duration postTime) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        Table actual =
                t.updateBy(UpdateByOperation.RollingProduct("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"));
        Table expected =
                t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"))
                        .update("bigIntCol=prodBigInt(bigIntCol)",
                                "bigDecimalCol=prodBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                // Do a fuzzy compare.
                BigDecimal diff = actualVal.subtract(expectedVal, mathContextDefault).abs();
                if (!diff.equals(BigDecimal.ZERO)) {
                    BigDecimal diffFraction = diff.divide(actualVal, mathContextDefault).abs();
                    Assert.eqTrue(allowableFraction.compareTo(diffFraction) == 1, "values match");
                }
            }
        }
    }

    private void doTestStaticBucketedBigNumbers(final QueryTable t, final int prevTicks, final int postTicks) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        Table actual =
                t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym");
        Table expected =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym")
                        .update("bigIntCol=prodBigInt(bigIntCol)",
                                "bigDecimalCol=prodBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                // Do a fuzzy compare.
                BigDecimal diff = actualVal.subtract(expectedVal, mathContextDefault).abs();
                if (!diff.equals(BigDecimal.ZERO)) {
                    BigDecimal diffFraction = diff.divide(actualVal, mathContextDefault).abs();
                    Assert.eqTrue(allowableFraction.compareTo(diffFraction) == 1, "values match");
                }
            }
        }
    }

    private void doTestStaticBucketedTimedBigNumbers(final QueryTable t, final Duration prevTime,
            final Duration postTime) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        Table actual =
                t.updateBy(UpdateByOperation.RollingProduct("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"),
                        "Sym");
        Table expected = t
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym")
                .update("bigIntCol=prodBigInt(bigIntCol)", "bigDecimalCol=prodBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                // Do a fuzzy compare.
                BigDecimal diff = actualVal.subtract(expectedVal, mathContextDefault).abs();
                if (!diff.equals(BigDecimal.ZERO)) {
                    BigDecimal diffFraction = diff.divide(actualVal, mathContextDefault).abs();
                    Assert.eqTrue(allowableFraction.compareTo(diffFraction) == 1, "values match");
                }
            }
        }
    }
    // endregion Object Helper functions

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final int prevTicks = 1;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 10;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 10;
        final int postTicks = -5;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(15);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    private void doTestStaticZeroKey(final int prevTicks, final int postTicks) {
        final QueryTable t = createSmallTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table actual = t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, primitiveColumns));
        final Table expected = t.update(getCastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(getFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact,
                TableDiff.DiffItems.DoubleFraction);

        doTestStaticZeroKeyBigNumbers(t, prevTicks, postTicks);
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        final QueryTable t = createSmallTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final Table actual = t.updateBy(UpdateByOperation.RollingProduct("ts", prevTime, postTime, primitiveColumns));
        final Table expected = t.update(getCastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(getFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact,
                TableDiff.DiffItems.DoubleFraction);

        doTestStaticZeroKeyTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    @Test
    public void testStaticGroupedBucketedTimed() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table actual =
                t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, primitiveColumns), "Sym");
        final Table expected = t.update(getCastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns), "Sym")
                .update(getFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact,
                TableDiff.DiffItems.DoubleFraction);

        doTestStaticBucketedBigNumbers(t, prevTicks, postTicks);
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final Table actual =
                t.updateBy(UpdateByOperation.RollingProduct("ts", prevTime, postTime, primitiveColumns), "Sym");
        final Table expected = t.update(getCastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(getFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expected, actual, TableDiff.DiffItems.DoublesExact,
                TableDiff.DiffItems.DoubleFraction);

        doTestStaticBucketedTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Append Only Tests

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, columns),
                                        "Sym")
                                : t.updateBy(UpdateByOperation.RollingProduct(prevTicks, postTicks, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(
                                UpdateByOperation.RollingProduct("ts", prevTime, postTime, columns), "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingProduct("ts", prevTime, postTime, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    // endregion Append Only Tests

    // region General Ticking Tests

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final long prevTicks = 100;
        final long fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingRevExclusive() {
        final long prevTicks = 100;
        final long fwdTicks = -50;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwd() {
        final long prevTicks = 0;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final long prevTicks = -50;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestTicking(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long fwdTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(
                                UpdateByOperation.RollingProduct(prevTicks, fwdTicks, columns), "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingProduct(prevTicks, fwdTicks, columns));
                    }
                }
        };


        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(
                                UpdateByOperation.RollingProduct("ts", prevTime, postTime, columns), "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingProduct("ts", prevTime, postTime, columns));
                    }
                }
        };


        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingProduct(prevTicks, postTicks, columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    @Test
    public void testBucketedGeneralTickingTimedRevRedirected() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                new FuzzyEvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingProduct("ts", prevTime, postTime, columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion
}
