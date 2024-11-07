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
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.function.Basic.isNull;

@Category(OutOfBandTest.class)
public class TestRollingWAvg extends BaseUpdateByTest {
    private final BigDecimal allowableDelta = BigDecimal.valueOf(0.000000001);
    private final BigDecimal allowableFraction = BigDecimal.valueOf(0.000001);
    final static MathContext mathContextDefault = UpdateByControl.mathContextDefault();

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
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 5_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    private String[] getFormulas(String[] columns, String weightCol) {
        return Arrays.stream(columns)
                .map(c -> String.format("%s=wavg(%s,%s)", c, c, weightCol))
                .toArray(String[]::new);
    }

    // For verification, we will upcast some columns and use already-defined Numeric class functions.
    private String[] getCastingFormulas(String[] columns) {
        return Arrays.stream(columns)
                .map(c -> c.equals("charCol")
                        ? String.format("%s=(short)%s", c, c)
                        : null)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    // region Object Helper functions
    public interface BiFunction<T1, T2, R> {
        R apply(T1 val1, T2 val2);
    }

    @SuppressWarnings("unused") // Functions used via QueryLibrary
    @VisibleForTesting
    @TestUseOnly
    public static class Helpers {

        public static BigDecimal wavgBigIntDouble(ObjectVector<BigInteger> bigIntegerObjectVector,
                DoubleVector doubleVector) {
            if (bigIntegerObjectVector == null || doubleVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = bigIntegerObjectVector.size();

            for (long i = 0; i < n; i++) {
                final BigInteger val = bigIntegerObjectVector.get(i);
                final double weightVal = doubleVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal decVal = new BigDecimal(val);
                    final BigDecimal weightDecVal = BigDecimal.valueOf(weightVal);

                    final BigDecimal weightedVal = decVal.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigIntShort(ObjectVector<BigInteger> valueVector, ShortVector weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigInteger val = valueVector.get(i);
                final short weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal decVal = new BigDecimal(val);
                    final BigDecimal weightDecVal = BigDecimal.valueOf(weightVal);

                    final BigDecimal weightedVal = decVal.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigIntBigInt(ObjectVector<BigInteger> valueVector,
                ObjectVector<BigInteger> weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigInteger val = valueVector.get(i);
                final BigInteger weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal decVal = new BigDecimal(val);
                    final BigDecimal weightDecVal = new BigDecimal(weightVal);

                    final BigDecimal weightedVal = decVal.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigIntBigDec(ObjectVector<BigInteger> valueVector,
                ObjectVector<BigDecimal> weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigInteger val = valueVector.get(i);
                final BigDecimal weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal decVal = new BigDecimal(val);

                    final BigDecimal weightedVal = decVal.multiply(weightVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigDecDouble(ObjectVector<BigDecimal> valueVector, DoubleVector weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigDecimal val = valueVector.get(i);
                final double weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal weightDecVal = BigDecimal.valueOf(weightVal);

                    final BigDecimal weightedVal = val.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigDecShort(ObjectVector<BigDecimal> valueVector, ShortVector weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigDecimal val = valueVector.get(i);
                final short weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal weightDecVal = BigDecimal.valueOf(weightVal);

                    final BigDecimal weightedVal = val.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigDecBigInt(ObjectVector<BigDecimal> valueVector,
                ObjectVector<BigInteger> weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigDecimal val = valueVector.get(i);
                final BigInteger weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal weightDecVal = new BigDecimal(weightVal);

                    final BigDecimal weightedVal = val.multiply(weightDecVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightDecVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }

        public static BigDecimal wavgBigDecBigDec(ObjectVector<BigDecimal> valueVector,
                ObjectVector<BigDecimal> weightVector) {
            if (valueVector == null || weightVector == null) {
                return null;
            }

            BigDecimal weightValueSum = new BigDecimal(0);
            BigDecimal weightSum = new BigDecimal(0);
            long count = 0;

            final long n = valueVector.size();

            for (long i = 0; i < n; i++) {
                final BigDecimal val = valueVector.get(i);
                final BigDecimal weightVal = weightVector.get(i);
                if (!isNull(val) && !isNull(weightVal)) {
                    final BigDecimal weightedVal = val.multiply(weightVal, mathContextDefault);
                    weightValueSum = weightValueSum.add(weightedVal, mathContextDefault);
                    weightSum = weightSum.add(weightVal, mathContextDefault);
                    count++;
                }
            }
            if (count == 0 || weightSum.equals(BigDecimal.ZERO)) {
                return null;
            }
            return weightValueSum.divide(weightSum, mathContextDefault);
        }
    }

    private void doTestStaticBigNumbers(final QueryTable t,
            final int prevTicks,
            final int fwdTicks,
            final boolean bucketed,
            final String weightCol,
            final String bigIntFunction,
            final String bigDecFunction) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final String[] updateCols = new String[] {
                String.format("bigIntCol=%s(bigIntCol, %s)", bigIntFunction, weightCol),
                String.format("bigDecimalCol=%s(bigDecimalCol, %s)", bigDecFunction, weightCol),
        };

        final Table actual;
        final Table expected;
        if (bucketed) {
            actual = t.updateBy(
                    UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, "bigIntCol", "bigDecimalCol"), "Sym");
            expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol",
                    weightCol), "Sym")
                    .update(updateCols);
        } else {
            actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, "bigIntCol",
                    "bigDecimalCol"));
            expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, "bigIntCol", "bigDecimalCol",
                    weightCol))
                    .update(updateCols);
        }

        BigDecimal[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigDecimal.class).toArray();
        BigDecimal[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigDecimal.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigDecimal actualVal = biActual[ii];
            BigDecimal expectedVal = biExpected[ii];

            Assert.eqTrue(fuzzyEquals(actualVal, expectedVal), "values match");
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];

            Assert.eqTrue(fuzzyEquals(actualVal, expectedVal), "values match");
        }
    }

    private void doTestStaticTimedBigNumbers(final QueryTable t,
            final Duration prevTime,
            final Duration postTime,
            final boolean bucketed,
            final String weightCol,
            final String bigIntFunction,
            final String bigDecFunction) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        final String[] updateCols = new String[] {
                String.format("bigIntCol=%s(bigIntCol, %s)", bigIntFunction, weightCol),
                String.format("bigDecimalCol=%s(bigDecimalCol, %s)", bigDecFunction, weightCol),
        };

        final Table actual;
        final Table expected;
        if (bucketed) {
            actual = t.updateBy(
                    UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, "bigIntCol", "bigDecimalCol"),
                    "Sym");
            expected = t
                    .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol",
                            weightCol), "Sym")
                    .update(updateCols);
        } else {
            actual = t.updateBy(
                    UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, "bigIntCol", "bigDecimalCol"));
            expected =
                    t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol",
                            weightCol))
                            .update(updateCols);
        }

        BigDecimal[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigDecimal.class).toArray();
        BigDecimal[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigDecimal.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigDecimal actualVal = biActual[ii];
            BigDecimal expectedVal = biExpected[ii];

            Assert.eqTrue(fuzzyEquals(actualVal, expectedVal), "values match");
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];

            Assert.eqTrue(fuzzyEquals(actualVal, expectedVal), "values match");
        }
    }
    // endregion

    // region Static Zero Key Tests

    private void doTestStatic(boolean bucketed, int prevTicks, int fwdTicks) {
        // Test with a double type weight value
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {"charCol", weightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)
                }).t;

        Table actual;
        Table expected;

        if (bucketed) {
            actual =
                    t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns), "Sym");
            // We need the weight column as vector here for comparison
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                            UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)),
                    "Sym")
                    .update(getFormulas(primitiveColumns, weightCol));
        } else {
            actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns));
            // We need the weight column as vector here for comparison
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                            UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)))
                    .update(getFormulas(primitiveColumns, weightCol));
        }

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        doTestStaticBigNumbers(t, prevTicks, fwdTicks, bucketed, weightCol, "wavgBigIntDouble", "wavgBigDecDouble");

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {"charCol", weightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)
                }).t;

        if (bucketed) {
            actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns), "Sym");
            // We need the weight column as vector here for comparison
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                            UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)),
                    "Sym")
                    .update(getFormulas(primitiveColumns, weightCol));
        } else {
            actual = t.updateBy(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, weightCol, primitiveColumns));
            // We need the weight column as vector here for comparison
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup(prevTicks, fwdTicks, primitiveColumns),
                            UpdateByOperation.RollingGroup(prevTicks, fwdTicks, weightCol)))
                    .update(getFormulas(primitiveColumns, weightCol));
        }

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        doTestStaticBigNumbers(t, prevTicks, fwdTicks, bucketed, weightCol, "wavgBigIntShort", "wavgBigDecShort");

        ///////////////////////////////////////////////////////////////////////////

        // Test with BigInteger/BigDecimal weight values

        weightCol = "bigIntWeightCol";
        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {
                        new BigIntegerGenerator(BigInteger.valueOf(-10), BigInteger.valueOf(10), .1)}).t;

        doTestStaticBigNumbers(t, prevTicks, fwdTicks, bucketed, weightCol, "wavgBigIntBigInt", "wavgBigDecBigInt");

        weightCol = "bigDecWeightCol";
        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {weightCol},
                new TestDataGenerator[] {
                        new BigDecimalGenerator(BigInteger.valueOf(1), BigInteger.valueOf(2), 5, .1)}).t;

        doTestStaticBigNumbers(t, prevTicks, fwdTicks, bucketed, weightCol, "wavgBigIntBigDec", "wavgBigDecBigDec");
    }

    private void doTestStaticTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        String weightCol = "doubleWeightCol";

        QueryTable t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol", weightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)
                }).t;


        Table actual;
        Table expected;

        if (bucketed) {
            actual =
                    t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns),
                            "Sym");
            // We need the weight column as vector here for comparison.
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                            UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)),
                    "Sym")
                    .update(getFormulas(primitiveColumns, weightCol));
        } else {
            actual =
                    t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns));
            // We need the weight column as vector here for comparison.
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                            UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)))
                    .update(getFormulas(primitiveColumns, weightCol));
        }

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        doTestStaticTimedBigNumbers(t, prevTime, postTime, bucketed, weightCol, "wavgBigIntDouble", "wavgBigDecDouble");

        ///////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        weightCol = "shortWeightCol";

        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol", weightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)
                }).t;

        if (bucketed) {
            actual = t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns),
                    "Sym");
            // We need the weight column as vector here for comparison.
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                            UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)),
                    "Sym")
                    .update(getFormulas(primitiveColumns, weightCol));
        } else {
            actual = t.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, weightCol, primitiveColumns));
            // We need the weight column as vector here for comparison.
            expected = t.update(getCastingFormulas(primitiveColumns)).updateBy(
                    List.of(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns),
                            UpdateByOperation.RollingGroup("ts", prevTime, postTime, weightCol)))
                    .update(getFormulas(primitiveColumns, weightCol));
        }

        // Drop the weight column before comparing.
        TstUtils.assertTableEquals(
                expected.dropColumns(weightCol),
                actual.dropColumns(weightCol),
                TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        doTestStaticTimedBigNumbers(t, prevTime, postTime, bucketed, weightCol, "wavgBigIntShort", "wavgBigDecShort");

        ///////////////////////////////////////////////////////////////////////////

        // Test with BigInteger/BigDecimal weight values

        weightCol = "bigIntWeightCol";
        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new BigIntegerGenerator(BigInteger.valueOf(-10), BigInteger.valueOf(10), .1)}).t;

        doTestStaticTimedBigNumbers(t, prevTime, postTime, bucketed, weightCol, "wavgBigIntBigInt", "wavgBigDecBigInt");

        weightCol = "bigDecWeightCol";
        t = createTestTable(STATIC_TABLE_SIZE, bucketed, false, false, 0x31313131,
                new String[] {"ts", weightCol}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new BigDecimalGenerator(BigInteger.valueOf(1), BigInteger.valueOf(2), 5, .1)}).t;

        doTestStaticTimedBigNumbers(t, prevTime, postTime, bucketed, weightCol, "wavgBigIntBigDec", "wavgBigDecBigDec");
    }

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final int prevTicks = 1;
        final int postTicks = 0;

        doTestStatic(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestStatic(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestStatic(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestStatic(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestStatic(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int fwdTicks = 100;

        doTestStatic(false, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        doTestStaticTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticTimed(false, prevTime, postTime);
    }
    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestStatic(true, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticGroupedBucketedTimed() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestStatic(true, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestStatic(true, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestStatic(true, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestStatic(true, prevTicks, fwdTicks);
    }

    @Test
    public void testStaticBucketedTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestStaticTimed(true, prevTime, postTime);
    }
    // endregion

    // region Append Only Tests

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int fwdTicks = 50;

        doTestAppendOnly(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestAppendOnly(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestAppendOnly(true, prevTicks, fwdTicks);
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

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int fwdTicks) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol", doubleWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)
                });

        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol", shortWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)
                });

        final QueryTable t2 = result2.t;
        t2.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns),
                                        "Sym")
                                : t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", doubleWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", shortWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;
        t2.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                                columns),
                                        "Sym")
                                : t2.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
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
        final int fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int fwdTicks = -50;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int fwdTicks = 100;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int fwdTicks = 100;

        doTestTicking(true, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int fwdTicks = 50;

        doTestTicking(true, prevTicks, fwdTicks);
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
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol", doubleWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)
                });
        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol", shortWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)
                });
        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns),
                                        "Sym")
                                : t2.updateBy(
                                        UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {
        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", doubleWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns),
                                        "Sym")
                                : t.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                                columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", shortWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t2.updateBy(
                                        UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                                columns),
                                        "Sym")
                                : t2.updateBy(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int fwdTicks = 0;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"charCol", doubleWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)
                });

        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, doubleWeightCol, columns)),
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

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"charCol", shortWeightCol},
                new TestDataGenerator[] {
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)
                });

        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t2.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg(prevTicks, fwdTicks, shortWeightCol, columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos, nuggets);
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

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        // Test with a double type weight value
        final String doubleWeightCol = "doubleWeightCol";
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", doubleWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new DoubleGenerator(10.1, 20.1, .1)});

        final QueryTable t = result.t;

        EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, doubleWeightCol,
                                        columns)),
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

        ///////////////////////////////////////////////////////////////////////////////

        // Test with a short type weight value
        final String shortWeightCol = "shortWeightCol";

        final CreateResult result2 = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0xFFFABBBC,
                new String[] {"ts", "charCol", shortWeightCol}, new TestDataGenerator[] {
                        new SortedInstantGenerator(
                                DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                                DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1),
                        new ShortGenerator((short) -6000, (short) 65535, .1)});

        final QueryTable t2 = result2.t;

        nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return t2.updateBy(control,
                                List.of(UpdateByOperation.RollingWAvg("ts", prevTime, postTime, shortWeightCol,
                                        columns)),
                                ColumnName.from("Sym"));
                    }
                }
        };

        billy.setSeed(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t2, result2.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion
}
