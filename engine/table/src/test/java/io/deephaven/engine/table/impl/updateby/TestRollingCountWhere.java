//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.Predicate;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;

@Category(OutOfBandTest.class)
public class TestRollingCountWhere extends BaseUpdateByTest {
    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    // region Object Helper functions
    private static class TestHelper {

        private static long countWhereInt(final IntVector intVector, final Predicate.Int predicate) {
            if (intVector == null || intVector.isEmpty()) {
                return 0L;
            }

            final long n = intVector.size();
            long count = 0;

            for (long i = 0; i < n; i++) {
                if (predicate.call(intVector.get(i))) {
                    count++;
                }
            }
            return count;
        }

        private static <T> long countWhereObject(final ObjectVector<T> objectVector,
                final Predicate.Unary<T> predicate) {
            if (objectVector == null || objectVector.isEmpty()) {
                return 0L;
            }

            final long n = objectVector.size();
            long count = 0;

            for (long i = 0; i < n; i++) {
                if (predicate.call(objectVector.get(i))) {
                    count++;
                }
            }
            return count;
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
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val > 50));
            }
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol <= 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val <= 50));
            }
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "true"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "true"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10 && intCol <= 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 10 && val <= 50));
            }
        }

        // Test chunk, then conditional filter, int > 10 && int % 2 == 0
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10", "intCol % 2 == 0"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 10 && val % 2 == 0));
            }
        }

        // Test on String column (representing all Object)
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                        "Sym != null && Sym.startsWith(`A`)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "SymGroup=Sym"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<String>> expectedGroupIt = expected.columnIterator("SymGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<String> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val != null && val.startsWith("A")));
            }
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val < 25 || val > 75));
            }
        }

        // Test AND of chunkfilter and OR filter
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                Filter.and(Filter.and(Filter.from("intCol > 50")),
                        Filter.or(Filter.from("intCol < 25", "intCol > 75")))));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 50 && (val < 25 || val > 75)));
            }
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if ((intVal < 25 || intVal > 75) && (longVal < 25 || longVal > 75)) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10", "longCol <= 50"));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 && longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10 || longCol <= 50"));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 || longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", filter));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"));

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val >= 0 && val < 30));
            }
        }

        // Test Boolean column
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                        "!isNull(boolCol) && boolCol"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "boolColGroup=boolCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<Boolean>> expectedGroupIt =
                        expected.columnIterator("boolColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<Boolean> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val != null && val));
            }
        }

        // Test Instant column
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                        "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "tsGroup=ts"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<Instant>> expectedGroupIt = expected.columnIterator("tsGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<Instant> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val.isAfter(DateTimeUtils.parseInstant("2022-03-09T12:00:00.000 NY"))));
            }
        }
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val > 50));
            }
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol <= 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val <= 50));
            }
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "true"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> false));
            }
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10 && intCol <= 50"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 10 && val <= 50));
            }
        }

        // Test on String column (representing all Object)
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                        "Sym != null && Sym.startsWith(`A`)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "SymGroup=Sym"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<String>> expectedGroupIt = expected.columnIterator("SymGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<String> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val != null && val.startsWith("A")));
            }
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val < 25 || val > 75));
            }
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if ((intVal < 25 || intVal > 75) && (longVal < 25 || longVal > 75)) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10", "longCol <= 50"));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 && longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10 || longCol <= 50"));
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 || longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", filter));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"));

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val >= 0 && val < 30));
            }
        }

        // Test Boolean column
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                        "!isNull(boolCol) && boolCol"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "boolColGroup=boolCol"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<Boolean>> expectedGroupIt =
                        expected.columnIterator("boolColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<Boolean> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val != null && val));
            }
        }

        // Test Instant column
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                        "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "tsGroup=ts"));

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<ObjectVector<Instant>> expectedGroupIt = expected.columnIterator("tsGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final ObjectVector<Instant> expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereObject(expectedValGroup,
                                val -> val.isAfter(DateTimeUtils.parseInstant("2022-03-09T12:00:00.000 NY"))));
            }
        }
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

        Table actual;
        Table expected;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val > 50));
            }
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol <= 50"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val <= 50));
            }
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "true"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "true"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10 && intCol <= 50"),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 10 && val <= 50));
            }
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val < 25 || val > 75));
            }
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if ((intVal < 25 || intVal > 75) && (longVal < 25 || longVal > 75)) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10", "longCol <= 50"),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 && longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 10 || longCol <= 50"),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 || longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", filter), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "intColGroup=intCol"), "Sym");

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val >= 0 && val < 30));
            }
        }
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;
        Table expected;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val > 50));
            }
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol <= 50"),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> val <= 50));
            }
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "true"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "true"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", TestHelper.countWhereInt(expectedValGroup, val -> true));
            }
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10 && intCol <= 50"),
                "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val > 10 && val <= 50));
            }
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val < 25 || val > 75));
            }
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if ((intVal < 25 || intVal > 75) && (longVal < 25 || longVal > 75)) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10", "longCol <= 50"),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 && longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 10 || longCol <= 50"),
                "Sym");
        expected = t.updateBy(
                UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol", "longColGroup=longCol"),
                "Sym");

        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroup1It = expected.columnIterator("intColGroup");
                final CloseableIterator<LongVector> expectedGroup2It = expected.columnIterator("longColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup1 = expectedGroup1It.next();
                final LongVector expectedValGroup2 = expectedGroup2It.next();

                long expectedVal = 0;
                for (int ii = 0; ii < expectedValGroup1.size(); ii++) {
                    final int intVal = expectedValGroup1.get(ii);
                    final long longVal = expectedValGroup2.get(ii);

                    if (intVal > 10 || longVal <= 50) {
                        expectedVal++;
                    }
                }

                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match", expectedVal);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", filter), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "intColGroup=intCol"), "Sym");

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<IntVector> expectedGroupIt = expected.columnIterator("intColGroup")) {
            while (actualIt.hasNext()) {
                final long actualVal = actualIt.nextLong();
                final IntVector expectedValGroup = expectedGroupIt.next();
                // Use a lambda over expectedValGroup to compute the expected val.
                Assert.eq(actualVal, "values match",
                        TestHelper.countWhereInt(expectedValGroup, val -> val >= 0 && val < 30));
            }
        }
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
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "false"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "false"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && longCol < 90")))
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
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && longCol < 90")))
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

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "false"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "false"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && longCol < 90")))
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
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(
                                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"),
                                "Sym")
                        : t.updateBy(
                                UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50", "longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && longCol < 90")))
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
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"), "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "intCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && intCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "false"), "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50",
                                "longCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count",
                                "intCol > 50 && longCol < 90"),
                        "Sym"))
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
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50"), "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50",
                                "intCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && intCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "false"), "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count", "intCol > 50",
                                "longCol < 90"),
                        "Sym")),
                EvalNugget.from(() -> t.updateBy(control,
                        UpdateByOperation.RollingCountWhere("ts", prevTime, postTime, "count",
                                "intCol > 50 && longCol < 90"),
                        "Sym"))
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

    @Test
    public void testProxy() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131).t;

        final int prevTicks = 100;
        final int postTicks = 0;

        Table actual;
        Table expected;

        // Compare the merged proxy table to the bucketed version (need to sort by symbol to get alignment).
        PartitionedTable pt = t.partitionBy("Sym");
        actual = pt.proxy()
                .updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"))
                .target().merge().sort("Sym");
        expected = t
                .updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol > 50"), "Sym")
                .sort("Sym");
        assertTableEquals(expected, actual);

        actual = pt.proxy()
                .updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol <= 50"))
                .target().merge().sort("Sym");
        expected = t
                .updateBy(UpdateByOperation.RollingCountWhere(prevTicks, postTicks, "count", "intCol <= 50"), "Sym")
                .sort("Sym");
        assertTableEquals(expected, actual);
    }
}
