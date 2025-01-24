//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.Predicate;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;

@Category(OutOfBandTest.class)
public class TestCumCountWhere extends BaseUpdateByTest {
    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    // region Object Helper functions
    private static class TestHelper {
        private static void assertWhereInt(
                final CloseablePrimitiveIteratorOfLong actualIt,
                final CloseablePrimitiveIteratorOfInt valueIt,
                final Predicate.Int predicate) {
            if (!actualIt.hasNext()) {
                return;
            }

            long count = 0;
            while (actualIt.hasNext()) {
                if (predicate.call(valueIt.nextInt())) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        private static <T> void assertWhereObject(
                final CloseablePrimitiveIteratorOfLong actualIt,
                final CloseableIterator<T> valueIt,
                final Predicate.Unary<T> predicate) {
            if (!actualIt.hasNext()) {
                return;
            }

            long count = 0;
            while (actualIt.hasNext()) {
                if (predicate.call(valueIt.next())) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }
    }
    // endregion Object Helper functions

    // region Zero Key Tests

    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 50);
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val <= 50);
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "true"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> true);
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "false"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> false);
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10 && intCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 10 && val <= 50);
        }

        // Test on String column (representing all Object)
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count",
                        "Sym != null && Sym.startsWith(`A`)"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<String> expectedIt = t.columnIterator("Sym")) {
            TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val.startsWith("A"));
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val < 25 || val > 75);
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if ((val1 < 25 || val1 > 75) && (val2 < 25 || val2 > 75)) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10", "longCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if (val1 > 10 && val2 <= 50) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10 || longCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if (val1 > 10 || val2 <= 50) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", filter));

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val >= 0 && val <= 29);
        }

        // Test Boolean column
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "!isNull(boolCol) && boolCol"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<Boolean> expectedIt = t.columnIterator("boolCol")) {
            TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val);
        }

        // Test Instant column
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<Instant> expectedIt = t.columnIterator("ts")) {
            TestHelper.assertWhereObject(actualIt, expectedIt,
                    val -> val.isAfter(DateTimeUtils.parseInstant("2022-03-09T12:00:00.000 NY")));
        }
    }

    @Test
    public void testStaticZeroKeyAllNulls() {
        final QueryTable t = createTestTableAllNull(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        Table actual;

        // Test simple ChunkFilter, int > 50
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 50);
        }

        // Test simple ChunkFilter, int <= 50
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val <= 50);
        }

        // Test simple conditional filter, true
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "true"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> true);
        }

        // Test simple conditional filter, false
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "false"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> false);
        }

        // Test complex conditional filter, int > 10 && int <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10 && intCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 10 && val <= 50);
        }

        // Test on String column (representing all Object)
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count",
                        "Sym != null && Sym.startsWith(`A`)"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<String> expectedIt = t.columnIterator("Sym")) {
            TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val.startsWith("A"));
        }

        // Test OR filter (processed as a WhereFilter)
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                Filter.or(Filter.from("intCol < 25", "intCol > 75"))));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val < 25 || val > 75);
        }

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                        Filter.or(Filter.from("longCol < 25", "longCol > 75")))));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if ((val1 < 25 || val1 > 75) && (val2 < 25 || val2 > 75)) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test multi-column chunk filter, int > 10, longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10", "longCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if (val1 > 10 && val2 <= 50) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        actual = t.updateBy(
                UpdateByOperation.CumCountWhere("count", "intCol > 10 || longCol <= 50"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt1 = t.integerColumnIterator("intCol");
                final CloseablePrimitiveIteratorOfLong expectedIt2 = t.longColumnIterator("longCol")) {

            long count = 0;
            while (actualIt.hasNext()) {
                final int val1 = expectedIt1.nextInt();
                final long val2 = expectedIt2.nextLong();

                if (val1 > 10 || val2 <= 50) {
                    count++;
                }
                Assert.eq(actualIt.nextLong(), "values match", count);
            }
        }

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", filter));

        // The set table contains 0-29, so we can compare against an equivalent filter.
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseablePrimitiveIteratorOfInt expectedIt = t.integerColumnIterator("intCol")) {
            TestHelper.assertWhereInt(actualIt, expectedIt, val -> val >= 0 && val <= 29);
        }

        // Test Boolean column
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count", "!isNull(boolCol) && boolCol"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<Boolean> expectedIt = t.columnIterator("boolCol")) {
            TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val);
        }

        // Test Instant column
        actual = t.updateBy(UpdateByOperation.CumCountWhere("count",
                "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"));
        try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                final CloseableIterator<Instant> expectedIt = t.columnIterator("ts")) {
            TestHelper.assertWhereObject(actualIt, expectedIt,
                    val -> val.isAfter(DateTimeUtils.parseInstant("2022-03-09T12:00:00.000 NY")));
        }
    }

    // endregion

    // region Bucketed Tests

    @Test
    public void testStaticBucketed() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final PartitionedTable preOp = t.partitionBy("Sym");

        PartitionedTable postOp;

        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 50);
            }
            return source;
        });

        // Test simple ChunkFilter, int <= 50
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol <= 50"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> val <= 50);
            }
            return source;
        });

        // Test simple conditional filter, true
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "true"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> true);
            }
            return source;
        });

        // Test simple conditional filter, false
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "false"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> false);
            }
            return source;
        });

        // Test complex conditional filter, int > 10 && int <= 50
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 10 && intCol <= 50"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> val > 10 && val <= 50);
            }
            return source;
        });

        // Test on String column (representing all Object)
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "Sym != null && Sym.startsWith(`A`)"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseableIterator<String> expectedIt = source.columnIterator("Sym")) {
                TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val.startsWith("A"));
            }
            return source;
        });

        // Test OR filter (processed as a WhereFilter)
        postOp = t.updateBy(
                UpdateByOperation.CumCountWhere("count", Filter.or(Filter.from("intCol < 25", "intCol > 75"))), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> val < 25 || val > 75);
            }
            return source;
        });

        // Test ANDing two OR filter (processed as sequential WhereFilters)
        postOp = t
                .updateBy(UpdateByOperation.CumCountWhere("count",
                        Filter.and(Filter.or(Filter.from("intCol < 25", "intCol > 75")),
                                Filter.or(Filter.from("longCol < 25", "longCol > 75")))),
                        "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt1 = source.integerColumnIterator("intCol");
                    final CloseablePrimitiveIteratorOfLong expectedIt2 = source.longColumnIterator("longCol")) {

                long count = 0;
                while (actualIt.hasNext()) {
                    final int val1 = expectedIt1.nextInt();
                    final long val2 = expectedIt2.nextLong();

                    if ((val1 < 25 || val1 > 75) && (val2 < 25 || val2 > 75)) {
                        count++;
                    }
                    Assert.eq(actualIt.nextLong(), "values match", count);
                }
            }
            return source;
        });

        // Test multi-column chunk filter, int > 10, longCol <= 50
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 10", "longCol <= 50"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt1 = source.integerColumnIterator("intCol");
                    final CloseablePrimitiveIteratorOfLong expectedIt2 = source.longColumnIterator("longCol")) {

                long count = 0;
                while (actualIt.hasNext()) {
                    final int val1 = expectedIt1.nextInt();
                    final long val2 = expectedIt2.nextLong();

                    if (val1 > 10 && val2 <= 50) {
                        count++;
                    }
                    Assert.eq(actualIt.nextLong(), "values match", count);
                }
            }
            return source;
        });

        // Test multi-column conditional filter, int > 10 || longCol <= 50
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 10 || longCol <= 50"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt1 = source.integerColumnIterator("intCol");
                    final CloseablePrimitiveIteratorOfLong expectedIt2 = source.longColumnIterator("longCol")) {

                long count = 0;
                while (actualIt.hasNext()) {
                    final int val1 = expectedIt1.nextInt();
                    final long val2 = expectedIt2.nextLong();

                    if (val1 > 10 || val2 <= 50) {
                        count++;
                    }
                    Assert.eq(actualIt.nextLong(), "values match", count);
                }
            }
            return source;
        });

        // Test DynamicWhereFilter to ensure we have supported generic WhereFilters correctly.
        final QueryTable setTable = (QueryTable) TableTools.emptyTable(30).update("intCol = (int)ii");
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("intCol", "intCol"));

        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", filter), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseablePrimitiveIteratorOfInt expectedIt = source.integerColumnIterator("intCol")) {
                TestHelper.assertWhereInt(actualIt, expectedIt, val -> val >= 0 && val <= 29);
            }
            return source;
        });


        // Test Boolean column
        postOp = t.updateBy(UpdateByOperation.CumCountWhere("count", "!isNull(boolCol) && boolCol"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseableIterator<Boolean> expectedIt = source.columnIterator("boolCol")) {
                TestHelper.assertWhereObject(actualIt, expectedIt, val -> val != null && val);
            }
            return source;
        });

        // Test Instant column
        postOp = t
                .updateBy(UpdateByOperation.CumCountWhere("count",
                        "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"), "Sym")
                .partitionBy("Sym");
        preOp.partitionedTransform(postOp, (source, actual) -> {
            try (final CloseablePrimitiveIteratorOfLong actualIt = actual.longColumnIterator("count");
                    final CloseableIterator<Instant> expectedIt = source.columnIterator("ts")) {
                TestHelper.assertWhereObject(actualIt, expectedIt,
                        val -> val.isAfter(DateTimeUtils.parseInstant("2022-03-09T12:00:00.000 NY")));
            }
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
        final int seed = 0xB177B177;
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, seed,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        if (appendOnly) {
            t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"),
                                "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50",
                                "intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50",
                                "intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "intCol > 50 && intCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "intCol > 50 && intCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count", "false"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count", "false"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50",
                                "longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50",
                                "longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "intCol > 50 && longCol < 90"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "intCol > 50 && longCol < 90"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "boolCol != null && boolCol"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "boolCol != null && boolCol"))),
                EvalNugget.from(() -> bucketed
                        ? t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)"), "Sym")
                        : t.updateBy(UpdateByOperation.CumCountWhere("count",
                                "ts > DateTimeUtils.parseInstant(`2022-03-09T12:00:00.000 NY`)")))
        };

        final Random random = new Random(seed);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            if (appendOnly) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                        () -> generateAppends(DYNAMIC_UPDATE_SIZE, random, t, result.infos));
                TstUtils.validate("Table", nuggets);
            } else {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, random, t, result.infos, nuggets);
            }
        }
    }
    // endregion

    @Test
    public void testProxy() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131).t;

        Table actual;
        Table expected;

        // Compare the merged proxy table to the bucketed version (need to sort by symbol to get alignment).
        PartitionedTable pt = t.partitionBy("Sym");
        actual = pt.proxy()
                .updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"))
                .target().merge().sort("Sym");
        expected = t
                .updateBy(UpdateByOperation.CumCountWhere("count", "intCol > 50"), "Sym")
                .sort("Sym");
        assertTableEquals(expected, actual);

        actual = pt.proxy()
                .updateBy(UpdateByOperation.CumCountWhere("count", "intCol <= 50"))
                .target().merge().sort("Sym");
        expected = t
                .updateBy(UpdateByOperation.CumCountWhere("count", "intCol <= 50"), "Sym")
                .sort("Sym");
        assertTableEquals(expected, actual);
    }
}
