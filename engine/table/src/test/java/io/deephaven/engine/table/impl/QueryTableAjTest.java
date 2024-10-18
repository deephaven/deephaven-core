//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.table.impl.AsOfJoinMatchFactory.AsOfJoinResult;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.testutil.QueryTableTestBase.JoinIncrement;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import gnu.trove.list.array.TIntArrayList;
import io.deephaven.util.type.ArrayTypeUtils;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.api.TableOperationsDefaults.splitToCollection;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.QueryTableTestBase.intColumn;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.util.QueryConstants.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

@Category(OutOfBandTest.class)
public class QueryTableAjTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testAjConflict() {
        final Table left = TableTools.newTable(
                col("Bucket", "A", "B", "A", "C", "D", "A"),
                longCol("LeftStamp", 1L, 10L, 50L, 3L, 4L, 60L));
        final Table right = TableTools.newTable(
                col("Bucket", "A", "B", "A", "B", "A", "D", "E"),
                longCol("RightStamp", 1L, 5L, 10L, 25L, 50L, 5L, 3L),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        try {
            left.aj(right, "LeftStamp>=RightStamp");
            TestCase.fail("Expected conflicting column exception!");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "Conflicting column names [Bucket]");
        }
    }

    @Test
    public void testAjNull() {
        final Table left = TableTools.newTable(
                col("Bucket", "A", "B", "A", "C", "D", "A"),
                longCol("LeftStamp", 1L, 10L, 50L, 3L, 4L, 60L));

        try {
            left.aj(null, "LeftStamp>=RightStamp");
            TestCase.fail("Expected null argument exception!");
        } catch (RuntimeException e) {
            assertEquals("aj() requires a non-null right hand side table.", e.getMessage());
        }
    }

    private interface MakeColumn {
        @SuppressWarnings("unchecked")
        <T> ColumnHolder<T> make(String name, T... data);
    }

    @Test
    public void testAjStatic() {
        // noinspection unchecked
        testAjStatic(TableTools::col, TableTools::col);
    }

    @Test
    public void testAjStaticGroupedBoth() {
        // noinspection unchecked
        testAjStatic(TstUtils::colIndexed, TstUtils::colIndexed);
    }

    @Test
    public void testAjStaticGroupedLeftOnly() {
        // noinspection unchecked
        testAjStatic(TstUtils::colIndexed, TableTools::col);
    }

    @Test
    public void testAjStaticGroupedRightOnly() {
        // noinspection unchecked
        testAjStatic(TableTools::col, TstUtils::colIndexed);
    }

    public void testAjStatic(MakeColumn leftMaker, MakeColumn rightMaker) {
        final Table left = TstUtils.testTable(
                leftMaker.make("Bucket", "A", "B", "A", "C", "D", "A"),
                longCol("LeftStamp", 1L, 10L, 50L, 3L, 4L, 60L));
        final Table right = TstUtils.testTable(
                rightMaker.make("Bucket", "A", "B", "A", "B", "A", "D", "E"),
                longCol("RightStamp", 1L, 5L, 10L, 25L, 50L, 5L, 3L),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 2, 5, NULL_INT, NULL_INT, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 2, 3, NULL_INT, NULL_INT, 5},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 4, 5, NULL_INT, 6, NULL_INT},
                intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 4, NULL_INT, NULL_INT, 6, NULL_INT},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjStaticIndexedBoth() {
        // noinspection unchecked
        testAjStaticIndexed(true, true);
    }

    @Test
    public void testAjStaticIndexedLeftOnly() {
        // noinspection unchecked
        testAjStaticIndexed(true, false);
    }

    @Test
    public void testAjStaticIndexedRightOnly() {
        // noinspection unchecked
        testAjStaticIndexed(false, true);
    }

    public void testAjStaticIndexed(boolean leftIndexed, boolean rightIndexed) {
        final Table left = TstUtils.testTable(
                col("BucketA", "A", "B", "A", "C", "D", "A"),
                col("BucketB", "A", "A", "A", "A", "A", "A"),
                longCol("LeftStamp", 1L, 10L, 50L, 3L, 4L, 60L));
        final Table right = TstUtils.testTable(
                col("BucketA", "A", "B", "A", "B", "A", "D", "E"),
                col("BucketB", "A", "A", "A", "A", "A", "A", "A"),
                longCol("RightStamp", 1L, 5L, 10L, 25L, 50L, 5L, 3L),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7));

        if (leftIndexed) {
            DataIndexer.getOrCreateDataIndex(left, "BucketA", "BucketB");
        }
        if (rightIndexed) {
            DataIndexer.getOrCreateDataIndex(right, "BucketA", "BucketB");
        }

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        final Table result = left.aj(right, "BucketA,BucketB,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 2, 5, NULL_INT, NULL_INT, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "BucketA,BucketB,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 2, 3, NULL_INT, NULL_INT, 5},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "BucketA,BucketB,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 4, 5, NULL_INT, 6, NULL_INT},
                intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "BucketA,BucketB,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 4, NULL_INT, NULL_INT, 6, NULL_INT},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjBoolean() {
        final Table left = TableTools.newTable(
                col("Bucket", "A", "A", "B", "A", "B", "C", "C", "A"),
                col("LeftStamp", true, false, true, false, false, true, false, null));

        final Table right = TableTools.newTable(
                col("Bucket", "A", "A", "A", "B", "C"),
                col("RightStamp", null, false, true, true, false),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjDateTime() {
        final Instant first = DateTimeUtils.parseInstant("2019-06-14T08:30:00 NY");
        final Instant second = DateTimeUtils.parseInstant("2019-06-14T19:30:00 NY");

        final Table left = TableTools.newTable(
                col("Bucket", "A", "A", "B", "A", "B", "C", "C", "A"),
                col("LeftStamp", second, first, second, first, first, second, first, null));

        final Table right = TableTools.newTable(
                col("Bucket", "A", "A", "A", "B", "C"),
                col("RightStamp", null, first, second, second, first),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjEmpty() {
        final Table left = TableTools.newTable(
                col("Bucket"),
                intCol("LeftStamp"));

        final Table right = TableTools.newTable(
                col("Bucket", "A", "A", "A", "B", "C"),
                intCol("RightStamp", 1, 2, 3, 4, 5),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(ArrayTypeUtils.EMPTY_INT_ARRAY, intColumn(result, "Sentinel"));
    }

    @Test
    public void testAjMissingState() {
        final Table left = TableTools.newTable(
                col("Bucket", 1, 1, 2),
                intCol("LeftStamp", 1, 1, 1));

        final Table right = TableTools.newTable(
                col("Bucket", 2, 3),
                intCol("RightStamp", 1, 1),
                intCol("Sentinel", 1, 2));

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, NULL_INT, 1}, intColumn(result, "Sentinel"));

        final Table left2 = TableTools.newTable(
                col("Bucket", 1, 2),
                intCol("LeftStamp", 1, 1));

        final Table right2 = TableTools.newTable(
                col("Bucket", 2, 3, 3),
                intCol("RightStamp", 1, 1, 1),
                intCol("Sentinel", 1, 2, 3));

        final Table result2 = left2.aj(right2, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 1}, intColumn(result2, "Sentinel"));
    }

    @Test
    public void testAjStrings() {
        final Table left = TableTools.newTable(
                col("Bucket", "A", "A", "B", "A", "B", "C", "C", "A"),
                col("LeftStamp", "t", "f", "t", "f", "f", "t", "f", null));

        final Table right = TableTools.newTable(
                col("Bucket", "A", "A", "A", "B", "C"),
                col("RightStamp", null, "f", "t", "t", "f"),
                intCol("Sentinel", 1, 2, 3, 4, 5));

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjFloats() {
        final Table left = TableTools.newTable(
                doubleCol("LeftStampD", 1.0, Double.NaN, NULL_DOUBLE, 2.0, 3.0, Double.NaN),
                floatCol("LeftStampF", 1.0f, Float.NaN, NULL_FLOAT, 2.0f, 3.0f, Float.NaN));

        final Table right = TableTools.newTable(
                doubleCol("RightStampD", NULL_DOUBLE, 1.0, 2.5, 3.0, Double.NaN, Double.NaN),
                floatCol("RightStampF", NULL_FLOAT, 1.0f, 2.5f, 3.0f, Float.NaN, Float.NaN),
                intCol("Sentinel", 0, 1, 2, 3, 4, 5));

        System.out.println("Left");
        TableTools.show(left);
        System.out.println("Right");
        TableTools.show(right);

        doFloatTest(left, right, "LeftStampD", "RightStampD");
        doFloatTest(left, right, "LeftStampF", "RightStampF");
    }

    private void doFloatTest(Table left, Table right, final String leftStamp, final String rightStamp) {
        final Table result = left.aj(right, leftStamp + ">=" + rightStamp, "Sentinel");
        System.out.println("Result");
        TableTools.showWithRowSet(result);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                result.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 5, 0, 1, 3, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, leftStamp + ">" + rightStamp, "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {0, 3, NULL_INT, 1, 2, 3}, intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, leftStamp + "<=" + rightStamp, "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {1, 4, 0, 2, 3, 4}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, leftStamp + "<" + rightStamp, "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        BaseArrayTestCase.assertEquals(new int[] {2, NULL_INT, 1, 2, 4, NULL_INT},
                intColumn(reverseResultGt, "Sentinel"));
    }

    private void tickCheck(Table left, boolean key, final String stampColumn, final String firstUnsorted,
            final String secondUnsorted) {
        final QueryTable right = TstUtils.testRefreshingTable(stringCol("SingleKey", "Key", "Key", "Key"),
                byteCol("ByteCol", (byte) 1, (byte) 2, (byte) 3),
                longCol("LongCol", 1, 2, 3),
                doubleCol("DoubleCol", 1, 2.0, 3),
                col("BoolCol", null, false, true),
                stringCol("StringCol", "A", "B", "C"));

        final QueryTable result1 =
                (QueryTable) left.aj(right, (key ? "SingleKey," : "") + stampColumn, "Dummy<=LongCol");
        try {
            base.setExpectError(true);
            final io.deephaven.engine.table.impl.ErrorListener listener =
                    new io.deephaven.engine.table.impl.ErrorListener(result1);
            result1.addUpdateListener(listener);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(right, i(4, 5, 6),
                        stringCol("SingleKey", "Key", "Key", "Key"),
                        byteCol("ByteCol", (byte) 4, (byte) 6, (byte) 5),
                        longCol("LongCol", 4, 6, 5),
                        doubleCol("DoubleCol", 4, 6, 5),
                        stringCol("StringCol", "A", "D", "C"),
                        col("BoolCol", null, true, false));
                right.notifyListeners(i(4, 5, 6), i(), i());
            });

            assertNotNull(listener.originalException());
            assertEquals(
                    "Right stamp columns must be sorted, but are not for " + (key ? "Key " : "[] (zero key columns) ")
                            + firstUnsorted + " came before " + secondUnsorted,
                    listener.originalException().getMessage());
        } finally {
            base.setExpectError(false);
        }
    }

    @Test
    public void testAjRandomStatic() {
        for (int seed = 0; seed < 10; ++seed) {
            for (int leftSize = 10; leftSize <= 10000; leftSize *= 10) {
                for (int rightSize = 10; rightSize <= 10000; rightSize *= 10) {
                    for (boolean reverse : new boolean[] {false, true}) {
                        for (boolean noexact : new boolean[] {false, true}) {
                            System.out.println("Seed=" + seed + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                                    + ", reverse=" + reverse + ", noexact=" + noexact);
                            testAjRandomStatic(seed, leftSize, rightSize, reverse, noexact,
                                    ColumnInfo.ColAttributes.None, ColumnInfo.ColAttributes.None);
                            testAjRandomStatic(seed, leftSize, rightSize, reverse, noexact,
                                    ColumnInfo.ColAttributes.Indexed, ColumnInfo.ColAttributes.None);
                            testAjRandomStatic(seed, leftSize, rightSize, reverse, noexact,
                                    ColumnInfo.ColAttributes.None, ColumnInfo.ColAttributes.Indexed);
                            testAjRandomStatic(seed, leftSize, rightSize, reverse, noexact,
                                    ColumnInfo.ColAttributes.Indexed, ColumnInfo.ColAttributes.Indexed);
                        }
                    }
                }
            }
        }
    }

    private void testAjRandomStatic(int seed, int leftSize, int rightSize, boolean reverse, boolean noexact,
            ColumnInfo.ColAttributes leftAttributes, ColumnInfo.ColAttributes rightAttributes) {
        final Random random = new Random(seed);

        final QueryTable leftTable = getTable(false, leftSize, random,
                initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                        Arrays.asList(Collections.singletonList(leftAttributes), Collections.emptyList(),
                                Collections.emptyList()),
                        new SetGenerator<>("Alpha", "Bravo", "Charlie", "Delta"),
                        new IntGenerator(0, 10000),
                        new IntGenerator(10_000_000, 10_010_000)));
        final QueryTable rightTable = getTable(false, rightSize, random,
                initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                        Arrays.asList(Collections.singletonList(rightAttributes), Collections.emptyList(),
                                Collections.emptyList()),
                        new SetGenerator<>("Alpha", "Bravo", "Charlie", "Echo"),
                        new SortedIntGenerator(0, 10000),
                        new IntGenerator(20_000_000, 20_010_000)));

        final String stampMatch =
                "LeftStamp" + (noexact ? (reverse ? "<" : ">") : (reverse ? "<=" : ">=")) + "RightStamp";
        final Table result;
        if (reverse) {
            result = leftTable.raj(rightTable, stampMatch, "RightSentinel");
        } else {
            result = leftTable.aj(rightTable, stampMatch, "RightSentinel");
        }

        checkAjResult(leftTable, rightTable, result, reverse, noexact);

        final Table resultBucket;
        if (reverse) {
            resultBucket = leftTable.raj(rightTable, "Bucket," + stampMatch, "RightSentinel");
        } else {
            resultBucket = leftTable.aj(rightTable, "Bucket," + stampMatch, "RightSentinel");
        }
        checkAjResults(resultBucket.partitionBy("Bucket"),
                leftTable.partitionBy("Bucket"), rightTable.partitionBy("Bucket"),
                reverse, noexact);
    }

    @Test
    public void testAjRandomStaticOverflow() {
        testAjRandomStaticOverflow(0, 32000, 32000);
    }

    @SuppressWarnings("SameParameterValue")
    private void testAjRandomStaticOverflow(int seed, int leftSize, int rightSize) {
        final Random random = new Random(seed);

        final QueryTable leftTable = getTable(false, leftSize, random,
                initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                        new StringGenerator(leftSize / 10),
                        new IntGenerator(0, 100000),
                        new IntGenerator(10_000_000, 10_010_000)));
        final QueryTable rightTable = getTable(false, rightSize, random,
                initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                        new StringGenerator(rightSize / 10),
                        new SortedIntGenerator(0, 100000),
                        new IntGenerator(20_000_000, 20_010_000)));

        final Table result = AsOfJoinHelper.asOfJoin(QueryTableJoinTest.SMALL_LEFT_CONTROL, leftTable,
                (QueryTable) rightTable.reverse(), MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                MatchPairFactory.getExpressions("RightStamp", "RightSentinel"), SortingOrder.Descending, true);
        checkAjResults(result.partitionBy("Bucket"), leftTable.partitionBy("Bucket"), rightTable.partitionBy("Bucket"),
                true, true);
    }

    @Test
    public void testAjRandomLeftIncrementalRightStatic() {
        final int maxLeftSize;
        final int maxRightSize;
        final int leftFactor;
        final int rightFactor;
        if (SHORT_TESTS) {
            maxLeftSize = 1_250;
            maxRightSize = 1_250;
            leftFactor = 5;
            rightFactor = 5;
        } else {
            maxLeftSize = 10_000;
            maxRightSize = 10_000;
            leftFactor = 10;
            rightFactor = 10;
        }
        for (int seed = 0; seed < 2; ++seed) {
            for (int leftSize = 10; leftSize <= maxLeftSize; leftSize *= leftFactor) {
                for (int rightSize = 10; rightSize <= maxRightSize; rightSize *= rightFactor) {
                    System.out.println("Seed=" + seed + ", leftSize=" + leftSize + ", rightSize=" + rightSize);
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStep, seed, leftSize, rightSize, true, false, false, false);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStep, seed, leftSize, rightSize, true, false, true, false);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStep, seed, leftSize, rightSize, true, false, false, true);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStep, seed, leftSize, rightSize, true, false, true, true);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStepShift, seed, leftSize, rightSize, true, false, false,
                                false);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStepShift, seed, leftSize, rightSize, true, false, true,
                                false);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStepShift, seed, leftSize, rightSize, true, false, false,
                                true);
                    }
                    try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                        testAjRandomIncremental(base.leftStepShift, seed, leftSize, rightSize, true, false, true, true);
                    }
                }
            }
        }
    }

    @Test
    public void testAjRandomLeftStaticRightIncremental() {
        final int tableMultiplier = 10;
        final int initialTableSize = 10;
        final int maximumTableSize = 1000;
        final int nodeMultiplier = 8;
        final int initialNodeSize = 4;
        final int maximumNodeSize = 256;
        final int seedCount = 5;

        for (int leftSize = initialTableSize; leftSize <= maximumTableSize; leftSize *= tableMultiplier) {
            for (int rightSize = initialTableSize; rightSize <= maximumTableSize; rightSize *= tableMultiplier) {
                for (int nodeSize = initialNodeSize; nodeSize <= maximumNodeSize; nodeSize *= nodeMultiplier) {
                    for (int seed = 0; seed < seedCount; ++seed) {
                        if (nodeSize / nodeMultiplier > rightSize) {
                            continue;
                        }

                        System.out.println("Seed=" + seed + ", nodeSize=" + nodeSize + ", leftSize=" + leftSize
                                + ", rightSize=" + rightSize);
                        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, false, false);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, true, false);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, false, true);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, true, true);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testAjBothIncremental() {
        final int tableMultiplier = 10;
        final int initialTableSize = 10;
        final int maximumTableSize = 100;
        final int nodeMultiplier = 8;
        final int initialNodeSize = 4;
        final int maximumNodeSize = 256;
        final int seedCount = 1;

        final long startTime = System.currentTimeMillis();
        int configurations = 0;

        for (int leftSize = initialTableSize; leftSize <= maximumTableSize; leftSize *= tableMultiplier) {
            for (int rightSize = initialTableSize; rightSize <= maximumTableSize; rightSize *= tableMultiplier) {
                for (int leftNodeSize = initialNodeSize; leftNodeSize <= maximumNodeSize; leftNodeSize *=
                        nodeMultiplier) {
                    if (leftNodeSize / nodeMultiplier > leftSize) {
                        continue;
                    }

                    for (int rightNodeSize = initialNodeSize; rightNodeSize <= maximumNodeSize; rightNodeSize *=
                            nodeMultiplier) {
                        if (rightNodeSize / nodeMultiplier > rightSize) {
                            continue;
                        }

                        for (int seed = 0; seed < seedCount; ++seed) {
                            for (final JoinIncrement joinIncrement : new JoinIncrement[] {base.leftRightStepShift,
                                    base.leftRightConcurrentStepShift}) {
                                System.out.println((System.currentTimeMillis() - startTime) + ": Seed=" + seed
                                        + ", leftNodeSize=" + leftNodeSize + ", rightNodeSize=" + rightNodeSize
                                        + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", joinIncrement="
                                        + joinIncrement);
                                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                                    testAjRandomBothIncremental(seed, leftNodeSize, rightNodeSize, leftSize, rightSize,
                                            joinIncrement, int.class);
                                }
                            }
                            configurations++;
                        }
                    }
                }
            }
        }

        System.out.println(
                "Executed " + configurations + " configurations in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    @Test
    public void testAjCharIncremental() {
        final int initialTableSize = 10;
        final int tableMultiplier;
        final int maximumTableSize;
        if (SHORT_TESTS) {
            tableMultiplier = 5;
            maximumTableSize = 250;

        } else {
            tableMultiplier = 10;
            maximumTableSize = 1000;
        }
        final int nodeMultiplier = 8;
        final int initialNodeSize = 4;
        final int maximumNodeSize = 256;
        final int seedCount = 5;

        final long startTime = System.currentTimeMillis();
        int configurations = 0;

        for (int leftSize = initialTableSize; leftSize <= maximumTableSize; leftSize *= tableMultiplier) {
            for (int rightSize = initialTableSize; rightSize <= maximumTableSize; rightSize *= tableMultiplier) {
                for (int leftNodeSize = initialNodeSize; leftNodeSize <= maximumNodeSize; leftNodeSize *=
                        nodeMultiplier) {
                    if (leftNodeSize / nodeMultiplier > leftSize) {
                        continue;
                    }

                    for (int rightNodeSize = initialNodeSize; rightNodeSize <= maximumNodeSize; rightNodeSize *=
                            nodeMultiplier) {
                        if (rightNodeSize / nodeMultiplier > rightSize) {
                            continue;
                        }

                        for (int seed = 0; seed < seedCount; ++seed) {
                            for (JoinIncrement joinIncrement : new JoinIncrement[] {base.leftRightStepShift,
                                    base.leftRightConcurrentStepShift}) {
                                System.out.println((System.currentTimeMillis() - startTime) + ": Seed=" + seed
                                        + ", leftNodeSize=" + leftNodeSize + ", rightNodeSize=" + rightNodeSize
                                        + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", joinIncrement="
                                        + joinIncrement);
                                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                                    testAjRandomBothIncremental(seed, leftNodeSize, rightNodeSize, leftSize, rightSize,
                                            joinIncrement, char.class);
                                }
                            }
                            configurations++;
                        }
                    }
                }
            }
        }

        System.out.println(
                "Executed " + configurations + " configurations in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    @Test
    public void testAjBothIncrementalOverflow() {
        final int tableMultiplier = 10;
        final int initialTableSize = 100;
        final int maximumTableSize = 1000;
        final int nodeMultiplier = 8;
        final int initialNodeSize = 256;
        final int maximumNodeSize = 256;
        final int seedCount = 5;

        final long startTime = System.currentTimeMillis();
        int configurations = 0;

        for (int leftSize = initialTableSize; leftSize <= maximumTableSize; leftSize *= tableMultiplier) {
            for (int rightSize = initialTableSize; rightSize <= maximumTableSize; rightSize *= tableMultiplier) {
                for (int leftNodeSize = initialNodeSize; leftNodeSize <= maximumNodeSize; leftNodeSize *=
                        nodeMultiplier) {
                    if (leftNodeSize / nodeMultiplier > leftSize) {
                        continue;
                    }

                    for (int rightNodeSize = initialNodeSize; rightNodeSize <= maximumNodeSize; rightNodeSize *=
                            nodeMultiplier) {
                        if (rightNodeSize / nodeMultiplier > rightSize) {
                            continue;
                        }

                        for (int seed = 0; seed < seedCount; ++seed) {
                            for (JoinIncrement joinIncrement : new JoinIncrement[] {base.leftRightStepShift,
                                    base.leftRightConcurrentStepShift}) {
                                System.out.println((System.currentTimeMillis() - startTime) + ": Seed=" + seed
                                        + ", leftNodeSize=" + leftNodeSize + ", rightNodeSize=" + rightNodeSize
                                        + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", joinIncrement="
                                        + joinIncrement);
                                final int fRightNodeSize = rightNodeSize;
                                final int fLeftNodeSize = leftNodeSize;
                                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                                    testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize,
                                            rightSize, joinIncrement, true, true, false, false, true, false,
                                            new JoinControl() {
                                                @Override
                                                int initialBuildSize() {
                                                    return 1 << 2;
                                                }

                                                @Override
                                                double getMaximumLoadFactor() {
                                                    return 0.75;
                                                }

                                                @Override
                                                double getTargetLoadFactor() {
                                                    return 19.0;
                                                }

                                                @Override
                                                int rightSsaNodeSize() {
                                                    return fRightNodeSize;
                                                }

                                                @Override
                                                int leftSsaNodeSize() {
                                                    return fLeftNodeSize;
                                                }
                                            }, int.class);
                                }
                            }
                            configurations++;
                        }
                    }
                }
            }
        }

        System.out.println(
                "Executed " + configurations + " configurations in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private void testAjRandomLeftStaticRightIncremental(int seed, int nodeSize, int leftSize, int rightSize,
            boolean leftIndexed, boolean rightIndexed) {
        testAjRandomIncrementalWithInitial(seed, -1, nodeSize, leftSize, rightSize, base.rightStepShift, false, true,
                false, true, true, true, leftIndexed, rightIndexed, getJoinControlWithNodeSize(-1, nodeSize),
                int.class);
    }

    private void testAjRandomBothIncremental(int seed, int leftNodeSize, int rightNodeSize, int leftSize, int rightSize,
            JoinIncrement joinIncrement, Class<?> stampType) {
        // zero keys
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement, true,
                true, false, true, false, false, stampType);
        // buckets
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement, true,
                true, false, false, true, false, stampType);
    }

    @SuppressWarnings("SameParameterValue")
    private void testAjRandomIncrementalWithInitial(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize, JoinIncrement joinIncrement, boolean leftRefreshing, boolean rightRefreshing,
            boolean initialOnly, boolean withZeroKeys, boolean withBuckets, boolean withReverse, Class<?> stampType) {
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement,
                leftRefreshing, rightRefreshing, initialOnly, withZeroKeys, withBuckets, withReverse,
                getJoinControlWithNodeSize(leftNodeSize, rightNodeSize), stampType);
    }

    private void testAjRandomIncrementalWithInitial(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize, JoinIncrement joinIncrement, boolean leftRefreshing, boolean rightRefreshing,
            boolean initialOnly, boolean withZeroKeys, boolean withBuckets, boolean withReverse,
            JoinControl control, Class<?> stampType) {
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement,
                leftRefreshing, rightRefreshing, initialOnly, withZeroKeys, withBuckets, withReverse, false, false,
                getJoinControlWithNodeSize(leftNodeSize, rightNodeSize), stampType);
    }

    @Test
    public void testAjBothIncrementalIndexed() {
        final int tableMultiplier = 10;
        final int initialTableSize = 10;
        final int maximumTableSize = 100;
        final int nodeMultiplier = 8;
        final int initialNodeSize = 4;
        final int maximumNodeSize = 256;
        final int seedCount = 1;

        final long startTime = System.currentTimeMillis();
        int configurations = 0;

        for (int leftSize = initialTableSize; leftSize <= maximumTableSize; leftSize *= tableMultiplier) {
            for (int rightSize = initialTableSize; rightSize <= maximumTableSize; rightSize *= tableMultiplier) {
                for (int leftNodeSize = initialNodeSize; leftNodeSize <= maximumNodeSize; leftNodeSize *=
                        nodeMultiplier) {
                    if (leftNodeSize / nodeMultiplier > leftSize) {
                        continue;
                    }

                    for (int rightNodeSize = initialNodeSize; rightNodeSize <= maximumNodeSize; rightNodeSize *=
                            nodeMultiplier) {
                        if (rightNodeSize / nodeMultiplier > rightSize) {
                            continue;
                        }

                        for (int seed = 0; seed < seedCount; ++seed) {
                            for (final JoinIncrement joinIncrement : new JoinIncrement[] {base.leftRightStepShift,
                                    base.leftRightConcurrentStepShift}) {
                                System.out.println((System.currentTimeMillis() - startTime) + ": Seed=" + seed
                                        + ", leftNodeSize=" + leftNodeSize + ", rightNodeSize=" + rightNodeSize
                                        + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", joinIncrement="
                                        + joinIncrement);
                                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                                    testAjRandomBothIncrementalIndexed(seed, leftNodeSize, rightNodeSize, leftSize,
                                            rightSize,
                                            joinIncrement, int.class);
                                    testAjRandomBothIncrementalLeftIndexed(seed, leftNodeSize, rightNodeSize, leftSize,
                                            rightSize,
                                            joinIncrement, int.class);
                                    testAjRandomBothIncrementalRightIndexed(seed, leftNodeSize, rightNodeSize, leftSize,
                                            rightSize,
                                            joinIncrement, int.class);
                                }
                            }
                            configurations++;
                        }
                    }
                }
            }
        }

        System.out.println(
                "Executed " + configurations + " configurations in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private void testAjRandomBothIncrementalIndexed(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize,
            JoinIncrement joinIncrement, Class<?> stampType) {
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement, true,
                true, false, false, true, false, true, true,
                getJoinControlWithNodeSize(leftNodeSize, rightNodeSize), stampType);
    }

    private void testAjRandomBothIncrementalLeftIndexed(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize,
            JoinIncrement joinIncrement, Class<?> stampType) {
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement, true,
                true, false, false, true, false, true, false,
                getJoinControlWithNodeSize(leftNodeSize, rightNodeSize), stampType);
    }

    private void testAjRandomBothIncrementalRightIndexed(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize,
            JoinIncrement joinIncrement, Class<?> stampType) {
        testAjRandomIncrementalWithInitial(seed, leftNodeSize, rightNodeSize, leftSize, rightSize, joinIncrement, true,
                true, false, false, true, false, false, true,
                getJoinControlWithNodeSize(leftNodeSize, rightNodeSize), stampType);
    }

    @SuppressWarnings("SameParameterValue")
    private void testAjRandomIncrementalWithInitial(int seed, int leftNodeSize, int rightNodeSize, int leftSize,
            int rightSize, JoinIncrement joinIncrement, boolean leftRefreshing, boolean rightRefreshing,
            boolean initialOnly, boolean withZeroKeys, boolean withBuckets, boolean withReverse, boolean leftIndexing,
            boolean rightIndexing, JoinControl control,
            Class<?> stampType) {
        final Random random = new Random(seed);
        final int maxSteps = 10;

        final ColumnInfo<?, ?>[] leftColumnInfo;
        final String[] smallSet = {"Alpha", "Bravo", "Charlie", "Delta", "Echo"};
        final Set<String> set1;
        final Set<String> set2;
        final int smallestSize = Math.min(leftSize, rightSize);
        if (smallSet.length > smallestSize / 4) {
            set1 = Arrays.stream(smallSet).filter(x -> random.nextDouble() < 0.75).collect(Collectors.toSet());
            set2 = Arrays.stream(smallSet).filter(x -> random.nextDouble() < 0.75).collect(Collectors.toSet());
        } else {
            set1 = IntStream.range(0, smallestSize * 2).filter(x -> random.nextDouble() < 0.75).mapToObj(x -> "B" + x)
                    .collect(Collectors.toSet());
            set2 = IntStream.range(0, smallestSize * 2).filter(x -> random.nextDouble() < 0.75).mapToObj(x -> "B" + x)
                    .collect(Collectors.toSet());
        }

        final TestDataGenerator<?, ?> leftStampGenerator;
        final TestDataGenerator<?, ?> rightStampGenerator;
        final boolean sortRight;

        if (stampType == int.class) {
            leftStampGenerator = new IntGenerator(0, 10000);
            rightStampGenerator = new IntGenerator(0, 10000);
            sortRight = false;
        } else if (stampType == char.class) {
            leftStampGenerator = new CharGenerator('a', 'z', 0.1);
            rightStampGenerator = new CharGenerator('a', 'z', 0.1);
            sortRight = true;
        } else {
            throw new IllegalArgumentException();
        }

        final QueryTable leftTable = getTable(leftRefreshing, leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"Truthiness", "Bucket", "LeftStamp", "LeftSentinel"},
                        new BooleanGenerator(),
                        new SetGenerator<>(String.class, set1),
                        leftStampGenerator,
                        new IntGenerator(10_000_000, 10_010_000)));
        if (leftIndexing) {
            DataIndexer.getOrCreateDataIndex(leftTable, "Bucket");
        }
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightRefreshing, rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"Truthiness", "Bucket", "RightStamp", "RightSentinel"},
                        new BooleanGenerator(),
                        new SetGenerator<>(String.class, set2),
                        rightStampGenerator,
                        new IntGenerator(20_000_000, 20_010_000)));

        final QueryTable rightSorted = sortRight ? (QueryTable) rightTable.sort("RightStamp") : rightTable;
        if (rightIndexing) {
            // Indexing doesn't currently survive sorting.
            DataIndexer.getOrCreateDataIndex(rightSorted, "Bucket");
        }
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left: ");
            TableTools.showWithRowSet(leftTable);
            System.out.println("Right: ");
            TableTools.showWithRowSet(rightTable, 20);
        }

        // we compare our initial values to the static case; which we have a separate test for. This is meant to give
        // us some confidence in our initial algorithm, whcih we then use to compare the incrmental results.
        if (withZeroKeys) {
            doInitialAjComparison(leftTable, rightSorted, "LeftStamp>=RightStamp", false, false, control);
            doInitialAjComparison(leftTable, rightSorted, "LeftStamp>RightStamp", false, true, control);
            if (withReverse) {
                doInitialAjComparison(leftTable, rightSorted, "LeftStamp<=RightStamp", true, false, control);
                doInitialAjComparison(leftTable, rightSorted, "LeftStamp<RightStamp", true, true, control);
            }
        }
        if (withBuckets) {
            doInitialAjComparison(leftTable, rightSorted, "Bucket,LeftStamp>=RightStamp", false, false, control);
            doInitialAjComparison(leftTable, rightSorted, "Bucket,LeftStamp>RightStamp", false, true, control);
            if (withReverse) {
                doInitialAjComparison(leftTable, rightSorted, "Bucket,LeftStamp<=RightStamp", true, false, control);
                doInitialAjComparison(leftTable, rightSorted, "Bucket,LeftStamp<RightStamp", true, true, control);
            }
        }

        if (initialOnly) {
            return;
        }

        final QueryTable rightReversed = (QueryTable) rightSorted.reverse();
        if (rightIndexing) {
            // Indexing doesn't currently survive reversal.
            DataIndexer.getOrCreateDataIndex(rightReversed, "Bucket");
        }

        final EvalNuggetInterface[] en = Stream.concat(Stream.concat(!withZeroKeys ? Stream.empty()
                : Stream.concat(
                        Stream.of(
                                // aj
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightSorted,
                                        MatchPairFactory.getExpressions("LeftStamp=RightStamp"),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Ascending, false)),
                                // > aj
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightSorted,
                                        oldStyleArray(AsOfJoinMatchFactory.getAjExpressions("LeftStamp>RightStamp")),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Ascending, true))),
                        !withReverse ? Stream.empty()
                                : Stream.of(
                                        // raj
                                        EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightReversed,
                                                MatchPairFactory.getExpressions("LeftStamp=RightStamp"),
                                                MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                                SortingOrder.Descending, false)),
                                        // < raj
                                        EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightReversed,
                                                oldStyleArray(
                                                        AsOfJoinMatchFactory.getRajExpressions("LeftStamp<RightStamp")),
                                                MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                                SortingOrder.Descending, true)))),
                !withBuckets ? Stream.empty()
                        : Stream.of(
                                // aj, with a bucket
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightSorted,
                                        MatchPairFactory.getExpressions("Truthiness", "Bucket", "LeftStamp=RightStamp"),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Ascending, false)),
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightSorted,
                                        MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Ascending, false)),
                                // > aj, with a bucket
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightSorted,
                                        oldStyleArray(AsOfJoinMatchFactory.getAjExpressions("Bucket",
                                                "LeftStamp>RightStamp")),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Ascending, true)))),
                !withBuckets || !withReverse ? Stream.empty()
                        : Stream.of(
                                // raj, with a bucket
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightReversed,
                                        MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Descending, false)),
                                // < raj, with a bucket
                                EvalNugget.from(() -> AsOfJoinHelper.asOfJoin(control, leftTable, rightReversed,
                                        oldStyleArray(AsOfJoinMatchFactory.getRajExpressions("Bucket",
                                                "LeftStamp<RightStamp")),
                                        MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                                        SortingOrder.Descending, true))))
                .toArray(EvalNuggetInterface[]::new);

        for (int step = 0; step < maxSteps; step++) {
            System.out.println("Step = " + step + (leftNodeSize > 0 ? ", leftNodeSize=" + leftNodeSize : "")
                    + ", rightNodeSize=" + rightNodeSize + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                    + ", seed = " + seed + ", joinIncrement=" + joinIncrement);
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Left Table:" + leftTable.size());
                TableTools.showWithRowSet(leftTable, 100);
                System.out.println("Left Table (sorted):");
                show(leftTable.update("TrackingWritableRowSet=k").sort("LeftStamp")
                        .moveColumnsUp("TrackingWritableRowSet"), 100);
                System.out.println("Right Table:" + rightTable.size());
                TableTools.showWithRowSet(rightTable, 100);
                System.out.println("Right Table Sorted:" + rightSorted.size());
                show(rightTable.update("TrackingWritableRowSet=k").sort("RightStamp")
                        .moveColumnsUp("TrackingWritableRowSet"), 100);
                if (withReverse) {
                    System.out.println("Right Table (reversed):");
                    TableTools.showWithRowSet(rightReversed, 100);
                }
            }
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    private void doInitialAjComparison(QueryTable leftTable, QueryTable rightTable, String columnsToMatch,
            boolean reverse, boolean disallowMatch, JoinControl control) {
        final Table staticResult =
                reverse ? leftTable.silent().raj(rightTable.silent(), columnsToMatch, "RightSentinel")
                        : leftTable.silent().aj(rightTable.silent(), columnsToMatch, "RightSentinel");
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Static: ");
            TableTools.showWithRowSet(staticResult);
        }

        AsOfJoinResult r = reverse
                ? AsOfJoinMatchFactory.getRajExpressions(splitToCollection(columnsToMatch))
                : AsOfJoinMatchFactory.getAjExpressions(splitToCollection(columnsToMatch));

        MatchPair[] oldStyleMatches = Stream.concat(
                r.matches.stream().map(MatchPair::of),
                Stream.of(oldStyle(r)))
                .toArray(MatchPair[]::new);

        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table refreshingResult = AsOfJoinHelper.asOfJoin(control, leftTable,
                    reverse ? ((QueryTable) rightTable.reverse()) : rightTable, oldStyleMatches,
                    MatchPairFactory.getExpressions("RightStamp", "RightSentinel"),
                    reverse ? SortingOrder.Descending : SortingOrder.Ascending, disallowMatch);

            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Refreshing: ");
                TableTools.showWithRowSet(refreshingResult);
            }

            assertTableEquals(staticResult, refreshingResult);
        }
    }

    private static MatchPair oldStyle(AsOfJoinResult result) {
        return new MatchPair(result.joinMatch.leftColumn().name(), result.joinMatch.rightColumn().name());
    }

    private static MatchPair[] oldStyleArray(AsOfJoinResult result) {
        return new MatchPair[] {oldStyle(result)};
    }

    @NotNull
    private JoinControl getJoinControlWithNodeSize(int leftNodeSize, int rightNodeSize) {
        return new JoinControl() {
            @Override
            int rightSsaNodeSize() {
                return rightNodeSize;
            }

            @Override
            int leftSsaNodeSize() {
                return leftNodeSize;
            }

            @Override
            public int rightChunkSize() {
                return rightSsaNodeSize();
            }

            @Override
            public int leftChunkSize() {
                return leftSsaNodeSize();
            }
        };
    }


    private void testAjRandomIncremental(JoinIncrement joinIncrement, int seed, int leftSize, int rightSize,
            boolean leftRefreshing, boolean rightRefreshing, boolean leftIndexed, boolean rightIndexed) {
        final Random random = new Random(seed);
        final int maxSteps = 10;

        final ColumnInfo<?, ?>[] leftColumnInfo;
        final QueryTable leftTable = getTable(leftRefreshing, leftSize, random,
                leftColumnInfo = initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                        new SetGenerator<>("Alpha", "Bravo", "Charlie", "Delta"),
                        new IntGenerator(0, 10000),
                        new IntGenerator(10_000_000, 10_010_000)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(rightRefreshing, rightSize, random,
                rightColumnInfo = initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                        new SetGenerator<>("Alpha", "Bravo", "Charlie", "Echo"),
                        new SortedIntGenerator(0, 10000),
                        new IntGenerator(20_000_000, 20_010_000)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.aj(rightTable, "LeftStamp>=RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.aj(rightTable, "LeftStamp>RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.raj(rightTable, "LeftStamp<=RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.raj(rightTable, "LeftStamp<RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.aj(rightTable, "Bucket,LeftStamp>=RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.aj(rightTable, "Bucket,LeftStamp>RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.raj(rightTable, "Bucket,LeftStamp<=RightStamp", "RightSentinel");
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return leftTable.raj(rightTable, "Bucket,LeftStamp<RightStamp", "RightSentinel");
                    }
                }
        };

        for (int step = 0; step < maxSteps; step++) {
            System.out.println("Step = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", seed = "
                    + seed + ", joinIncrement=" + joinIncrement);
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Left Table:" + leftTable.size());
                TableTools.showWithRowSet(leftTable, 100);
                System.out.println("Right Table:" + rightTable.size());
                TableTools.showWithRowSet(rightTable, 100);
            }
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    @Test
    public void testAjRandomLeftIncrementalRightStaticOverflow() {
        final JoinIncrement joinIncrement = base.leftStepShift;
        final int seed = 0;
        final Random random = new Random(seed);
        final int maxSteps = 3;

        final ColumnInfo<?, ?>[] leftColumnInfo;
        final int leftSize = 32000;
        final int rightSize = 32000;
        final QueryTable leftTable = getTable(true, 100000, random,
                leftColumnInfo = initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                        new StringGenerator(leftSize),
                        new IntGenerator(0, 100000),
                        new IntGenerator(10_000_000, 10_010_000)));
        final ColumnInfo<?, ?>[] rightColumnInfo;
        final QueryTable rightTable = getTable(false, 100000, random,
                rightColumnInfo = initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                        new StringGenerator(leftSize),
                        new SortedIntGenerator(0, 100000),
                        new IntGenerator(20_000_000, 20_010_000)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return AsOfJoinHelper.asOfJoin(QueryTableJoinTest.SMALL_RIGHT_CONTROL, leftTable, rightTable,
                                MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                                MatchPairFactory.getExpressions("RightSentinel"), SortingOrder.Ascending, false);
                    }
                },
        };

        for (int step = 0; step < maxSteps; step++) {
            System.out.println("Step = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize + ", seed = "
                    + seed + ", joinIncrement=" + joinIncrement);
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Left Table:" + leftTable.size());
                TableTools.showWithRowSet(leftTable, 100);
                System.out.println("Right Table:" + rightTable.size());
                TableTools.showWithRowSet(rightTable, 100);
            }
            joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en, random);
        }
    }

    private void checkAjResults(
            PartitionedTable bucketResults, PartitionedTable leftBucket, PartitionedTable rightBucket,
            boolean reverse, boolean noexact) {
        final Table correlated = bucketResults.table()
                .naturalJoin(leftBucket.table(), "Bucket", "Left=" + leftBucket.constituentColumnName())
                .naturalJoin(rightBucket.table(), "Bucket", "Right=" + rightBucket.constituentColumnName());
        try (final CloseableIterator<Table> results =
                correlated.objectColumnIterator(bucketResults.constituentColumnName());
                final CloseableIterator<Table> lefts = correlated.objectColumnIterator("Left");
                final CloseableIterator<Table> rights = correlated.objectColumnIterator("Right")) {
            while (results.hasNext()) {
                checkAjResult(lefts.next(), rights.next(), results.next(), reverse, noexact);
            }
        }
    }

    private void checkAjResult(Table leftTable, Table rightTable, Table result, boolean reverse, boolean noexact) {
        leftTable = leftTable.withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true));

        final TIntArrayList expectedStamp = new TIntArrayList();
        final TIntArrayList expectedSentinel = new TIntArrayList();

        final int[] leftStampArray = ColumnVectors.ofInt(leftTable, "LeftStamp").toArray();
        final int[] rightStampArray = rightTable == null
                ? ArrayTypeUtils.EMPTY_INT_ARRAY
                : ColumnVectors.ofInt(rightTable, "RightStamp").toArray();
        final int[] rightSentinelArray = rightTable == null
                ? ArrayTypeUtils.EMPTY_INT_ARRAY
                : ColumnVectors.ofInt(rightTable, "RightSentinel").toArray();

        for (final int leftStamp : leftStampArray) {
            final int rightPosition = Arrays.binarySearch(rightStampArray, leftStamp);
            int positionToUse;
            if (!reverse && rightPosition == -1) {
                expectedStamp.add(NULL_INT);
                expectedSentinel.add(NULL_INT);
            } else if (reverse && rightPosition == -rightStampArray.length - 1) {
                expectedStamp.add(NULL_INT);
                expectedSentinel.add(NULL_INT);
            } else {
                if (rightPosition >= 0) {
                    if (noexact) {
                        positionToUse = rightPosition;
                        if (reverse) {
                            while (positionToUse < rightStampArray.length
                                    && rightStampArray[positionToUse] == leftStamp) {
                                positionToUse++;
                            }
                            if (positionToUse == rightStampArray.length) {
                                expectedStamp.add(NULL_INT);
                                expectedSentinel.add(NULL_INT);
                                continue;
                            }
                        } else {
                            while (positionToUse >= 0 && rightStampArray[positionToUse] == leftStamp) {
                                positionToUse--;
                            }
                            if (positionToUse < 0) {
                                expectedStamp.add(NULL_INT);
                                expectedSentinel.add(NULL_INT);
                                continue;
                            }
                        }
                    } else {
                        positionToUse = rightPosition;
                        if (reverse) {
                            while (positionToUse > 0
                                    && rightStampArray[positionToUse] == rightStampArray[positionToUse - 1]) {
                                positionToUse--;
                            }
                        } else {
                            while (positionToUse < rightStampArray.length - 1
                                    && rightStampArray[positionToUse] == rightStampArray[positionToUse + 1]) {
                                positionToUse++;
                            }
                        }
                    }
                } else {
                    if (reverse) {
                        positionToUse = -rightPosition - 1;
                    } else {
                        positionToUse = -rightPosition - 2;
                    }
                }
                expectedStamp.add(rightStampArray[positionToUse]);
                expectedSentinel.add(rightSentinelArray[positionToUse]);
            }
        }

        QueryScope.addParam("__rightStampExpected", expectedStamp);
        QueryScope.addParam("__rightSentinelExpected", expectedSentinel);
        final Table expected = leftTable.update("RightStamp=__rightStampExpected.get(i)",
                "RightSentinel=__rightSentinelExpected.get(i)");

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Left:");
            TableTools.showWithRowSet(leftTable);
            System.out.println("Right:");
            if (rightTable != null) {
                TableTools.showWithRowSet(rightTable);
            } else {
                System.out.println("null");
            }
            System.out.println("Result:");
            TableTools.showWithRowSet(result, 150, 170);
            System.out.println("Expected:");
            TableTools.showWithRowSet(expected, 150, 170);
        }

        QueryScope.addParam("__rightStampExpected", null);
        QueryScope.addParam("__rightSentinelExpected", null);

        assertTableEquals(expected, result);
    }

    @Test
    public void testIds5293() {
        final Random random = new Random(0);
        final int size = 100;
        final int scale = 1000;
        final long timeOffset = Clock.system().currentTimeNanos();
        final String[] columnNames = {"MyBoolean", "MyChar"};

        QueryScope.addParam("random", random);
        QueryScope.addParam("scale", scale);
        QueryScope.addParam("timeOffset", timeOffset);

        try {

            final Table staticOne = emptyTable(size)
                    .update("Timestamp= i%23 == 0 ? null : DateTimeUtils.epochNanosToInstant(timeOffset + (long)(scale*(Math.random()*2-0.1))*100_000_000L)",
                            "OtherTimestamp= i%24 == 0 ? null : DateTimeUtils.epochNanosToInstant(timeOffset + (long)(scale*(Math.random()*2-0.05))*100_000_000L)",
                            "MyString=(i%11==0? null : `a`+(int)(scale*(Math.random()*2-1)))",
                            "MyInt=(i%12==0 ? null : (int)(scale*(Math.random()*2-1)))",
                            "MyLong=(i%13==0 ? null : (long)(scale*(Math.random()*2-1)))",
                            "MyFloat=(float)(i%14==0 ? null : i%10==0 ? 1.0F/0.0F: i%5==0 ? -1.0F/0.0F : (float) scale*(Math.random()*2-1))",
                            "MyDouble=(double)(i%16==0 ? null : i%10==0 ? 1.0D/0.0D: i%5==0 ? -1.0D/0.0D : (double) scale*(Math.random()*2-1))",
                            "MyBoolean = (i%17==0 ? null : (int)(10*Math.random())%2==0)",
                            "MyChar = (i%18==0 ? null : new Character((char) (((26*Math.random())%26)+97)) )",
                            "MyShort=(short)(i%19==0 ? null : (int)(scale*(Math.random()*2-1)))",
                            "MyByte=(Byte)(i%19==0 ? null : new Byte( Integer.toString((int)(Byte.MAX_VALUE*(Math.random()*2-1)))))",
                            "MyBigDecimal=(i%21==0 ? null : new java.math.BigDecimal(scale*(Math.random()*2-1)))",
                            "MyBigInteger=(i%22==0 ? null : new java.math.BigInteger(Integer.toString((int)(scale*(Math.random()*2-1)))))");

            final Table staticTwo = emptyTable(size)
                    .update("Timestamp= i%23 == 0 ? null : DateTimeUtils.epochNanosToInstant(timeOffset + (long)(scale*(Math.random()*2-0.1))*100_000_000L)",
                            "OtherTimestamp= i%24 == 0 ? null : DateTimeUtils.epochNanosToInstant(timeOffset + (long)(scale*(Math.random()*2-0.05))*100_000_000L)",
                            "MyString=(i%11==0? null : `a`+(int)(scale*(Math.random()*2-1)))",
                            "MyInt=(i%12==0 ? null : (int)(scale*(Math.random()*2-1)))",
                            "MyLong=(i%13==0 ? null : (long)(scale*(Math.random()*2-1)))",
                            "MyFloat=(float)(i%14==0 ? null : i%10==0 ? 1.0F/0.0F: i%5==0 ? -1.0F/0.0F : (float) scale*(Math.random()*2-1))",
                            "MyDouble=(double)(i%16==0 ? null : i%10==0 ? 1.0D/0.0D: i%5==0 ? -1.0D/0.0D : (double) scale*(Math.random()*2-1))",
                            "MyBoolean = (i%17==0 ? null : (int)(10*Math.random())%2==0)",
                            "MyChar = (i%18==0 ? null : new Character((char) (((26*Math.random())%26)+97)) )",
                            "MyShort=(short)(i%19==0 ? null : (int)(scale*(Math.random()*2-1)))",
                            "MyByte=(Byte)(i%19==0 ? null : new Byte( Integer.toString((int)(Byte.MAX_VALUE*(Math.random()*2-1)))))",
                            "MyBigDecimal=(i%21==0 ? null : new java.math.BigDecimal(scale*(Math.random()*2-1)))",
                            "MyBigInteger=(i%22==0 ? null : new java.math.BigInteger(Integer.toString((int)(scale*(Math.random()*2-1)))))");

            final Table static2ts = staticTwo.sort("Timestamp");

            for (final String column : columnNames) {
                TableTools.showWithRowSet(static2ts);
                final Table resultZk = staticOne.aj(staticTwo.sort(column), column,
                        "Extra=OtherTimestamp,Extra2=MyLong,Check=" + column);
                TableTools.showWithRowSet(resultZk);
                final Table resultTs = staticOne.aj(static2ts, column + ",Timestamp",
                        "Extra=OtherTimestamp,Extra2=MyLong,Check=" + column);
                TableTools.showWithRowSet(resultTs);
            }
        } finally {
            QueryScope.addParam("random", null);
            QueryScope.addParam("scale", null);
            QueryScope.addParam("timeOffset", null);
        }
    }

    @Test
    public void testIds6898() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final JoinIncrement joinIncrement = base.leftRightStep;
            final int seed = 0;
            final Random random = new Random(seed);
            final int maxSteps = 5;

            final ColumnInfo<?, ?>[] leftColumnInfo;
            final int leftSize = 32000;
            final int rightSize = 32000;
            final QueryTable leftTable = getTable(true, 100000, random,
                    leftColumnInfo = initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                            new StringGenerator(leftSize),
                            new IntGenerator(0, 100000),
                            new IntGenerator(10_000_000, 10_010_000)));
            final ColumnInfo<?, ?>[] rightColumnInfo;
            final QueryTable rightTable = getTable(true, 100000, random,
                    rightColumnInfo = initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                            new StringGenerator(leftSize),
                            new SortedIntGenerator(0, 100000),
                            new IntGenerator(20_000_000, 20_010_000)));

            final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                    new EvalNugget() {
                        @Override
                        protected Table e() {
                            return AsOfJoinHelper.asOfJoin(QueryTableJoinTest.SMALL_RIGHT_CONTROL,
                                    (QueryTable) leftTable.sort("LeftStamp"), rightTable,
                                    MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                                    MatchPairFactory.getExpressions("RightSentinel"), SortingOrder.Ascending, false);
                        }
                    },
            };

            for (int step = 0; step < maxSteps; step++) {
                System.out.println("Step = " + step + ", leftSize=" + leftSize + ", rightSize=" + rightSize
                        + ", seed = " + seed + ", joinIncrement=" + joinIncrement);
                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("Left Table:" + leftTable.size());
                    TableTools.showWithRowSet(leftTable, 100);
                    System.out.println("Right Table:" + rightTable.size());
                    TableTools.showWithRowSet(rightTable, 100);
                }
                joinIncrement.step(leftSize, rightSize, leftTable, rightTable, leftColumnInfo, rightColumnInfo, en,
                        random);
            }
        }
    }

    /**
     * Reproduction of the error from DHC issue #3080.
     */
    @Test
    public void testDHC3080() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final int seed = 0;
            final Random random = new Random(seed);

            final int leftSize = 32000;

            // fairly small LHS will speed up detection of the error but will not affect correctness
            final QueryTable leftTable = getTable(true, 1000, random,
                    initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                            new StringGenerator(leftSize),
                            new IntGenerator(0, 100000),
                            new IntGenerator(10_000_000, 10_010_000)));

            // need RHS with unique bucket count > rehash threshold of 4096
            final QueryTable rightTable = getTable(true, 32000, random,
                    initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                            new StringGenerator(leftSize),
                            new SortedIntGenerator(0, 100000),
                            new IntGenerator(20_000_000, 20_010_000)));

            final Table result = AsOfJoinHelper.asOfJoin(QueryTableJoinTest.SMALL_LEFT_CONTROL, leftTable,
                    (QueryTable) rightTable.reverse(),
                    MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                    MatchPairFactory.getExpressions("RightStamp", "RightSentinel"), SortingOrder.Descending, true);

            // force compare results of the bucketed output, we cannot compare static to incremental as in other tests
            // because static will experience the same error when performing `rehashInternalFull()`
            checkAjResults(result.partitionBy("Bucket"), leftTable.partitionBy("Bucket"),
                    rightTable.partitionBy("Bucket"),
                    true, true);
        }
    }

    /**
     * Reproduction of the error from DHC issue #4700. The root cause is that the cookies were not being migrated
     * properly during a partial rehash. This repro creates small initial tables, then generates large updates that
     * force a partial rehash and migration.
     */
    @Test
    public void testDHC4700() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final int seed = 0;
            final Random random = new Random(seed);

            final ColumnInfo<?, ?>[] leftColumnInfo;
            final ColumnInfo<?, ?>[] rightColumnInfo;

            // Small initial tables.
            final int leftSize = 2;
            final int rightSize = 2;
            final QueryTable leftTable = getTable(true, leftSize, random,
                    leftColumnInfo = initColumnInfos(new String[] {"Bucket", "LeftStamp", "LeftSentinel"},
                            new StringGenerator(100_000),
                            new IntGenerator(0, 100_000),
                            new IntGenerator(10_000_000, 10_010_000)));
            final QueryTable rightTable = getTable(true, rightSize, random,
                    rightColumnInfo = initColumnInfos(new String[] {"Bucket", "RightStamp", "RightSentinel"},
                            new StringGenerator(100_000),
                            new SortedIntGenerator(0, 100_000),
                            new IntGenerator(20_000_000, 20_010_000)));

            final Table result = AsOfJoinHelper.asOfJoin(QueryTableJoinTest.SMALL_LEFT_CONTROL, leftTable,
                    (QueryTable) rightTable.reverse(),
                    MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                    MatchPairFactory.getExpressions("RightStamp", "RightSentinel"), SortingOrder.Descending, true);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                // Large updates to force a partial rehash.
                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, 100_000,
                        random, leftTable, leftColumnInfo);
                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, 100_000,
                        random, rightTable, rightColumnInfo);
            });

            // Compare results of the bucketed output.
            checkAjResults(result.partitionBy("Bucket"), leftTable.partitionBy("Bucket"),
                    rightTable.partitionBy("Bucket"),
                    true, true);
        }
    }
}
