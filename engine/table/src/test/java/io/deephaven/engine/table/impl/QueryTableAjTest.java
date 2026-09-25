//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.table.impl.AsOfJoinMatchFactory.AsOfJoinResult;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.testing.Asserts;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalHashedAsOfJoinStateManager;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.exceptions.MismatchedJoinKeyException;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.testutil.QueryTableTestBase.JoinIncrement;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.sources.TestColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import static org.junit.Assert.*;

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
            fail("Expected conflicting column exception!");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "Conflicting column names [Bucket]");
        }
    }

    @Test
    public void testAjMismatchedKeyTypes() {
        final Table left = TableTools.newTable(intCol("Key", 1), intCol("LeftStamp", 5));
        final Table right = TableTools.newTable(longCol("Key", 1L), intCol("RightStamp", 1), intCol("Sentinel", 1));

        try {
            left.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel");
            fail("Expected mismatched key type exception!");
        } catch (MismatchedJoinKeyException e) {
            assertEquals("Mismatched join types, Key=Key: int != long", e.getMessage());
        }

        final Table instantLeft = TableTools.newTable(instantCol("Key", DateTimeUtils.epochNanosToInstant(1)),
                intCol("LeftStamp", 5));
        try {
            instantLeft.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel");
            fail("Expected mismatched key type exception!");
        } catch (MismatchedJoinKeyException e) {
            assertEquals("Mismatched join types, Key=Key: class java.time.Instant != long", e.getMessage());
        }
    }

    @Test
    public void testAjEmptyMatch() {
        final Table left = TableTools.newTable(intCol("LeftStamp", 5));
        final Table right = TableTools.newTable(intCol("RightStamp", 1), intCol("Sentinel", 1));

        for (final String match : new String[] {"", " ", " , "}) {
            final IllegalArgumentException ajEmpty =
                    assertThrows(IllegalArgumentException.class, () -> left.aj(right, match, "Sentinel"));
            assertEquals("aj() requires at least one column to match!", ajEmpty.getMessage());
            final IllegalArgumentException rajEmpty =
                    assertThrows(IllegalArgumentException.class, () -> left.raj(right, match));
            assertEquals("raj() requires at least one column to match!", rajEmpty.getMessage());
        }
    }

    @Test
    public void testAjMismatchedTypesWithEmptyLeft() {
        final Table right = TableTools.newTable(longCol("Key", 1L), longCol("RightStamp", 1L), intCol("Sentinel", 1));
        final Table emptyIntKeyLeft = TableTools.newTable(intCol("Key"), longCol("LeftStamp"));
        final MismatchedJoinKeyException keyMismatch = assertThrows(MismatchedJoinKeyException.class,
                () -> emptyIntKeyLeft.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel"));
        assertEquals("Mismatched join types, Key=Key: int != long", keyMismatch.getMessage());

        final Table emptyIntStampLeft = TableTools.newTable(longCol("Key"), intCol("LeftStamp"));
        final MismatchedJoinKeyException stampMismatch = assertThrows(MismatchedJoinKeyException.class,
                () -> emptyIntStampLeft.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel"));
        assertEquals("Can not aj() with different stamp types: left=int, right=long", stampMismatch.getMessage());
    }

    @Test
    public void testAjMismatchedStampTypes() {
        final Table left = TableTools.newTable(instantCol("LeftStamp", DateTimeUtils.epochNanosToInstant(5)));
        final Table right = TableTools.newTable(longCol("RightStamp", 1L), intCol("Sentinel", 1));

        try {
            left.aj(right, "LeftStamp>=RightStamp", "Sentinel");
            fail("Expected mismatched stamp type exception!");
        } catch (MismatchedJoinKeyException e) {
            assertEquals("Can not aj() with different stamp types: left=class java.time.Instant, right=long",
                    e.getMessage());
        }
    }

    @Test
    public void testRajMismatchedStampTypes() {
        final Table left = TableTools.newTable(instantCol("LeftStamp", DateTimeUtils.epochNanosToInstant(5)));
        final Table right = TableTools.newTable(longCol("RightStamp", 1L), intCol("Sentinel", 1));

        final MismatchedJoinKeyException stampMismatch = assertThrows(MismatchedJoinKeyException.class,
                () -> left.raj(right, "LeftStamp<=RightStamp", "Sentinel"));
        assertEquals("Can not raj() with different stamp types: left=class java.time.Instant, right=long",
                stampMismatch.getMessage());
    }

    /**
     * Builds a one-row table whose ZonedDateTime column is backed by a nanosecond source, which can be reinterpreted to
     * long, alongside an int column.
     */
    private static QueryTable convertibleZonedTable(final String zonedName, final long epochNanos,
            final String intName, final int intValue) {
        final Table instants = TableTools.newTable(instantCol("Ts", DateTimeUtils.epochNanosToInstant(epochNanos)),
                intCol(intName, intValue));
        final ColumnSource<ZonedDateTime> zoned =
                ((ConvertibleTimeSource) instants.getColumnSource("Ts")).toZonedDateTime(ZoneId.of("UTC"));
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        sources.put(zonedName, zoned);
        sources.put(intName, instants.getColumnSource(intName));
        return new QueryTable(instants.getRowSet().copy().toTracking(), sources);
    }

    private static ZonedDateTime utc(final long epochNanos) {
        return ZonedDateTime.ofInstant(DateTimeUtils.epochNanosToInstant(epochNanos), ZoneId.of("UTC"));
    }

    @Test
    public void testAjZonedDateTimeStampConvertibleAndObjectSources() {
        final Table right = TableTools.newTable(col("RightStamp", utc(1_000L)), intCol("Sentinel", 1));
        final Table objectLeft = TableTools.newTable(col("Stamp", utc(5_000L)), intCol("Other", 0));
        final Table expected = objectLeft.aj(right, "Stamp>=RightStamp", "Sentinel");

        final QueryTable convertibleLeft = convertibleZonedTable("Stamp", 5_000L, "Other", 0);
        final Table result = convertibleLeft.aj(right, "Stamp>=RightStamp", "Sentinel");
        assertTableEquals(expected.view("Other", "Sentinel"), result.view("Other", "Sentinel"));
    }

    @Test
    public void testAjZonedDateTimeKeyConvertibleAndObjectSources() {
        final Table right =
                TableTools.newTable(col("Key", utc(1_000L)), intCol("RightStamp", 1), intCol("Sentinel", 1));
        final Table objectLeft = TableTools.newTable(col("Key", utc(1_000L)), intCol("LeftStamp", 5));
        final Table expected = objectLeft.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel");

        final QueryTable convertibleLeft = convertibleZonedTable("Key", 1_000L, "LeftStamp", 5);
        final Table result = convertibleLeft.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel");
        assertTableEquals(expected.view("LeftStamp", "Sentinel"), result.view("LeftStamp", "Sentinel"));
    }

    /**
     * When several right rows share the closest stamp, aj matches the last of them and raj the first, whether the
     * tables are static or refreshing and whether or not there are exact match keys.
     */
    @Test
    public void testAjDuplicateRightStampTieBreak() {
        for (final boolean leftRefreshing : new boolean[] {false, true}) {
            for (final boolean rightRefreshing : new boolean[] {false, true}) {
                final QueryTable left = leftRefreshing
                        ? testRefreshingTable(i(0, 1).toTracking(), col("Key", "A", "B"), intCol("LeftStamp", 5, 5))
                        : testTable(i(0, 1).toTracking(), col("Key", "A", "B"), intCol("LeftStamp", 5, 5));
                final ColumnHolder<?>[] rightColumns = new ColumnHolder<?>[] {
                        col("Key", "A", "A", "A", "B", "B", "B"),
                        intCol("RightStamp", 5, 5, 5, 5, 5, 5),
                        intCol("Sentinel", 1, 2, 3, 4, 5, 6)};
                final QueryTable right = rightRefreshing
                        ? testRefreshingTable(i(0, 1, 2, 3, 4, 5).toTracking(), rightColumns)
                        : testTable(i(0, 1, 2, 3, 4, 5).toTracking(), rightColumns);
                final String context = "leftRefreshing=" + leftRefreshing + ", rightRefreshing=" + rightRefreshing;

                Asserts.assertEquals(context, new int[] {6, 6},
                        intColumn(left.aj(right, "LeftStamp>=RightStamp", "Sentinel"), "Sentinel"));
                Asserts.assertEquals(context, new int[] {1, 1},
                        intColumn(left.raj(right, "LeftStamp<=RightStamp", "Sentinel"), "Sentinel"));
                Asserts.assertEquals(context, new int[] {3, 6},
                        intColumn(left.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel"), "Sentinel"));
                Asserts.assertEquals(context, new int[] {1, 4},
                        intColumn(left.raj(right, "Key,LeftStamp<=RightStamp", "Sentinel"), "Sentinel"));
            }
        }
    }

    /**
     * A data index on either side of a keyed as-of join supplies key columns in the same representation as the table
     * sources, even when a nanosecond-backed ZonedDateTime key meets an object-backed one.
     */
    @Test
    public void testAjZonedDateTimeKeyConvertibleAndObjectSourcesWithDataIndex() {
        final Table objectRight =
                TableTools.newTable(col("Key", utc(1_000L)), intCol("RightStamp", 1), intCol("Sentinel", 1));
        final Table objectLeft = TableTools.newTable(col("Key", utc(1_000L)), intCol("LeftStamp", 5));
        final Table expected = objectLeft.aj(objectRight, "Key,LeftStamp>=RightStamp", "Sentinel");

        final QueryTable indexedConvertibleLeft = convertibleZonedTable("Key", 1_000L, "LeftStamp", 5);
        DataIndexer.getOrCreateDataIndex(indexedConvertibleLeft, "Key");
        assertTableEquals(expected.view("LeftStamp", "Sentinel"),
                indexedConvertibleLeft.aj(objectRight, "Key,LeftStamp>=RightStamp", "Sentinel")
                        .view("LeftStamp", "Sentinel"));

        final QueryTable indexedObjectRight = (QueryTable) TableTools.newTable(col("Key", utc(1_000L)),
                intCol("RightStamp", 1), intCol("Sentinel", 1));
        DataIndexer.getOrCreateDataIndex(indexedObjectRight, "Key");
        final QueryTable convertibleLeft = convertibleZonedTable("Key", 1_000L, "LeftStamp", 5);
        assertTableEquals(expected.view("LeftStamp", "Sentinel"),
                convertibleLeft.aj(indexedObjectRight, "Key,LeftStamp>=RightStamp", "Sentinel")
                        .view("LeftStamp", "Sentinel"));
    }

    @Test
    public void testAjNull() {
        final Table left = TableTools.newTable(
                col("Bucket", "A", "B", "A", "C", "D", "A"),
                longCol("LeftStamp", 1L, 10L, 50L, 3L, 4L, 60L));

        try {
            left.aj(null, "LeftStamp>=RightStamp");
            fail("Expected null argument exception!");
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

        Asserts.assertEquals(new int[] {1, 2, 5, NULL_INT, NULL_INT, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {NULL_INT, 2, 3, NULL_INT, NULL_INT, 5},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {1, 4, 5, NULL_INT, 6, NULL_INT},
                intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {3, 4, NULL_INT, NULL_INT, 6, NULL_INT},
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

        Asserts.assertEquals(new int[] {1, 2, 5, NULL_INT, NULL_INT, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "BucketA,BucketB,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {NULL_INT, 2, 3, NULL_INT, NULL_INT, 5},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "BucketA,BucketB,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {1, 4, 5, NULL_INT, 6, NULL_INT},
                intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "BucketA,BucketB,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("BucketA", "BucketB", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {3, 4, NULL_INT, NULL_INT, 6, NULL_INT},
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

        Asserts.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
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

        Asserts.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
                intColumn(reverseResultGt, "Sentinel"));
    }

    @Test
    public void testAjEmpty() {
        final Table left = TableTools.newTable(
                stringCol("Bucket"),
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

        Asserts.assertEquals(ArrayTypeUtils.EMPTY_INT_ARRAY, intColumn(result, "Sentinel"));
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

        Asserts.assertEquals(new int[] {NULL_INT, NULL_INT, 1}, intColumn(result, "Sentinel"));

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

        Asserts.assertEquals(new int[] {NULL_INT, 1}, intColumn(result2, "Sentinel"));
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

        Asserts.assertEquals(new int[] {3, 2, 4, 2, NULL_INT, 5, 5, 1}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, "Bucket,LeftStamp>RightStamp", "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {2, 1, NULL_INT, 1, NULL_INT, 5, NULL_INT, NULL_INT},
                intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, "Bucket,LeftStamp<=RightStamp", "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {3, 2, 4, 2, 4, NULL_INT, 5, 1}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, "Bucket,LeftStamp<RightStamp", "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("Bucket", "LeftStamp", "RightStamp", "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {NULL_INT, 3, NULL_INT, 3, 4, NULL_INT, NULL_INT, 2},
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

        Asserts.assertEquals(new int[] {1, 5, 0, 1, 3, 5}, intColumn(result, "Sentinel"));

        final Table ltResult = left.aj(right, leftStamp + ">" + rightStamp, "Sentinel");
        System.out.println("LT Result");
        TableTools.showWithRowSet(ltResult);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                ltResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {0, 3, NULL_INT, 1, 2, 3}, intColumn(ltResult, "Sentinel"));

        final Table reverseResult = left.raj(right, leftStamp + "<=" + rightStamp, "Sentinel");
        System.out.println("Reverse Result");
        TableTools.showWithRowSet(reverseResult);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                reverseResult.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {1, 4, 0, 2, 3, 4}, intColumn(reverseResult, "Sentinel"));

        final Table reverseResultGt = left.raj(right, leftStamp + "<" + rightStamp, "Sentinel");
        System.out.println("Reverse Result GT");
        TableTools.showWithRowSet(reverseResultGt);
        assertEquals(Arrays.asList("LeftStampD", "LeftStampF", rightStamp, "Sentinel"),
                reverseResultGt.getDefinition().getColumnNames());

        Asserts.assertEquals(new int[] {2, NULL_INT, 1, 2, 4, NULL_INT},
                intColumn(reverseResultGt, "Sentinel"));
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
                        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, false, false);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, true, false);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                            testAjRandomLeftStaticRightIncremental(seed, nodeSize, leftSize, rightSize, false, true);
                        }
                        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
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
                                try (final SafeCloseable ignored =
                                        LivenessScopeStack.open(new LivenessScope(true), true)) {
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
                                try (final SafeCloseable ignored =
                                        LivenessScopeStack.open(new LivenessScope(true), true)) {
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
                                try (final SafeCloseable ignored =
                                        LivenessScopeStack.open(new LivenessScope(true), true)) {
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
                control, stampType);
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
                                try (final SafeCloseable ignored =
                                        LivenessScopeStack.open(new LivenessScope(true), true)) {
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

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
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

        final IntArrayList expectedStamp = new IntArrayList();
        final IntArrayList expectedSentinel = new IntArrayList();

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
        final Table expected = leftTable.update("RightStamp=__rightStampExpected.getInt(i)",
                "RightSentinel=__rightSentinelExpected.getInt(i)");

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

    /**
     * Reproduction of the error from DHC issue #3080.
     */
    @Test
    public void testDHC3080() {
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

    /**
     * Reproduction of the error from DHC issue #4700. The root cause is that the cookies were not being migrated
     * properly during a partial rehash. This repro creates small initial tables, then generates large updates that
     * force a partial rehash and migration.
     */
    @Test
    public void testDHC4700() {
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

    /**
     * A static left table joined against a refreshing right table takes the bucketed right-ticking path, which reuses
     * one per-slot builder array for removals, additions, shifts and modifications. A cycle whose only change is a
     * modification of a right column that is neither the stamp nor a bucket key leaves the removal and addition sets
     * empty, so the builder array must still be sized for the modified rows before the modification pass probes into
     * it.
     */
    @Test
    public void testRightModifyNonStampColumnWithStaticLeft() {
        final QueryTable left = testTable(i(0).toTracking(),
                col("Bucket", "A"), intCol("LeftStamp", 5));
        final QueryTable right = testRefreshingTable(i(0).toTracking(),
                col("Bucket", "A"), intCol("RightStamp", 1), intCol("Sentinel", 1), intCol("Other", 0));

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel,Other");
        assertTableEquals(newTable(col("Bucket", "A"), intCol("LeftStamp", 5), intCol("RightStamp", 1),
                intCol("Sentinel", 1), intCol("Other", 0)), result);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(0), col("Bucket", "A"), intCol("RightStamp", 1), intCol("Sentinel", 1),
                    intCol("Other", 7));
            right.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                    right.newModifiedColumnSet("Other")));
        });

        assertTableEquals(newTable(col("Bucket", "A"), intCol("LeftStamp", 5), intCol("RightStamp", 1),
                intCol("Sentinel", 1), intCol("Other", 7)), result);
    }

    /**
     * A bucketed join over two refreshing tables builds a bucket for a right key it has not seen before, so the
     * per-slot output arrays must be sized for the added rows rather than for the buckets that already exist. A cycle
     * whose only change is right additions in new buckets reaches the build with no earlier operation having grown
     * those arrays.
     */
    @Test
    public void testRightAddInNewBucketWithBothTicking() {
        final QueryTable left = testRefreshingTable(i(0).toTracking(),
                col("Bucket", "A"), intCol("LeftStamp", 5));
        final QueryTable right = testRefreshingTable(i(0).toTracking(),
                col("Bucket", "A"), intCol("RightStamp", 1), intCol("Sentinel", 1));

        final Table result = left.aj(right, "Bucket,LeftStamp>=RightStamp", "Sentinel");
        final Table expected = newTable(col("Bucket", "A"), intCol("LeftStamp", 5), intCol("RightStamp", 1),
                intCol("Sentinel", 1));
        assertTableEquals(expected, result);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(1, 2), col("Bucket", "B", "C"), intCol("RightStamp", 1, 1), intCol("Sentinel", 2, 3));
            right.notifyListeners(i(1, 2), i(), i());
        });

        // the new buckets have no left rows, so the result is unchanged
        assertTableEquals(expected, result);
    }

    /**
     * A right column that is also a stamp match column is added to the result automatically under its own name. Naming
     * it in columnsToAdd renames that automatic addition, and naming it twice under two different left names produces
     * both renamed columns. The left stamp column is always carried through under its own name.
     */
    @Test
    public void testAjAddStampColumnUnderTwoNames() {
        final Table left = TableTools.newTable(intCol("LeftStamp", 5));
        final Table right = TableTools.newTable(intCol("RightStamp", 1), intCol("Sentinel", 100));

        final Table result = left.aj(right, "LeftStamp>=RightStamp", "A=RightStamp,B=RightStamp,Sentinel");

        assertEquals(Arrays.asList("LeftStamp", "A", "B", "Sentinel"), result.getDefinition().getColumnNames());
        Asserts.assertEquals(new int[] {5}, intColumn(result, "LeftStamp"));
        Asserts.assertEquals(new int[] {1}, intColumn(result, "A"));
        Asserts.assertEquals(new int[] {1}, intColumn(result, "B"));
        Asserts.assertEquals(new int[] {100}, intColumn(result, "Sentinel"));
    }

    /**
     * The stamp match column appears in the result under its right hand name when columnsToAdd does not mention it, and
     * under the requested name when it does. Either way the left stamp column keeps its own name, and an added column
     * may not take a name the left table already uses.
     */
    @Test
    public void testAjStampColumnNamingInOutput() {
        final Table left = TableTools.newTable(intCol("LeftStamp", 5));
        final Table right = TableTools.newTable(intCol("RightStamp", 1), intCol("Sentinel", 100));

        // not mentioned in columnsToAdd, so the match column is carried through under its own name
        final Table automatic = left.aj(right, "LeftStamp>=RightStamp", "Sentinel");
        assertEquals(Arrays.asList("LeftStamp", "RightStamp", "Sentinel"),
                automatic.getDefinition().getColumnNames());
        Asserts.assertEquals(new int[] {1}, intColumn(automatic, "RightStamp"));

        // a single alias renames that automatic addition rather than adding a second copy
        final Table renamed = left.aj(right, "LeftStamp>=RightStamp", "A=RightStamp,Sentinel");
        assertEquals(Arrays.asList("LeftStamp", "A", "Sentinel"), renamed.getDefinition().getColumnNames());
        Asserts.assertEquals(new int[] {1}, intColumn(renamed, "A"));

        // the left stamp column is untouched by the join and is never renamed
        final Table sameName = TableTools.newTable(intCol("Stamp", 5));
        final Table rightSameName = TableTools.newTable(intCol("Stamp", 1), intCol("Sentinel", 100));
        final Table shared = sameName.aj(rightSameName, "Stamp>=Stamp", "Sentinel");
        assertEquals(Arrays.asList("Stamp", "Sentinel"), shared.getDefinition().getColumnNames());
        Asserts.assertEquals(new int[] {5}, intColumn(shared, "Stamp"));

        // an added column may not be renamed onto a column the left table already has
        try {
            left.aj(right, "LeftStamp>=RightStamp", "LeftStamp=RightStamp,Sentinel");
            fail("expected a conflict for an added column named LeftStamp");
        } catch (RuntimeException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("LeftStamp"));
        }
    }

    /**
     * Naming a stamp match column under its own name and under an alias requests both output columns, in either order.
     */
    @Test
    public void testAjAddStampColumnWithAndWithoutAlias() {
        final Table left = TableTools.newTable(intCol("LeftStamp", 5));
        final Table right = TableTools.newTable(intCol("RightStamp", 1), intCol("Sentinel", 100));

        final Table originalFirst = left.aj(right, "LeftStamp>=RightStamp", "RightStamp,A=RightStamp,Sentinel");
        assertEquals(Arrays.asList("LeftStamp", "RightStamp", "A", "Sentinel"),
                originalFirst.getDefinition().getColumnNames());

        final Table aliasFirst = left.aj(right, "LeftStamp>=RightStamp", "A=RightStamp,RightStamp,Sentinel");
        assertEquals(Arrays.asList("LeftStamp", "RightStamp", "A", "Sentinel"),
                aliasFirst.getDefinition().getColumnNames());
    }

    /**
     * Churns bucket keys through a bucketed as-of join over many cycles: keys appear, lose all of their rows on one or
     * both sides, and some return after leaving. A small hash table forces rehashes while emptied buckets are released,
     * and each cycle is compared against a static join of the current tables.
     */
    @Test
    public void testAjChurningBuckets() {
        for (int seed = 0; seed < 3; ++seed) {
            for (final boolean leftRefreshing : new boolean[] {true, false}) {
                for (final boolean reverse : new boolean[] {false, true}) {
                    try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                        testAjChurningBuckets(seed, leftRefreshing, reverse);
                    }
                }
            }
        }
    }

    private void testAjChurningBuckets(final int seed, final boolean leftRefreshing, final boolean reverse) {
        final Random random = new Random(seed);
        final JoinControl control = new JoinControl() {
            @Override
            int initialBuildSize() {
                return 1 << 3;
            }

            @Override
            int rightSsaNodeSize() {
                return 4;
            }

            @Override
            int leftSsaNodeSize() {
                return 4;
            }

            @Override
            public int rightChunkSize() {
                return 4;
            }

            @Override
            public int leftChunkSize() {
                return 4;
            }
        };

        final ChurnSide leftSide = new ChurnSide();
        final ChurnSide rightSide = new ChurnSide();

        final QueryTable left;
        final int staticLeftKeys = 24;
        if (leftRefreshing) {
            left = testRefreshingTable(i().toTracking(), intCol("Bucket"), intCol("LeftStamp"));
        } else {
            // a static left side holds a fixed set of keys, while the right side churns over a wider range
            for (int key = 0; key < staticLeftKeys; ++key) {
                leftSide.stage(key, 1 + random.nextInt(3), random);
            }
            left = testTable(leftSide.addedRows().toTracking(), intCol("Bucket", leftSide.addedKeys()),
                    intCol("LeftStamp", leftSide.addedStamps()));
            leftSide.clearStaged();
        }
        final QueryTable right =
                testRefreshingTable(i().toTracking(), intCol("Bucket"), intCol("RightStamp"), intCol("Sentinel"));

        // raj is a descending as-of join against the reversed right table, which is what makes it match the first of
        // several duplicate right stamps
        final Table result = AsOfJoinHelper.asOfJoin(control, left, reverse ? (QueryTable) right.reverse() : right,
                MatchPairFactory.getExpressions("Bucket", "LeftStamp=RightStamp"),
                MatchPairFactory.getExpressions("Sentinel"),
                reverse ? SortingOrder.Descending : SortingOrder.Ascending, false);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final IntArrayList departedKeys = new IntArrayList();
        int nextKey = leftRefreshing ? 0 : staticLeftKeys / 2;
        int sentinel = 0;

        for (int cycle = 0; cycle < 150; ++cycle) {
            // remove every row of some keys, from both sides or from only one of them
            for (final int key : leftSide.union(rightSide)) {
                if (random.nextInt(3) != 0) {
                    continue;
                }
                final int sides = random.nextInt(4);
                if (leftRefreshing && sides != 1) {
                    leftSide.removeKey(key);
                }
                if (sides != 2) {
                    rightSide.removeKey(key);
                }
                if (!leftSide.contains(key) && !rightSide.contains(key)) {
                    departedKeys.add(key);
                }
            }

            // introduce new keys, some on only one side
            final int newKeys = 1 + random.nextInt(3);
            for (int ii = 0; ii < newKeys; ++ii) {
                final int key = nextKey++;
                final int sides = random.nextInt(4);
                if (leftRefreshing && sides != 1) {
                    leftSide.stage(key, 1 + random.nextInt(3), random);
                }
                if (sides != 2) {
                    sentinel = rightSide.stage(key, 1 + random.nextInt(3), random, sentinel);
                }
            }

            // bring back a key that previously lost all of its rows
            if (!departedKeys.isEmpty() && random.nextBoolean()) {
                final int key = departedKeys.removeInt(random.nextInt(departedKeys.size()));
                if (leftRefreshing && random.nextBoolean()) {
                    leftSide.stage(key, 1 + random.nextInt(2), random);
                }
                sentinel = rightSide.stage(key, 1 + random.nextInt(2), random, sentinel);
            }

            updateGraph.runWithinUnitTestCycle(() -> {
                if (leftRefreshing) {
                    leftSide.apply(left, false);
                }
                rightSide.apply(right, true);
            });

            final Table leftSnapshot = left.snapshot();
            final Table rightSnapshot = right.snapshot();
            final Table expected = reverse
                    ? leftSnapshot.raj(rightSnapshot, "Bucket,LeftStamp<=RightStamp", "Sentinel")
                    : leftSnapshot.aj(rightSnapshot, "Bucket,LeftStamp>=RightStamp", "Sentinel");
            assertTableEquals(expected.view("Bucket", "LeftStamp", "Sentinel"),
                    result.view("Bucket", "LeftStamp", "Sentinel"));
        }
    }

    /**
     * A right incremental as-of join state manager releases buckets that lose all of their rows, so a stream of keys
     * that each live for a single cycle occupies a hash table sized to the live keys rather than to every key seen.
     */
    @Test
    public void testAjStateManagerReleasesEmptyBuckets() {
        final int keysPerCycle = 10;
        final int cycles = 1000;
        final Table keyTable = TableTools.newTable(intCol("Key", IntStream.range(0, keysPerCycle * cycles).toArray()));
        final ColumnSource<?>[] keySources = new ColumnSource<?>[] {keyTable.getColumnSource("Key")};

        final RightIncrementalHashedAsOfJoinStateManager stateManager = TypedHasherFactory.make(
                RightIncrementalAsOfJoinStateManagerTypedBase.class, keySources, keySources, 1 << 3, 0.75, 0.7);
        final IntegerArraySource slots = new IntegerArraySource();
        final ObjectArraySource<RowSetBuilderSequential> builders =
                new ObjectArraySource<>(RowSetBuilderSequential.class);

        for (int cycle = 0; cycle < cycles; ++cycle) {
            if (cycle > 0) {
                // every row of the previous cycle's keys goes away
                try (final RowSet removed = RowSetFactory.fromRange((long) (cycle - 1) * keysPerCycle,
                        (long) cycle * keysPerCycle - 1)) {
                    final int removedSlots = stateManager.markForRemoval(removed, keySources, slots, builders);
                    assertEquals(keysPerCycle, removedSlots);
                    stateManager.ensureTombstoneCandidateCapacity(removedSlots);
                    for (int slotIndex = 0; slotIndex < removedSlots; ++slotIndex) {
                        final int slot = slots.getInt(slotIndex);
                        final WritableRowSet leftRowSet = stateManager.getLeftRowSet(slot);
                        try (final RowSet slotRemoved = builders.get(slotIndex).build()) {
                            builders.set(slotIndex, null);
                            leftRowSet.remove(slotRemoved);
                        }
                        assertTrue(leftRowSet.isEmpty());
                        stateManager.addTombstoneCandidate(slot);
                    }
                }
            }

            try (final RowSet added =
                    RowSetFactory.fromRange((long) cycle * keysPerCycle, (long) (cycle + 1) * keysPerCycle - 1)) {
                final int addedSlots = stateManager.buildAdditions(true, added, keySources, slots, builders);
                assertEquals(keysPerCycle, addedSlots);
                for (int slotIndex = 0; slotIndex < addedSlots; ++slotIndex) {
                    stateManager.setLeftRowSet(slots.getInt(slotIndex), builders.get(slotIndex).build());
                    builders.set(slotIndex, null);
                }
            }

            stateManager.releaseEmptyBuckets();
            assertEquals(keysPerCycle, stateManager.getNumEntries());
            assertTrue("tableSize=" + stateManager.getTableSize(), stateManager.getTableSize() <= 64);
        }
    }

    /**
     * With a static left table and a refreshing right table, each update reports the columns modified in that cycle
     * alone: a modification of one added right column reports just that column, even after an earlier cycle reported
     * every right column.
     */
    @Test
    public void testRightTickingModifiedColumnSetIsPerCycle() {
        for (final String match : new String[] {"LeftStamp>=RightStamp", "Key,LeftStamp>=RightStamp"}) {
            final QueryTable left = testTable(i(0).toTracking(), col("Key", "K"), intCol("LeftStamp", 10));
            final QueryTable right = testRefreshingTable(i(0).toTracking(), col("Key", "K"),
                    intCol("RightStamp", 5), intCol("ColumnA", 1), intCol("ColumnB", 2));
            final QueryTable result = (QueryTable) left.aj(right, match, "ColumnA,ColumnB");
            final SimpleListener listener = new SimpleListener(result);
            result.addUpdateListener(listener);

            // an added right row reports every right column
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(right, i(1), col("Key", "K"), intCol("RightStamp", 3), intCol("ColumnA", 3),
                        intCol("ColumnB", 4));
                right.notifyListeners(i(1), i(), i());
            });

            listener.reset();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(right, i(0), col("Key", "K"), intCol("RightStamp", 5), intCol("ColumnA", 1),
                        intCol("ColumnB", 20));
                right.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                        right.newModifiedColumnSet("ColumnB")));
            });
            assertEquals(match, 1, listener.getCount());
            assertEquals(match, i(0), listener.getUpdate().modified());
            assertEquals(match, result.newModifiedColumnSet("ColumnB"), listener.getUpdate().modifiedColumnSet());
            result.removeUpdateListener(listener);
        }
    }

    /**
     * With both sides refreshing, the first right row of a bucket that held only left rows modifies just the left rows
     * that it matches.
     */
    @Test
    public void testFirstRightRowOfBucketModifiesOnlyMatchedLeftRows() {
        final QueryTable left = testRefreshingTable(i(0, 1, 2).toTracking(), col("Key", "A", "A", "A"),
                intCol("LeftStamp", 1, 2, 3));
        final QueryTable right = testRefreshingTable(i(0).toTracking(), col("Key", "B"), intCol("RightStamp", 1),
                intCol("Sentinel", 0));
        final QueryTable result = (QueryTable) left.aj(right, "Key,LeftStamp>=RightStamp", "Sentinel");
        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(right, i(10), col("Key", "A"), intCol("RightStamp", 2), intCol("Sentinel", 10));
            right.notifyListeners(i(10), i(), i());
        });

        Asserts.assertEquals(new int[] {NULL_INT, 10, 10}, ColumnVectors.ofInt(result, "Sentinel").toArray());
        assertEquals(1, listener.getCount());
        assertEquals(i(1, 2), listener.getUpdate().modified());
        result.removeUpdateListener(listener);
    }

    /**
     * Shifts a range of rows of a refreshing test table by a positive delta, which may move rows onto keys that other
     * rows of the same range vacate, and notifies listeners.
     */
    private static void shiftTestTable(final QueryTable table, final long start, final long end, final long delta) {
        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        shiftBuilder.shiftRange(start, end, delta);
        final RowSetShiftData shifted = shiftBuilder.build();
        shifted.apply((beginRange, endRange, shiftDelta) -> {
            for (final ColumnSource<?> column : table.getColumnSources()) {
                ((TestColumnSource<?>) column).shift(beginRange, endRange, shiftDelta);
            }
        });
        shifted.apply(table.getRowSet().writableCast());
        table.notifyListeners(new TableUpdateImpl(i(), i(), i(), shifted, ModifiedColumnSet.EMPTY));
    }

    /**
     * A static left table joined to a refreshing right table on two key columns, when one right update shifts several
     * ranges of rows, restamps every shifted right row with the same result as a static join.
     */
    @Test
    public void testLeftStaticKeyedAjSeveralRightShiftRanges() {
        final int rightSize = 12;
        final String[] rightFirstKeys = new String[rightSize];
        final int[] rightSecondKeys = new int[rightSize];
        final int[] rightStamps = new int[rightSize];
        for (int ii = 0; ii < rightSize; ++ii) {
            rightFirstKeys[ii] = ii % 2 == 0 ? "A" : "B";
            rightSecondKeys[ii] = ii % 3;
            rightStamps[ii] = ii * 10;
        }
        final QueryTable right = testRefreshingTable(RowSetFactory.flat(rightSize).toTracking(),
                col("First", rightFirstKeys), intCol("Second", rightSecondKeys), intCol("RightStamp", rightStamps),
                intCol("Sentinel", IntStream.range(0, rightSize).toArray()));

        final int leftSize = 36;
        final String[] leftFirstKeys = new String[leftSize];
        final int[] leftSecondKeys = new int[leftSize];
        final int[] leftStamps = new int[leftSize];
        for (int ii = 0; ii < leftSize; ++ii) {
            leftFirstKeys[ii] = ii % 2 == 0 ? "A" : "B";
            leftSecondKeys[ii] = ii % 3;
            leftStamps[ii] = ii * 4;
        }
        final QueryTable left = testTable(RowSetFactory.flat(leftSize).toTracking(), col("First", leftFirstKeys),
                intCol("Second", leftSecondKeys), intCol("LeftStamp", leftStamps));

        final String match = "First,Second,LeftStamp>=RightStamp";
        final Table result = left.aj(right, match, "Sentinel");
        assertTableEquals(left.aj(right.snapshot(), match, "Sentinel"), result);

        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
        shiftBuilder.shiftRange(0, 3, 100);
        shiftBuilder.shiftRange(4, 7, 200);
        shiftBuilder.shiftRange(8, 11, 300);
        final RowSetShiftData shifted = shiftBuilder.build();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            shifted.apply((beginRange, endRange, shiftDelta) -> {
                for (final ColumnSource<?> column : right.getColumnSources()) {
                    ((TestColumnSource<?>) column).shift(beginRange, endRange, shiftDelta);
                }
            });
            shifted.apply(right.getRowSet().writableCast());
            right.notifyListeners(new TableUpdateImpl(i(), i(), i(), shifted, ModifiedColumnSet.EMPTY));
        });
        assertEquals(i(100, 101, 102, 103, 204, 205, 206, 207, 308, 309, 310, 311), right.getRowSet());
        assertTableEquals(left.aj(right.snapshot(), match, "Sentinel"), result);
    }

    /**
     * Positive shifts of more rows than the join's chunk size are applied a chunk at a time, from the highest row keys
     * down. Each shift moves every row of a range onto the key that the next row of its bucket vacates, on the right
     * and the left side, for zero-key and bucketed joins with a static or refreshing left table.
     */
    @Test
    public void testPositiveShiftLargerThanChunk() {
        final int chunkSize = 4;
        final JoinControl control = new JoinControl() {
            @Override
            int rightSsaNodeSize() {
                return chunkSize;
            }

            @Override
            int leftSsaNodeSize() {
                return chunkSize;
            }

            @Override
            public int rightChunkSize() {
                return chunkSize;
            }
        };

        final int rightSize = 40;
        final long[] rightKeys = new long[rightSize];
        final String[] rightBuckets = new String[rightSize];
        final int[] rightStamps = new int[rightSize];
        for (int ii = 0; ii < rightSize; ++ii) {
            rightKeys[ii] = 2L * ii;
            rightBuckets[ii] = ii % 2 == 0 ? "A" : "B";
            // stamps decrease as the row key increases, so the left rows matched to a right row sit just after those
            // matched to the right row whose key it is shifted onto
            rightStamps[ii] = (rightSize - ii) * 5;
        }
        final int leftSize = 80;
        final long[] leftKeys = new long[leftSize];
        final String[] leftBuckets = new String[leftSize];
        final int[] leftStamps = new int[leftSize];
        for (int ii = 0; ii < leftSize; ++ii) {
            leftKeys[ii] = 2L * ii;
            leftBuckets[ii] = ii % 2 == 0 ? "A" : "B";
            leftStamps[ii] = (ii / 2) * 5;
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (final boolean keyed : new boolean[] {false, true}) {
            for (final boolean leftRefreshing : new boolean[] {false, true}) {
                for (final boolean disallowExactMatch : new boolean[] {false, true}) {
                    final QueryTable left = leftRefreshing
                            ? testRefreshingTable(i(leftKeys).toTracking(), col("Bucket", leftBuckets),
                                    intCol("LeftStamp", leftStamps))
                            : testTable(i(leftKeys).toTracking(), col("Bucket", leftBuckets),
                                    intCol("LeftStamp", leftStamps));
                    final QueryTable right = testRefreshingTable(i(rightKeys).toTracking(),
                            col("Bucket", rightBuckets), intCol("RightStamp", rightStamps),
                            intCol("Sentinel", IntStream.range(0, rightSize).toArray()));

                    final MatchPair stamp = new MatchPair("LeftStamp", "RightStamp");
                    final MatchPair[] matches = keyed ? new MatchPair[] {new MatchPair("Bucket", "Bucket"), stamp}
                            : new MatchPair[] {stamp};
                    final String matchString = (keyed ? "Bucket," : "") + "LeftStamp"
                            + (disallowExactMatch ? ">" : ">=") + "RightStamp";
                    final Table result = AsOfJoinHelper.asOfJoin(control, left, right, matches,
                            MatchPairFactory.getExpressions("RightStamp", "Sentinel"), SortingOrder.Ascending,
                            disallowExactMatch);
                    final String description = matchString + ", leftRefreshing=" + leftRefreshing;
                    // each row moves onto the key of the next row of its bucket
                    final long shiftDelta = keyed ? 4 : 2;
                    assertTableEquals(description, left.snapshot().aj(right.snapshot(), matchString, "Sentinel"),
                            result);

                    updateGraph
                            .runWithinUnitTestCycle(() -> shiftTestTable(right, 0, 2L * (rightSize - 1), shiftDelta));
                    assertTableEquals(description, left.snapshot().aj(right.snapshot(), matchString, "Sentinel"),
                            result);

                    if (leftRefreshing) {
                        updateGraph
                                .runWithinUnitTestCycle(() -> shiftTestTable(left, 0, 2L * (leftSize - 1), shiftDelta));
                        assertTableEquals(description, left.snapshot().aj(right.snapshot(), matchString, "Sentinel"),
                                result);
                    }
                }
            }
        }
    }

    /**
     * The rows of one side of a churning bucketed join, grouped by key, along with the additions and removals staged
     * for the next cycle. Rows are appended in increasing row key order.
     */
    private static class ChurnSide {
        private final Map<Integer, LongArrayList> rowsByKey = new LinkedHashMap<>();
        private final IntArrayList stagedKeys = new IntArrayList();
        private final IntArrayList stagedStamps = new IntArrayList();
        private final IntArrayList stagedSentinels = new IntArrayList();
        private RowSetBuilderRandom removed = RowSetFactory.builderRandom();
        private long nextRow = 0;
        private long firstStagedRow = 0;

        boolean contains(final int key) {
            return rowsByKey.containsKey(key);
        }

        Set<Integer> union(final ChurnSide other) {
            final Set<Integer> keys = new LinkedHashSet<>(rowsByKey.keySet());
            keys.addAll(other.rowsByKey.keySet());
            return keys;
        }

        void removeKey(final int key) {
            final LongArrayList rows = rowsByKey.remove(key);
            if (rows != null) {
                rows.forEach(removed::addKey);
            }
        }

        void stage(final int key, final int count, final Random random) {
            for (int ii = 0; ii < count; ++ii) {
                stageRow(key, random.nextInt(500_000), 0);
            }
        }

        /**
         * Stage right rows drawn from a few stamp values, so that buckets hold duplicate stamps and the expected match
         * depends on which of the duplicates the join chooses.
         */
        int stage(final int key, final int count, final Random random, int sentinel) {
            for (int ii = 0; ii < count; ++ii) {
                stageRow(key, random.nextInt(5) * 100_000, sentinel);
                ++sentinel;
            }
            return sentinel;
        }

        private void stageRow(final int key, final int stamp, final int sentinel) {
            rowsByKey.computeIfAbsent(key, unused -> new LongArrayList()).add(nextRow++);
            stagedKeys.add(key);
            stagedStamps.add(stamp);
            stagedSentinels.add(sentinel);
        }

        WritableRowSet addedRows() {
            return nextRow == firstStagedRow ? i() : RowSetFactory.fromRange(firstStagedRow, nextRow - 1);
        }

        int[] addedKeys() {
            return stagedKeys.toIntArray();
        }

        int[] addedStamps() {
            return stagedStamps.toIntArray();
        }

        void clearStaged() {
            stagedKeys.clear();
            stagedStamps.clear();
            stagedSentinels.clear();
            firstStagedRow = nextRow;
        }

        void apply(final QueryTable table, final boolean rightSide) {
            final RowSet removedRows = removed.build();
            removed = RowSetFactory.builderRandom();
            final RowSet addedRows = addedRows();
            removeRows(table, removedRows);
            if (rightSide) {
                addToTable(table, addedRows, intCol("Bucket", addedKeys()), intCol("RightStamp", addedStamps()),
                        intCol("Sentinel", stagedSentinels.toIntArray()));
            } else {
                addToTable(table, addedRows, intCol("Bucket", addedKeys()), intCol("LeftStamp", addedStamps()));
            }
            clearStaged();
            table.notifyListeners(addedRows, removedRows, i());
        }
    }
}
