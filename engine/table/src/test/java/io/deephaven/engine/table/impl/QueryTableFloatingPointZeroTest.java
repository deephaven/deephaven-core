//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.MultiJoinFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.assertEquals;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;

@Category(OutOfBandTest.class)
public class QueryTableFloatingPointZeroTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testFloatJoinKey() {
        final Table t1 = newTable(floatCol("KeyNeg", -0.0f), floatCol("Neg", -0.0f));
        final Table t2 = newTable(floatCol("KeyPos", 0.0f), floatCol("Pos", 0.0f));
        final Table ajT1 = newTable(intCol("KeyA", 0), floatCol("KeyNeg", -0.0f), floatCol("Neg", -0.0f));
        final Table ajT2 = newTable(intCol("KeyA", 0), floatCol("KeyPos", 0.0f), floatCol("Pos", 0.0f));
        for (Table out : Arrays.asList(
                t1.naturalJoin(t2, "KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg"),
                t1.exactJoin(t2, "KeyNeg=KeyPos"),
                t2.exactJoin(t1, "KeyPos=KeyNeg"),
                t1.join(t2, "KeyNeg=KeyPos"),
                t2.join(t1, "KeyPos=KeyNeg"),
                ajT1.aj(ajT2, "KeyA,KeyNeg>=KeyPos"),
                ajT2.aj(ajT1, "KeyA,KeyPos>=KeyNeg"),
                ajT1.aj(ajT2, "KeyNeg=KeyPos,KeyA"),
                ajT2.aj(ajT1, "KeyPos=KeyNeg,KeyA"))) {
            final long key = oneKey(out);
            floatToBitsEquals(out, "KeyNeg", key, -0.0f);
            floatToBitsEquals(out, "KeyPos", key, 0.0f);
            floatToBitsEquals(out, "Neg", key, -0.0f);
            floatToBitsEquals(out, "Pos", key, 0.0f);
        }
    }

    @Test
    public void testDoubleJoinKey() {
        final Table t1 = newTable(doubleCol("KeyNeg", -0.0), doubleCol("Neg", -0.0));
        final Table t2 = newTable(doubleCol("KeyPos", 0.0), doubleCol("Pos", 0.0));
        final Table ajT1 = newTable(intCol("KeyA", 0), doubleCol("KeyNeg", -0.0), doubleCol("Neg", -0.0));
        final Table ajT2 = newTable(intCol("KeyA", 0), doubleCol("KeyPos", 0.0), doubleCol("Pos", 0.0));
        for (Table out : Arrays.asList(
                t1.naturalJoin(t2, "KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg"),
                t1.exactJoin(t2, "KeyNeg=KeyPos"),
                t2.exactJoin(t1, "KeyPos=KeyNeg"),
                t1.join(t2, "KeyNeg=KeyPos"),
                t2.join(t1, "KeyPos=KeyNeg"),
                ajT1.aj(ajT2, "KeyA,KeyNeg>=KeyPos"),
                ajT2.aj(ajT1, "KeyA,KeyPos>=KeyNeg"),
                ajT1.aj(ajT2, "KeyNeg=KeyPos,KeyA"),
                ajT2.aj(ajT1, "KeyPos=KeyNeg,KeyA"))) {
            final long key = oneKey(out);
            doubleToBitsEquals(out, "KeyNeg", key, -0.0);
            doubleToBitsEquals(out, "KeyPos", key, 0.0);
            doubleToBitsEquals(out, "Neg", key, -0.0);
            doubleToBitsEquals(out, "Pos", key, 0.0);
        }
    }

    @Test
    public void testFloatJoinMultiKey() {
        final Table t1 = newTable(intCol("Key1", 0), floatCol("KeyNeg", -0.0f), floatCol("Neg", -0.0f));
        final Table t2 = newTable(intCol("Key1", 0), floatCol("KeyPos", 0.0f), floatCol("Pos", 0.0f));
        final Table ajT1 =
                newTable(intCol("KeyA", 0), intCol("Key1", 0), floatCol("KeyNeg", -0.0f), floatCol("Neg", -0.0f));
        final Table ajT2 =
                newTable(intCol("KeyA", 0), intCol("Key1", 0), floatCol("KeyPos", 0.0f), floatCol("Pos", 0.0f));
        for (Table out : Arrays.asList(
                t1.naturalJoin(t2, "Key1,KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "Key1,KeyPos=KeyNeg"),
                t1.exactJoin(t2, "Key1,KeyNeg=KeyPos"),
                t2.exactJoin(t1, "Key1,KeyPos=KeyNeg"),
                t1.join(t2, "Key1,KeyNeg=KeyPos"),
                t2.join(t1, "Key1,KeyPos=KeyNeg"),

                t1.naturalJoin(t2, "KeyNeg=KeyPos,Key1"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg,Key1"),
                t1.exactJoin(t2, "KeyNeg=KeyPos,Key1"),
                t2.exactJoin(t1, "KeyPos=KeyNeg,Key1"),
                t1.join(t2, "KeyNeg=KeyPos,Key1"),
                t2.join(t1, "KeyPos=KeyNeg,Key1"),

                ajT1.aj(ajT2, "KeyA,Key1,KeyNeg>=KeyPos"),
                ajT2.aj(ajT1, "KeyA,Key1,KeyPos>=KeyNeg"),

                ajT1.aj(ajT2, "KeyA,KeyNeg=KeyPos,Key1"),
                ajT2.aj(ajT1, "KeyA,KeyPos=KeyNeg,Key1"),

                ajT1.aj(ajT2, "KeyNeg=KeyPos,KeyA,Key1"),
                ajT2.aj(ajT1, "KeyPos=KeyNeg,KeyA,Key1"))) {
            final long key = oneKey(out);
            floatToBitsEquals(out, "KeyNeg", key, -0.0f);
            floatToBitsEquals(out, "KeyPos", key, 0.0f);
            floatToBitsEquals(out, "Neg", key, -0.0f);
            floatToBitsEquals(out, "Pos", key, 0.0f);
        }
    }

    @Test
    public void testDoubleJoinMultiKey() {
        final Table t1 = newTable(intCol("Key1", 0), doubleCol("KeyNeg", -0.0), doubleCol("Neg", -0.0));
        final Table t2 = newTable(intCol("Key1", 0), doubleCol("KeyPos", 0.0), doubleCol("Pos", 0.0));
        final Table ajT1 =
                newTable(intCol("KeyA", 0), intCol("Key1", 0), doubleCol("KeyNeg", -0.0), doubleCol("Neg", -0.0));
        final Table ajT2 =
                newTable(intCol("KeyA", 0), intCol("Key1", 0), doubleCol("KeyPos", 0.0), doubleCol("Pos", 0.0));

        for (Table out : Arrays.asList(
                t1.naturalJoin(t2, "Key1,KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "Key1,KeyPos=KeyNeg"),
                t1.exactJoin(t2, "Key1,KeyNeg=KeyPos"),
                t2.exactJoin(t1, "Key1,KeyPos=KeyNeg"),
                t1.join(t2, "Key1,KeyNeg=KeyPos"),
                t2.join(t1, "Key1,KeyPos=KeyNeg"),

                t1.naturalJoin(t2, "KeyNeg=KeyPos,Key1"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg,Key1"),
                t1.exactJoin(t2, "KeyNeg=KeyPos,Key1"),
                t2.exactJoin(t1, "KeyPos=KeyNeg,Key1"),
                t1.join(t2, "KeyNeg=KeyPos,Key1"),
                t2.join(t1, "KeyPos=KeyNeg,Key1"),

                ajT1.aj(ajT2, "KeyA,Key1,KeyNeg>=KeyPos"),
                ajT2.aj(ajT1, "KeyA,Key1,KeyPos>=KeyNeg"),

                ajT1.aj(ajT2, "KeyA,KeyNeg=KeyPos,Key1"),
                ajT2.aj(ajT1, "KeyA,KeyPos=KeyNeg,Key1"),

                ajT1.aj(ajT2, "KeyNeg=KeyPos,KeyA,Key1"),
                ajT2.aj(ajT1, "KeyPos=KeyNeg,KeyA,Key1"))) {
            final long key = oneKey(out);
            doubleToBitsEquals(out, "KeyNeg", key, -0.0);
            doubleToBitsEquals(out, "KeyPos", key, 0.0);
            doubleToBitsEquals(out, "Neg", key, -0.0);
            doubleToBitsEquals(out, "Pos", key, 0.0);
        }
    }

    @Test
    public void testFloatMultiJoin() {
        // Note: table equals check doesn't account for -0.0 v 0.0, so explicitly testing that as well
        final Table expected = newTable(
                floatCol("Key", 0.0f),
                intCol("S1", 1),
                intCol("S2", 2),
                intCol("S3", 3));
        // preserves Key from first table
        {
            final Table t1 = newTable(floatCol("Key", -0.0f), intCol("S1", 1));
            final Table t2 = newTable(floatCol("Key", 0.0f), intCol("S2", 2));
            final Table t3 = newTable(floatCol("Key", -0.0f), intCol("S3", 3));
            final Table actual = MultiJoinFactory.of("Key", t1, t2, t3).table();
            final long key = oneKey(actual);
            floatToBitsEquals(actual, "Key", key, -0.0f);
            assertTableEquals(expected, actual);
        }
        {
            final Table t1 = newTable(floatCol("Key", 0.0f), intCol("S1", 1));
            final Table t2 = newTable(floatCol("Key", 0.0f), intCol("S2", 2));
            final Table t3 = newTable(floatCol("Key", -0.0f), intCol("S3", 3));
            final Table actual = MultiJoinFactory.of("Key", t1, t2, t3).table();
            final long key = oneKey(actual);
            floatToBitsEquals(actual, "Key", key, 0.0f);
            assertTableEquals(expected, actual);
        }
    }

    @Test
    public void testDoubleMultiJoin() {
        // Note: table equals check doesn't account for -0.0 v 0.0, so explicitly testing that as well
        final Table expected = newTable(
                doubleCol("Key", 0.0),
                intCol("S1", 1),
                intCol("S2", 2),
                intCol("S3", 3));
        // preserves Key from first table
        {
            final Table t1 = newTable(doubleCol("Key", -0.0), intCol("S1", 1));
            final Table t2 = newTable(doubleCol("Key", 0.0), intCol("S2", 2));
            final Table t3 = newTable(doubleCol("Key", -0.0), intCol("S3", 3));
            final Table actual = MultiJoinFactory.of("Key", t1, t2, t3).table();
            final long key = oneKey(actual);
            doubleToBitsEquals(actual, "Key", key, -0.0);
            assertTableEquals(expected, actual);
        }
        {
            final Table t1 = newTable(doubleCol("Key", 0.0), intCol("S1", 1));
            final Table t2 = newTable(doubleCol("Key", 0.0), intCol("S2", 2));
            final Table t3 = newTable(doubleCol("Key", -0.0), intCol("S3", 3));
            final Table actual = MultiJoinFactory.of("Key", t1, t2, t3).table();
            final long key = oneKey(actual);
            doubleToBitsEquals(actual, "Key", key, 0.0);
            assertTableEquals(expected, actual);
        }
    }

    @Test
    public void testFloatAggKey() {
        {
            final Table xy = newTable(floatCol("X", -0.0f, 0.0f), intCol("Y", 0, 1));
            final Table grouped = xy.groupBy("X");
            floatToBitsEquals(grouped, "X", oneKey(grouped), -0.0f);
        }
        {
            final Table xy = newTable(floatCol("X", 0.0f, -0.0f), intCol("Y", 0, 1));
            final Table grouped = xy.groupBy("X");
            floatToBitsEquals(grouped, "X", oneKey(grouped), 0.0f);
        }
    }

    @Test
    public void testDoubleAggKey() {
        {
            final Table xy = newTable(doubleCol("X", -0.0, 0.0), intCol("Y", 0, 1));
            final Table grouped = xy.groupBy("X");
            doubleToBitsEquals(grouped, "X", oneKey(grouped), -0.0);
        }
        {
            final Table xy = newTable(doubleCol("X", 0.0, -0.0), intCol("Y", 0, 1));
            final Table grouped = xy.groupBy("X");
            doubleToBitsEquals(grouped, "X", oneKey(grouped), 0.0);
        }
    }

    @Test
    public void testFloatDoubleAggKey() {
        {
            final Table xyz = newTable(
                    floatCol("X", -0.0f, 0.0f),
                    doubleCol("Y", -0.0, 0.0),
                    intCol("Z", 0, 1));
            final Table grouped = xyz.groupBy("X", "Y");
            floatToBitsEquals(grouped, "X", oneKey(grouped), -0.0f);
            doubleToBitsEquals(grouped, "Y", oneKey(grouped), -0.0);
        }
        {
            final Table xyz = newTable(
                    floatCol("X", -0.0f, 0.0f),
                    doubleCol("Y", -0.0, 0.0),
                    intCol("Z", 0, 1));
            final Table grouped = xyz.groupBy("Y", "X");
            floatToBitsEquals(grouped, "X", oneKey(grouped), -0.0f);
            doubleToBitsEquals(grouped, "Y", oneKey(grouped), -0.0);
        }
        {
            final Table xyz = newTable(
                    floatCol("X", -0.0f, 0.0f),
                    doubleCol("Y", 0.0, -0.0),
                    intCol("Z", 0, 1));
            final Table grouped = xyz.groupBy("X", "Y");
            floatToBitsEquals(grouped, "X", oneKey(grouped), -0.0f);
            doubleToBitsEquals(grouped, "Y", oneKey(grouped), 0.0);
        }
        {
            final Table xyz = newTable(
                    floatCol("X", -0.0f, 0.0f),
                    doubleCol("Y", 0.0, -0.0),
                    intCol("Z", 0, 1));
            final Table grouped = xyz.groupBy("Y", "X");
            floatToBitsEquals(grouped, "X", oneKey(grouped), -0.0f);
            doubleToBitsEquals(grouped, "Y", oneKey(grouped), 0.0);
        }
    }

    @Test
    public void testFloatMinMaxAggAndFormulaResult() {
        {
            final Table negFirst = newTable(floatCol("X", -0.0f, 0.0f));
            for (Table table : Arrays.asList(
                    negFirst.minBy(),
                    negFirst.maxBy(),
                    negFirst.groupBy().view("X=min(X)"),
                    negFirst.groupBy().view("X=max(X)"))) {
                floatToBitsEquals(table, "X", oneKey(table), -0.0f);
            }
        }
        {
            final Table posFirst = newTable(floatCol("X", 0.0f, -0.0f));
            for (Table table : Arrays.asList(
                    posFirst.minBy(),
                    posFirst.maxBy(),
                    posFirst.groupBy().view("X=min(X)"),
                    posFirst.groupBy().view("X=max(X)"))) {
                floatToBitsEquals(table, "X", oneKey(table), 0.0f);
            }
        }
    }

    @Test
    public void testDoubleMinMaxAggAndFormulaResult() {
        {
            final Table negFirst = newTable(doubleCol("X", -0.0, 0.0));
            for (Table table : Arrays.asList(
                    negFirst.minBy(),
                    negFirst.maxBy(),
                    negFirst.groupBy().view("X=min(X)"),
                    negFirst.groupBy().view("X=max(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), -0.0);
            }
        }
        {
            final Table posFirst = newTable(doubleCol("X", 0.0, -0.0));
            for (Table table : Arrays.asList(
                    posFirst.minBy(),
                    posFirst.maxBy(),
                    posFirst.groupBy().view("X=min(X)"),
                    posFirst.groupBy().view("X=max(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), 0.0);
            }
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testFloatMedianAggResult() {
        {
            final Table negFirst = newTable(floatCol("X", -2.0f, -1.0f, -0.0f, 0.0f, 1.0f));
            for (Table table : Arrays.asList(
                    negFirst.medianBy())) {
                floatToBitsEquals(table, "X", oneKey(table), -0.0f);
            }
        }
        {
            final Table posFirst = newTable(floatCol("X", -2.0f, -1.0f, 0.0f, -0.0f, 1.0f));
            for (Table table : Arrays.asList(
                    posFirst.medianBy())) {
                floatToBitsEquals(table, "X", oneKey(table), 0.0f);
            }
        }
    }

    // Note: broken into a different test than above b/c median(FloatVector) returns double :(
    @Test
    @Ignore("Unstable sort?")
    public void testFloatMedianFormulaResult() {
        {
            final Table negFirst = newTable(floatCol("X", -2.0f, -1.0f, -0.0f, 0.0f, 1.0f));
            for (Table table : Arrays.asList(
                    negFirst.groupBy().view("X=median(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), -0.0);
            }
        }
        {
            final Table posFirst = newTable(floatCol("X", -2.0f, -1.0f, 0.0f, -0.0f, 1.0f));
            for (Table table : Arrays.asList(
                    posFirst.groupBy().view("X=median(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), 0.0);
            }
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testDoubleMedianAggAndFormulaResult() {
        {
            final Table negFirst = newTable(doubleCol("X", -2.0, -1.0, -0.0, 0.0, 1.0));
            for (Table table : Arrays.asList(
                    negFirst.medianBy(),
                    negFirst.groupBy().view("X=median(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), -0.0);
            }
        }
        {
            final Table posFirst = newTable(doubleCol("X", -2.0, -1.0, 0.0, -0.0, 1.0));
            for (Table table : Arrays.asList(
                    posFirst.medianBy(),
                    posFirst.groupBy().view("X=median(X)"))) {
                doubleToBitsEquals(table, "X", oneKey(table), 0.0);
            }
        }
    }

    @Test
    public void testCountDistinctFloatAggResults() {
        final Table expected = newTable(longCol("X", 1));
        for (Table x : Arrays.asList(
                newTable(floatCol("X", -0.0f, 0.0f)),
                newTable(floatCol("X", 0.0f, -0.0f)),
                newTable(floatCol("X", -0.0f, 0.0f, -0.0f)),
                newTable(floatCol("X", 0.0f, -0.0f, 0.0f)))) {
            final Table actual = x.aggAllBy(AggSpec.countDistinct());
            assertTableEquals(expected, actual);
        }
    }

    @Test
    public void testCountDistinctDoubleAggResults() {
        final Table expected = newTable(longCol("X", 1));
        for (Table x : Arrays.asList(
                newTable(doubleCol("X", -0.0, 0.0)),
                newTable(doubleCol("X", 0.0, -0.0)),
                newTable(doubleCol("X", -0.0, 0.0, -0.0)),
                newTable(doubleCol("X", 0.0, -0.0, 0.0)))) {
            final Table actual = x.aggAllBy(AggSpec.countDistinct());
            assertTableEquals(expected, actual);
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testDistinctFloatAggResults() {
        {
            final Table x = newTable(floatCol("X", -0.0f, 0.0f));
            final Table actual = x.aggAllBy(AggSpec.distinct()).ungroup();
            floatToBitsEquals(actual, "X", oneKey(actual), -0.0f);
        }
        {
            final Table x = newTable(floatCol("X", 0.0f, -0.0f));
            final Table actual = x.aggAllBy(AggSpec.distinct()).ungroup();
            floatToBitsEquals(actual, "X", oneKey(actual), 0.0f);
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testDistinctDoubleAggResults() {
        {
            final Table x = newTable(doubleCol("X", -0.0, 0.0));
            final Table actual = x.aggAllBy(AggSpec.distinct()).ungroup();
            doubleToBitsEquals(actual, "X", oneKey(actual), -0.0);
        }
        {
            final Table x = newTable(doubleCol("X", 0.0, -0.0));
            final Table actual = x.aggAllBy(AggSpec.distinct()).ungroup();
            doubleToBitsEquals(actual, "X", oneKey(actual), 0.0);
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testUniqueFloatAggResults() {
        {
            final Table x = newTable(floatCol("X", -0.0f, 0.0f));
            final Table actual = x.aggAllBy(AggSpec.unique());
            floatToBitsEquals(actual, "X", oneKey(actual), -0.0f);
        }
        {
            final Table x = newTable(floatCol("X", 0.0f, -0.0f));
            final Table actual = x.aggAllBy(AggSpec.unique());
            floatToBitsEquals(actual, "X", oneKey(actual), 0.0f);
        }
    }

    @Test
    @Ignore("Unstable sort?")
    public void testUniqueDoubleAggResults() {
        {
            final Table x = newTable(doubleCol("X", -0.0, 0.0));
            final Table actual = x.aggAllBy(AggSpec.unique());
            doubleToBitsEquals(actual, "X", oneKey(actual), -0.0);
        }
        {
            final Table x = newTable(doubleCol("X", 0.0, -0.0));
            final Table actual = x.aggAllBy(AggSpec.unique());
            doubleToBitsEquals(actual, "X", oneKey(actual), 0.0);
        }
    }

    @Test
    public void testFloatSortedFirstLastAggResults() {
        final Table expected = newTable(intCol("YFirst", 0), intCol("YLast", 1));
        for (Table x : Arrays.asList(
                newTable(floatCol("X", -0.0f, 0.0f), intCol("Y", 0, 1)),
                newTable(floatCol("X", 0.0f, -0.0f), intCol("Y", 0, 1)))) {
            final Table actual = x.aggBy(List.of(
                    Aggregation.AggSortedFirst("X", "YFirst=Y"),
                    Aggregation.AggSortedLast("X", "YLast=Y")));
            assertTableEquals(expected, actual);
        }
    }

    @Test
    public void testDoubleSortedFirstLastAggResults() {
        final Table expected = newTable(intCol("YFirst", 0), intCol("YLast", 1));
        for (Table x : Arrays.asList(
                newTable(doubleCol("X", -0.0, 0.0), intCol("Y", 0, 1)),
                newTable(doubleCol("X", 0.0, -0.0), intCol("Y", 0, 1)))) {
            final Table actual = x.aggBy(List.of(
                    Aggregation.AggSortedFirst("X", "YFirst=Y"),
                    Aggregation.AggSortedLast("X", "YLast=Y")));
            assertTableEquals(expected, actual);
        }
    }

    @Test
    public void testSortFloatResults() {
        {
            final Table x = newTable(floatCol("X", -0.0f, 0.0f));
            final Table actual = x.sort("X").head(1);
            floatToBitsEquals(actual, "X", oneKey(actual), -0.0f);
        }
        {
            final Table x = newTable(floatCol("X", 0.0f, -0.0f));
            final Table actual = x.sort("X").head(1);
            floatToBitsEquals(actual, "X", oneKey(actual), 0.0f);
        }
    }

    @Test
    public void testSortDoubleResults() {
        {
            final Table x = newTable(doubleCol("X", -0.0, 0.0));
            final Table actual = x.sort("X").head(1);
            doubleToBitsEquals(actual, "X", oneKey(actual), -0.0);
        }
        {
            final Table x = newTable(doubleCol("X", 0.0, -0.0));
            final Table actual = x.sort("X").head(1);
            doubleToBitsEquals(actual, "X", oneKey(actual), 0.0);
        }
    }

    @Test
    public void testWhereFloatFilter() {
        final Table posZero = newTable(floatCol("X", 0.0f));
        final Table negZero = newTable(floatCol("X", -0.0f));
        for (float f : new float[] {-0.0f, 0.0f}) {
            final Table x = newTable(floatCol("X", f));
            for (Table actual : Arrays.asList(
                    x.where("X == 0.0f"),
                    x.where("X == -0.0f"),
                    x.where("X >= 0.0f"),
                    x.where("X >= -0.0f"),
                    x.where("X <= 0.0f"),
                    x.where("X <= -0.0f"),
                    x.where(FilterComparison.eq(ColumnName.of("X"), Literal.of(0.0f))),
                    x.where(FilterComparison.eq(ColumnName.of("X"), Literal.of(-0.0f))),
                    x.where(FilterComparison.geq(ColumnName.of("X"), Literal.of(0.0f))),
                    x.where(FilterComparison.geq(ColumnName.of("X"), Literal.of(-0.0f))),
                    x.where(FilterComparison.leq(ColumnName.of("X"), Literal.of(0.0f))),
                    x.where(FilterComparison.leq(ColumnName.of("X"), Literal.of(-0.0f))),
                    x.whereIn(posZero, "X"),
                    x.whereIn(negZero, "X"))) {
                floatToBitsEquals(actual, "X", oneKey(actual), f);
            }
        }
    }

    @Test
    public void testWhereDoubleFilter() {
        final Table posZero = newTable(doubleCol("X", 0.0));
        final Table negZero = newTable(doubleCol("X", -0.0));
        for (double d : new double[] {-0.0, 0.0}) {
            final Table x = newTable(doubleCol("X", d));
            for (Table actual : Arrays.asList(
                    x.where("X == 0.0"),
                    x.where("X == -0.0"),
                    x.where("X >= 0.0"),
                    x.where("X >= -0.0"),
                    x.where("X <= 0.0"),
                    x.where("X <= -0.0"),
                    x.where(FilterComparison.eq(ColumnName.of("X"), Literal.of(0.0))),
                    x.where(FilterComparison.eq(ColumnName.of("X"), Literal.of(-0.0))),
                    x.where(FilterComparison.geq(ColumnName.of("X"), Literal.of(0.0))),
                    x.where(FilterComparison.geq(ColumnName.of("X"), Literal.of(-0.0))),
                    x.where(FilterComparison.leq(ColumnName.of("X"), Literal.of(0.0))),
                    x.where(FilterComparison.leq(ColumnName.of("X"), Literal.of(-0.0))),
                    x.whereIn(posZero, "X"),
                    x.whereIn(negZero, "X"))) {
                doubleToBitsEquals(actual, "X", oneKey(actual), d);
            }
        }
    }

    @Test
    public void testWhereNotInFloatFilter() {
        final Table posZero = newTable(floatCol("X", 0.0f));
        final Table negZero = newTable(floatCol("X", -0.0f));
        final Table expected = newTable(floatCol("X"));
        for (float d : new float[] {-0.0f, 0.0f}) {
            final Table x = newTable(floatCol("X", d));
            for (Table actual : Arrays.asList(
                    x.where("X != 0.0f"),
                    x.where("X != -0.0f"),
                    x.where(FilterComparison.neq(ColumnName.of("X"), Literal.of(0.0f))),
                    x.where(FilterComparison.neq(ColumnName.of("X"), Literal.of(-0.0f))),
                    x.whereNotIn(posZero, "X"),
                    x.whereNotIn(negZero, "X"))) {
                assertTableEquals(expected, actual);
            }
        }
    }

    @Test
    public void testWhereNotInDoubleFilter() {
        final Table posZero = newTable(doubleCol("X", 0.0));
        final Table negZero = newTable(doubleCol("X", -0.0));
        final Table expected = newTable(doubleCol("X"));
        for (double d : new double[] {-0.0, 0.0}) {
            final Table x = newTable(doubleCol("X", d));
            for (Table actual : Arrays.asList(
                    x.where("X != 0.0"),
                    x.where("X != -0.0"),
                    x.where(FilterComparison.neq(ColumnName.of("X"), Literal.of(0.0))),
                    x.where(FilterComparison.neq(ColumnName.of("X"), Literal.of(-0.0))),
                    x.whereNotIn(posZero, "X"),
                    x.whereNotIn(negZero, "X"))) {
                assertTableEquals(expected, actual);
            }
        }
    }

    private static long oneKey(Table table) {
        TestCase.assertEquals(1, table.size());
        return table.getRowSet().firstRowKey();
    }

    private static void floatToBitsEquals(Table table, String column, long key, float expectedExact) {
        final float result = table.getColumnSource(column, float.class).getFloat(key);
        TestCase.assertEquals(Float.floatToIntBits(expectedExact), Float.floatToIntBits(result));
    }

    private static void doubleToBitsEquals(Table table, String column, long key, double expectedExact) {
        final double result = table.getColumnSource(column, double.class).getDouble(key);
        TestCase.assertEquals(Double.doubleToLongBits(expectedExact), Double.doubleToLongBits(result));
    }
}
