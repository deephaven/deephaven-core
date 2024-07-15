//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecAbsSum;
import io.deephaven.api.agg.spec.AggSpecApproximatePercentile;
import io.deephaven.api.agg.spec.AggSpecAvg;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import io.deephaven.api.agg.spec.AggSpecDistinct;
import io.deephaven.api.agg.spec.AggSpecFirst;
import io.deephaven.api.agg.spec.AggSpecFormula;
import io.deephaven.api.agg.spec.AggSpecFreeze;
import io.deephaven.api.agg.spec.AggSpecGroup;
import io.deephaven.api.agg.spec.AggSpecLast;
import io.deephaven.api.agg.spec.AggSpecMax;
import io.deephaven.api.agg.spec.AggSpecMedian;
import io.deephaven.api.agg.spec.AggSpecMin;
import io.deephaven.api.agg.spec.AggSpecPercentile;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecStd;
import io.deephaven.api.agg.spec.AggSpecSum;
import io.deephaven.api.agg.spec.AggSpecTDigest;
import io.deephaven.api.agg.spec.AggSpecUnique;
import io.deephaven.api.agg.spec.AggSpecVar;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.api.object.UnionObject;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSet;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;

@Category(OutOfBandTest.class)
public class QueryTableNegativeZeroTest {

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
    public void testDhc3768_float_join_key() {
        final Table t1 = newTable(floatCol("KeyNeg", -0.0f), floatCol("Neg", -0.0f));
        final Table t2 = newTable(floatCol("KeyPos", 0.0f), floatCol("Pos", 0.0f));
        for (Table out : List.of(
                t1.naturalJoin(t2, "KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg"),
                t1.exactJoin(t2, "KeyNeg=KeyPos"),
                t2.exactJoin(t1, "KeyPos=KeyNeg"),
                t1.join(t2, "KeyNeg=KeyPos"),
                t2.join(t1, "KeyPos=KeyNeg"),
                t1.aj(t2, "KeyNeg>=KeyPos"),
                t2.aj(t1, "KeyPos>=KeyNeg"))) {
            final long key;
            try (final RowSet.Iterator iterator = out.getRowSet().iterator()) {
                key = iterator.nextLong();
            }
            floatToBitsEquals(out, "KeyNeg", key, -0.0f);
            floatToBitsEquals(out, "KeyPos", key, 0.0f);
            floatToBitsEquals(out, "Neg", key, -0.0f);
            floatToBitsEquals(out, "Pos", key, 0.0f);
        }
    }

    @Test
    public void testDhc3768_double_join_key() {
        final Table t1 = newTable(doubleCol("KeyNeg", -0.0), doubleCol("Neg", -0.0));
        final Table t2 = newTable(doubleCol("KeyPos", 0.0), doubleCol("Pos", 0.0));
        for (Table out : List.of(
                t1.naturalJoin(t2, "KeyNeg=KeyPos"),
                t2.naturalJoin(t1, "KeyPos=KeyNeg"),
                t1.exactJoin(t2, "KeyNeg=KeyPos"),
                t2.exactJoin(t1, "KeyPos=KeyNeg"),
                t1.join(t2, "KeyNeg=KeyPos"),
                t2.join(t1, "KeyPos=KeyNeg"),
                t1.aj(t2, "KeyNeg>=KeyPos"),
                t2.aj(t1, "KeyPos>=KeyNeg"))) {
            final long key = oneKey(out);
            doubleToBitsEquals(out, "KeyNeg", key, -0.0);
            doubleToBitsEquals(out, "KeyPos", key, 0.0);
            doubleToBitsEquals(out, "Neg", key, -0.0);
            doubleToBitsEquals(out, "Pos", key, 0.0);
        }
    }

    @Test
    public void testDhc3768_float_agg_key() {
        final Table xy = newTable(
                floatCol("X", -0.0f, 0.0f),
                intCol("Y", 0, 1),
                intCol("B", 0, 1));
        final CreateAggSpecsDhc3768 cas = new CreateAggSpecsDhc3768();
        AggSpec.visitAll(cas);

        for (AggSpec aggSpec : cas.out) {
            TestCase.assertEquals(1, xy.aggAllBy(aggSpec, "X").size());
        }

        {
            final List<Aggregation> allAggregations = new ArrayList<>();
            for (AggSpec aggSpec : cas.out) {
                allAggregations.add(Aggregation.of(aggSpec, String.format("Out_%d=Y", allAggregations.size())));
            }
            allAggregations.add(Aggregation.AggCount("Count"));
            allAggregations.add(Aggregation.AggFirstRowKey("FirstRowKey"));
            allAggregations.add(Aggregation.AggLastRowKey("LastRowKey"));
            TestCase.assertEquals(1, xy.aggBy(allAggregations, "X").size());
        }
    }

    @Test
    public void testDhc3768_double_agg_key() {
        final Table xy = newTable(
                doubleCol("X", -0.0, 0.0),
                intCol("Y", 0, 1),
                intCol("B", 0, 1));
        final CreateAggSpecsDhc3768 cas = new CreateAggSpecsDhc3768();
        AggSpec.visitAll(cas);

        for (AggSpec aggSpec : cas.out) {
            TestCase.assertEquals(1, xy.aggAllBy(aggSpec, "X").size());
        }

        {
            final List<Aggregation> allAggregations = new ArrayList<>();
            for (AggSpec aggSpec : cas.out) {
                allAggregations.add(Aggregation.of(aggSpec, String.format("Out_%d=Y", allAggregations.size())));
            }
            allAggregations.add(Aggregation.AggCount("Count"));
            allAggregations.add(Aggregation.AggFirstRowKey("FirstRowKey"));
            allAggregations.add(Aggregation.AggLastRowKey("LastRowKey"));
            TestCase.assertEquals(1, xy.aggBy(allAggregations, "X").size());
        }
    }

    @Test
    public void testDhc3768_min_max_result() {
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
    public void testDhc3768_countDistinct_float_agg_results() {
        final Table x = newTable(floatCol("X", -0.0f, 0.0f));
        final Table expected = newTable(longCol("X", 1));
        final Table actual = x.aggAllBy(AggSpec.countDistinct());
        assertTableEquals(expected, actual);
    }

    @Test
    public void testDhc3768_countDistinct_double_agg_results() {
        final Table x = newTable(doubleCol("X", -0.0, 0.0));
        final Table expected = newTable(longCol("X", 1));
        final Table actual = x.aggAllBy(AggSpec.countDistinct());
        assertTableEquals(expected, actual);
    }

    @Test
    @Ignore(value = "io.deephaven.chunk.WritableChunk.sort() is unstable wrt -0.0 & 0.0")
    public void testDhc3768_distinct_float_agg_results() {
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
    @Ignore(value = "io.deephaven.chunk.WritableChunk.sort() is unstable wrt -0.0 & 0.0")
    public void testDhc3768_distinct_double_agg_results() {
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
    @Ignore(value = "io.deephaven.chunk.WritableChunk.sort() is unstable wrt -0.0 & 0.0")
    public void testDhc3768_unique_float_agg_results() {
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
    @Ignore(value = "io.deephaven.chunk.WritableChunk.sort() is unstable wrt -0.0 & 0.0")
    public void testDhc3768_unique_double_agg_results() {
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
    public void testDhc3768_sort_float_results() {
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
    public void testDhc3768_sort_double_results() {
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

    private static long oneKey(Table table) {
        final long key;
        try (final RowSet.Iterator iterator = table.getRowSet().iterator()) {
            TestCase.assertTrue(iterator.hasNext());
            key = iterator.nextLong();
            TestCase.assertFalse(iterator.hasNext());
        }
        return key;
    }

    private static void floatToBitsEquals(Table table, String column, long key, float expectedExact) {
        final float result = table.getColumnSource(column, float.class).getFloat(key);
        TestCase.assertEquals(Float.floatToIntBits(expectedExact), Float.floatToIntBits(result));
    }

    private static void doubleToBitsEquals(Table table, String column, long key, double expectedExact) {
        final double result = table.getColumnSource(column, double.class).getDouble(key);
        TestCase.assertEquals(Double.doubleToLongBits(expectedExact), Double.doubleToLongBits(result));
    }

    private static class CreateAggSpecsDhc3768 implements AggSpec.Visitor {

        private final List<AggSpec> out = new ArrayList<>();

        @Override
        public void visit(AggSpecAbsSum absSum) {
            out.add(AggSpecAbsSum.of());
        }

        @Override
        public void visit(AggSpecApproximatePercentile approxPct) {
            out.add(AggSpecApproximatePercentile.of(0.25));
            out.add(AggSpecApproximatePercentile.of(0.25, 50));
        }

        @Override
        public void visit(AggSpecAvg avg) {
            out.add(AggSpecAvg.of());
        }

        @Override
        public void visit(AggSpecCountDistinct countDistinct) {
            out.add(AggSpecCountDistinct.of(false));
            out.add(AggSpecCountDistinct.of(true));
        }

        @Override
        public void visit(AggSpecDistinct distinct) {
            out.add(AggSpecDistinct.of(false));
            out.add(AggSpecDistinct.of(true));
        }

        @Override
        public void visit(AggSpecFirst first) {
            out.add(AggSpecFirst.of());
        }

        @Override
        public void visit(AggSpecFormula formula) {
            out.add(AggSpecFormula.of("each"));
        }

        @Override
        public void visit(AggSpecFreeze freeze) {
            // freeze needs a different construction for testing
            // java.lang.IllegalStateException: FreezeBy only allows one row per state!
            // out.add(AggSpecFreeze.of());
        }

        @Override
        public void visit(AggSpecGroup group) {
            out.add(AggSpecGroup.of());
        }

        @Override
        public void visit(AggSpecLast last) {
            out.add(AggSpecLast.of());
        }

        @Override
        public void visit(AggSpecMax max) {
            out.add(AggSpecMax.of());
        }

        @Override
        public void visit(AggSpecMedian median) {
            out.add(AggSpecMedian.of(false));
            out.add(AggSpecMedian.of(true));
        }

        @Override
        public void visit(AggSpecMin min) {
            out.add(AggSpecMin.of());
        }

        @Override
        public void visit(AggSpecPercentile pct) {
            out.add(AggSpecPercentile.of(0.25, false));
            out.add(AggSpecPercentile.of(0.25, true));
        }

        @Override
        public void visit(AggSpecSortedFirst sortedFirst) {
            out.add(AggSpecSortedFirst.builder().addColumns(SortColumn.asc(ColumnName.of("B"))).build());
        }

        @Override
        public void visit(AggSpecSortedLast sortedLast) {
            out.add(AggSpecSortedLast.builder().addColumns(SortColumn.asc(ColumnName.of("B"))).build());
        }

        @Override
        public void visit(AggSpecStd std) {
            out.add(AggSpecStd.of());
        }

        @Override
        public void visit(AggSpecSum sum) {
            out.add(AggSpecSum.of());
        }

        @Override
        public void visit(AggSpecTDigest tDigest) {
            out.add(AggSpec.tDigest());
            out.add(AggSpec.tDigest(50));
        }

        @Override
        public void visit(AggSpecUnique unique) {
            out.add(AggSpecUnique.of(false, null));
            out.add(AggSpecUnique.of(true, null));

            // all columns are numeric, can be casted
            out.add(AggSpecUnique.of(false, UnionObject.of((byte) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((byte) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((short) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((short) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of(-1)));
            out.add(AggSpecUnique.of(true, UnionObject.of(-1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((long) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((long) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((float) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((float) -1)));
            out.add(AggSpecUnique.of(false, UnionObject.of((double) -1)));
            out.add(AggSpecUnique.of(true, UnionObject.of((double) -1)));
        }

        @Override
        public void visit(AggSpecWAvg wAvg) {
            out.add(AggSpecWAvg.of(ColumnName.of("B")));
        }

        @Override
        public void visit(AggSpecWSum wSum) {
            out.add(AggSpecWSum.of(ColumnName.of("B")));
        }

        @Override
        public void visit(AggSpecVar var) {
            out.add(AggSpecVar.of());
        }
    }
}
