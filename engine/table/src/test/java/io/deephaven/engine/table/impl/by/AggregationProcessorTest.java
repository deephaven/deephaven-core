//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.sources.IntegerSingleValueSource;
import io.deephaven.engine.table.impl.sources.LongSingleValueSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.*;
import org.junit.*;

import java.time.Instant;
import java.util.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.longCol;
import static org.junit.Assert.assertEquals;

public class AggregationProcessorTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        UpdatePerformanceTracker.resetForUnitTests();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        UpdatePerformanceTracker.resetForUnitTests();
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testMinMaxSecondaryTypes() {
        // If we have a min and a max operator that use the "same" input column, we want to avoid repeating the work.
        // This interacted with the NullValueColumnSource singletons, when we had a Timestamp followed by a Long in the
        // same aggregation that were both Nulls.
        final Map<String, ColumnSource<?>> csmap = new LinkedHashMap<>();
        csmap.put("Timestamp", NullValueColumnSource.getInstance(Instant.class, null));
        csmap.put("LongValue", NullValueColumnSource.getInstance(long.class, null));

        final QueryTable input = new QueryTable(i(0).toTracking(), csmap);
        input.setRefreshing(true);

        final List<Aggregation> aggs =
                Arrays.asList(AggMin("MnT=Timestamp", "MnL=LongValue"), AggMax("MxT=Timestamp", "MxL=LongValue"));
        final Table agged = input.aggBy(aggs);

        assertEquals(Instant.class, agged.getColumnSource("MnT").getType());
        assertEquals(long.class, agged.getColumnSource("MnL").getType());
        assertEquals(Instant.class, agged.getColumnSource("MxT").getType());
        assertEquals(long.class, agged.getColumnSource("MxL").getType());

        // this part of the test just verifies that we have the secondary operators we expect
        final AggregationContext ac = AggregationProcessor.forAggregation(aggs).makeAggregationContext(input, false);
        Arrays.stream(ac.operators).forEach(o -> System.out.println(o.getClass().getCanonicalName()));
        assertEquals(4, ac.operators.length);
        assertEquals(2, Arrays.stream(ac.operators).filter(o -> o instanceof SsmChunkedMinMaxOperator).count());
        assertEquals(2, Arrays.stream(ac.operators)
                .filter(o -> o.getClass().getCanonicalName().contains(".SecondaryOperator")).count());
    }

    @Test
    public void testMinMaxSecondaryTypesBoolean() {
        // This was not actually triggered by the Fuzzer bug that testMinMaxSecondaryTypes is checking for, but it is
        // a possibly equivalent case so I added a test anyway.
        final Map<String, ColumnSource<?>> csmap = new LinkedHashMap<>();
        csmap.put("BV", NullValueColumnSource.getInstance(Boolean.class, null));
        csmap.put("ByteValue", NullValueColumnSource.getInstance(byte.class, null));

        final QueryTable input = new QueryTable(i(0).toTracking(), csmap);
        input.setRefreshing(true);

        final List<Aggregation> aggs =
                Arrays.asList(AggMax("BV1=BV", "BV2=BV", "ByteMx=ByteValue"), AggMin("BV3=BV", "ByteMn=ByteValue"));

        final Table agged = input.aggBy(aggs);

        TableTools.show(agged);
        assertEquals(Boolean.class, agged.getColumnSource("BV1").getType());
        assertEquals(Boolean.class, agged.getColumnSource("BV2").getType());
        assertEquals(Boolean.class, agged.getColumnSource("BV3").getType());
        assertEquals(byte.class, agged.getColumnSource("ByteMx").getType());
        assertEquals(byte.class, agged.getColumnSource("ByteMn").getType());

        // this part of the test just verifies that we have the secondary operators we expect
        final AggregationContext ac = AggregationProcessor.forAggregation(aggs).makeAggregationContext(input, false);
        Arrays.stream(ac.operators).forEach(o -> System.out.println(o.getClass().getCanonicalName()));
        assertEquals(5, ac.operators.length);
        assertEquals(2, Arrays.stream(ac.operators).filter(o -> o instanceof SsmChunkedMinMaxOperator).count());
        assertEquals(3, Arrays.stream(ac.operators)
                .filter(o -> o.getClass().getCanonicalName().contains(".SecondaryOperator")).count());

    }

    @Test
    public void testGroupReuse() {
        // We should only need a single group by operator; but we want to make sure our output order is correct
        final Map<String, ColumnSource<?>> csmap = new LinkedHashMap<>();
        csmap.put("Timestamp", NullValueColumnSource.getInstance(Instant.class, null));
        csmap.put("LongValue", NullValueColumnSource.getInstance(long.class, null));
        csmap.put("IntValue", NullValueColumnSource.getInstance(int.class, null));

        final QueryTable input = new QueryTable(i(0).toTracking(), csmap);
        input.setRefreshing(true);

        final List<Aggregation> aggs =
                Arrays.asList(AggGroup("Timestamp"), AggGroup("LV=LongValue", "IV=IntValue"), AggGroup("TS=Timestamp"));
        final Table agged = input.aggBy(aggs);

        assertEquals(ObjectVector.class, agged.getColumnSource("Timestamp").getType());
        assertEquals(Instant.class, agged.getColumnSource("Timestamp").getComponentType());
        assertEquals(LongVector.class, agged.getColumnSource("LV").getType());
        assertEquals(IntVector.class, agged.getColumnSource("IV").getType());
        assertEquals(ObjectVector.class, agged.getColumnSource("TS").getType());
        assertEquals(Instant.class, agged.getColumnSource("TS").getComponentType());

        final ObjectVector<Instant> tsVec = new ObjectVectorDirect<>(new Instant[] {null});
        final LongVector longVec = new LongVectorDirect(QueryConstants.NULL_LONG);
        final IntVector intVec = new IntVectorDirect(QueryConstants.NULL_INT);
        final Table expected =
                TableTools.newTable(col("Timestamp", tsVec), col("LV", longVec), col("IV", intVec), col("TS", tsVec));
        assertTableEquals(expected, agged);

        // this part of the test just verifies that we have the secondary operators we expect
        final AggregationContext ac = AggregationProcessor.forAggregation(aggs).makeAggregationContext(input, false);
        Arrays.stream(ac.operators).forEach(o -> System.out.println(o.getClass().getCanonicalName()));
        assertEquals(3, ac.operators.length);
        assertEquals(1, Arrays.stream(ac.operators).filter(o -> o instanceof GroupByChunkedOperator).count());
        assertEquals(2, Arrays.stream(ac.operators)
                .filter(o -> o.getClass().getCanonicalName().contains("ResultExtractor")).count());
    }

    @Test
    public void testFormulaGroupReuse() {
        final Map<String, ColumnSource<?>> csmap = new LinkedHashMap<>();
        csmap.put("LongValue", new LongSingleValueSource());
        csmap.put("IntValue", new IntegerSingleValueSource());

        ((LongSingleValueSource) csmap.get("LongValue")).set(10L);
        ((IntegerSingleValueSource) csmap.get("IntValue")).set(20);

        final QueryTable input = new QueryTable(i(0).toTracking(), csmap);
        input.setRefreshing(true);

        final List<Aggregation> aggs =
                Arrays.asList(AggGroup("LongValue"), AggFormula("LS=sum(LongValue)"), AggFormula("IS=sum(IntValue)"));
        final Table agged = input.aggBy(aggs);

        final LongVectorDirect lvd = new LongVectorDirect(10L);
        assertTableEquals(
                TableTools.newTable(col("LongValue", (LongVector) lvd), longCol("LS", 10L), longCol("IS", 20L)), agged);

        // this part of the test just verifies that we have the secondary operators we expect
        final AggregationContext ac = AggregationProcessor.forAggregation(aggs).makeAggregationContext(input, false);
        Arrays.stream(ac.operators).forEach(o -> System.out.println(o.getClass().getCanonicalName()));
        assertEquals(3, ac.operators.length);
        assertEquals(1, Arrays.stream(ac.operators).filter(o -> o instanceof GroupByChunkedOperator).count());
        assertEquals(2,
                Arrays.stream(ac.operators).filter(o -> o instanceof FormulaMultiColumnChunkedOperator).count());
    }
}
