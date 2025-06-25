//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.base.FileUtils;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.UnionRedirection;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.QueryTableTestBase.TableComparator;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.sources.TestColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.layout.ParquetKeyValuePartitionedLayout;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.ObjectVector;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.api.agg.spec.AggSpec.percentile;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.parquet.base.ParquetUtils.PARQUET_FILE_EXTENSION;
import static io.deephaven.util.QueryConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
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
}
