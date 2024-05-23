//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.base.FileUtils;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.QueryTableTestBase.TableComparator;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.UnionRedirection;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.*;
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
import static io.deephaven.util.QueryConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static io.deephaven.parquet.base.ParquetUtils.PARQUET_FILE_EXTENSION;

@Category(OutOfBandTest.class)
public class QueryTableAggregationTest {
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

    // region Static chunked groupBy() tests

    private static AggregationContextFactory makeGroupByACF(
            @NotNull final Table table, @NotNull final String... groupByColumns) {
        return AggregationProcessor.forAggregation(List.of(
                QueryTable.singleAggregation(AggSpec.group(), ColumnName.from(groupByColumns),
                        table.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                                .map(ColumnName::of).collect(Collectors.toList()))
                        .orElseThrow()));
    }

    private static Table individualStaticByTest(@NotNull final Table input,
            @Nullable final AggregationControl aggregationControl, @NotNull final String... keyColumns) {
        final Table adjustedInput = input.update("__Pre_Agg_II__=ii");

        final Table expectedKeys;
        final Table expected;
        {
            // NB: We can't re-use SelectColumns across calls, hence the duplicate extraction for expressions and key
            // names.
            final SelectColumn[] keySelectColumns = SelectColumnFactory.getExpressions(keyColumns);
            final String[] keyNames =
                    Arrays.stream(keySelectColumns).map(SelectColumn::getName).distinct().toArray(String[]::new);

            if (keyColumns.length == 0) {
                expectedKeys = TableTools.emptyTable(!adjustedInput.isEmpty() ? 1 : 0);
                expected = adjustedInput;
            } else {
                final Set<String> retainedColumns =
                        new LinkedHashSet<>(adjustedInput.getDefinition().getColumnNameSet());
                Arrays.asList(keyNames).forEach(retainedColumns::remove);
                final List<SelectColumn> allSelectColumns =
                        Stream.concat(Arrays.stream(keySelectColumns), retainedColumns.stream().map(SourceColumn::new))
                                .collect(Collectors.toList());
                final Table adjustedInputWithAllColumns = adjustedInput.view(allSelectColumns);
                expectedKeys = adjustedInputWithAllColumns.selectDistinct(keyNames);
                expected = adjustedInputWithAllColumns.sort(keyNames);
            }
        }

        final Table actualKeys;
        final Table actual;
        {
            final Table aggregatedInput = ChunkedOperatorAggregationHelper.aggregation(
                    aggregationControl == null ? AggregationControl.DEFAULT : aggregationControl,
                    makeGroupByACF(adjustedInput, keyColumns),
                    (QueryTable) adjustedInput, false, null, ColumnName.from(keyColumns));
            actualKeys = keyColumns.length == 0
                    ? aggregatedInput.dropColumns(aggregatedInput.getDefinition().getColumnNamesArray())
                    : aggregatedInput.view(keyColumns);
            actual = aggregatedInput.sort(keyColumns).ungroup();
        }

        assertTableEquals(expectedKeys, actualKeys);
        assertTableEquals(expected, actual);

        return actual.dropColumns("__Pre_Agg_II__");
    }

    @Test
    public void testStaticNoKeyByWithChunks() {
        individualStaticByTest(emptyTable(0).update("A=Integer.toString(i % 5)", "B=i / 5"), null);
        individualStaticByTest(emptyTable(10000).update("A=Integer.toString(i % 5)", "B=i / 5"), null);
    }

    @Test
    public void testStaticReinterpretableKeyByWithChunks() {
        final String nowName = "__now_" + Thread.currentThread().hashCode() + "__";
        QueryScope.addParam(nowName, DateTimeUtils.now());
        final Table input = emptyTable(10000).update("A=ii % 100 == 0 ? null : plus(" + nowName + ", (long) (ii / 5))",
                "B=ii % 100 == 0 ? null : (ii & 1) == 0");

        individualStaticByTest(input, null, "A", "B");
        individualStaticByTest(input, null, "B", "A");
    }

    @Test
    public void testStaticByWithChunksAndAggressiveOverflow() {
        final AggregationControl control = new AggregationControl() {
            @Override
            public int initialHashTableSize(@NotNull final Table table) {
                return 8;
            }
        };

        final Table input1 = emptyTable(10000).update("A=i", "B=i/2", "C=i/3");
        final Table input2 = emptyTable(10000).update("A=i", "B=i%2", "C=i%3");
        final Table input3 = emptyTable(10000).update("D=i % 2048");
        final Table input4 = emptyTable(10000).update("D=i % 4096");
        final Table input5 = emptyTable(10000).update("E=(ii & 1) == 0 ? ii : (ii - 1 + 0xFFFFFFFFL)");
        final Table input6 = emptyTable(10000).update("A=i", "B=i%2", "C=i%3", "D=ii");

        individualStaticByTest(individualStaticByTest(individualStaticByTest(input1, control, "C"), control, "B"),
                control, "A");
        individualStaticByTest(individualStaticByTest(individualStaticByTest(input2, control, "C"), control, "B"),
                control, "A");
        individualStaticByTest(input3, control, "D");
        individualStaticByTest(input4, control, "D");
        individualStaticByTest(input5, control, "E");
        individualStaticByTest(input6, control, "A", "B", "C");
    }

    @Test
    public void testStaticGroupedByWithChunks() {
        final Table input1 = emptyTable(10000).update("A=Integer.toString(i % 5)", "B=i / 5");

        DataIndexer.getOrCreateDataIndex(input1, "A");
        DataIndexer.getOrCreateDataIndex(input1, "B");

        individualStaticByTest(input1, null, "A");
        individualStaticByTest(input1, null, "B");
    }

    @Test
    public void testStaticNameReusingByWithChunks() {
        individualStaticByTest(
                emptyTable(10000).update("A=i").updateView("A=Integer.toString(A % 5)", "A=A.hashCode()", "A=A / 2"),
                null, "A");
    }

    // endregion Static chunked groupBy() tests

    // region Incremental chunked groupBy() tests

    /**
     * {@link Supplier} for {@link Table}s to use in testing incremental groupBy().
     */
    private static class IncrementalFirstStaticAfterByResultSupplier implements Supplier<Table> {

        private final AggregationControl control;
        private final QueryTable input;
        private final String[] columns;

        private final AggregationContextFactory acf;

        private final AtomicBoolean firstTime = new AtomicBoolean(true);

        private IncrementalFirstStaticAfterByResultSupplier(@NotNull final AggregationControl control,
                @NotNull final QueryTable input, @NotNull String... columns) {
            this.control = control;
            this.input = input;
            this.columns = columns;
            acf = makeGroupByACF(input, columns);
        }

        /**
         * Return an incremental groupBy() result on first invocation, in order to establish the enclosing
         * {@link EvalNugget}'s baseline "original table". Return a static groupBy() result on subsequent invocations,
         * in order to use the static implementation to validate the incremental implementation. Note that the static
         * implementation is well tested by its own unit tests that don't rely on groupBy().
         *
         * @return The appropriate {@link Table}
         */
        @Override
        public final Table get() {
            if (firstTime.compareAndSet(true, false)) {
                return ChunkedOperatorAggregationHelper
                        .aggregation(control, acf, input, false, null, ColumnName.from(columns)).sort(columns);
            }
            return ChunkedOperatorAggregationHelper
                    .aggregation(control, acf, (QueryTable) input.silent(), false, null, ColumnName.from(columns))
                    .sort(columns);
        }
    }

    private static EvalNugget incrementalByEvalNugget(@NotNull final AggregationControl control,
            @NotNull final QueryTable input, @NotNull String... columns) {
        final Supplier<Table> tableSupplier = new IncrementalFirstStaticAfterByResultSupplier(control, input, columns);
        return new EvalNugget() {
            @Override
            protected final Table e() {
                return tableSupplier.get();
            }
        };
    }

    private static EvalNugget incrementalByEvalNugget(@NotNull final QueryTable input, @NotNull String... columns) {
        return incrementalByEvalNugget(AggregationControl.DEFAULT, input, columns);

    }

    @Test
    public void testIncrementalByDownstreamFromMerge() {
        final long mergeChunkMultiple = UnionRedirection.ALLOCATION_UNIT_ROW_KEYS;

        final String nowName = "__now_" + Thread.currentThread().hashCode() + "__";
        QueryScope.addParam(nowName, DateTimeUtils.now());
        final String tableIndexName = "__tableIndex_" + Thread.currentThread().hashCode() + "__";

        final QueryTable[] parents = IntStream.range(0, 20).mapToObj((final int tableIndex) -> {
            final QueryTable parent = (QueryTable) TableTools.emptyTable(2 * mergeChunkMultiple);
            parent.setRefreshing(true);
            return parent;
        }).toArray(QueryTable[]::new);

        final QueryTable[] inputs = IntStream.range(0, 5).mapToObj((final int tableIndex) -> {
            // noinspection AutoBoxing
            QueryScope.addParam(tableIndexName, tableIndex);
            parents[tableIndex].setAttribute(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true);
            final QueryTable result = (QueryTable) parents[tableIndex].update(
                    "StrCol = Long.toString((long) (ii / 5))",
                    "IntCol = " + tableIndexName + " * 1_000_000 + i",
                    "TimeCol = ii % 100 == 0 ? null : plus(" + nowName + ", ii * 100)");
            // Hide part of the table's row set from downstream, initially.
            result.getRowSet().writableCast().removeRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1);
            return result;
        }).toArray(QueryTable[]::new);

        // NB: Merge lets us produce large shifts "naturally".
        final QueryTable merged = (QueryTable) TableTools.merge(inputs);

        final AggregationControl controlSize8 = new AggregationControl() {
            @Override
            public int initialHashTableSize(@NotNull final Table table) {
                return 8;
            }
        };

        final EvalNugget[] ens = new EvalNugget[] {
                incrementalByEvalNugget(controlSize8, merged),
                incrementalByEvalNugget(merged),

                incrementalByEvalNugget(controlSize8, merged, "StrCol"),
                incrementalByEvalNugget(AggregationControl.DEFAULT, merged, "StrCol"),
                incrementalByEvalNugget(controlSize8, merged, "IntCol"),
                incrementalByEvalNugget(merged, "IntCol"),
                incrementalByEvalNugget(controlSize8, merged, "TimeCol"),
                incrementalByEvalNugget(AggregationControl.DEFAULT, merged, "TimeCol"),
                incrementalByEvalNugget(controlSize8,
                        (QueryTable) merged.updateView("TimeCol=isNull(TimeCol) ? NULL_LONG : epochNanos(TimeCol)"),
                        "TimeCol"),
                incrementalByEvalNugget(
                        (QueryTable) merged.updateView("TimeCol=isNull(TimeCol) ? NULL_LONG : epochNanos(TimeCol)"),
                        "TimeCol"),

                incrementalByEvalNugget(controlSize8, merged, "StrCol", "IntCol"),
                incrementalByEvalNugget(merged, "StrCol", "IntCol"),

                new EvalNugget() {
                    @Override
                    protected final Table e() {
                        return merged.groupBy("StrCol").update("IntColSum=sum(IntCol)");
                    }
                }
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[0].getRowSet().writableCast().insertRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1);
            inputs[0].notifyListeners(RowSetFactory.fromRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1), i(),
                    i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[1].getRowSet().writableCast().removeRange(mergeChunkMultiple - 1_000, mergeChunkMultiple - 1);
            inputs[1].notifyListeners(i(), RowSetFactory.fromRange(mergeChunkMultiple - 1_000, mergeChunkMultiple - 1),
                    i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[2].getRowSet().writableCast().insertRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1);
            inputs[2].notifyListeners(RowSetFactory.fromRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1), i(),
                    i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[0].getRowSet().writableCast().removeRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1);
            inputs[0].notifyListeners(i(), RowSetFactory.fromRange(mergeChunkMultiple, 2 * mergeChunkMultiple - 1),
                    i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[0].getRowSet().writableCast().removeRange(0, mergeChunkMultiple - 1);
            inputs[0].notifyListeners(i(), RowSetFactory.fromRange(0, mergeChunkMultiple - 1), i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[4].getModifiedColumnSetForUpdates().clear();
            inputs[4].getModifiedColumnSetForUpdates().setAll("StrCol");
            inputs[4].notifyListeners(new TableUpdateImpl(i(), i(), RowSetFactory.fromRange(0, mergeChunkMultiple / 2),
                    RowSetShiftData.EMPTY, inputs[4].getModifiedColumnSetForUpdates()));
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[4].getModifiedColumnSetForUpdates().clear();
            inputs[4].getModifiedColumnSetForUpdates().setAll("IntCol");
            inputs[4].notifyListeners(new TableUpdateImpl(i(), i(), RowSetFactory.fromRange(0, mergeChunkMultiple / 2),
                    RowSetShiftData.EMPTY, inputs[4].getModifiedColumnSetForUpdates()));
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            inputs[4].getModifiedColumnSetForUpdates().clear();
            inputs[4].getModifiedColumnSetForUpdates().setAll("TimeCol");
            inputs[4].notifyListeners(new TableUpdateImpl(i(), i(), RowSetFactory.fromRange(0, mergeChunkMultiple / 2),
                    RowSetShiftData.EMPTY, inputs[4].getModifiedColumnSetForUpdates()));
        });
        validate(ens);
    }

    @Test
    public void testIncrementalNoKeyBy() {
        final QueryTable input1 =
                (QueryTable) TableTools.emptyTable(100).update("StrCol=Long.toString(ii)", "IntCol=i");
        input1.setRefreshing(true);
        final QueryTable input2 =
                (QueryTable) TableTools.emptyTable(100).update("StrCol=Long.toString(ii)", "IntCol=i");
        input2.getRowSet().writableCast().remove(input2.getRowSet());
        input2.setRefreshing(true);

        final EvalNugget[] ens = new EvalNugget[] {
                incrementalByEvalNugget(input1),
                incrementalByEvalNugget(input2),
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return input1.groupBy().update("IntColSum=sum(IntCol)");
                    }
                }
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            input1.getRowSet().writableCast().removeRange(50, 99);
            input1.notifyListeners(i(), RowSetFactory.fromRange(50, 99), i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input1.getRowSet().writableCast().removeRange(0, 49);
            input1.notifyListeners(i(), RowSetFactory.fromRange(0, 49), i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input2.getRowSet().writableCast().insertRange(0, 49);
            input2.notifyListeners(RowSetFactory.fromRange(0, 49), i(), i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input2.getRowSet().writableCast().insertRange(50, 99);
            input2.notifyListeners(RowSetFactory.fromRange(50, 99), i(), i());
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input2.notifyListeners(new TableUpdateImpl(i(0, 1), i(0, 1), i(), RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY));
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input2.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(2, 3), RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });
        validate(ens);

        updateGraph.runWithinUnitTestCycle(() -> {
            input2.notifyListeners(
                    new TableUpdateImpl(i(), i(), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });
        validate(ens);
    }

    // endregion Incremental chunked groupBy() tests

    // region Legacy static groupBy() tests

    @Test
    public void testStaticBy() {
        Table table = newTable(intCol("V"));
        try {
            table.groupBy("i");
            TestCase.fail("Previous statement should have thrown an exception");
        } catch (Exception e) {
            TestCase.assertEquals("Invalid column name \"i\": \"i\" is a reserved keyword", e.getMessage());
        }
        TestCase.assertEquals(0, table.updateView("j=i").groupBy("j").size());
        TestCase.assertEquals(2, table.updateView("j=i").groupBy("j").numColumns());
        TestCase.assertEquals(int.class, table.updateView("j=i").groupBy("j").getColumnSource("j").getType());

        table = newTable(intCol("V", 100));
        TestCase.assertEquals(1, table.updateView("j=i").groupBy("j").size());
        TestCase.assertEquals(2, table.updateView("j=i").groupBy("j").numColumns());
        TestCase.assertEquals(int.class,
                table.updateView("j=i").groupBy("j").getDefinition().getColumn("j").getDataType());

        table = testRefreshingTable(RowSetFactory.fromRange(0, 2).toTracking(),
                col("S", "c", "e", "g"), col("I", 2, 4, 6));

        TestCase.assertEquals(3, table.groupBy("S").size());
        TestCase.assertEquals(2, table.groupBy("S").numColumns());
        TestCase.assertEquals(String.class, table.groupBy("S").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class, table.groupBy("S").getDefinition().getColumn("I").getDataType());
        TestCase.assertEquals(Arrays.asList("c", "e", "g"),
                Arrays.asList(DataAccessHelpers.getColumn(table.groupBy("S"), "S").get(0, 3)));
        IntVector[] intGroups = (IntVector[]) DataAccessHelpers.getColumn(table.groupBy("S"), "I").getDirect();
        TestCase.assertEquals(3, intGroups.length);
        TestCase.assertEquals(1, intGroups[0].size());
        TestCase.assertEquals(1, intGroups[1].size());
        TestCase.assertEquals(1, intGroups[2].size());
        TestCase.assertEquals(2, intGroups[0].get(0));
        TestCase.assertEquals(4, intGroups[1].get(0));
        TestCase.assertEquals(6, intGroups[2].get(0));

        table = testRefreshingTable(RowSetFactory.fromRange(0, 2).toTracking(),
                col("S", "e", "c", "g"),
                col("I", 4, 2, 6));

        TestCase.assertEquals(3, table.groupBy("S").size());
        TestCase.assertEquals(2, table.groupBy("S").numColumns());
        TestCase.assertEquals(String.class, table.groupBy("S").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class, table.groupBy("S").getDefinition().getColumn("I").getDataType());
        TestCase.assertEquals(Arrays.asList("e", "c", "g"),
                Arrays.asList(DataAccessHelpers.getColumn(table.groupBy("S"), "S").get(0, 3)));
        intGroups = (IntVector[]) DataAccessHelpers.getColumn(table.groupBy("S"), "I").getDirect();
        TestCase.assertEquals(3, intGroups.length);
        TestCase.assertEquals(1, intGroups[0].size());
        TestCase.assertEquals(1, intGroups[1].size());
        TestCase.assertEquals(1, intGroups[2].size());
        TestCase.assertEquals(4, intGroups[0].get(0));
        TestCase.assertEquals(2, intGroups[1].get(0));
        TestCase.assertEquals(6, intGroups[2].get(0));

        table = testRefreshingTable(RowSetFactory.fromRange(0, 2).toTracking(),
                col("S", "e", "c", "g"),
                col("X", 4, 2, 6),
                col("Y", 1, 2, 3));
        TestCase.assertEquals(3, table.updateView("Z=X+Y").groupBy("Z").size());
        TestCase.assertEquals(4, table.updateView("Z=X+Y").groupBy("Z").numColumns());
        TestCase.assertEquals(ObjectVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("X").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Y").getDataType());
        TestCase.assertEquals(int.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Z").getDataType());
        ObjectVector<?>[] sValues =
                (ObjectVector<?>[]) DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "S")
                        .getDirect();
        TestCase.assertEquals(3, sValues.length);
        TestCase.assertEquals(1, sValues[0].size());
        TestCase.assertEquals(1, sValues[1].size());
        TestCase.assertEquals(1, sValues[2].size());
        TestCase.assertEquals("e", sValues[0].get(0));
        TestCase.assertEquals("c", sValues[1].get(0));
        TestCase.assertEquals("g", sValues[2].get(0));
        TestCase.assertEquals(Arrays.asList(5, 4, 9),
                Arrays.asList(DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "Z").get(0, 3)));

        table = testRefreshingTable(
                col("S", "e", "c", "g"),
                col("X", 4, 2, 6),
                col("Y", 4, 2, 2));
        TestCase.assertEquals(2, table.updateView("Z=X+Y").groupBy("Z").size());
        TestCase.assertEquals(4, table.updateView("Z=X+Y").groupBy("Z").numColumns());
        TestCase.assertEquals(ObjectVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("X").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Y").getDataType());
        TestCase.assertEquals(int.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Z").getDataType());
        sValues = (ObjectVector<?>[]) DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "S")
                .getDirect();
        TestCase.assertEquals(2, sValues.length);
        TestCase.assertEquals(2, sValues[0].size());
        TestCase.assertEquals(1, sValues[1].size());
        TestCase.assertEquals("e", sValues[0].get(0));
        TestCase.assertEquals("c", sValues[1].get(0));
        TestCase.assertEquals("g", sValues[0].get(1));
        TestCase.assertEquals(Arrays.asList(8, 4),
                Arrays.asList(DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "Z").get(0, 2)));

        table = testRefreshingTable(
                col("S", "e", "c", "g"),
                colIndexed("X", 4, 2, 6),
                col("Y", 4, 2, 2));
        TestCase.assertEquals(2, table.updateView("Z=X+Y").groupBy("Z").size());
        TestCase.assertEquals(4, table.updateView("Z=X+Y").groupBy("Z").numColumns());
        TestCase.assertEquals(ObjectVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("X").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Y").getDataType());
        TestCase.assertEquals(int.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Z").getDataType());
        sValues = (ObjectVector<?>[]) DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "S")
                .getDirect();
        TestCase.assertEquals(2, sValues.length);
        TestCase.assertEquals(2, sValues[0].size());
        TestCase.assertEquals(1, sValues[1].size());
        TestCase.assertEquals("e", sValues[0].get(0));
        TestCase.assertEquals("c", sValues[1].get(0));
        TestCase.assertEquals("g", sValues[0].get(1));
        TestCase.assertEquals(Arrays.asList(8, 4),
                Arrays.asList(DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "Z").get(0, 2)));

        table = testRefreshingTable(
                col("S", "e", "c", "g"),
                col("X", 4, 2, 6),
                colIndexed("Y", 4, 2, 2));
        TestCase.assertEquals(2, table.updateView("Z=X+Y").groupBy("Z").size());
        TestCase.assertEquals(4, table.updateView("Z=X+Y").groupBy("Z").numColumns());
        TestCase.assertEquals(ObjectVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("X").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Y").getDataType());
        TestCase.assertEquals(int.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Z").getDataType());
        sValues = (ObjectVector<?>[]) DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "S")
                .getDirect();
        TestCase.assertEquals(2, sValues.length);
        TestCase.assertEquals(2, sValues[0].size());
        TestCase.assertEquals(1, sValues[1].size());
        TestCase.assertEquals("e", sValues[0].get(0));
        TestCase.assertEquals("c", sValues[1].get(0));
        TestCase.assertEquals("g", sValues[0].get(1));
        TestCase.assertEquals(Arrays.asList(8, 4),
                Arrays.asList(DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "Z").get(0, 2)));

        table = testRefreshingTable(
                col("S", "e", "c", "g"),
                colIndexed("X", 4, 2, 6),
                colIndexed("Y", 4, 3, 2));
        TestCase.assertEquals(2, table.updateView("Z=X+Y").groupBy("Z").size());
        TestCase.assertEquals(4, table.updateView("Z=X+Y").groupBy("Z").numColumns());
        TestCase.assertEquals(ObjectVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("X").getDataType());
        TestCase.assertEquals(IntVector.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Y").getDataType());
        TestCase.assertEquals(int.class,
                table.updateView("Z=X+Y").groupBy("Z").getDefinition().getColumn("Z").getDataType());
        sValues = (ObjectVector<?>[]) DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "S")
                .getDirect();
        TestCase.assertEquals(2, sValues.length);
        TestCase.assertEquals(2, sValues[0].size());
        TestCase.assertEquals(1, sValues[1].size());
        TestCase.assertEquals("e", sValues[0].get(0));
        TestCase.assertEquals("c", sValues[1].get(0));
        TestCase.assertEquals("g", sValues[0].get(1));
        TestCase.assertEquals(Arrays.asList(8, 5),
                Arrays.asList(DataAccessHelpers.getColumn(table.updateView("Z=X+Y").groupBy("Z"), "Z").get(0, 2)));

        table = testRefreshingTable(
                col("S", "c", null, "g"),
                col("I", 2, 4, 6));

        TestCase.assertEquals(3, table.groupBy("S").size());
        TestCase.assertEquals(2, table.groupBy("S").numColumns());
        TestCase.assertEquals(String.class, table.groupBy("S").getDefinition().getColumn("S").getDataType());
        TestCase.assertEquals(IntVector.class, table.groupBy("S").getDefinition().getColumn("I").getDataType());
        TestCase.assertEquals(Arrays.asList("c", null, "g"),
                Arrays.asList(DataAccessHelpers.getColumn(table.groupBy("S"), "S").get(0, 3)));
        intGroups = (IntVector[]) DataAccessHelpers.getColumn(table.groupBy("S"), "I").getDirect();
        TestCase.assertEquals(3, intGroups.length);
        TestCase.assertEquals(1, intGroups[0].size());
        TestCase.assertEquals(1, intGroups[1].size());
        TestCase.assertEquals(1, intGroups[2].size());
        TestCase.assertEquals(2, intGroups[0].get(0));
        TestCase.assertEquals(4, intGroups[1].get(0));
        TestCase.assertEquals(6, intGroups[2].get(0));
    }

    // endregion Legacy static groupBy() tests

    @Test
    public void testLastByIterative() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bc", "aa", "aa"),
                col("intCol", 10, 20, 30, 50),
                col("doubleCol", 0.1, 0.2, 0.3, 0.5));
        final QueryTable queryTableGrouped = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bc", "aa", "aa"),
                col("intCol", 10, 20, 30, 50),
                col("doubleCol", 0.1, 0.2, 0.3, 0.5));
        final Table table = queryTable.select();
        final Table tableGrouped = queryTableGrouped.select();
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy("Sym").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy("Sym", "intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy("Sym", "intCol").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return tableGrouped.lastBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return tableGrouped.lastBy("Sym").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.lastBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return tableGrouped.lastBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return tableGrouped.lastBy("Sym");
                    }
                }};
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "aa"), col("intCol", 20, 10), col("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(7, 9), i(), i());
        });

        validate(en);
    }

    @Test
    public void testFirstByLastByIncremental() {
        final Random random = new Random(0);

        final int size = 500;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "Sym2", "IntSet", "boolCol", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                        new SetGenerator<>("ee", "ff", "gg", "hh", "ii"),
                        new SetGenerator<>(1, 2),
                        new BooleanGenerator(),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(table::lastBy),
                EvalNugget.from(table::firstBy),
                EvalNugget.from(() -> table.firstBy("Sym").sort("Sym")),
                EvalNugget.from(() -> table.sort("Sym", "intCol").firstBy("Sym").sort("Sym")),
                EvalNugget.from(() -> table.sort("Sym", "intCol").lastBy("Sym").sort("Sym")),
                new UpdateValidatorNugget(table.sort("Sym", "intCol").firstBy("Sym")),
                new UpdateValidatorNugget(table.sort("Sym", "intCol").lastBy("Sym")),
                EvalNugget.from(
                        () -> table.sort("Sym", "intCol").lastBy("Sym").sort("Sym")),
                EvalNugget.from(
                        () -> table.sort("Sym", "intCol").firstBy("Sym").sort("Sym")),
                EvalNugget.from(() -> table.firstBy("boolCol").sort("boolCol")),
                EvalNugget.from(() -> table.firstBy("boolCol", "Sym").sort("boolCol", "Sym")),
                EvalNugget.from(() -> table.firstBy("Sym", "Sym2", "IntSet", "boolCol").sort("Sym", "Sym2", "IntSet",
                        "boolCol")),
        };


        for (int i = 0; i < 100; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    @Test
    public void testFirstOrLastByStatic() {
        for (int size = 10; size < 1000; size *= 10) {
            for (int seed = 0; seed < 100; seed++) {
                testFirstOrLastByStatic(seed, size);
            }
        }
    }

    private void testFirstOrLastByStatic(int seed, int size) {
        final Random random = new Random(seed);

        final QueryTable table = getTable(false, size, random,
                initColumnInfos(new String[] {"Sym", "Sym2", "IntSet", "boolCol", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                        new SetGenerator<>("ee", "ff", "gg", "hh", "ii"),
                        new SetGenerator<>(1, 2),
                        new BooleanGenerator(),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));
        TableTools.showWithRowSet(table);

        final Set<String> firstSet = new HashSet<>();
        final Set<String> skSet = new HashSet<>();

        QueryScope.addParam("firstSet", firstSet);
        QueryScope.addParam("skSet", skSet);

        assertTableEquals(table.tail(1), table.lastBy());
        assertTableEquals(table.head(1), table.firstBy());

        final Table expected = table.update("First=firstSet.add(Sym)").where("First").dropColumns("First");
        final Table firstBy = table.firstBy("Sym");
        assertTableEquals(expected, firstBy);

        firstSet.clear();
        final Table lastBy = table.lastBy("Sym").sort("Sym");
        final Table expectedLast =
                table.reverse().update("First=firstSet.add(Sym)").where("First").dropColumns("First").sort("Sym");
        assertTableEquals(expectedLast, lastBy);

        final Table expectedFirstComposite =
                table.update("First=skSet.add(new io.deephaven.tuple.ArrayTuple(Sym, intCol))")
                        .where("First").dropColumns("First").moveColumnsUp("Sym", "intCol");
        final Table firstByComposite = table.firstBy("Sym", "intCol");
        assertTableEquals(expectedFirstComposite, firstByComposite);

        skSet.clear();
        final Table lastByComposite = table.lastBy("Sym", "intCol").sort("Sym", "intCol");
        final Table expectedLastComposite =
                table.reverse().update("First=skSet.add(new io.deephaven.tuple.ArrayTuple(Sym, intCol))")
                        .where("First").dropColumns("First").sort("Sym", "intCol").moveColumnsUp("Sym", "intCol");
        assertTableEquals(expectedLastComposite, lastByComposite);
    }

    private <T> void powerSet(T[] elements, Consumer<T[]> consumer) {
        final boolean[] included = new boolean[elements.length];
        powerSetInternal(0, included, elements, consumer);
    }

    private <T> void powerSetInternal(int depth, boolean[] included, T[] elements, Consumer<T[]> consumer) {
        if (depth == included.length) {
            // noinspection unchecked
            consumer.accept(IntStream.range(0, included.length).filter(i -> included[i]).mapToObj(i -> elements[i])
                    .toArray(n -> (T[]) Array.newInstance(elements.getClass().getComponentType(), n)));
            return;
        }
        included[depth] = false;
        powerSetInternal(depth + 1, included, elements, consumer);
        included[depth] = true;
        powerSetInternal(depth + 1, included, elements, consumer);
    }

    @Test
    public void testKeyColumnTypes() {
        final Random random = new Random(0);

        final int size = 10;

        final QueryTable table = getTable(size, random,
                initColumnInfos(
                        new String[] {"Sym", "Date", "intCol", "doubleCol", "BooleanCol", "ByteCol", "CharCol",
                                "ShortCol", "FloatCol", "LongCol", "BigDecimalCol", "NonKey"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                        new UnsortedInstantLongGenerator(DateTimeUtils.parseInstant("2018-10-15T09:30:00 NY"),
                                DateTimeUtils.parseInstant("2018-10-15T16:00:00 NY")),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100),
                        new BooleanGenerator(),
                        new ByteGenerator((byte) 65, (byte) 95),
                        new CharGenerator('a', 'z'),
                        new ShortGenerator(),
                        new FloatGenerator(),
                        new LongGenerator(),
                        new BigDecimalGenerator(),
                        new IntGenerator()));

        final Set<String> keyColumnSet = new LinkedHashSet<>(table.getDefinition().getColumnNameSet());
        keyColumnSet.remove("NonKey");
        final String[] keyColumns = keyColumnSet.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);

        table.lastBy("Date", "Sym");

        // noinspection MismatchedQueryAndUpdateOfCollection
        final List<Table> tables = new ArrayList<>();
        powerSet(keyColumns, (String[] cols) -> tables.add(table.lastBy(cols)));
    }

    @Test
    public void testLastBySumByIterative() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bc", "ab", "bc"),
                col("USym", "a", "b", "a", "b"),
                col("intCol", 10, 20, 40, 60));
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.lastBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.lastBy("Sym").view("USym", "intCol").sumBy("USym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.lastBy("Sym").sumBy("Sym", "USym");
                    }
                },
        };
        validate(en);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9),
                    col("Sym", "aa", "bc"),
                    col("USym", "a", "b"),
                    col("intCol", 70, 90));

            queryTable.notifyListeners(i(7, 9), i(), i());
        });

        validate(en);

    }

    @Test
    public void testAddOnlyLastAttribute() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("USym", "a", "b", "a", "b"),
                col("intCol", 10, 20, 40, 60));

        queryTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);

        final Table lastBy = queryTable.lastBy("USym");

        final Table expected = newTable(col("USym", "a", "b"), intCol("intCol", 40, 60));
        assertTableEquals(expected, lastBy);

        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();

        addToTable(queryTable, i(7, 9),
                col("USym", "a", "b"),
                col("intCol", 70, 90));

        queryTable.notifyListeners(i(7, 9), i(), i());
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();

        final Table expected2 = newTable(col("USym", "a", "b"), intCol("intCol", 70, 90));

        assertTableEquals(expected2, lastBy);
    }

    // region Legacy incremental groupBy() tests

    @Test
    public void testIncrementalBy() {
        final QueryTable queryTable = testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bc", "aa", "aa"),
                col("intCol", 10, 20, 30, 50),
                col("doubleCol", 0.1, 0.2, 0.3, 0.5));
        final Table table = queryTable.select();
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.dropColumns("Sym").sumBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.sumBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.dropColumns("Sym").sumBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sumBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.dropColumns("Sym").sumBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.sumBy("Sym").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.dropColumns("Sym").sumBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.groupBy("Sym").select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sumBy("Sym").select();
                    }
                }
        };
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "aa"), col("intCol", 20, 10), col("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(7, 9), i(), i());
        });

        validate(en);
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "bc", "bc"), col("intCol", 21, 11), col("doubleCol", 2.2, 2.3));
            queryTable.notifyListeners(i(), i(), i(7, 9));
        });
        validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "bc"), col("intCol", 20, 15), col("doubleCol", 2.1, 2.3));
            queryTable.notifyListeners(i(), i(), i(7, 9));
        });
        validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "bc"), col("intCol", 20, 15),
                    col("doubleCol", Double.NEGATIVE_INFINITY,
                            Double.POSITIVE_INFINITY));
            queryTable.notifyListeners(i(), i(), i(7, 9));
        });
        validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "bc"), col("intCol", 20, 15),
                    col("doubleCol", Double.POSITIVE_INFINITY, Double.NaN));
            queryTable.notifyListeners(i(), i(), i(7, 9));
        });
        validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(7, 9), col("Sym", "aa", "bc"), col("intCol", 20, 15), col("doubleCol", 1.2, 2.2));
            queryTable.notifyListeners(i(), i(), i(7, 9));
        });
        validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(queryTable, i(2, 9));
            queryTable.notifyListeners(i(), i(2, 9), i());
        });
        validate(en);
    }

    private static void incrementalByTestSuite2() {
        final int seed = 0;
        final Random random = new Random(seed);

        final int size = 10;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                        new IntGenerator(0, 100),
                        new DoubleGenerator(0, 100)));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.dropColumns("Sym").sumBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.sumBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.dropColumns("Sym").sumBy().select();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym").select().sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.sumBy("Sym").select().sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym").update("intColSum=sum(intCol)").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return table.groupBy("Sym").update("intColSum=sum(intCol)").ungroup().sort("Sym");
                    }
                },
                new UpdateValidatorNugget(table.groupBy().update("intColSum=sum(intCol)")),
                new UpdateValidatorNugget(table.groupBy().update("intColSum=sum(intCol)").select()),
                new UpdateValidatorNugget(table.groupBy().update("intColSum=sum(intCol)").ungroup()),
                new UpdateValidatorNugget(table.groupBy("Sym").update("intColSum=sum(intCol)").ungroup()),
                new UpdateValidatorNugget(table.groupBy("Sym").update("intColSum=cumsum(intCol)").ungroup()),
                new UpdateValidatorNugget(table.groupBy("Sym").update("doubleColSum=cumsum(doubleCol)").ungroup()),
        };

        for (int step = 0; step < 100; ++step) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", step=" + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    // endregion Legacy incremental groupBy() tests

    // TODO test aggregation in a dynamic setting:
    // a) adding to the same key
    // b) adding to a new key
    // c) removing all elements for a key
    // d) removing some elements for a key
    // e) re-adding to a key after elements have been removed


    @Test
    public void testApplyToAllBy() {
        final Table table = testRefreshingTable(i(1, 5, 7, 8).toTracking(),
                col("Sym", "aa", "bc", "aa", "aa"),
                col("intCol", 10, 20, 30, 50),
                col("doubleCol", 0.1, 0.2, 0.3, 0.5));
        Table result = table.avgBy("Sym");
        TestCase.assertEquals(3, result.numColumns());
        TestCase.assertEquals(result.getDefinition().getColumns().get(0).getName(), "Sym");
        TestCase.assertEquals(result.getDefinition().getColumns().get(1).getName(), "intCol");
        TestCase.assertEquals(result.getDefinition().getColumns().get(2).getName(), "doubleCol");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(30.0, 20.0),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.3, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.sumBy("Sym");
        TestCase.assertEquals(3, result.numColumns());
        TestCase.assertEquals(result.getDefinition().getColumns().get(0).getName(), "Sym");
        TestCase.assertEquals(result.getDefinition().getColumns().get(1).getName(), "intCol");
        TestCase.assertEquals(result.getDefinition().getColumns().get(2).getName(), "doubleCol");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(90L, 20L),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.9, 0.2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.stdBy("Sym");
        TestCase.assertEquals(3, result.numColumns());
        TestCase.assertEquals(result.getDefinition().getColumns().get(0).getName(), "Sym");
        TestCase.assertEquals(result.getDefinition().getColumns().get(1).getName(), "intCol");
        TestCase.assertEquals(result.getDefinition().getColumns().get(2).getName(), "doubleCol");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(20.0, Double.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.19999999999999996, Double.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.minBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(10, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.1, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.maxBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(50, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.5, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.varBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(400.0, Double.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.03999999999999998, Double.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.lastBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(50, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(.5, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));

        result = table.updateView("Sym1=Sym").lastBy("Sym", "Sym1");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(50, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(.5, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));

        result = table.updateView("Sym1=Sym").lastBy("intCol", "Sym1");
        TestCase.assertEquals(result.size(), 4);
        TestCase.assertEquals(Arrays.asList(10, 20, 30, 50),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 4)));
        TestCase.assertEquals(Arrays.asList(0.1, 0.2, 0.3, 0.5),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 4)));
        TestCase.assertEquals(Arrays.asList("aa", "bc", "aa", "aa"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 4)));
        TestCase.assertEquals(Arrays.asList("aa", "bc", "aa", "aa"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym1").get(0, 4)));

        result = table.firstBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(10, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.1, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));

        result = table.updateView("Sym1=Sym").firstBy("Sym", "Sym1");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(10, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.1, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));

        result = table.updateView("Sym1=Sym").firstBy("intCol", "Sym1");
        TestCase.assertEquals(result.size(), 4);
        TestCase.assertEquals(Arrays.asList(10, 20, 30, 50),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 4)));
        TestCase.assertEquals(Arrays.asList(0.1, 0.2, 0.3, 0.5),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 4)));
        TestCase.assertEquals(Arrays.asList("aa", "bc", "aa", "aa"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 4)));
        TestCase.assertEquals(Arrays.asList("aa", "bc", "aa", "aa"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym1").get(0, 4)));

        result = table.view("intCol").avgBy();
        TestCase.assertEquals(result.size(), 1);
        TestCase.assertEquals(1, result.numColumns());
        TestCase.assertEquals(result.getDefinition().getColumns().get(0).getName(), "intCol");
        TestCase.assertEquals(Collections.singletonList(27.5),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 1)));

        result = table.lastBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(50, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(.5, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList("aa", "bc"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Sym").get(0, 2)));

        result = table.firstBy("Sym");
        TestCase.assertEquals(result.size(), 2);
        TestCase.assertEquals(Arrays.asList(10, 20),
                Arrays.asList(DataAccessHelpers.getColumn(result, "intCol").get(0, 2)));
        TestCase.assertEquals(Arrays.asList(0.1, .2),
                Arrays.asList(DataAccessHelpers.getColumn(result, "doubleCol").get(0, 2)));
    }


    @Test
    public void testSumByStatic() {
        final int[] sizes = {10, 100, 1000};
        for (final int size : sizes) {
            testSumByStatic(size, false, false);
            testSumByStatic(size, false, true);
        }
        testSumByStatic(20000, true, false);
        testSumByStatic(20000, true, true);
    }

    private void testSumByStatic(int size, boolean lotsOfStrings, boolean grouped) {
        final Random random = new Random(0);
        final List<ColumnInfo.ColAttributes> ea = Collections.emptyList();
        final QueryTable queryTable = getTable(false, size, random, initColumnInfos(new String[] {"Sym",
                "charCol", "byteCol",
                "shortCol", "intCol", "longCol",
                "doubleCol",
                "doubleNanCol",
                "boolCol",
                "bigI",
                "bigD"
        },
                Arrays.asList(grouped ? Collections.singletonList(ColumnInfo.ColAttributes.Indexed) : ea, ea, ea, ea,
                        ea, ea, ea, ea, ea, ea, ea),
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BooleanGenerator(0.5, 0.1),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final Table result = queryTable.dropColumns("Sym").sumBy();
        final List<String> updates = queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.sumFunction(c) + "(" + c + ")")
                .collect(Collectors.toList());
        final Table updateResult = queryTable.dropColumns("Sym").groupBy().update(Selectable.from(updates));
        assertTableEquals(updateResult, result, TableDiff.DiffItems.DoublesExact);

        final Table resultKeyed = queryTable.sumBy("Sym");
        final List<String> updateKeyed = queryTable.getDefinition().getColumnNames().stream()
                .filter(c -> !c.equals("Sym"))
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.sumFunction(c) + "(" + c + ")")
                .collect(Collectors.toList());
        final Table updateKeyedResult = queryTable.groupBy("Sym").update(Selectable.from(updateKeyed));
        assertTableEquals(updateKeyedResult, resultKeyed, TableDiff.DiffItems.DoublesExact);

        final Table resultAbs = queryTable.dropColumns("Sym").absSumBy();
        final List<String> updatesAbs =
                queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                        .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.absSumFunction(c, c))
                        .collect(Collectors.toList());
        final Table updateResultAbs = queryTable.dropColumns("Sym").groupBy().update(Selectable.from(updatesAbs));
        TableTools.show(resultAbs);
        TableTools.show(updateResultAbs);
        assertTableEquals(updateResultAbs, resultAbs, TableDiff.DiffItems.DoublesExact);

        final Table resultKeyedAbs = queryTable.absSumBy("Sym");
        final List<String> updateKeyedAbs =
                queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                        .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.absSumFunction(c, c))
                        .collect(Collectors.toList());
        final Table updateKeyedResultAbs = queryTable.groupBy("Sym").update(Selectable.from(updateKeyedAbs));
        assertTableEquals(updateKeyedResultAbs, resultKeyedAbs, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testMinMaxByStatic() {
        final int[] sizes = {10, 100, 1000};
        for (final int size : sizes) {
            testMinMaxByStatic(size, false);
        }
        testMinMaxByStatic(20000, true);
    }

    private void testMinMaxByStatic(int size, boolean lotsOfStrings) {
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(false, size, random, initColumnInfos(new String[] {"Sym",
                "charCol", "byteCol",
                "shortCol", "intCol", "longCol",
                "doubleCol",
                "doubleNanCol",
                "boolCol",
                "bigI",
                "bigD",
                "dt",
                "boolCol"
        },
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BooleanGenerator(0.5, 0.1),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1),
                new UnsortedInstantGenerator(DateTimeUtils.parseInstant("2019-12-17T00:00:00 NY"),
                        DateTimeUtils.parseInstant("2019-12-17T23:59:59 NY"), 0.1),
                new BooleanGenerator(0.4, 0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final Table result = queryTable.minBy();
        final List<String> updates = queryTable.getDefinition().getColumnNames().stream()
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.minFunction(c))
                .collect(Collectors.toList());
        final Table updateResult = queryTable.groupBy().update(Selectable.from(updates));
        assertTableEquals(updateResult, result);

        final Table resultKeyed = queryTable.minBy("Sym");
        final List<String> updateKeyed =
                queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                        .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.minFunction(c))
                        .collect(Collectors.toList());
        final Table updateKeyedResult = queryTable.groupBy("Sym").update(Selectable.from(updateKeyed));
        assertTableEquals(updateKeyedResult, resultKeyed);

        final Table resultMax = queryTable.maxBy();
        final List<String> updatesMax = queryTable.getDefinition().getColumnNames().stream()
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.maxFunction(c))
                .collect(Collectors.toList());
        final Table updateResultMax = queryTable.groupBy().update(Selectable.from(updatesMax));
        TableTools.show(resultMax);
        TableTools.show(updateResultMax);
        assertTableEquals(updateResultMax, resultMax);

        final Table resultKeyedMax = queryTable.maxBy("Sym");
        final List<String> updateKeyedMax =
                queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                        .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.maxFunction(c))
                        .collect(Collectors.toList());
        final Table updateKeyedResultMax = queryTable.groupBy("Sym").update(Selectable.from(updateKeyedMax));
        assertTableEquals(updateKeyedResultMax, resultKeyedMax);
    }

    @Test
    public void testAvgByStatic() {
        final int[] sizes = {10, 100, 1000};
        for (final int size : sizes) {
            testAvgByStatic(size, false);
        }
        testAvgByStatic(20000, true);
    }

    private void testAvgByStatic(int size, boolean lotsOfStrings) {
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(false, size, random, initColumnInfos(new String[] {"Sym",
                "charCol", "byteCol",
                "shortCol", "intCol", "longCol",
                "doubleCol",
                "doubleNanCol",
                "bigI",
                "bigD"
        },
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final Table result = queryTable.dropColumns("Sym").avgBy();
        final List<String> updates = queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                .flatMap(c -> Stream.of(
                        c + "_Sum=" + QueryTableAggregationTestFormulaStaticMethods.sumFunction(c) + "(" + c + ")",
                        c + "_Count=" + QueryTableAggregationTestFormulaStaticMethods.countFunction(c) + "(" + c + ")",
                        avgExpr(c)))
                .collect(Collectors.toList());
        final List<String> sumsAndCounts =
                queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                        .flatMap(c -> Stream.of(c + "_Sum", c + "_Count")).collect(Collectors.toList());
        final Table updateResult =
                queryTable.dropColumns("Sym").groupBy().update(Selectable.from(updates)).dropColumns(sumsAndCounts);
        assertTableEquals(updateResult, result, TableDiff.DiffItems.DoublesExact);

        final Table resultKeyed = queryTable.avgBy("Sym");
        final Table updateKeyedResult =
                queryTable.groupBy("Sym").update(Selectable.from(updates)).dropColumns(sumsAndCounts);
        assertTableEquals(updateKeyedResult, resultKeyed, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testVarByStatic() {
        final int[] sizes = {10, 100, 1000};
        for (final int size : sizes) {
            testVarByStatic(size, false);
        }
        testVarByStatic(20000, true);
    }

    private void testVarByStatic(int size, boolean lotsOfStrings) {
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(false, size, random, initColumnInfos(new String[] {"Sym",
                "charCol",
                "byteCol",
                "shortCol", "intCol", "longCol",
                "doubleCol",
                "doubleNanCol",
                "bigI",
                "bigD"
        },
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final Table result = queryTable.dropColumns("Sym").varBy();
        final List<String> updates = queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.varFunction(c))
                .collect(Collectors.toList());
        final Table updateResult = queryTable.dropColumns("Sym").groupBy().update(Selectable.from(updates));
        assertTableEquals(updateResult, result, TableDiff.DiffItems.DoublesExact, TableDiff.DiffItems.DoubleFraction);

        final Table resultKeyed = queryTable.varBy("Sym");
        final Table updateKeyedResult = queryTable.groupBy("Sym").update(Selectable.from(updates));

        TableTools.showWithRowSet(queryTable.where("Sym=`mjku`"));
        assertTableEquals(updateKeyedResult, resultKeyed, TableDiff.DiffItems.DoublesExact,
                TableDiff.DiffItems.DoubleFraction);
    }

    @Test
    public void testStdByStatic() {
        final int[] sizes = {10, 100, 1000};
        for (final int size : sizes) {
            testStdByStatic(size, false);
        }
        testStdByStatic(20000, true);
    }

    private void testStdByStatic(int size, boolean lotsOfStrings) {
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(false, size, random, initColumnInfos(new String[] {"Sym",
                "charCol",
                "byteCol",
                "shortCol", "intCol", "longCol",
                "doubleCol",
                "doubleNanCol",
                "bigI",
                "bigD"
        },
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final Table result = queryTable.dropColumns("Sym").stdBy();
        final List<String> updates = queryTable.getDefinition().getColumnNames().stream().filter(c -> !c.equals("Sym"))
                .map(c -> c + "=" + QueryTableAggregationTestFormulaStaticMethods.stdFunction(c))
                .collect(Collectors.toList());
        final Table updateResult = queryTable.dropColumns("Sym").groupBy().update(Selectable.from(updates));
        assertTableEquals(updateResult, result, TableDiff.DiffItems.DoublesExact);

        final Table resultKeyed = queryTable.stdBy("Sym");
        final Table updateKeyedResult = queryTable.groupBy("Sym").update(Selectable.from(updates));
        assertTableEquals(updateKeyedResult, resultKeyed, TableDiff.DiffItems.DoublesExact);
    }

    @NotNull
    private String avgExpr(String c) {
        if ("bigI".equals(c)) {
            return c + "=" + c + "_Count == 0 ? null : new java.math.BigDecimal(" + c
                    + "_Sum).divide(java.math.BigDecimal.valueOf(" + c + "_Count), java.math.BigDecimal.ROUND_HALF_UP)";
        }
        if ("bigD".equals(c)) {
            return c + "=" + c + "_Count == 0 ? null : " + c + "_Sum.divide(java.math.BigDecimal.valueOf(" + c
                    + "_Count), java.math.BigDecimal.ROUND_HALF_UP)";
        }
        // I would expect us to return a null for an average of nothing, but we instead return a NaN
        // return c + "=" + c + "_Count == 0 ? null : ((double)" + c + "_Sum / (double)" + c + "_Count)";
        return c + "=((double)(" + c + "_Count == 0 ? 0.0 : " + c + "_Sum) / (double)" + c + "_Count)";
    }

    @Test
    public void testSumByIncremental() {
        final int[] sizes;
        if (SHORT_TESTS) {
            sizes = new int[] {100, 1_000};
        } else {
            sizes = new int[] {10, 100, 4_000, 10_000};
        }
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                ChunkPoolReleaseTracking.enableStrict();
                System.out.println("Size = " + size + ", Seed = " + seed);
                testSumByIncremental(size, seed, true, true);
                testSumByIncremental(size, seed, true, false);
                testSumByIncremental(size, seed, false, true);
                testSumByIncremental(size, seed, false, false);
                ChunkPoolReleaseTracking.checkAndDisable();
            }
        }
    }

    private void testSumByIncremental(final int size, final int seed, boolean grouped, boolean lotsOfStrings) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            doTestSumByIncremental(size, seed, grouped, lotsOfStrings);
        }
    }

    private void doTestSumByIncremental(final int size, final int seed, boolean grouped, boolean lotsOfStrings) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final List<ColumnInfo.ColAttributes> ea = Collections.emptyList();
        final List<ColumnInfo.ColAttributes> ga = Collections.singletonList(ColumnInfo.ColAttributes.Indexed);
        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(
                new String[] {"Sym", "charCol", "byteCol", "shortCol", "intCol", "longCol", "bigI", "bigD",
                        "doubleCol", "doubleNanCol", "boolCol"},
                Arrays.asList(grouped ? ga : ea, ea, ea, ea, ea, ea, ea, ea, ea, ea, ea),
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BooleanGenerator(0.5, 0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sumBy()),
                EvalNugget.Sorted.from(() -> queryTable.sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.dropColumns("Sym").sort("intCol").sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").sumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").update("x=intCol+1").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(
                        () -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").absSumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.dropColumns("Sym").sort("intCol").absSumBy("intCol"),
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").absSumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").update("x=intCol+1").absSumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .absSumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(
                        () -> queryTable.sort("Sym", "intCol").update("x=intCol+1").absSumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").absSumBy("Sym"),
                        "Sym"),
        };

        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", step=" + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testAbsSumBySimple() {
        final QueryTable table = testRefreshingTable(i(2, 4, 6).toTracking(),
                col("BigI", BigInteger.valueOf(-1), BigInteger.valueOf(2), BigInteger.valueOf(-3)),
                col("DoubleCol", -1.0, 2.0, -3.0), col("BoolCol", new Boolean[] {null, null, null}));

        final Table result = table.absSumBy();
        TableTools.show(result);
        TestCase.assertEquals(1, result.size());
        BigInteger absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        double absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);
        BigInteger expected = BigInteger.valueOf(6);
        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(expected.doubleValue(), absSumDouble);
        TestCase.assertEquals(NULL_LONG, DataAccessHelpers.getColumn(result, "BoolCol").getLong(0));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), col("BigI", BigInteger.valueOf(5)), col("DoubleCol", 5.0),
                    col("BoolCol", true));
            table.notifyListeners(i(8), i(), i());
        });
        show(result);
        absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);
        TestCase.assertEquals(1L, DataAccessHelpers.getColumn(result, "BoolCol").get(0));

        expected = BigInteger.valueOf(11);
        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(expected.doubleValue(), absSumDouble);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(2));
            table.notifyListeners(i(), i(2), i());
        });
        show(result);
        absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);

        expected = BigInteger.valueOf(10);
        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(expected.doubleValue(), absSumDouble);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), col("BigI", BigInteger.valueOf(4)), col("DoubleCol", 4.0),
                    col("BoolCol", false));
            table.notifyListeners(i(), i(), i(8));
        });
        show(result);
        absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);
        TestCase.assertEquals(0L, DataAccessHelpers.getColumn(result, "BoolCol").get(0));

        expected = BigInteger.valueOf(9);
        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(expected.doubleValue(), absSumDouble);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(10), col("BigI", BigInteger.valueOf(0)), col("DoubleCol", Double.NaN),
                    col("BoolCol", true));
            table.notifyListeners(i(10), i(), i());
        });
        show(result);
        absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);

        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(Double.NaN, absSumDouble);
        TestCase.assertEquals(1L, DataAccessHelpers.getColumn(result, "BoolCol").getLong(0));

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(10));
            table.notifyListeners(i(), i(10), i());
        });
        show(result);
        absSum = (BigInteger) DataAccessHelpers.getColumn(result, "BigI").get(0);
        absSumDouble = DataAccessHelpers.getColumn(result, "DoubleCol").getDouble(0);

        TestCase.assertEquals(expected, absSum);
        TestCase.assertEquals(expected.doubleValue(), absSumDouble);
        TestCase.assertEquals(0L, DataAccessHelpers.getColumn(result, "BoolCol").getLong(0));

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(12, 14), col("BigI", BigInteger.valueOf(0), BigInteger.valueOf(0)),
                    doubleCol("DoubleCol", 0.0, 0.0), col("BoolCol", true, true));
            table.notifyListeners(i(12, 14), i(), i());
        });
        show(result);
        TestCase.assertEquals(2L, DataAccessHelpers.getColumn(result, "BoolCol").getLong(0));
    }

    @Test
    public void testAbsSumByNull() {
        final QueryTable table = testRefreshingTable(i(2).toTracking(),
                intCol("IntCol", NULL_INT),
                floatCol("FloatCol", QueryConstants.NULL_FLOAT));

        final Table result = table.absSumBy();
        TableTools.show(result);
        TestCase.assertEquals(1, result.size());
        long absSum = DataAccessHelpers.getColumn(result, "IntCol").getLong(0);
        TestCase.assertEquals(NULL_LONG, absSum);
        float absSumF = DataAccessHelpers.getColumn(result, "FloatCol").getFloat(0);
        TestCase.assertEquals(QueryConstants.NULL_FLOAT, absSumF);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), col("IntCol", 5), floatCol("FloatCol", -5.5f));
            table.notifyListeners(i(8), i(), i());
        });
        show(result);
        absSum = DataAccessHelpers.getColumn(result, "IntCol").getLong(0);
        absSumF = DataAccessHelpers.getColumn(result, "FloatCol").getFloat(0);
        TestCase.assertEquals(5L, absSum);
        TestCase.assertEquals(5.5f, absSumF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(8));
            table.notifyListeners(i(), i(8), i());
        });
        show(result);
        absSum = DataAccessHelpers.getColumn(result, "IntCol").getLong(0);
        absSumF = DataAccessHelpers.getColumn(result, "FloatCol").getFloat(0);
        TestCase.assertEquals(NULL_LONG, absSum);
        TestCase.assertEquals(QueryConstants.NULL_FLOAT, absSumF);
    }

    @Test
    public void testAvgInfinities() {
        final QueryTable table = testRefreshingTable(i(2).toTracking(),
                intCol("IntCol", NULL_INT),
                floatCol("FloatCol", QueryConstants.NULL_FLOAT));

        final Table result = table.avgBy();
        TableTools.show(result);
        TableTools.show(result.meta());
        TestCase.assertEquals(1, result.size());
        double avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        TestCase.assertEquals(Double.NaN, avg);
        double avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(Double.NaN, avgF);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), col("IntCol", 5), floatCol("FloatCol", 5f));
            table.notifyListeners(i(8), i(), i());
        });
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(5.0, avg);
        TestCase.assertEquals(5.0, avgF);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(9), col("IntCol", 6), floatCol("FloatCol", Float.POSITIVE_INFINITY));
            table.notifyListeners(i(9), i(), i());
        });
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(5.5, avg);
        TestCase.assertEquals(Double.POSITIVE_INFINITY, avgF);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(10), col("IntCol", 7), floatCol("FloatCol", Float.NEGATIVE_INFINITY));
            table.notifyListeners(i(10), i(), i());
        });
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(6.0, avg);
        TestCase.assertEquals(Double.NaN, avgF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9));
            table.notifyListeners(i(), i(9), i());
        });
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(6.0, avg);
        TestCase.assertEquals(Double.NEGATIVE_INFINITY, avgF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(10));
            addToTable(table, i(11), col("IntCol", 6), floatCol("FloatCol", Float.NaN));
            table.notifyListeners(i(11), i(10), i());
        });
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(5.5, avg);
        TestCase.assertEquals(Double.NaN, avgF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(11));
            table.notifyListeners(i(), i(11), i());
        });
        show(table);
        show(result);
        avg = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        avgF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(5.0, avg);
        TestCase.assertEquals(5.0, avgF);
    }

    @Test
    public void testVarInfinities() {
        final QueryTable table = testRefreshingTable(i(2).toTracking(),
                intCol("IntCol", NULL_INT),
                floatCol("FloatCol", QueryConstants.NULL_FLOAT));

        final Table result = table.varBy();
        TableTools.show(result);
        TableTools.show(result.meta());
        TestCase.assertEquals(1, result.size());
        double var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        TestCase.assertEquals(Double.NaN, var);
        double varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(Double.NaN, varF);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7, 8), col("IntCol", 4, 5), floatCol("FloatCol", 4f, 5f));
            table.notifyListeners(i(7, 8), i(), i());
        });
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(0.5, var);
        TestCase.assertEquals(0.5, varF);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(9), col("IntCol", 6), floatCol("FloatCol", Float.POSITIVE_INFINITY));
            table.notifyListeners(i(9), i(), i());
        });
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(1.0, var);
        TestCase.assertEquals(Double.NaN, varF);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(10), col("IntCol", 7), floatCol("FloatCol", Float.NEGATIVE_INFINITY));
            table.notifyListeners(i(10), i(), i());
        });
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(1.0 + 2.0 / 3.0, var, 0.001);
        TestCase.assertEquals(Double.NaN, varF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(9));
            table.notifyListeners(i(), i(9), i());
        });
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(2.0 + 1.0 / 3.0, var, 0.001);
        TestCase.assertEquals(Double.NaN, varF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(10));
            addToTable(table, i(11), col("IntCol", 6), floatCol("FloatCol", Float.NaN));
            table.notifyListeners(i(11), i(10), i());
        });
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(1.0, var);
        TestCase.assertEquals(Double.NaN, varF);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(table, i(11));
            table.notifyListeners(i(), i(11), i());
        });
        show(table);
        show(result);
        var = DataAccessHelpers.getColumn(result, "IntCol").getDouble(0);
        varF = DataAccessHelpers.getColumn(result, "FloatCol").getDouble(0);
        TestCase.assertEquals(0.5, var);
        TestCase.assertEquals(0.5, varF);
    }

    @Test
    public void testAvgByIncremental() {
        final int[] sizes = {10, 50, 200};
        for (int size : sizes) {
            testAvgByIncremental(size);
        }
    }

    private void testAvgByIncremental(int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "doubleCol", "floatCol", "bigI", "bigD", "byteCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 100),
                        new BigIntegerGenerator(),
                        new BigDecimalGenerator(),
                        new ByteGenerator()));
        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.dropColumns("Sym").avgBy()),
                EvalNugget.from(() -> queryTable.sort("Sym").avgBy("Sym")),
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sort("intCol").avgBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").avgBy("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1").avgBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .avgBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").avgBy("Sym", "intCol")
                        .sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").avgBy("Sym").sort("Sym")),
        };
        for (int i = 0; i < 50; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testStdVarByIncremental() {
        final int[] sizes = {10, 50, 200};
        for (int size : sizes) {
            testStdVarByIncremental(size);
        }
    }

    private void testStdVarByIncremental(int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "byteCol", "charCol", "shortCol", "intCol", "longCol", "floatCol",
                                "doubleCol", "bigI", "bigD"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new ByteGenerator((byte) (Byte.MIN_VALUE + 1), Byte.MAX_VALUE, 0.1),
                        new CharGenerator('a', 'z', 0.1),
                        new ShortGenerator((short) (Short.MIN_VALUE + 1), Short.MAX_VALUE, 0.1),
                        new IntGenerator(10, 100, 0.1),
                        new LongGenerator(-100, 100000, 0.1),
                        new FloatGenerator(0, 100, 0.1),
                        new DoubleGenerator(0, 100, 0.1),
                        new BigIntegerGenerator(),
                        new BigDecimalGenerator()));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final String integerCmp =
                "DiffI=((isNull(doubleI) || isNaN(doubleI)) && isNull(bigI)) || (!isNull(bigI) && (doubleI - bigI.doubleValue() < (0.01 * doubleI)))";
        final String decimalCmp =
                integerCmp.replaceAll("DiffI", "DiffD").replaceAll("doubleI", "doubleD").replaceAll("bigI", "bigD");
        final Table trueForSyms =
                queryTable.countBy("DiffI", "Sym").view("Sym", "DiffI=true", "DiffD=true").sort("Sym");

        final Table bigAsDouble = queryTable
                .view("Sym", "bigI", "bigD", "doubleI=bigI.doubleValue()", "doubleD=bigD.doubleValue()").sort("Sym");
        final Table bigVsDoubleVar = bigAsDouble.varBy("Sym");
        final Table doubleComparisonVar = bigVsDoubleVar.view("Sym", integerCmp, decimalCmp);
        final Table bigVsDoubleStd = bigAsDouble.stdBy("Sym");
        final Table doubleComparisonStd = bigVsDoubleStd.view("Sym", integerCmp, decimalCmp);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").stdBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("Sym").varBy("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.dropColumns("Sym").stdBy();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.dropColumns("Sym").varBy();
                    }
                },
                new TableComparator(doubleComparisonVar, trueForSyms) {
                    @Override
                    public void show() {
                        System.out.println("Big vs Double (var):");
                        TableTools.showWithRowSet(bigVsDoubleVar);
                        System.out.println("Double Comparison (var)");
                        TableTools.showWithRowSet(doubleComparisonVar);
                    }
                },
                new TableComparator(doubleComparisonStd, trueForSyms) {
                    @Override
                    public void show() {
                        System.out.println("Big vs Double (std):");
                        TableTools.showWithRowSet(bigVsDoubleStd);
                        System.out.println("Double Comparison (std)");
                        TableTools.showWithRowSet(doubleComparisonStd);
                    }
                }
        };
        for (int i = 0; i < 50; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testWeightedAvgByLong() {
        final QueryTable table = testRefreshingTable(i(2, 4, 6).toTracking(),
                col("Long1", 2L, 4L, 6L), col("Long2", 1L, 2L, 3L));
        final Table result = table.wavgBy("Long2");
        TableTools.show(result);
        TestCase.assertEquals(1, result.size());
        double wavg = DataAccessHelpers.getColumn(result, "Long1").getDouble(0);
        long wsum = 2 + 8 + 18;
        long sumw = 6;
        double expected = (double) wsum / (double) sumw;
        TestCase.assertEquals(expected, wavg);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(8), col("Long1", (long) Integer.MAX_VALUE), col("Long2", 7L));
            table.notifyListeners(i(8), i(), i());
        });
        show(result);
        wavg = DataAccessHelpers.getColumn(result, "Long1").getDouble(0);

        wsum = wsum + (7L * (long) Integer.MAX_VALUE);
        sumw = sumw + (7L);
        expected = (double) wsum / (double) sumw;
        TestCase.assertEquals(expected, wavg);
    }

    @Test
    public void testWeightedAvgByIncremental() {
        final int[] sizes = {10, 50, 200};
        for (int size : sizes) {
            for (int seed = 0; seed < 2; ++seed) {
                testWeightedAvgByIncremental(size, seed);
            }
        }
    }

    private void testWeightedAvgByIncremental(int size, int seed) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2",
                                "floatCol", "charCol", "byteCol", "shortCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new IntGenerator(1, 1000),
                        new DoubleGenerator(0, 100),
                        new DoubleGenerator(0, 100, 0.1, 0.001),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 100, 0.1, 0.001),
                        new CharGenerator('a', 'z'),
                        new ByteGenerator(),
                        new ShortGenerator()));

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Original Source Table:");
            TableTools.showWithRowSet(queryTable);
        }


        // long columns result in overflows when doing randomized tests
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.view("intCol", "doubleCol").wavgBy("intCol")),
                EvalNugget.Sorted.from(() -> queryTable.view("intCol", "Sym", "doubleCol").wavgBy("intCol", "Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(
                        () -> queryTable.view("doubleCol", "intCol", "intCol2", "Sym").wavgBy("doubleCol", "Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wavgBy("doubleCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wavgBy("floatCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wavgBy("charCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wavgBy("byteCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wavgBy("shortCol", "Sym"), "Sym"),
                new TableComparator(queryTable.view("intCol2", "intCol").wavgBy("intCol2"), "wavg",
                        queryTable.updateView("W=intCol*intCol2").groupBy()
                                .update("WSum=sum(W)", "C=sum(intCol2)", "intCol=WSum/C").view("intCol"),
                        "update"),
        };
        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", Step = " + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testWeightedSumByIncremental() {
        final int[] sizes = {10, 50, 200};
        for (int size : sizes) {
            for (int seed = 0; seed < 2; ++seed) {
                testWeightedSumByIncremental(size, seed);
            }
        }
    }

    private void testWeightedSumByIncremental(int size, int seed) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "intCol2", "doubleCol", "doubleNullCol", "doubleCol2",
                                "floatCol", "charCol", "byteCol", "shortCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new IntGenerator(1, 1000),
                        new DoubleGenerator(0, 100),
                        new DoubleGenerator(0, 100, 0.1, 0.001),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 100, 0.1, 0.001),
                        new CharGenerator('a', 'z'),
                        new ByteGenerator(),
                        new ShortGenerator()));

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Original Source Table:");
            TableTools.showWithRowSet(queryTable);
        }


        // long columns result in overflows when doing randomized tests
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.view("intCol", "doubleCol").wsumBy("intCol")),
                EvalNugget.Sorted.from(() -> queryTable.view("intCol", "Sym", "doubleCol").wsumBy("intCol", "Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(
                        () -> queryTable.view("doubleCol", "intCol", "intCol2", "Sym").wsumBy("doubleCol", "Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wsumBy("doubleCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wsumBy("floatCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wsumBy("charCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wsumBy("byteCol", "Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.wsumBy("shortCol", "Sym"), "Sym"),
                new TableComparator(queryTable.view("intCol2", "intCol").wsumBy("intCol2"), "wsum",
                        queryTable.updateView("W=intCol*intCol2").groupBy().update("intCol=(long)sum(W)")
                                .view("intCol"),
                        "update"),
                new TableComparator(queryTable.view("intCol2", "doubleCol").wsumBy("intCol2"), "wsum",
                        queryTable.updateView("W=doubleCol*intCol2").groupBy().update("doubleCol=sum(W)")
                                .view("doubleCol"),
                        "update"),
                new TableComparator(queryTable.view("doubleCol", "intCol").wsumBy("doubleCol"), "wsum",
                        queryTable.updateView("W=doubleCol*intCol").groupBy().update("intCol=sum(W)").view("intCol"),
                        "update"),
                new TableComparator(queryTable.view("doubleCol", "doubleCol2").wsumBy("doubleCol2"), "wsum",
                        queryTable.updateView("W=doubleCol*doubleCol2").groupBy().update("doubleCol=sum(W)")
                                .view("doubleCol"),
                        "update"),
        };
        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", Step = " + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }

    }

    @Test
    public void testCountByIncremental() {
        final int[] sizes = {5, 10, 50};
        for (int size : sizes) {
            testCountByIncremental(size);
        }
    }

    private void testCountByIncremental(int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
                                "p"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.countBy("Count", "Sym").sort("Sym")),
                new UpdateValidatorNugget(
                        queryTable.sort("intCol").countBy("Count", "Sym").view("Count=Count * 2", "Sym")),
                new UpdateValidatorNugget(
                        queryTable.sort("doubleCol").avgBy("Sym").view("doubleCol=doubleCol*2", "intCol")),
        };

        for (int i = 0; i < 100; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testMinMaxByIncremental() {
        final int[] sizes = {10, 20, 50, 200};
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                testMinMaxByIncremental(size, seed);
            }
        }
    }

    private void testMinMaxByIncremental(int size, int seed) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "shortCol", "byteCol", "doubleCol", "Timestamp", "boolCol",
                                "betterDoubleCol", "floatCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100, 0.1),
                        new ShortGenerator((short) 10, (short) 100, 0.1),
                        new ByteGenerator((byte) 10, (byte) 100, 0.1),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new UnsortedInstantGenerator(DateTimeUtils.parseInstant("2020-01-01T00:00:00 NY"),
                                DateTimeUtils.parseInstant("2020-01-25T00:00:00 NY")),
                        new BooleanGenerator(0.4, 0.2),
                        new DoubleGenerator(Double.MIN_NORMAL, Double.MIN_NORMAL, 0.05, 0.05),
                        new FloatGenerator(Float.MIN_NORMAL, Float.MIN_NORMAL, 0.05, 0.05)));

        final String[] minQueryStrings = queryTable.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(name -> !name.equals("Sym"))
                .map(name -> name.equals("Timestamp") || name.equals("boolCol") ? name + " = minObj(" + name + ")"
                        : name + " = min(" + name + ")")
                .toArray(String[]::new);

        final String[] maxQueryStrings = queryTable.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(name -> !name.equals("Sym"))
                .map(name -> name.equals("Timestamp") || name.equals("boolCol") ? name + " = maxObj(" + name + ")"
                        : name + " = max(" + name + ")")
                .toArray(String[]::new);

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.Sorted.from(() -> queryTable.maxBy("Sym"), "Sym"),
                EvalNugget.from(() -> queryTable.sort("Sym").maxBy("Sym")),
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sort("intCol").maxBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").maxBy("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1").maxBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .maxBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").maxBy("Sym", "intCol")
                        .sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").maxBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.minBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.sort("Sym").minBy("Sym")),
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sort("intCol").minBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").minBy("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1").minBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .minBy("intCol").sort("intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").minBy("Sym", "intCol")
                        .sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").minBy("Sym").sort("Sym")),
                new TableComparator(queryTable.maxBy("Sym").sort("Sym"),
                        queryTable.groupBy("Sym").update(maxQueryStrings).sort("Sym")),
                new TableComparator(queryTable.minBy("Sym").sort("Sym"),
                        queryTable.groupBy("Sym").update(minQueryStrings).sort("Sym")),
        };
        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", size=" + size + ", step=" + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testMinMaxByAppend() {
        final int[] sizes = {25};
        for (int size : sizes) {
            testMinMaxByAppend(size);
        }
    }

    private static <T extends Table> T setAddOnly(@NotNull final T table) {
        // noinspection unchecked
        return (T) table.withAttributes(Map.of(Table.ADD_ONLY_TABLE_ATTRIBUTE, true));
    }

    private void testMinMaxByAppend(int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = setAddOnly(getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100, 0.1),
                        new SetGenerator<>(10.1, 20.1, 30.1))));
        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.maxBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.dropColumns("Sym").update("x = k")).maxBy("intCol").sort("intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.updateView("x = k").maxBy("Sym", "intCol").sort("Sym", "intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).maxBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1").dropColumns("Sym")).maxBy("intCol")
                                .sort("intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).maxBy("Sym", "intCol").sort("Sym", "intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).maxBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.minBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.dropColumns("Sym").update("x = k")).minBy("intCol").sort("intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.updateView("x = k").minBy("Sym", "intCol").sort("Sym", "intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).minBy("Sym").sort("Sym");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1").dropColumns("Sym")).minBy("intCol")
                                .sort("intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).minBy("Sym", "intCol").sort("Sym", "intCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return setAddOnly(queryTable.update("x=intCol+1")).minBy("Sym").sort("Sym");
                    }
                },
                new TableComparator(queryTable.maxBy("Sym").sort("Sym"),
                        queryTable.applyToAllBy("max(each)", "Sym").sort("Sym")),
                new TableComparator(queryTable.minBy("Sym").sort("Sym"),
                        queryTable.applyToAllBy("min(each)", "Sym").sort("Sym")),
        };
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 50; step++) {
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet keysToAdd =
                        newIndex(random.nextInt(size / 2 + 1), queryTable.getRowSet(), random);
                final ColumnHolder<?>[] columnAdditions = new ColumnHolder[columnInfo.length];
                for (int column = 0; column < columnAdditions.length; column++) {
                    columnAdditions[column] = columnInfo[column].generateUpdateColumnHolder(keysToAdd, random);
                }
                addToTable(queryTable, keysToAdd, columnAdditions);
                queryTable.notifyListeners(keysToAdd, i(), i());
            });
            validate("i = " + step, en);
        }

    }

    @Test
    public void testMedianByIncremental() {
        final int[] sizes = {10, 50, 200};
        for (int size : sizes) {
            testMedianByIncremental(size);
        }
    }

    private void testMedianByIncremental(int size) {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "floatCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(0, 100.0f)));
        final Table withoutFloats = queryTable.dropColumns("floatCol");
        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.dropColumns("Sym").medianBy()),
                EvalNugget.from(() -> queryTable.view("doubleCol").medianBy()),
                EvalNugget.Sorted.from(() -> queryTable.medianBy("Sym"), "Sym"),
                new UpdateValidatorNugget(queryTable.medianBy("Sym")),
                EvalNugget.from(() -> withoutFloats.aggAllBy(percentile(0.25), "Sym").sort("Sym")),
                EvalNugget.from(() -> withoutFloats.aggAllBy(percentile(0.75), "Sym").sort("Sym")),
                EvalNugget.from(() -> withoutFloats.aggAllBy(percentile(0.1), "Sym").sort("Sym")),
                EvalNugget.from(() -> withoutFloats.aggAllBy(percentile(0.99), "Sym").sort("Sym")),
                EvalNugget.from(() -> withoutFloats.where("Sym=`a`").aggAllBy(percentile(0.99), "Sym").sort("Sym"))
        };
        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("size=" + size + ", step=" + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    private static class RMSE {
        long count = 0;
        double squaredError = 0;

        public void add(double error) {
            ++count;
            squaredError += error * error;
        }

        public void add(double... errors) {
            for (double error : errors) {
                add(error);
            }
        }

        public double rmse() {
            return Math.sqrt(squaredError / count);
        }
    }

    private static void checkTDigestError(double error) {
        // if we are within 1/2% we'll pass it
        final double threshold = 0.005;
        assertThat(error)
                .withFailMessage("TDigest error too high. %s >= %s", error, threshold)
                .isLessThan(threshold);
    }

    @Test
    public void testTDigest() {
        final int size = 10000;
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(size, random,
                initColumnInfos(new String[] {"Sym", "intCol", "doubleCol", "floatCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new DoubleGenerator(-10000, 10000, 0.05, 0.05),
                        new FloatGenerator(0, 100.0f)));

        final Table aggregated = ApproximatePercentile.approximatePercentileBy(queryTable.dropColumns("Sym"), 0.99);
        TableTools.showWithRowSet(aggregated);

        final Table aggregatedBySym = ApproximatePercentile.approximatePercentileBy(queryTable, 0.99, "Sym");
        TableTools.showWithRowSet(aggregatedBySym);

        checkTableP99(queryTable, aggregated);
        for (final String sym : new String[] {"a", "b", "c", "d"}) {
            System.out.println("Checking: " + sym);
            checkTableP99(queryTable.where("Sym=`" + sym + "`"), aggregatedBySym.where("Sym=`" + sym + "`"));
        }
    }

    @Test
    public void testTDigestMulti() {
        // Note: when updating t-digest version number or implementation details, we can compare larger sample by
        // commenting out code in checkTDigestError, and upping the number of trials here.
        //
        // With 1000 trials, the current implementation (as of the commit where this line has changed) with
        // t-digest 3.2, achieves RMSE = 9.035463339150259E-4
        final int trials = 1;
        final RMSE rmse = new RMSE();
        for (int seed = 0; seed < trials; ++seed) {
            testTDigestMulti(seed, rmse);
        }
        System.out.println("RMSE: " + rmse.rmse());
    }

    private void testTDigestMulti(int seed, RMSE rmse) {
        final int size = 10000;
        final Random random = new Random(seed);
        final QueryTable queryTable = getTable(size, random,
                initColumnInfos(new String[] {"Sym", "doubleCol", "floatCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new DoubleGenerator(-10000, 10000, 0.05, 0.05),
                        new FloatGenerator(0, 100.0f)));

        final Collection<? extends Aggregation> aggregations = List.of(
                AggApproxPct("doubleCol", PctOut(0.75, "DP75"), PctOut(0.95, "DP95"), PctOut(0.99, "DP99"),
                        PctOut(0.999, "DP999")),
                AggApproxPct("floatCol", PctOut(0.75, "FP75"), PctOut(0.99, "FP99")));
        final Table aggregated = queryTable.dropColumns("Sym").aggBy(aggregations);
        TableTools.showWithRowSet(aggregated);

        final Table aggregatedBySym = queryTable.aggBy(aggregations, "Sym");
        TableTools.showWithRowSet(aggregatedBySym);

        checkTableComboPercentiles(queryTable, aggregated, rmse);
        for (final String sym : new String[] {"a", "b", "c", "d"}) {
            System.out.println("Checking: " + sym);
            checkTableComboPercentiles(queryTable.where("Sym=`" + sym + "`"),
                    aggregatedBySym.where("Sym=`" + sym + "`"), rmse);
        }
    }

    @Test
    public void testTDigestAccumulation() {
        final int size = 10000;
        final Random random = new Random(0);
        final QueryTable queryTable = getTable(size, random,
                initColumnInfos(new String[] {"Sym", "doubleCol", "floatCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new DoubleGenerator(-10000, 10000, 0.05, 0.05),
                        new FloatGenerator(0, 100.0f)));

        final Collection<? extends Aggregation> aggregations33 = List.of(
                AggTDigest(33, "Digest=doubleCol"),
                AggApproxPct("doubleCol", PctOut(0.95, "P95")));
        final Table aggregated = queryTable.dropColumns("Sym").aggBy(aggregations33);
        TableTools.showWithRowSet(aggregated);

        final Collection<? extends Aggregation> aggregations100 = List.of(
                AggTDigest(100, "Digest=doubleCol"),
                AggApproxPct("doubleCol", PctOut(0.95, "P95")));
        final Table aggregatedBySym = queryTable.aggBy(aggregations100, "Sym");
        TableTools.showWithRowSet(aggregatedBySym);

        final Table accumulated = aggregatedBySym.dropColumns("Sym").groupBy()
                .update("Digest=io.deephaven.engine.table.impl.by.ApproximatePercentile.accumulateDigests(Digest)")
                .update("P95=Digest.quantile(0.95)");
        TableTools.show(accumulated);

        final double singleValue = DataAccessHelpers.getColumn(aggregated, "P95").getDouble(0);
        final double accumulatedValue = DataAccessHelpers.getColumn(accumulated, "P95").getDouble(0);
        final double error = Math.abs(singleValue - accumulatedValue) / singleValue;
        if (error > 0.002) {
            System.err.println("Single Value: " + singleValue);
            System.err.println("Accumulated Value: " + accumulatedValue);
        }
        checkTDigestError(error);
    }

    private void checkTableP99(Table queryTable, Table aggregated) {
        final double[] dValues = (double[]) DataAccessHelpers
                .getColumn(queryTable.where("!Double.isNaN(doubleCol) && !isNull(doubleCol)"), "doubleCol").getDirect();
        Arrays.sort(dValues);
        final double dValue = dValues[(dValues.length * 99) / 100];
        final double dtValue = DataAccessHelpers.getColumn(aggregated, "doubleCol").getDouble(0);
        final double derror = Math.abs((dValue - dtValue) / dValue);
        System.out.println("Double: " + dValue + ", " + dtValue + ", Error: " + derror);
        checkTDigestError(derror);

        final float[] fValues = (float[]) DataAccessHelpers
                .getColumn(queryTable.where("!Float.isNaN(floatCol) && !isNull(floatCol)"), "floatCol").getDirect();
        Arrays.sort(fValues);
        final float fValue = fValues[(fValues.length * 99) / 100];
        final double ftValue = DataAccessHelpers.getColumn(aggregated, "floatCol").getDouble(0);
        final double ferror = Math.abs((fValue - ftValue) / fValue);
        System.out.println("Float: " + fValue + ", " + ftValue + ", Error: " + ferror);
        checkTDigestError(ferror);

        final int[] iValues =
                (int[]) DataAccessHelpers.getColumn(queryTable.where("!isNull(intCol)"), "intCol").getDirect();
        Arrays.sort(iValues);
        final float iValue = iValues[(iValues.length * 99) / 100];
        final double itValue = DataAccessHelpers.getColumn(aggregated, "intCol").getDouble(0);
        final double ierror = Math.abs((iValue - itValue) / iValue);
        System.out.println("Int: " + iValue + ", " + itValue + ", Error: " + ierror);
        checkTDigestError(ferror);
    }

    private void checkTableComboPercentiles(Table queryTable, Table aggregated, RMSE rmse) {
        final double[] dValues = (double[]) DataAccessHelpers
                .getColumn(queryTable.where("!Double.isNaN(doubleCol) && !isNull(doubleCol)"), "doubleCol").getDirect();
        Arrays.sort(dValues);
        final double dValue75 = dValues[(dValues.length * 75) / 100];
        final double dtValue75 = DataAccessHelpers.getColumn(aggregated, "DP75").getDouble(0);
        final double derror75 = Math.abs((dValue75 - dtValue75) / dValue75);
        System.out.println("Double 75: " + dValue75 + ", " + dtValue75 + ", Error: " + derror75);
        checkTDigestError(derror75);

        final double dValue99 = dValues[(dValues.length * 99) / 100];
        final double dtValue99 = DataAccessHelpers.getColumn(aggregated, "DP99").getDouble(0);
        final double derror99 = Math.abs((dValue99 - dtValue99) / dValue99);
        System.out.println("Double 99: " + dValue99 + ", " + dtValue99 + ", Error: " + derror99);
        checkTDigestError(derror99);

        final double dValue999 = dValues[(dValues.length * 999) / 1000];
        final double dtValue999 = DataAccessHelpers.getColumn(aggregated, "DP999").getDouble(0);
        final double derror999 = Math.abs((dValue999 - dtValue999) / dValue999);
        System.out.println("Double 99.9:  " + dValue999 + ", " + dtValue999 + ", Error: " + derror999);
        checkTDigestError(derror999);

        final float[] fValues = (float[]) DataAccessHelpers
                .getColumn(queryTable.where("!Float.isNaN(floatCol) && !isNull(floatCol)"), "floatCol").getDirect();
        Arrays.sort(fValues);
        final float fValue75 = fValues[(fValues.length * 75) / 100];
        final double ftValue75 = DataAccessHelpers.getColumn(aggregated, "FP75").getDouble(0);
        final double ferror75 = Math.abs((fValue75 - ftValue75) / fValue75);
        System.out.println("Float 75: " + fValue75 + ", " + ftValue75 + ", Error: " + ferror75);
        checkTDigestError(ferror75);

        final float fValue99 = fValues[(fValues.length * 99) / 100];
        final double ftValue99 = DataAccessHelpers.getColumn(aggregated, "FP99").getDouble(0);
        final double ferror99 = Math.abs((fValue99 - ftValue99) / fValue99);
        System.out.println("Float 99: " + fValue99 + ", " + ftValue99 + ", Error: " + ferror99);
        checkTDigestError(ferror99);

        rmse.add(derror75, derror99, derror999, ferror75, ferror99);
    }

    @Test
    public void testTDigestIncremental() {
        final int size = 10000;
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfos;
        final QueryTable queryTable = getTable(size, random,
                columnInfos = initColumnInfos(new String[] {"Sym", "doubleCol", "longCol"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new DoubleGenerator(10.1, 20.1, 0.05, 0.05),
                        new LongGenerator(0, 1_000_000_000L)));

        final Collection<? extends Aggregation> aggregations = List.of(
                AggApproxPct("doubleCol", PctOut(0.75, "DP75"), PctOut(0.95, "DP95"), PctOut(0.99, "DP99"),
                        PctOut(0.999, "DP999")),
                AggApproxPct("longCol", PctOut(0.75, "LP75"), PctOut(0.95, "LP95"), PctOut(0.99, "LP99"),
                        PctOut(0.999, "LP999")));

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return queryTable.aggBy(aggregations);
                    }

                    @Override
                    protected void checkDifferences(String msg, Table recomputed) {
                        final Table rc = forComparison(recomputed);
                        final Table ov = forComparison(originalValue);

                        TestCase.assertEquals(rc.getRowSet(), i(0));
                        TestCase.assertEquals(ov.getRowSet(), i(0));

                        for (final Map.Entry<String, ? extends ColumnSource<?>> columnSourceEntry : rc
                                .getColumnSourceMap()
                                .entrySet()) {
                            final String name = columnSourceEntry.getKey();
                            final ColumnSource<?> rcs = columnSourceEntry.getValue();
                            final ColumnSource<?> ocs = ov.getColumnSource(name);

                            final double recomputedPercentile = rcs.getDouble(0);
                            final double originalPercentile = ocs.getDouble(0);

                            final double error =
                                    Math.abs((recomputedPercentile - originalPercentile) / recomputedPercentile);
                            if (error > .01) {
                                throw new ComparisonFailure("Bad percentile for " + name,
                                        Double.toString(recomputedPercentile), Double.toString(originalPercentile));
                            }
                        }
                    }
                },
                new EvalNugget.Sorted(new String[] {"Sym"}) {
                    @Override
                    protected Table e() {
                        return queryTable.aggBy(aggregations, "Sym");
                    }

                    @Override
                    protected Table forComparison(Table t) {
                        return super.forComparison(t).flatten();
                    }

                    @Override
                    protected void checkDifferences(String msg, Table recomputed) {
                        final Table rc = forComparison(recomputed);
                        final Table ov = forComparison(originalValue);

                        TestCase.assertEquals(rc.getRowSet(), i(0, 1, 2, 3));
                        TestCase.assertEquals(ov.getRowSet(), i(0, 1, 2, 3));

                        for (final Map.Entry<String, ? extends ColumnSource<?>> columnSourceEntry : rc
                                .getColumnSourceMap()
                                .entrySet()) {
                            final String name = columnSourceEntry.getKey();
                            final ColumnSource<?> rcs = columnSourceEntry.getValue();
                            final ColumnSource<?> ocs = ov.getColumnSource(name);

                            if (name.equals("Sym")) {
                                for (int ii = 0; ii < 4; ++ii) {
                                    TestCase.assertEquals(rcs.get(ii), ocs.get(ii));
                                }
                            } else {
                                for (int ii = 0; ii < 4; ++ii) {
                                    final double recomputedPercentile = rcs.getDouble(ii);
                                    final double originalPercentile = ocs.getDouble(ii);

                                    final double error = Math
                                            .abs((recomputedPercentile - originalPercentile) / recomputedPercentile);
                                    if (error > .03) {
                                        throw new ComparisonFailure("Bad percentile for " + name + ", error=" + error,
                                                Double.toString(recomputedPercentile),
                                                Double.toString(originalPercentile));
                                    }
                                }
                            }
                        }
                    }
                }
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 10; ++step) {
            final int fstep = step;

            System.out.println("Step = " + step);

            // Modifies and Adds in post-shift keyspace.
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(size * (fstep + 1), size * (fstep + 2) - 1);
                queryTable.getRowSet().writableCast().insert(added);

                // Modifies and Adds in post-shift keyspace.
                final ColumnHolder<?>[] columnHolders = new ColumnHolder[columnInfos.length];
                for (int ii = 0; ii < columnInfos.length; ii++) {
                    final ColumnInfo<?, ?> ci = columnInfos[ii];
                    columnHolders[ii] = ci.generateUpdateColumnHolder(added, random);
                }
                addToTable(queryTable, added, columnHolders);

                queryTable.notifyListeners(added, i(), i());
                validate("step = " + fstep, en);
            });
        }
    }

    @Test
    public void testMedianTypes() {
        final Boolean[] booleans = new Boolean[] {null, false, true};
        QueryScope.addParam("booleans", booleans);

        final Table table = emptyTable(10)
                .update("Timestamp='2020-03-14T00:00:00 NY' + DateTimeUtils.MINUTE * i",
                        "MyString=Integer.toString(i)",
                        "MyInt=i",
                        "MyLong=ii",
                        "MyFloat=i * 1.0f",
                        "MyDouble=i * 1.0",
                        "MyBoolean = booleans[i % booleans.length]",
                        "MyChar = (char)('a' + i)",
                        "MyShort=(short)(10 + i)",
                        "MyByte=(byte)(20 + i)",
                        "MyBigDecimal=java.math.BigDecimal.TEN.add(java.math.BigDecimal.valueOf(i))",
                        "MyBigInteger=java.math.BigInteger.ZERO.add(java.math.BigInteger.valueOf(i))");

        TableTools.showWithRowSet(table.meta());
        TableTools.showWithRowSet(table);

        final Table median = table.medianBy();
        final Table percentile10 = table.aggAllBy(percentile(0.1));
        final Table percentile90 = table.aggAllBy(percentile(0.9));
        TableTools.showWithRowSet(median);
        TableTools.showWithRowSet(percentile10);
        TableTools.showWithRowSet(percentile90);

        final Map<String, Object[]> expectedResults = new HashMap<>();
        expectedResults.put("Timestamp",
                new Object[] {
                        DateTimeUtils.parseInstant("2020-03-14T00:01:00 NY"),
                        DateTimeUtils.parseInstant("2020-03-14T00:05:00 NY"),
                        DateTimeUtils.parseInstant("2020-03-14T00:08:00 NY")});
        expectedResults.put("MyString", new Object[] {"1", "5", "8"});
        expectedResults.put("MyInt", new Object[] {1, 4.5, 8});
        expectedResults.put("MyLong", new Object[] {1L, 4.5, 8L});
        expectedResults.put("MyFloat", new Object[] {1f, 4.5f, 8f});
        expectedResults.put("MyDouble", new Object[] {1.0, 4.5, 8.0});
        expectedResults.put("MyBoolean", new Object[] {false, true, true});
        expectedResults.put("MyChar", new Object[] {'b', 'f', 'i'});
        expectedResults.put("MyShort", new Object[] {(short) 11, (short) 15, (short) 18});
        expectedResults.put("MyByte", new Object[] {(byte) 21, (byte) 25, (byte) 28});
        expectedResults.put("MyBigDecimal",
                new Object[] {BigDecimal.valueOf(11), BigDecimal.valueOf(15), BigDecimal.valueOf(18)});
        expectedResults.put("MyBigInteger",
                new Object[] {BigInteger.valueOf(1), BigInteger.valueOf(5), BigInteger.valueOf(8)});

        for (final Map.Entry<String, Object[]> check : expectedResults.entrySet()) {
            final String key = check.getKey();
            final Object[] expectValues = check.getValue();
            final Object medianValue = DataAccessHelpers.getColumn(median, key).get(0);
            final Object p10Value = DataAccessHelpers.getColumn(percentile10, key).get(0);
            final Object p90Value = DataAccessHelpers.getColumn(percentile90, key).get(0);
            TestCase.assertEquals(key + " P10", expectValues[0], p10Value);
            TestCase.assertEquals(key + " median", expectValues[1], medianValue);
            TestCase.assertEquals(key + " P90", expectValues[2], p90Value);
        }

        QueryScope.addParam("booleans", null);

        final IncrementalReleaseFilter incrementalReleaseFilter = new IncrementalReleaseFilter(5, 10);
        final Table updated = table.update("KeyCol=`KeyCol`").where(incrementalReleaseFilter).select();

        final Table refreshing = updated.medianBy();
        final Table refreshingKeys = updated.medianBy("KeyCol");
        TableTools.show(updated);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(incrementalReleaseFilter::run);
        TableTools.show(updated);
        TableTools.show(refreshing);

        for (final Map.Entry<String, Object[]> check : expectedResults.entrySet()) {
            final String key = check.getKey();
            final Object[] expectValues = check.getValue();
            final Object medianValue = DataAccessHelpers.getColumn(refreshing, key).get(0);
            final Object medianKeyValue = DataAccessHelpers.getColumn(refreshingKeys, key).get(0);
            TestCase.assertEquals(key + " median", expectValues[1], medianValue);
            TestCase.assertEquals(key + " median", expectValues[1], medianKeyValue);
        }
    }

    @Test
    public void testCountBy() {
        try {
            newTable().countBy("x = 1");
            TestCase.fail("should throw an exception");
        } catch (RuntimeException e) {
            TestCase.assertTrue(e.getMessage().contains("x = 1"));
        }

        try {
            newTable().countBy("i");
            TestCase.fail("should throw an exception");
        } catch (RuntimeException e) {
            TestCase.assertEquals("Invalid column name \"i\": \"i\" is a reserved keyword", e.getMessage());
        }

        Table table = newTable();
        TestCase.assertEquals(0, table.countBy("count").size());
        TestCase.assertEquals(1, table.countBy("count").numColumns());
        table = emptyTable(10);
        TestCase.assertEquals(1, table.countBy("count").size());
        TestCase.assertEquals(10, DataAccessHelpers.getColumn(table.countBy("count"), "count").getLong(0));
        TestCase.assertEquals(1, table.countBy("count").numColumns());

        table = newTable(col("x", 1, 2, 3));
        TestCase.assertEquals(1, table.countBy("count").size());
        TestCase.assertEquals(3, DataAccessHelpers.getColumn(table.countBy("count"), "count").getLong(0));
        TestCase.assertEquals(1, table.countBy("count").numColumns());

        table = newTable(col("x", 1, 2, 3));
        TestCase.assertEquals(3, table.countBy("count", "x").size());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x"), "x").get(0, 3)));
        TestCase.assertEquals(Arrays.asList(1L, 1L, 1L),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x"), "count").get(0, 3)));
        TestCase.assertEquals(2, table.countBy("count", "x").numColumns());
        try {
            show(table.countBy("count", "x"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        table = newTable(col("x", 1, 2, 2, 2, 3, 3), col("y", 1, 2, 2, 2, 3, 3));
        try {
            show(table.countBy("count", "x", "y"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        TestCase.assertEquals(3, table.countBy("count", "x", "y").size());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "x").get(0, 3)));
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "y").get(0, 3)));
        TestCase.assertEquals(Arrays.asList(1L, 3L, 2L),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "count").get(0, 3)));
        TestCase.assertEquals(3, table.countBy("count", "x", "y").numColumns());

        table = newTable(col("x", 1, 2, 3), col("y", 1, 2, 3));
        TestCase.assertEquals(3, table.countBy("count", "x", "y").size());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "x").get(0, 3)));
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "y").get(0, 3)));
        TestCase.assertEquals(Arrays.asList(1L, 1L, 1L),
                Arrays.asList(DataAccessHelpers.getColumn(table.countBy("count", "x", "y"), "count").get(0, 3)));
        TestCase.assertEquals(3, table.countBy("count", "x", "y").numColumns());
        try {
            show(table.countBy("count", "x", "y"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSelectDistinct() {
        Table table = newTable();
        TestCase.assertEquals(0, table.selectDistinct().size());
        TestCase.assertEquals(0, table.selectDistinct().numColumns());

        table = newTable(col("x", 1, 2, 3, 1));
        System.out.println("Table:");
        show(table);

        Table result = table.selectDistinct("x");
        TestCase.assertEquals(3, result.size());
        TestCase.assertEquals(3, DataAccessHelpers.getColumn(result, "x").size());
        TestCase.assertEquals(1, result.numColumns());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(result, "x").get(0, 3)));

        table = newTable(col("x", 1, 2, 2, 2, 3, 3), col("y", 1, 2, 2, 3, 3, 3));
        System.out.println("Table:");
        show(table);
        result = table.selectDistinct("x");
        TestCase.assertEquals(3, result.size());
        TestCase.assertEquals(3, DataAccessHelpers.getColumn(result, "x").size());
        TestCase.assertEquals(1, result.numColumns());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(result, "x").get(0, 3)));

        result = table.selectDistinct("y");
        TestCase.assertEquals(3, result.size());
        TestCase.assertEquals(3, DataAccessHelpers.getColumn(result, "y").size());
        TestCase.assertEquals(1, result.numColumns());
        TestCase.assertEquals(Arrays.asList(1, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(result, "y").get(0, 3)));

        result = table.selectDistinct("x", "y");
        show(result);
        TestCase.assertEquals(4, result.size());
        TestCase.assertEquals(4, DataAccessHelpers.getColumn(result, "x").size());
        TestCase.assertEquals(4, DataAccessHelpers.getColumn(result, "y").size());
        TestCase.assertEquals(2, result.numColumns());
        TestCase.assertEquals(Arrays.asList(1, 2, 2, 3),
                Arrays.asList(DataAccessHelpers.getColumn(result, "x").get(0, 4)));
        TestCase.assertEquals(Arrays.asList(1, 2, 3, 3),
                Arrays.asList(DataAccessHelpers.getColumn(result, "y").get(0, 4)));
    }

    private static class SelectDistinctEvalNugget implements EvalNuggetInterface {
        String[] columns;
        Table sourceTable;
        Table originalValue;
        Throwable exception;

        SelectDistinctEvalNugget(Table sourceTable, String... columns) {
            this.sourceTable = sourceTable;
            this.columns = columns;
            this.originalValue = e();

            originalValue.addUpdateListener(new InstrumentedTableUpdateListener("Failure Listener") {
                @Override
                public void onUpdate(final TableUpdate update) {}

                @Override
                public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                    exception = originalException;
                }
            });
        }

        public Table e() {
            return sourceTable.selectDistinct(columns).sort(columns);
        }

        public void validate(final String msg) {
            Assert.assertNull(exception);
            // verify that if we recalculate from scratch the answer is the same
            final Table check1 = e();
            final String diff1 = diff(originalValue, check1, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
            Assert.assertEquals(msg, "", diff1);

            // we can also check the table's validity against a countBy
            final Table check2 = sourceTable.countBy("__TEMP__", columns).dropColumns("__TEMP__").sort(columns);
            final String diff2 = diff(originalValue, check2, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
            Assert.assertEquals(msg, "", diff2);
        }

        public void show() {
            System.out.println("Original value: " + originalValue.size());
            TableTools.show(originalValue, 60);

            final Table reevaluated = e();
            System.out.println("Reevaluate: " + reevaluated.size());
            TableTools.show(reevaluated, 60);
        }
    }

    @Test
    public void testSelectDistinctIncremental() {
        final Random random = new Random(0);
        final int size = 20;

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"C1", "C2"},
                new SetGenerator<>("a", "b", "c", "d"),
                new SetGenerator<>(10, 20, 30)));
        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new SelectDistinctEvalNugget(table, "C1"),
                new SelectDistinctEvalNugget(table, "C2"),
                new SelectDistinctEvalNugget(table, "C1", "C2"),
        };
        for (int i = 0; i < 100; i++) {
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, table, columnInfo, en);
        }
    }

    @Test
    public void testSelectDistinctUpdates() {
        final QueryTable table = testRefreshingTable(i(2, 4, 6, 8).toTracking(), col("x", 1, 2, 3, 2));
        final QueryTable result = (QueryTable) (table.selectDistinct("x"))
                .withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true));
        final QueryTableTestBase.ListenerWithGlobals listener;
        result.addUpdateListener(listener = base.newListenerWithGlobals(result));

        // this should result in an new output row
        System.out.println("Adding key 4.");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(7), col("x", 4));
            table.notifyListeners(i(7), i(), i());
        });

        show(table);
        show(result);

        TestCase.assertEquals(4, result.size());
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(i(3), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(), base.removed);

        // we're going to add a duplicate key, which should result in no changes.
        System.out.println("Adding duplicate 1.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            addToTable(table, i(9), col("x", 1));
            table.notifyListeners(i(9), i(), i());
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(4, result.size());
        TestCase.assertEquals(0, listener.getCount());

        // now let's remove one of our rows, but not the last one with a given value, also expecting no changes
        System.out.println("Removing original 1.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            removeRows(table, i(2));
            table.notifyListeners(i(), i(2), i());
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(4, result.size());
        TestCase.assertEquals(0, listener.getCount());

        // remove the last instance of 1, which should remove it from the output table
        System.out.println("Removing last 1.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            removeRows(table, i(9));
            table.notifyListeners(i(), i(9), i());
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(3, result.size());

        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(0), base.removed);

        // add it back
        System.out.println("Putting 1 back at place 9.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            addToTable(table, i(9), col("x", 1));
            table.notifyListeners(i(9), i(), i());
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(4, result.size());

        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(i(0), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(), base.removed);

        // and modify something, but keep the key the same
        System.out.println("False churn of key 1 (at 9).");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            addToTable(table, i(9), col("x", 1));
            table.notifyListeners(i(), i(), i(9));
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(4, result.size());

        TestCase.assertEquals(0, listener.getCount());

        // now modify it so that we generate a new key, but don't change the existing key's existence
        // and modify something, but keep the key the same
        System.out.println("Adding a 5, but not deleting what was at rowSet.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            addToTable(table, i(4), col("x", 5));
            table.notifyListeners(i(), i(), i(4));
        });
        show(table.update("TrackingWritableRowSet=k"));
        show(result.update("TrackingWritableRowSet=k"));

        TestCase.assertEquals(5, result.size());

        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(i(4), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(), base.removed);

        // now modify it so that we remove an existing key
        System.out.println("Adding 5 in a way that deletes 2.");
        updateGraph.runWithinUnitTestCycle(() -> {
            listener.reset();
            addToTable(table, i(8), col("x", 5));
            table.notifyListeners(i(), i(), i(8));
        });

        TestCase.assertEquals(4, result.size());

        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(i(), base.added);
        TestCase.assertEquals(i(), base.modified);
        TestCase.assertEquals(i(1), base.removed);
    }

    @Test
    public void testIds5942() {
        QueryScope.addParam("ids5942_scale", 1000);

        final Table randomValues = emptyTable(100)
                .update("MyInt=(i%12==0 ? null : (int)(ids5942_scale*(Math.random()*2-1)))",
                        "MyBoolean=i%3==0 ? null : (i % 3 == 1)",
                        "MyInstant=epochNanosToInstant(epochNanos(parseInstant(\"2020-01-28T00:00:00 NY\")) + 1000000000L * i)",
                        "MyBigDecimal=(i%21==0 ? null : new java.math.BigDecimal(ids5942_scale*(Math.random()*2-1)))",
                        "MyBigInteger=(i%22==0 ? null : new java.math.BigInteger(Integer.toString((int)(ids5942_scale*(Math.random()*2-1)))))");

        final Table result = randomValues.medianBy("MyInt");

        TableTools.showWithRowSet(result);

        QueryScope.addParam("ids5942_scale", null);
    }

    @Test
    public void testIds5944() {
        QueryScope.addParam("ids5944_scale", 1000);

        final Table randomValues = emptyTable(100)
                .update("MyInt=(i%12==0 ? null : (int)(ids5944_scale*(Math.random()*2-1)))",
                        "MyBigDecimal=(i%21==0 ? null : new java.math.BigDecimal(ids5944_scale*(Math.random()*2-1)))",
                        "MyBigInteger=(i%22==0 ? null : new java.math.BigInteger(Integer.toString((int)(ids5944_scale*(Math.random()*2-1)))))");

        final Table result = randomValues.headBy(10, "MyInt");

        TableTools.showWithRowSet(result);

        QueryScope.addParam("ids5944_scale", null);
    }

    @Test
    public void testLastByNoKeyShift() {
        final QueryTable table = testRefreshingTable(i(0, 1).toTracking(), intCol("Sentinel", 0, 1));
        final Table reversedFlat = table.reverse().flatten().where("Sentinel != 2");
        final Table last = reversedFlat.lastBy();

        final InstrumentedTableUpdateListenerAdapter adapter =
                new InstrumentedTableUpdateListenerAdapter(reversedFlat, false) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        System.out.println(upstream);
                    }
                };
        reversedFlat.addUpdateListener(adapter);

        assertTableEquals(newTable(col("Sentinel", 0)), last);

        TableTools.showWithRowSet(reversedFlat);
        TableTools.showWithRowSet(last);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), intCol("Sentinel", 2));
            table.notifyListeners(i(2), i(), i());
        });

        TableTools.showWithRowSet(reversedFlat);
        TableTools.showWithRowSet(last);

        assertTableEquals(newTable(col("Sentinel", 0)), last);
    }

    @Test
    public void testFirstByShift() {
        final QueryTable table = testRefreshingTable(i(1, 2, 4097).toTracking(),
                intCol("Sentinel", 1, 2, 4097),
                col("Bucket", "A", "B", "A"));

        final Table firstResult = table.firstBy("Bucket");
        final Table lastResult = table.lastBy("Bucket");

        System.out.println("Initial Result");
        TableTools.showWithRowSet(firstResult);
        TableTools.showWithRowSet(lastResult);

        TestCase.assertEquals(2, firstResult.size());
        TestCase.assertEquals(2, lastResult.size());

        TestCase.assertEquals(1, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(1));

        TestCase.assertEquals(4097, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(1));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0), intCol("Sentinel", 0), col("Bucket", "C"));
            table.notifyListeners(i(0), i(), i());
        });

        System.out.println("First C");
        TableTools.showWithRowSet(firstResult);
        TableTools.showWithRowSet(lastResult);

        TestCase.assertEquals(1, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(1));
        TestCase.assertEquals(0, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(2));

        TestCase.assertEquals(4097, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(1));
        TestCase.assertEquals(0, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(2));

        updateGraph.runWithinUnitTestCycle(() -> {
            for (int idx = 3; idx < 4097; ++idx) {
                addToTable(table, i(idx), intCol("Sentinel", idx), col("Bucket", "C"));
            }
            table.notifyListeners(RowSetFactory.fromRange(3, 4096), i(), i());
        });

        System.out.println("Fill in with C");
        TableTools.showWithRowSet(firstResult);
        TableTools.showWithRowSet(lastResult);

        TestCase.assertEquals(1, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(1));
        TestCase.assertEquals(0, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(2));

        TestCase.assertEquals(4097, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(1));
        TestCase.assertEquals(4096, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(2));

        updateGraph.runWithinUnitTestCycle(() -> {
            ((TestColumnSource<?>) table.getColumnSource("Sentinel")).shift(0, 4097, 4096);
            ((TestColumnSource<?>) table.getColumnSource("Bucket")).shift(0, 4097, 4096);
            table.getRowSet().writableCast().removeRange(0, 4095);
            table.getRowSet().writableCast().insertRange(4098, 8193);
            final TableUpdateImpl update = new TableUpdateImpl();
            update.removed = i();
            update.added = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 4097, 4096);
            update.shifted = builder.build();
            table.notifyListeners(update);
        });

        System.out.println("Shift Complete");
        TableTools.showWithRowSet(table);
        System.out.println("First");
        TableTools.showWithRowSet(firstResult);
        System.out.println("Last");
        TableTools.showWithRowSet(lastResult);

        TestCase.assertEquals(1, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(1));
        TestCase.assertEquals(0, DataAccessHelpers.getColumn(firstResult, "Sentinel").getInt(2));

        TestCase.assertEquals(4097, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(0));
        TestCase.assertEquals(2, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(1));
        TestCase.assertEquals(4096, DataAccessHelpers.getColumn(lastResult, "Sentinel").getInt(2));
    }

    @Test
    public void testFirstLastByAttributes() {
        final Random random = new Random(0);

        final int size = 100;
        final QueryTable table = getTable(size, random, initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("aa", "bb", "bc", "cc", "dd"),
                new IntGenerator(0, 100),
                new DoubleGenerator(0, 100)));

        final Object sentinal = new Object();
        table.setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, sentinal);
        for (int i = 0; i < 10; i++) {
            table.setAttribute("Attr" + i, i);
        }

        Table result = table.lastBy("Sym");
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(3, result.getAttributes().size());
            TestCase.assertEquals(
                    Set.of(Table.SYSTEMIC_TABLE_ATTRIBUTE,
                            Table.COLUMN_DESCRIPTIONS_ATTRIBUTE,
                            Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE),
                    result.getAttributes().keySet());
        } else {
            TestCase.assertEquals(1, result.getAttributes().size());
        }
        TestCase.assertEquals(result.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE), sentinal);

        result = table.firstBy("Sym");
        if (SystemicObjectTracker.isSystemicObjectMarkingEnabled()) {
            TestCase.assertEquals(3, result.getAttributes().size());
            TestCase.assertEquals(
                    Set.of(Table.SYSTEMIC_TABLE_ATTRIBUTE,
                            Table.COLUMN_DESCRIPTIONS_ATTRIBUTE,
                            Table.AGGREGATION_ROW_LOOKUP_ATTRIBUTE),
                    result.getAttributes().keySet());
        } else {
            TestCase.assertEquals(1, result.getAttributes().size());
        }
        TestCase.assertEquals(result.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE), sentinal);
    }

    @Test
    public void testIds6220() {
        final QueryTable table = testRefreshingTable(
                RowSetFactory.fromRange(0, 2).toTracking(),
                colIndexed("Key", "a", "b", "c"), col("I", 2, 4, 6));
        final IncrementalReleaseFilter filter = new IncrementalReleaseFilter(0, 10);
        final Table byTable = table.where(filter).groupBy("Key");
        TableTools.showWithRowSet(byTable);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(filter::run);

        TableTools.showWithRowSet(byTable);

        assertTableEquals(table, byTable.ungroup());
    }

    @Test
    public void testIds6203() {
        final String[] keyValues = new String[10000];
        Arrays.fill(keyValues, "Key");
        final int[] sentinels = new int[keyValues.length];
        for (int ii = 0; ii < sentinels.length; ++ii) {
            sentinels[ii] = ii;
        }
        final QueryTable table = testRefreshingTable(
                RowSetFactory.fromRange(100, 100 + keyValues.length - 1).toTracking(),
                stringCol("Key", keyValues), intCol("IntCol", sentinels));

        final Table flat = table.flatten();
        final PartitionedTable partitionedTable = flat.partitionBy("Key");
        final Table subTable = partitionedTable.constituentFor("Key");
        assertTableEquals(subTable, table);

        final FuzzerPrintListener printListener = new FuzzerPrintListener("original", table, 0);
        table.addUpdateListener(printListener);
        final FuzzerPrintListener flatPrintListener = new FuzzerPrintListener("flat", flat, 0);
        flat.addUpdateListener(flatPrintListener);
        final FuzzerPrintListener subPrintListener = new FuzzerPrintListener("subTable", subTable, 0);
        subTable.addUpdateListener(subPrintListener);

        final int newSize = 5;
        final int[] sentinel2 = new int[newSize];
        for (int ii = 0; ii < sentinel2.length; ++ii) {
            sentinel2[ii] = 10000 + ii;
        }
        final String[] keys2 = new String[newSize];
        Arrays.fill(keys2, "Key");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions5 = RowSetFactory.fromRange(0, newSize - 1);
            addToTable(table, additions5, col("Key", keys2), intCol("IntCol", sentinel2));
            table.notifyListeners(additions5, i(), i());
        });

        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removals6 = RowSetFactory.fromRange(100, 100 + newSize - 1);
            removeRows(table, removals6);
            table.notifyListeners(i(), removals6, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        for (int ii = 0; ii < sentinel2.length; ++ii) {
            sentinel2[ii] = 20000 + ii;
        }

        // changed delta
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions4 = RowSetFactory.fromRange(newSize, newSize + newSize - 1);
            final RowSet removals5 = RowSetFactory.fromRange(6000, 6000 + newSize - 3);
            addToTable(table, additions4, col("Key", keys2), intCol("IntCol", sentinel2));
            removeRows(table, removals5);
            table.notifyListeners(additions4, removals5, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        // polarity reversal
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions3 = RowSetFactory.fromRange(newSize * 2, newSize * 3 - 1);
            final RowSet removals4 = RowSetFactory.fromRange(6000 + newSize, 6000 + newSize * 3);
            addToTable(table, additions3, col("Key", keys2), intCol("IntCol", sentinel2));
            removeRows(table, removals4);
            table.notifyListeners(additions3, removals4, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        // prepare a hole
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removals3 = RowSetFactory.fromRange(7000, 7100);
            removeRows(table, removals3);
            table.notifyListeners(i(), removals3, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        for (int ii = 0; ii < sentinel2.length; ++ii) {
            sentinel2[ii] = 30000 + ii;
        }

        // intervening keys
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions1 = RowSetFactory.fromRange(newSize * 3, newSize * 4 - 1);
            final RowSet additions2 = RowSetFactory.fromRange(7000, 7000 + newSize - 1);
            final RowSet removals = RowSetFactory.fromRange(6000 + newSize * 4, 6000 + newSize * 5 - 1);
            addToTable(table, additions1, col("Key", keys2), intCol("IntCol", sentinel2));
            addToTable(table, additions2, col("Key", keys2), intCol("IntCol", sentinel2));
            removeRows(table, removals);
            table.notifyListeners(additions1.union(additions2), removals, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);

        for (int ii = 0; ii < sentinel2.length; ++ii) {
            sentinel2[ii] = 40000 + ii;
        }

        // intervening keys without reversed polarity
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removals1 = RowSetFactory.fromRange(0, newSize - 1);
            final RowSet removals2 = RowSetFactory.fromRange(7000, 7000 + newSize - 1);
            final RowSet allRemovals = removals1.union(removals2);

            final RowSet additions = RowSetFactory.fromRange(6000 + newSize * 4, 6000 + newSize * 5 - 1);
            addToTable(table, additions, col("Key", keys2), intCol("IntCol", sentinel2));
            removeRows(table, allRemovals);
            table.notifyListeners(additions, allRemovals, i());
        });
        assertTableEquals(table, flat);
        assertTableEquals(table, subTable);
    }

    @Test
    public void testIds6321() {
        final QueryTable source = testRefreshingTable(i(9, 10).toTracking(),
                col("Key", "A", "A"), intCol("Sentinel", 9, 10));
        final FuzzerPrintListener soucePrinter = new FuzzerPrintListener("source", source);
        source.addUpdateListener(soucePrinter);

        final QueryTable exposedLastBy = (QueryTable) source.aggBy(Aggregation.AggLastRowKey("ExposedRowRedirection"),
                "Key");
        final TableUpdateValidator validator = TableUpdateValidator.make(exposedLastBy);
        final QueryTable validatorResult = validator.getResultTable();
        final FailureListener validatorListener = new FailureListener();
        validatorResult.addUpdateListener(validatorListener);
        final FuzzerPrintListener printListener = new FuzzerPrintListener("exposedLastBy", exposedLastBy);
        exposedLastBy.addUpdateListener(printListener);

        System.out.println("Starting:");
        TableTools.showWithRowSet(exposedLastBy);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(20), col("Key", "A"), col("Sentinel", 10));
            removeRows(source, i(10));
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = i();
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(10, 10, 10);
            update.shifted = builder.build();
            source.notifyListeners(update);
        });

        System.out.println("Shifted:");
        TableTools.showWithRowSet(exposedLastBy);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(20), col("Key", "A"), intCol("Sentinel", 20));
            source.notifyListeners(i(), i(), i(20));
        });

        System.out.println("Final:");
        TableTools.showWithRowSet(exposedLastBy);

        validator.validate();
    }

    @Test
    public void testIds6332() {
        final QueryTable source = testRefreshingTable(RowSetFactory.flat(10).toTracking(),
                col("Value", BigInteger.valueOf(0), new BigInteger("100"), BigInteger.valueOf(100),
                        new BigInteger("100"), new BigInteger("100"), new BigInteger("100"), new BigInteger("100"),
                        new BigInteger("100"), new BigInteger("100"), BigInteger.valueOf(200)));
        final Table percentile = source.aggAllBy(percentile(0.25));
        TableTools.show(percentile);
        TestCase.assertEquals(BigInteger.valueOf(100), DataAccessHelpers.getColumn(percentile, "Value").get(0));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removeRowSet = RowSetFactory.fromRange(2, 6);
            removeRows(source, removeRowSet);
            source.notifyListeners(i(), removeRowSet, i());
        });

        TableTools.show(percentile);
        TestCase.assertEquals(BigInteger.valueOf(100), DataAccessHelpers.getColumn(percentile, "Value").get(0));
    }

    @Test
    public void testIds7553() {
        final Table result = emptyTable(100).updateView("K=ii%10", "V=ii/10").groupBy("K").ungroup();
        final Table prevResult = prevTableColumnSources(result);
        assertTableEquals(result, prevResult);
    }

    @Test
    public void testInitialGroupsOrdering() {
        // Tests bucketed addition for static tables and static initial groups

        final Table data = testTable(col("S", "A", "B", "C", "D"), col("I", 10, 20, 30, 40));
        final Table distinct = data.selectDistinct();
        assertTableEquals(data, distinct);

        final Table reversed = data.reverse();
        final Table initializedDistinct =
                data.aggBy(List.of(Count.of("C")), false, reversed, ColumnName.from("S", "I")).dropColumns("C");
        assertTableEquals(reversed, initializedDistinct);
    }

    @Test
    public void testInitialGroupsWithGrouping() {
        // Tests grouped addition for static tables and static initial groups

        final Table data = testTable(col("S", "A", "A", "B", "B"), col("I", 10, 20, 30, 40));
        DataIndexer.getOrCreateDataIndex(data, "S");
        final Table distinct = data.selectDistinct("S");
        assertTableEquals(testTable(col("S", "A", "B")), distinct);

        final Table reversed = data.reverse();
        DataIndexer.getOrCreateDataIndex(reversed, "S");
        final Table initializedDistinct =
                data.aggBy(List.of(Count.of("C")), false, reversed, ColumnName.from("S")).dropColumns("C");
        assertTableEquals(testTable(col("S", "B", "A")), initializedDistinct);
    }

    @Test
    public void testInitialGroupsRefreshing() {
        // Tests bucketed addition for refreshing tables and refreshing initial groups

        final Collection<? extends Aggregation> aggs = List.of(
                AggCount("Count"),
                AggSum("SumI=I"),
                AggMax("MaxI=I"),
                AggMin("MinI=I"),
                AggGroup("GroupS=S"));

        final TrackingWritableRowSet inputRows = RowSetFactory.fromRange(0, 9).toTracking();
        final QueryTable input = testRefreshingTable(inputRows,
                col("S", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K"),
                col("C", 'A', 'A', 'B', 'B', 'C', 'C', 'D', 'D', 'E', 'E'),
                col("I", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        inputRows.removeRange(0, 8);

        final Table initialKeys = testRefreshingTable(col("C", 'A', 'B', 'C', 'D', 'E'));

        final Table aggregated = input.aggBy(aggs, true, initialKeys, ColumnName.from("C"));
        final Table initialState = aggregated.snapshot();
        TestCase.assertEquals(5, aggregated.size());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            inputRows.insertRange(0, 8);
            input.notifyListeners(RowSetFactory.fromRange(0, 8), i(), i());
        });
        TestCase.assertEquals(5, aggregated.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            inputRows.removeRange(0, 8);
            input.notifyListeners(i(), RowSetFactory.fromRange(0, 8), i());
        });
        TestCase.assertEquals(5, aggregated.size());

        assertTableEquals(initialState, aggregated);
    }

    @Test
    public void testPreserveEmptyNoKey() {
        final Collection<? extends Aggregation> aggs = List.of(
                AggCount("Count"),
                AggSum("SumI=I"),
                AggMax("MaxI=I"),
                AggMin("MinI=I"));

        final Table expectedEmpty = testTable(
                col("Count", 0L), col("SumI", NULL_LONG_BOXED), col("MaxI", NULL_INT_BOXED),
                col("MinI", NULL_INT_BOXED));

        final TrackingWritableRowSet inputRows = RowSetFactory.fromRange(0, 9).toTracking();
        final QueryTable input = testRefreshingTable(inputRows,
                col("S", "A", "B", "C", "D", "E", "F", "G", "H", "I", "K"),
                col("C", 'A', 'A', 'B', 'B', 'C', 'C', 'D', 'D', 'E', 'E'),
                col("I", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        inputRows.removeRange(0, 9);

        final Table aggregated = input.aggBy(aggs, true);
        TestCase.assertEquals(1, aggregated.size());
        assertTableEquals(expectedEmpty, aggregated);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            inputRows.insertRange(0, 9);
            input.notifyListeners(RowSetFactory.fromRange(0, 9), i(), i());
        });
        TestCase.assertEquals(1, aggregated.size());

        updateGraph.runWithinUnitTestCycle(() -> {
            inputRows.removeRange(0, 9);
            input.notifyListeners(i(), RowSetFactory.fromRange(0, 9), i());
        });
        TestCase.assertEquals(1, aggregated.size());
        assertTableEquals(expectedEmpty, aggregated);
    }

    @Test
    public void testKeyColumnMissing() {
        final Table data = testTable(col("S", "A", "B", "C", "D"), col("I", 10, 20, 30, 40));
        try {
            final Table agg = data.selectDistinct("NonExistentCol");
            fail("Should have thrown an exception");
        } catch (Exception ex) {
            io.deephaven.base.verify.Assert.instanceOf(ex, "ex", IllegalArgumentException.class);
            io.deephaven.base.verify.Assert.assertion(
                    ex.getMessage().contains("Missing columns: [NonExistentCol]"),
                    "ex.getMessage().contains(\"Missing columns: [NonExistentCol]\")",
                    ex.getMessage(),
                    "ex.getMessage()");
        }
    }

    @Test
    public void testMultiPartitionSymbolTableBy() throws IOException {
        final File testRootFile = Files.createTempDirectory(QueryTableAggregationTest.class.getName()).toFile();
        try {
            final Table t1 = new InMemoryTable(
                    new String[] {"StringKeys", "GroupedInts"},
                    new Object[] {
                            new String[] {"key1", "key1", "key2", "key2", "key3", "key2", "key4", "key2", "key1"},
                            new short[] {1, 1, 2, 2, 3, 2, 4, 2, 1}
                    });
            final Table t2 = new InMemoryTable(
                    new String[] {"StringKeys", "GroupedInts"},
                    new Object[] {
                            new String[] {"key4", null, "key3", null, "key1", null, null},
                            new short[] {4, 0, 3, 0, 1, 0, 0}
                    });
            final Table t3 = new InMemoryTable(
                    new String[] {"StringKeys", "GroupedInts"},
                    new Object[] {
                            new String[] {"key5", "key4", "key2", "key5", "key6"},
                            new short[] {5, 4, 2, 5, 6}
                    });
            final Table t4 = new InMemoryTable(
                    new String[] {"StringKeys", "GroupedInts"},
                    new Object[] {
                            new String[] {null, "key6", "key6", "key6", "key6", "key7", null},
                            new short[] {0, 6, 6, 6, 6, 7, 0}
                    });


            ParquetTools.writeTable(t1, new File(testRootFile,
                    "Date=2021-07-20" + File.separator + "Num=100" + File.separator + "file1.parquet"));
            ParquetTools.writeTable(t2, new File(testRootFile,
                    "Date=2021-07-20" + File.separator + "Num=200" + File.separator + "file2.parquet"));
            ParquetTools.writeTable(t3, new File(testRootFile,
                    "Date=2021-07-21" + File.separator + "Num=300" + File.separator + "file3.parquet"));
            ParquetTools.writeTable(t4, new File(testRootFile,
                    "Date=2021-07-21" + File.separator + "Num=400" + File.separator + "file4.parquet"));

            final Table merged = TableTools.merge(
                    t1.updateView("Date=`2021-07-20`", "Num=100"),
                    t2.updateView("Date=`2021-07-20`", "Num=200"),
                    t3.updateView("Date=`2021-07-21`", "Num=300"),
                    t4.updateView("Date=`2021-07-21`", "Num=400")).moveColumnsUp("Date", "Num");

            final Table loaded = ParquetTools.readPartitionedTableInferSchema(
                    new ParquetKeyValuePartitionedLayout(testRootFile.toURI(), 2, ParquetInstructions.EMPTY),
                    ParquetInstructions.EMPTY);

            // verify the sources are identical
            assertTableEquals(merged, loaded);

            final Table merged_summed = merged.aggBy(AggSum("GroupedInts"), "StringKeys");
            final Table loaded_summed = loaded.aggBy(AggSum("GroupedInts"), "StringKeys");

            TableTools.showWithRowSet(loaded_summed);

            // verify aggregations are identical
            assertTableEquals(merged_summed, loaded_summed);
        } finally {
            FileUtils.deleteRecursively(testRootFile);
        }
    }

    @Test
    public void testSymbolTableBy() throws IOException {
        diskBackedTestHarness((table) -> {
            final Table result = table.aggBy(AggSum("Value"), "Symbol");
            TableTools.showWithRowSet(result);

            final long[] values = new long[result.intSize()];
            final MutableInt pos = new MutableInt();
            result.longColumnIterator("Value").forEachRemaining((long value) -> {
                values[pos.intValue()] = value;
                pos.increment();
            });
            assertArrayEquals(new long[] {0, 5, 17, 23}, values);
        });
    }

    private void diskBackedTestHarness(Consumer<Table> testFunction) throws IOException {
        final File directory = Files.createTempDirectory("QueryTableAggregationTest").toFile();

        try {
            final Table table = makeDiskTable(directory);

            testFunction.accept(table);

            table.close();
        } finally {
            FileUtils.deleteRecursively(directory);
        }
    }

    @NotNull
    private Table makeDiskTable(File directory) throws IOException {
        final String[] syms = new String[] {"DragonFruit", "Apple", "Banana", "Cantaloupe",
                "Apple", "Cantaloupe", "Cantaloupe", "Banana", "Banana", "Cantaloupe"};

        final int[] values = new int[syms.length];
        for (int ii = 0; ii < values.length; ii++) {
            values[ii] = ii;
        }

        final TableDefaults result = testTable(stringCol("Symbol", syms),
                intCol("Value", values));

        final File outputFile = new File(directory, "disk_table" + PARQUET_FILE_EXTENSION);

        ParquetTools.writeTable(result, outputFile, result.getDefinition());

        return ParquetTools.readTable(outputFile);
    }
}
