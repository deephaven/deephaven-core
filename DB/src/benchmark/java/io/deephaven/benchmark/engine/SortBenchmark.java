package io.deephaven.benchmark.engine;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.v2.ModifiedColumnSet;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.ShiftAwareListener;
import io.deephaven.engine.v2.SortHelpers;
import io.deephaven.engine.v2.select.IncrementalReleaseFilter;
import io.deephaven.engine.v2.select.RollingReleaseFilter;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.IndexShiftData;
import io.deephaven.benchmarking.*;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 15)
@Measurement(iterations = 6, time = 10)
@Timeout(time = 30)
@Fork(1)
public class SortBenchmark {
    private QueryTable inputTable;
    private Table rollingSortTable;
    private BenchmarkTable bmTable;

    private long numSteps;
    private long workingSizeInSteps;
    private RollingReleaseFilter rollingReleaseFilter;

    private ModifiedColumnSet mcsWithSortColumn;
    private ModifiedColumnSet mcsWithoutSortColumn;

    // @Param({"D1", "L1", "Thingy"})
    @Param({"D1"})
    private String sortCol;

    // @Param({"symtab", "nosymtab"})
    @Param({"nosymtab"})
    private String symTab;

    // @Param({"3", "10000", "10000", "0.01", "0.1", "0.25", "0.5", "0.75", "0.99"})
    @Param({"1000"})
    private double enumSize;

    @Param({"Intraday"})
    private String tableType;

    @Param({"100"}) // , "10", "5", "1"})
    private int sparsity;

    // @Param({"250000", "2500000", "25000000"})
    @Param({"25000000"})
    private int tableSize;

    @Param({"20000000"})
    private int workingSize;

    @Param({"1000", "10000", "100000"})
    private int sizePerStep;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        Assert.eqTrue(tableSize % sizePerStep == 0, "Cannot evenly divide input table size by step size.");
        Assert.eqTrue(workingSize % sizePerStep == 0, "Cannot evenly divide working size by step size.");
        workingSizeInSteps = workingSize / sizePerStep;

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        final int nVals = (int) (enumSize < 1 ? enumSize * tableSize : enumSize);
        System.out.println("String Values: " + nVals);
        final EnumStringColumnGenerator enumStringyCol =
                (EnumStringColumnGenerator) BenchmarkTools.stringCol("Thingy", nVals, 6, 6, 0xB00FB00F);

        final BenchmarkTableBuilder builder;
        final int actualSize = BenchmarkTools.sizeWithSparsity(tableSize, sparsity);

        System.out.println("Actual Size: " + actualSize);

        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize)
                        .addGroupingColumns("Thingy")
                        .setPartitioningFormula("${autobalance_single}")
                        .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize);
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        switch (symTab) {
            case "symtab":
                SortHelpers.sortBySymbolTable = true;
                break;
            case "nosymtab":
                SortHelpers.sortBySymbolTable = false;
                break;
            default:
                throw new IllegalStateException("bad sort type: " + symTab);
        }

        bmTable = builder
                .setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF))
                // .addColumn(BenchmarkTools.stringCol("Stringy", 1, 10))
                // .addColumn(BenchmarkTools.numberCol("I1", int.class))
                .addColumn(BenchmarkTools.numberCol("D1", double.class, -10e6, 10e6))
                .addColumn(BenchmarkTools.numberCol("L1", long.class))
                // .addColumn(BenchmarkTools.numberCol("B1", byte.class))
                // .addColumn(BenchmarkTools.numberCol("S1", short.class))
                // .addColumn(BenchmarkTools.numberCol("F1", float.class))
                // .addColumn(BenchmarkTools.charCol("C1", 'A', 'Z'))
                .addColumn(enumStringyCol)
                .build();

        inputTable = (QueryTable) applySparsity(bmTable.getTable(), tableSize, sparsity, 0).coalesce();

        mcsWithSortColumn = inputTable.newModifiedColumnSet(sortCol);
        MutableInt ci = new MutableInt();
        final String[] sortColumns = new String[inputTable.getColumns().length - 1];
        inputTable.getColumnSourceMap().keySet().forEach(columnName -> {
            if (!columnName.equals(sortCol)) {
                sortColumns[ci.intValue()] = columnName;
                ci.increment();
            }
        });
        mcsWithoutSortColumn = inputTable.newModifiedColumnSet(sortColumns);

        numSteps = (inputTable.size() + sizePerStep - 1) / sizePerStep;
        rollingReleaseFilter = new RollingReleaseFilter(workingSize, sizePerStep);

        rollingSortTable = inputTable.where(rollingReleaseFilter).sort(sortCol);

        rollingInputRowSet = TrackingMutableRowSet.FACTORY.getEmptyRowSet();
        rollingInputTable = (QueryTable) inputTable.getSubTable(rollingInputRowSet);
        rollingInputTable.setRefreshing(true);
        rollingOutputTable = rollingInputTable.sort(sortCol);

        LiveTableMonitor.DEFAULT.enableUnitTestMode();
    }

    private long currStep = 0;
    private Table incrementalTable;
    private IncrementalReleaseFilter incrementalReleaseFilter;

    @Benchmark
    public Table incrementalSort() {
        if (currStep == 0) {
            incrementalReleaseFilter = new IncrementalReleaseFilter(sizePerStep, sizePerStep);
            incrementalTable = inputTable.where(incrementalReleaseFilter).sort(sortCol);
        }
        currStep = (currStep + 1) % numSteps;

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::refresh);

        return incrementalTable;
    }

    @Benchmark
    public Table rollingSort() {
        Assert.eq(rollingSortTable.size(), "result.size()", workingSize, "inputTable.size()");
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(rollingReleaseFilter::refresh);
        return rollingSortTable;
    }

    private TrackingMutableRowSet rollingInputRowSet;
    private QueryTable rollingInputTable;
    private Table rollingOutputTable;

    @Benchmark
    public Table rollingWithModNoSort() {
        long modMarker = ((currStep + numSteps - 1) % numSteps) * sizePerStep;
        long addMarker = currStep * sizePerStep;
        long rmMarker = ((currStep + numSteps - workingSizeInSteps) % numSteps) * sizePerStep;
        currStep = (currStep + 1) % numSteps;

        ShiftAwareListener.Update update = new ShiftAwareListener.Update();
        update.added = inputTable.getIndex().subSetByPositionRange(addMarker, addMarker + sizePerStep - 1);
        update.modified = inputTable.getIndex().subSetByPositionRange(modMarker, modMarker + sizePerStep - 1);
        update.removed = inputTable.getIndex().subSetByPositionRange(rmMarker, rmMarker + sizePerStep - 1);
        update.modified.retain(rollingInputRowSet);
        update.removed.retain(rollingInputRowSet);
        update.modifiedColumnSet = mcsWithoutSortColumn;
        update.shifted = IndexShiftData.EMPTY;

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            rollingInputRowSet.update(update.added, update.removed);
            rollingInputTable.notifyListeners(update);
        });

        Assert.eq(rollingOutputTable.getIndex().size(), "rollingOutputTable.build().size()",
                rollingInputRowSet.size(), "rollingInputRowSet.size()");
        return rollingOutputTable;
    }

    @Benchmark
    public Table rollingWithModSort() {
        long modMarker = ((currStep + numSteps - 1) % numSteps) * sizePerStep;
        long addMarker = currStep * sizePerStep;
        long rmMarker = ((currStep + numSteps - workingSizeInSteps) % numSteps) * sizePerStep;
        currStep = (currStep + 1) % numSteps;

        ShiftAwareListener.Update update = new ShiftAwareListener.Update();
        update.added = inputTable.getIndex().subSetByPositionRange(addMarker, addMarker + sizePerStep - 1);
        update.modified = inputTable.getIndex().subSetByPositionRange(modMarker, modMarker + sizePerStep - 1);
        update.removed = inputTable.getIndex().subSetByPositionRange(rmMarker, rmMarker + sizePerStep - 1);
        update.modified.retain(rollingInputRowSet);
        update.removed.retain(rollingInputRowSet);
        update.modifiedColumnSet = mcsWithSortColumn;
        update.shifted = IndexShiftData.EMPTY;

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            rollingInputRowSet.update(update.added, update.removed);
            rollingInputTable.notifyListeners(update);
        });

        Assert.eq(rollingOutputTable.getIndex().size(), "rollingOutputTable.build().size()",
                rollingInputRowSet.size(), "rollingInputRowSet.size()");
        return rollingOutputTable;
    }

    public static void main(final String[] args) {
        BenchUtil.run(SortBenchmark.class);
    }
}
