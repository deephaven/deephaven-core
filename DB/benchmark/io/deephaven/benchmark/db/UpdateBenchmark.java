package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.deephaven.benchmarking.BenchmarkTools.applySparsity;
import static io.deephaven.benchmarking.BenchmarkTools.sizeWithSparsity;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Timeout(time = 3)
@Fork(1)
public class UpdateBenchmark {
    private String[] updateClause;
    private String[] columnList = null;
    private String filterString;
    private TableBenchmarkState state;
    private Table setupTable;

    @Param({"byte",
            "short",
            "int",
            "long",
            "float",
            "double",
            "char",
            "Boolean",
            "String"})
    private String dataType;

    @Param({"OneColumnUnfiltered",
            "OneColumnSelected",
            "TwoColumn",
            "Ternary",
            "Chained"})
    private String testType;

    @Param({"Historical", "Intraday"})
    private String tableType;

    @Param({"100"}) // , "10000", "1000000"})
    private int tableSize;

    @Param({"100", "90", "50", "20"}) // , "10", "5", "1"})
    private int sparsity;

    @Param({"true", "false"})
    private boolean sort;

    @Param({"trivial", "simple"}) // ,"complex"})
    private String complexity;

    private String oneColumnUpdate() {
        switch (complexity) {
            case "trivial":
                return "U1=C2";
            case "simple":
                switch (dataType) {
                    case "String":
                        return "U1=C2 + `_1`";
                    case "double":
                    case "float":
                        return "U1=C2 / 2.0";
                    case "byte":
                    case "short":
                    case "int":
                    case "long":
                        return "U1=C2 / 2";
                    case "char":
                        return "U1=C2 << 2";
                    case "Boolean":
                        return "U1 = !C2";
                    default:
                        throw new IllegalStateException("Unrecognized data type.");
                }
            case "complex":
                // TBI
                return null;
            default:
                throw new IllegalStateException("Unrecognized complexity value.");
        }
    }

    private String twoColumnUpdate() {
        switch (complexity) {
            case "trivial":
                return "U1=C2, U2=C3";
            case "simple":
                switch (dataType) {
                    case "String":
                        return "U1=C2 + C3";
                    case "double":
                    case "float":
                    case "byte":
                    case "short":
                    case "int":
                    case "long":
                        return "U1=(C2 / 2) + (C3 / 2)";
                    case "char":
                        return "U1=C2 | C3";
                    case "Boolean":
                        return "U1 = C2 && C3";
                    default:
                        throw new IllegalStateException("Unrecognized data type.");
                }
            case "complex":
                // TBI
                return null;
            default:
                throw new IllegalStateException("Unrecognized complexity value.");
        }
    }

    private String ternaryUpdate() {
        switch (complexity) {
            case "trivial":
                return "U1=C2 > C3 ? C2 : C3";
            case "simple":
                switch (dataType) {
                    case "String":
                        return "U1=C2 > 4 ? `XYZ` : Stringy";
                    case "double":
                    case "float":
                        return "U1 = C2 > 0.1 ? C3 : -1.5";
                    case "byte":
                    case "short":
                    case "int":
                    case "long":
                        return "U1 = C2 > 1 ? C3 : -1";
                    case "char":
                        return "U1=C2 > 'Z' ? C2 : C3";
                    case "Boolean":
                        return "U1 = C2 ? C2 : C3";
                    default:
                        throw new IllegalStateException("Unrecognized data type.");
                }
            case "complex":
                // TBI
                return null;
            default:
                throw new IllegalStateException("Unrecognized complexity value.");
        }
    }

    private String chainedUpdate() {
        switch (complexity) {
            case "trivial":
                return "U1=C2, U2=U1";
            case "simple":
                switch (dataType) {
                    case "String":
                        return "U1=C2  + `XYZ`, U2 = U1 + `ABC`";
                    case "double":
                    case "float":
                        return "U1 = C2 / 3.1, U2 = U1 * 2.2";
                    case "byte":
                    case "short":
                    case "int":
                    case "long":
                        return "U1 = C2 / 3, U2 = U1 * 2";
                    case "char":
                        return "U1=C2 << 2, U3 = U2 >> 1";
                    case "Boolean":
                        return "U1 = !C2, U3 = !U2";
                    default:
                        throw new IllegalStateException("Unrecognized data type.");
                }
            case "complex":
                // TBI
                return null;
            default:
                throw new IllegalStateException("Unrecognized complexity value.");
        }
    }

    private BenchmarkTable bmTable;

    @Setup(Level.Trial)
    public void setupEnv(BenchmarkParams params) {
        final EnumStringColumnGenerator enumStringyCol =
            (EnumStringColumnGenerator) BenchmarkTools.stringCol("Thingy", 30, 6, 6, 0xB00FB00F);

        final BenchmarkTableBuilder builder;
        switch (tableType) {
            case "Historical":
                builder = BenchmarkTools
                    .persistentTableBuilder("Carlos", sizeWithSparsity(tableSize, sparsity))
                    .addGroupingColumns("Thingy")
                    .setPartitioningFormula("${autobalance_single}")
                    .setPartitionCount(10);
                break;
            case "Intraday":
                builder = BenchmarkTools.persistentTableBuilder("Carlos",
                    sizeWithSparsity(tableSize, sparsity));
                break;

            default:
                throw new IllegalStateException("Table type must be Historical or Intraday");
        }

        builder.setSeed(0xDEADBEEF);

        switch (dataType) {
            case "byte":
                builder.addColumn(BenchmarkTools.numberCol("C2", byte.class));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", byte.class));
                }
                break;
            case "short":
                builder.addColumn(BenchmarkTools.numberCol("C2", short.class));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", short.class));
                }
                break;
            case "int":
                builder.addColumn(BenchmarkTools.numberCol("C2", int.class));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", int.class));
                }
                break;
            case "long":
                builder.addColumn(BenchmarkTools.numberCol("C2", long.class));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", long.class));
                }
                break;
            case "float":
                builder.addColumn(BenchmarkTools.numberCol("C2", float.class, -10e6, 10e6));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", float.class, -10e6, 10e6));
                }
                break;
            case "double":
                builder.addColumn(BenchmarkTools.numberCol("C2", double.class));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.numberCol("C3", double.class));
                }
                break;
            case "char":
                builder.addColumn(BenchmarkTools.charCol("C2", '\u0001', '\ufffe'));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.charCol("C3", '\u0001', '\ufffe'));
                }
                break;
            case "String":
                builder.addColumn(BenchmarkTools.stringCol("C2", 1, 10));
                if (testType.equals("TwoColumn") || testType.equals("Ternary")) {
                    builder.addColumn(BenchmarkTools.stringCol("C3", 1, 10));
                }
                break;
        }

        bmTable = builder
            .addColumn(BenchmarkTools.stringCol("C4", 4, 5, 7, 0xFEEDBEEF))
            .addColumn(enumStringyCol)
            .build();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()),
            params.getWarmup().getCount());

        final List<String> uniqueThingyVals = Arrays.asList(enumStringyCol.getEnumVals());
        final String updateString;

        // filterString (where clause) is unused in the current version of this benchmark, but is
        // included
        // as a model in case it's needed in other benchmarks copied from this class.
        filterString = "";

        switch (testType) {
            case "OneColumnUnfiltered":
                updateString = oneColumnUpdate();
                break;
            case "OneColumnSelected":
                updateString = oneColumnUpdate();
                columnList = new String[] {"C2"};
                break;
            case "TwoColumn":
                updateString = twoColumnUpdate();
                break;
            case "Ternary":
                updateString = ternaryUpdate();
                break;
            case "Chained":
                updateString = chainedUpdate();
                break;
            default:
                throw new IllegalStateException("Can't touch this.");
        }

        updateClause = updateString.split(",");

    }

    @TearDown(Level.Trial)
    public void finishTrial() {
        try {
            state.logOutput();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
        setupTable = applySparsity(bmTable.getTable(), tableSize, sparsity, 0);
        if (columnList != null) {
            setupTable = setupTable.select(columnList);
        }
        if (!filterString.isEmpty()) {
            setupTable = setupTable.where(SelectFilterFactory.getExpression(filterString));
        }
        if (sort) {
            setupTable = setupTable.sort("C2");
        }
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        state.processResult(params);
    }

    @Benchmark
    public Table update() {
        return state.setResult(setupTable.update(updateClause)).coalesce();
    }
}
