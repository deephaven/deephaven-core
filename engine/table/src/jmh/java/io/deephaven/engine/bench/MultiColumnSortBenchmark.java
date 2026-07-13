//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;

/**
 * Compares {@link Table#sort} on two columns using the generated multi-column timsort kernel (which compares each
 * column in turn in a single pass) against the existing pipeline that sorts the first column, finds runs of equal
 * values, and then sorts the second column within those runs.
 *
 * <p>
 * The cardinality parameter is the domain size of the sort columns; it determines how many ties the primary column
 * produces and therefore how much secondary-column work the run-finding pipeline performs.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
public class MultiColumnSortBenchmark {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    @Param({"Sym,I1", "Sym,L1", "Sym,D1", "I1,Sym", "Sym,Sym2", "I1,L1", "Sym,I1,L1", "I1,L1,D1"})
    private String sortCols;

    @Param({"4000000"})
    private int tableSize;

    @Param({"100", "100000", "4000000"})
    private int cardinality;

    @Param({"pipeline", "kernel"})
    private String kernelMode;

    private EngineCleanup engine;
    private Table input;
    private String[] sortColumns;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        QueryTable.USE_GENERATED_SORT_KERNELS = "kernel".equals(kernelMode);
        sortColumns = sortCols.split(",");

        final Random random = new Random(0xDEADBEEF);

        final String[] symDict = new String[cardinality];
        for (int ii = 0; ii < cardinality; ++ii) {
            symDict[ii] = String.format("Sym%09d", ii);
        }
        // the secondary string column has a fixed, smaller domain so that it also contains ties to break
        final int sym2Cardinality = 1009;
        final String[] sym2Dict = new String[sym2Cardinality];
        for (int ii = 0; ii < sym2Cardinality; ++ii) {
            sym2Dict[ii] = String.format("T%06d", ii);
        }

        final String[] syms = new String[tableSize];
        final String[] sym2 = new String[tableSize];
        final int[] i1 = new int[tableSize];
        final long[] l1 = new long[tableSize];
        final double[] d1 = new double[tableSize];
        for (int ii = 0; ii < tableSize; ++ii) {
            syms[ii] = symDict[random.nextInt(cardinality)];
            sym2[ii] = sym2Dict[random.nextInt(sym2Cardinality)];
            i1[ii] = random.nextInt(cardinality);
            l1[ii] = random.nextInt(cardinality);
            d1[ii] = random.nextInt(cardinality);
        }

        input = TableTools.newTable(
                col("Sym", syms),
                col("Sym2", sym2),
                intCol("I1", i1),
                longCol("L1", l1),
                doubleCol("D1", d1));
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        input = null;
        engine.tearDown();
        engine = null;
    }

    @Benchmark
    public Table sort() {
        return input.sort(sortColumns);
    }
}
