package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.test.types.ParallelTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Random;
import java.util.function.Function;

@Category(ParallelTest.class)
public class TestSortIncrementalPerformance {
    @Test
    public void incrementalSort() {
        // warmup
        incrementalSort(0, 100_000, 10);

        for (int size = 10_000; size < 10_000_000; size *= 10) {
            for (int steps : new int[] {10}) {
                incrementalSort(size, steps);
            }
        }
    }

    private void incrementalSort(int size, int steps) {
        System.out.println("Size = " + size);

        final int count = 1;
        long sum = 0;

        // warmup
        incrementalSort(0, size, steps);

        for (int seed = 0; seed < count; ++seed) {
            final long duration = incrementalSort(seed, size, steps);
            System.out.println("Duration: " + (duration / 1000_000_000.0));
            sum += duration;
        }

        final double lgsize = Math.log(size) / Math.log(2);
        System.out.println("Size = " + size + ", Steps = " + steps + ", Average (s): "
                + ((double) sum / count) / 1000_000_000.0 + ", ns/Element: " + (double) (sum / (size * count))
                + ", ns/n lg n: " + (double) (sum / (size * lgsize * count)));
    }

    private long incrementalSort(int seed, long size, int steps) {
        final Random random = new Random(seed);
        QueryScope.addParam("random", random);
        final Table tableToSort =
                TableTools.emptyTable(size).update("Sentinel=ii", "D=random.nextDouble()", "L=random.nextLong()");

        final long start = System.nanoTime();
        final Table result = incrementalBenchmark(tableToSort, (Table t) -> t.sort("D"), steps);
        final long end = System.nanoTime();
        Assert.eq(result.size(), "result.size()", tableToSort.size(), "inputTable.size()");

        return (end - start);
    }

    private <R> R incrementalBenchmark(Table inputTable, Function<Table, R> function, int steps) {
        final long sizePerStep = Math.max(inputTable.size() / steps, 1);
        final IncrementalReleaseFilter incrementalReleaseFilter =
                new IncrementalReleaseFilter(sizePerStep, sizePerStep);
        final Table filtered = inputTable.where(incrementalReleaseFilter);

        final R result = function.apply(filtered);

        while (filtered.size() < inputTable.size()) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incrementalReleaseFilter::run);
        }

        return result;
    }

    @Rule
    public final EngineCleanup rule = new EngineCleanup();
}
