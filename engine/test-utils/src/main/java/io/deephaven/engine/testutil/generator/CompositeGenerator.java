package io.deephaven.engine.testutil.generator;

import io.deephaven.base.Pair;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

public class CompositeGenerator<T> implements TestDataGenerator<T, T> {
    @NotNull
    private final List<TestDataGenerator<T, T>> generators;
    @NotNull
    private final double[] fractions;

    public CompositeGenerator(List<TestDataGenerator<T, T>> generators, double... fractions) {
        if (fractions.length != generators.size() - 1) {
            throw new IllegalArgumentException("Generators must have one more element than fractions!");
        }
        final double sum = Arrays.stream(fractions).sum();
        if (sum > 1.0) {
            throw new IllegalArgumentException();
        }
        final TestDataGenerator<T, T> firstGenerator = generators.get(0);
        for (TestDataGenerator<T, T> generator : generators) {
            if (!generator.getType().equals(firstGenerator.getType())) {
                throw new IllegalArgumentException(
                        "Mismatched generator types: " + generator.getType() + " vs. " + firstGenerator.getType());
            }
            if (!generator.getColumnType().equals(firstGenerator.getColumnType())) {
                throw new IllegalArgumentException("Mismatched generator column types: " + generator.getType() + " vs. "
                        + firstGenerator.getType());
            }
        }
        this.generators = generators;
        this.fractions = fractions;
    }

    @Override
    public Chunk<Values> populateChunk(RowSet toAdd, Random random) {
        if (toAdd.isEmpty()) {
            return ObjectChunk.getEmptyChunk();
        }

        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[generators.size()];
        final RowSet[] builderRowSet = new RowSet[generators.size()];
        for (int ii = 0; ii < builders.length; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        toAdd.forAllRowKeys((long ll) -> builders[pickGenerator(random)].appendKey(ll));

        // noinspection unchecked
        final ObjectChunk<T, Values>[] intermediateResults =
                (ObjectChunk<T, Values>[]) new ObjectChunk[builders.length];
        final ChunkBoxer.BoxerKernel[] boxerKernel = new ChunkBoxer.BoxerKernel[builders.length];
        final MutableInt[] offsets = new MutableInt[builders.length];

        final PriorityQueue<Pair<RowSet.SearchIterator, Integer>> priorityQueue =
                new PriorityQueue<>(Comparator.comparingLong(a -> a.first.currentValue()));
        for (int ii = 0; ii < builders.length; ++ii) {
            builderRowSet[ii] = builders[ii].build().asRowSet();
            final Chunk<Values> generatorResults = generators.get(ii).populateChunk(builderRowSet[ii], random);
            boxerKernel[ii] = ChunkBoxer.getBoxer(generatorResults.getChunkType(), generatorResults.size());
            // noinspection unchecked
            intermediateResults[ii] = (ObjectChunk<T, Values>) boxerKernel[ii].box(generatorResults);

            offsets[ii] = new MutableInt();
            RowSet.SearchIterator it = builderRowSet[ii].searchIterator();
            if (it.hasNext()) {
                it.nextLong();
                priorityQueue.add(new Pair<>(it, ii));
            }
        }

        // noinspection unchecked
        final T[] result = (T[]) Array.newInstance(getType(), toAdd.intSize());
        Pair<RowSet.SearchIterator, Integer> top;
        int idx = 0;
        while ((top = priorityQueue.poll()) != null) {
            final int generatorNumber = top.second;
            result[idx++] = intermediateResults[generatorNumber].get(offsets[generatorNumber].getAndIncrement());
            if (top.first.hasNext()) {
                top.first.nextLong();
                priorityQueue.add(top);
            }
        }

        SafeCloseable.closeAll(boxerKernel);

        return ObjectChunk.chunkWrap(result);
    }

    @Override
    public Class<T> getType() {
        return generators.get(0).getType();
    }

    @Override
    public Class<T> getColumnType() {
        return generators.get(0).getColumnType();
    }

    private int pickGenerator(Random random) {
        final double whichGenerator = random.nextDouble();
        double sum = 0;
        int pickGenerator = 0;
        while (true) {
            if (pickGenerator == generators.size() - 1) {
                break;
            }
            sum += fractions[pickGenerator];
            if (sum < whichGenerator) {
                pickGenerator++;
            } else {
                break;
            }
        }
        return pickGenerator;
    }

    @Override
    public void onRemove(RowSet toRemove) {
        generators.forEach(gg -> gg.onRemove(toRemove));
    }

    @Override
    public void shift(long start, long end, long delta) {
        generators.forEach(gg -> gg.shift(start, end, delta));
    }
}
