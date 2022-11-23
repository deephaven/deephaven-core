package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

public class CompositeGenerator<T> implements Generator<T, T> {
    @NotNull
    private final List<Generator<T, T>> generators;
    @NotNull
    private final double[] fractions;

    public CompositeGenerator(List<Generator<T, T>> generators, double... fractions) {
        if (fractions.length != generators.size() - 1) {
            throw new IllegalArgumentException("Generators must have one more element than fractions!");
        }
        final double sum = Arrays.stream(fractions).sum();
        if (sum > 1.0) {
            throw new IllegalArgumentException();
        }
        final Generator<T, T> firstGenerator = generators.get(0);
        for (Generator<T, T> generator : generators) {
            if (!generator.getType().equals(firstGenerator.getType())) {
                throw new IllegalArgumentException(
                        "Mismatched generator types: " + generator.getType() + " vs. " + firstGenerator.getType());
            }
            if (!generator.getColumnType().equals(firstGenerator.getColumnType())) {
                throw new IllegalArgumentException("Mismatched generator column types: " + generator.getType()
                        + " vs. " + firstGenerator.getType());
            }
        }
        this.generators = generators;
        this.fractions = fractions;
    }

    @Override
    public TreeMap<Long, T> populateMap(TreeMap<Long, T> values, RowSet toAdd, Random random) {
        final TreeMap<Long, T> result = new TreeMap<>();
        if (toAdd.isEmpty()) {
            return result;
        }

        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[generators.size()];
        for (int ii = 0; ii < builders.length; ++ii) {
            builders[ii] = RowSetFactory.builderSequential();
        }
        toAdd.forAllRowKeys((long ll) -> builders[pickGenerator(random)].appendKey(ll));

        for (int ii = 0; ii < builders.length; ++ii) {
            final RowSet toAddWithGenerator = builders[ii].build();
            result.putAll(generators.get(ii).populateMap(values, toAddWithGenerator, random));
        }

        return result;
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
}
