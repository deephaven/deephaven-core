package io.deephaven.benchmarking.generator;

import io.deephaven.benchmarking.generator.random.ExtendedRandom;

public class FuzzyNumColumnGenerator<T extends Number> extends SequentialNumColumnGenerator<T> {
    private final double fuzz;
    private ExtendedRandom random;

    public FuzzyNumColumnGenerator(Class<T> type, String name, double start, double step,
        double fuzz) {
        super(type, name, start, step);
        this.fuzz = fuzz;
    }

    public FuzzyNumColumnGenerator(Class<T> type, String name, double start, double step,
        double max, double fuzz, Mode mode) {
        super(type, name, start, step, max, mode);
        this.fuzz = fuzz;
    }

    @Override
    public void init(ExtendedRandom random) {
        this.random = random;
    }

    @Override
    public double getDouble() {
        return super.getDouble() * random.nextDouble(1.0 - fuzz, 1.0 + fuzz);
    }
}
