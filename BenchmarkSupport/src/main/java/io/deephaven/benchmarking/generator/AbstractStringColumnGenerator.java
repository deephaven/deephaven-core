package io.deephaven.benchmarking.generator;

import io.deephaven.engine.table.ColumnDefinition;

/**
 * The basic implementation of a {@link ColumnGenerator<String>}.
 */
public abstract class AbstractStringColumnGenerator implements ColumnGenerator<String> {
    private final ColumnDefinition<String> def;
    private final int minLength;
    private final int maxLength;

    AbstractStringColumnGenerator(String name, int minLength, int maxLength) {
        this.def = ColumnDefinition.ofString(name);
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    @Override
    public ColumnDefinition<String> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(String)" + varName + ".get()";
    }

    @Override
    public String getName() {
        return def.getName();
    }

    public abstract String get();

    int getMaxLength() {
        return maxLength;
    }

    int getMinLength() {
        return minLength;
    }
}
