package io.deephaven.engine.testutil.generator;

public abstract class AbstractGenerator<T> extends AbstractReinterpretedGenerator<T, T> {
    public Class<T> getColumnType() {
        return getType();
    }
}
