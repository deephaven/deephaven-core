package io.deephaven.engine.testutil.generator;

/**
 * A generator that produces random values that are the same as the output column type.
 *
 * @param <T> the generated value and output column type
 */
public abstract class AbstractGenerator<T> extends AbstractReinterpretedGenerator<T, T> {
    @Override
    public Class<T> getColumnType() {
        return getType();
    }
}
