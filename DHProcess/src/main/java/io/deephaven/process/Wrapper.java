package io.deephaven.process;

import org.immutables.value.Value;

public abstract class Wrapper<T> {

    @Value.Parameter
    public abstract T value();

    @Override
    public int hashCode() {
        return value().hashCode();
    }
}
