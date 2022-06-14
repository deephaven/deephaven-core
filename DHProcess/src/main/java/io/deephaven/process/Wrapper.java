/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
