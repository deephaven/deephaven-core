/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking.generator;

public interface DataGenerator<T> {
    T get();
}
