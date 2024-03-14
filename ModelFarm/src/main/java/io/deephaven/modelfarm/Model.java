//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.modelfarm;

/**
 * A data driven model.
 */
public interface Model<T> {

    /**
     * Execute the model on a new dataset.
     *
     * @param data data snapshot to execute the model on.
     */
    void exec(final T data);
}
