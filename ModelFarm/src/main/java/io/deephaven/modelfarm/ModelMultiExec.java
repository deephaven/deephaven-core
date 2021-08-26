/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import io.deephaven.base.verify.Require;

import java.util.Iterator;

/**
 * A model for executing multiple models.
 */
public class ModelMultiExec<T> implements Model<T> {

    /**
     * An interface for determining which models get executed for each model data instance.
     *
     * @param <T> model input data type
     */
    public interface Models<T> {
        /**
         * Gets the synchronization lock to hold while executing the models in the iterator.
         *
         * @param data model data
         * @return synchronization lock to hold while executing the models in the iterator.
         */
        Object getLock(final T data);

        /**
         * Creates an iterator for the models to exec for the model data instance.
         *
         * @param data model data
         * @return iterator for the models to exec for the model data instance.
         */
        Iterator<Model<T>> iterator(final T data);
    }

    private final Models<T> models;

    /**
     * Creates a new model which is composed of other models.
     *
     * @param models constituent models
     */
    public ModelMultiExec(final Models<T> models) {
        Require.neqNull(models, "models");

        this.models = models;
    }

    @Override
    public void exec(final T data) {
        synchronized (models.getLock(data)) {
            final Iterator<Model<T>> it = models.iterator(data);

            while (it.hasNext()) {
                Model<T> m = it.next();
                m.exec(data);
            }
        }
    }

}
