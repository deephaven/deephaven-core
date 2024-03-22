//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * A subset of the methods of {@link java.util.Set}, to be used for storing a representation of the keys in a
 * {@link io.deephaven.engine.table.DataIndex}. Single key values are stored as reinterpreted-to-primitive, boxed
 * {@link Object Objects}. Compound keys are stored as {@link Object[] Object arrays} of the same.
 */
public interface DataIndexKeySet {

    boolean add(Object key);

    boolean remove(Object key);

    boolean contains(Object key);

    void forEach(@NotNull final Consumer<? super Object> action);

    Object[] toArray();
}
