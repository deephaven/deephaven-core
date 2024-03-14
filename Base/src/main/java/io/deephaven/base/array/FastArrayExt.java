//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.array;

import io.deephaven.base.Copyable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Supplier;

/**
 *
 */
public final class FastArrayExt<T extends Externalizable & Copyable<T>> extends FastArray<T>
        implements Externalizable, Copyable<FastArrayExt<T>> {

    public static <T extends Externalizable & Copyable<T>> Supplier<FastArrayExt<T>> createFactory(
            final Class<T> clazz, final Supplier<T> itemFactory) {
        return () -> new FastArrayExt<>(clazz, itemFactory);
    }

    /**
     * No empty args constructor. We should never be reading this directly off the wire, always goes through another
     * readExternalizable
     */

    public FastArrayExt(final Class<? extends T> clazz) {
        super(clazz);
    }

    public FastArrayExt(final Class<? extends T> clazz, final int initialSize) {
        super(clazz, initialSize);
    }

    public FastArrayExt(final Class<? extends T> clazz, final Supplier<? extends T> newInstance) {
        super(clazz, newInstance);
    }

    public FastArrayExt(final Class<? extends T> clazz, final Supplier<? extends T> newInstance,
            final int initialSize, final boolean preallocate) {
        super(clazz, newInstance, initialSize, preallocate);
    }

    public FastArrayExt(final Supplier<? extends T> newInstance) {
        super(newInstance);
    }


    @Override
    public void copyValues(final FastArrayExt<T> other) {
        copyValuesDeep(this, other);
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        writeExternal(this, out);
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal(this, in);
    }

    @Override
    public FastArrayExt<T> safeClone() {
        FastArrayExt<T> clone = new FastArrayExt<T>(clazz, newInstance);
        clone.copyValues(this);
        return clone;
    }


}
