//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Random;
import java.util.function.Function;

public class SetCopyGenerator<T> extends AbstractGenerator<T> {
    private final Function<T, T> copier;
    private final T[] set;
    private final Class<T> type;

    @SafeVarargs
    public SetCopyGenerator(Function<T, T> copier, T... set) {
        this.copier = copier;
        this.set = set;
        // noinspection unchecked
        type = (Class<T>) set.getClass().getComponentType();
    }

    public SetCopyGenerator(Class<T> type, Function<T, T> copier, Collection<T> set) {
        this.type = type;
        this.copier = copier;
        // noinspection unchecked
        this.set = set.toArray((T[]) Array.newInstance(type, set.size()));
    }

    @Override
    public T nextValue(Random random) {
        return copier.apply(set[random.nextInt(set.length)]);
    }

    @Override
    public Class<T> getType() {
        return type;
    }
}
