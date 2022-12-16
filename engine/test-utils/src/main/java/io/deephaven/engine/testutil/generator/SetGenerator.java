package io.deephaven.engine.testutil.generator;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Random;
import java.util.TreeMap;

public class SetGenerator<T> extends AbstractGenerator<T> {

    private final T[] set;
    private final Class<T> type;

    public SetGenerator(T... set) {
        this.set = set;
        // noinspection unchecked
        type = (Class<T>) set.getClass().getComponentType();
    }

    public SetGenerator(Class<T> type, Collection<T> set) {
        this.type = type;
        // noinspection unchecked
        this.set = set.toArray((T[]) Array.newInstance(type, set.size()));
    }

    @Override
    public T nextValue(TreeMap<Long, T> values, long key, Random random) {
        return set[random.nextInt(set.length)];
    }

    @Override
    public Class<T> getType() {
        return type;
    }
}
