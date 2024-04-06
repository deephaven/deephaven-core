//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Random;

public class SetGenerator<T> extends AbstractGenerator<T> {

    private final T[] set;
    private final Class<T> type;

    public SetGenerator(T... set) {
        this.set = set;
        // noinspection unchecked
        type = (Class<T>) set.getClass().getComponentType();
        scrubNulls(type);
    }

    public SetGenerator(Class<T> type, Collection<T> set) {
        this.type = type;
        // noinspection unchecked
        this.set = set.toArray((T[]) Array.newInstance(type, set.size()));
        scrubNulls(type);
    }

    private void scrubNulls(Class<T> type) {
        final Class<?> unboxed = TypeUtils.getUnboxedTypeIfBoxed(type);
        if (!unboxed.isPrimitive()) {
            return;
        }
        for (int ii = 0; ii < this.set.length; ++ii) {
            if (this.set[ii] == null) {
                if (unboxed == char.class) {
                    ((Character[]) this.set)[ii] = QueryConstants.NULL_CHAR;
                } else if (unboxed == byte.class) {
                    ((Byte[]) this.set)[ii] = QueryConstants.NULL_BYTE;
                } else if (unboxed == short.class) {
                    ((Short[]) this.set)[ii] = QueryConstants.NULL_SHORT;
                } else if (unboxed == int.class) {
                    ((Integer[]) this.set)[ii] = QueryConstants.NULL_INT;
                } else if (unboxed == long.class) {
                    ((Long[]) this.set)[ii] = QueryConstants.NULL_LONG;
                } else if (unboxed == float.class) {
                    ((Float[]) this.set)[ii] = QueryConstants.NULL_FLOAT;
                } else if (unboxed == double.class) {
                    ((Double[]) this.set)[ii] = QueryConstants.NULL_DOUBLE;
                }
            }
        }
    }

    @Override
    public T nextValue(Random random) {
        return set[random.nextInt(set.length)];
    }

    @Override
    public Class<T> getType() {
        return type;
    }
}
