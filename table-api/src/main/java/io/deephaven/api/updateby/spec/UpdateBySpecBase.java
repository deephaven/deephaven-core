//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.api.Pair;
import io.deephaven.api.updateby.ColumnUpdateOperation;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public abstract class UpdateBySpecBase implements UpdateBySpec {

    @Override
    public final ColumnUpdateOperation clause(String pair) {
        return clause(Pair.parse(pair));
    }

    @Override
    public final ColumnUpdateOperation clause(Pair pair) {
        return ColumnUpdateOperation.builder()
                .spec(this)
                .addColumns(pair)
                .build();
    }

    @Override
    public final ColumnUpdateOperation clause(String... pairs) {
        return clause(Arrays.stream(pairs)
                .map(Pair::parse)
                .collect(Collectors.toList()));
    }

    @Override
    public final ColumnUpdateOperation clause(Pair... pairs) {
        return ColumnUpdateOperation.builder()
                .spec(this)
                .addColumns(pairs)
                .build();
    }

    @Override
    public final ColumnUpdateOperation clause(Collection<? extends Pair> pairs) {
        return ColumnUpdateOperation.builder()
                .spec(this)
                .addAllColumns(pairs)
                .build();
    }

    @Override
    public final ColumnUpdateOperation clause() {
        return ColumnUpdateOperation.builder()
                .spec(this)
                .build();
    }

    /**
     * Returns {@code true} if the input class is a primitive or boxed numeric type
     * 
     * @param inputType the input class to test
     */
    static boolean applicableToNumeric(Class<?> inputType) {
        return
        // is primitive numeric?
        inputType == double.class || inputType == float.class
                || inputType == int.class || inputType == long.class || inputType == short.class
                || inputType == byte.class
                // is boxed numeric?
                || Number.class.isAssignableFrom(inputType);
    }
}
