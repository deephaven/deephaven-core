package io.deephaven.api.updateby.spec;

import io.deephaven.api.agg.Pair;
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
}
