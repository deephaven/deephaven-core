package io.deephaven.api.updateBy.spec;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateBy.ColumnUpdateClause;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public abstract class UpdateBySpecBase implements UpdateBySpec {

    @Override
    public final ColumnUpdateClause clause(String pair) {
        return clause(Pair.parse(pair));
    }

    @Override
    public final ColumnUpdateClause clause(Pair pair) {
        return ColumnUpdateClause.builder()
                .spec(this)
                .addColumns(pair)
                .build();
    }

    @Override
    public final ColumnUpdateClause clause(String... pairs) {
        return clause(Arrays.stream(pairs)
                .map(Pair::parse)
                .collect(Collectors.toList()));
    }

    @Override
    public final ColumnUpdateClause clause(Pair... pairs) {
        return ColumnUpdateClause.builder()
                .spec(this)
                .addColumns(pairs)
                .build();
    }

    @Override
    public final ColumnUpdateClause clause(Collection<? extends Pair> pairs) {
        return ColumnUpdateClause.builder()
                .spec(this)
                .addAllColumns(pairs)
                .build();
    }
}
