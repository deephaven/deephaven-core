package io.deephaven.db.v2.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FirstOrLastByAggregationFactory implements AggregationContextFactory {
    private final boolean isFirst;
    private final String exposeRedirection;

    public FirstOrLastByAggregationFactory(boolean isFirst) {
        this(isFirst, null);
    }

    public FirstOrLastByAggregationFactory(boolean isFirst, String exposeRedirection) {
        this.isFirst = isFirst;
        this.exposeRedirection = exposeRedirection == null ? null : NameValidator.validateColumnName(exposeRedirection);
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table, @NotNull final String... groupByColumns) {
        //noinspection unchecked
        final ChunkSource.WithPrev<Values>[] inputSource = new ChunkSource.WithPrev[1];
        inputSource[0] = null;

        final IterativeChunkedAggregationOperator[] operator = new IterativeChunkedAggregationOperator[1];
        final String [][] name = new String[1][0];
        name[0] = CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

        final Set<String> groupBySet = new HashSet<>(Arrays.asList(groupByColumns));
        final MatchPair[] matchPairs = table.getDefinition().getColumnNames().stream().filter(col -> !groupBySet.contains(col)).map(col -> new MatchPair(col, col)).toArray(MatchPair[]::new);

        if (table.isLive()) {
            if (((BaseTable) table).isStream()) {
                operator[0] = isFirst ? new StreamFirstChunkedOperator(matchPairs, table) : new StreamLastChunkedOperator(matchPairs, table);
            } else if (((BaseTable) table).isAddOnly()) {
                operator[0] = new AddOnlyFirstOrLastChunkedOperator(isFirst, matchPairs, table, exposeRedirection);
            } else {
                operator[0] = new FirstOrLastChunkedOperator(isFirst, matchPairs, table, exposeRedirection);
            }
        } else {
            operator[0] = new StaticFirstOrLastChunkedOperator(isFirst, matchPairs, table, exposeRedirection);
        }

        return new AggregationContext(operator, name, inputSource);
    }

    @Override
    public String toString() {
        return isFirst ? "FirstBy" : "LastBy";
    }
}
