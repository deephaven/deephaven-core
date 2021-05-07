package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SortedFirstOrLastByAggregationFactory implements AggregationContextFactory {
    private final boolean isFirst;
    private final String [] sortColumns;

    public SortedFirstOrLastByAggregationFactory(boolean isFirst, final String ... sortColumns) {
        this.isFirst = isFirst;
        this.sortColumns = sortColumns;
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table, @NotNull final String... groupByColumns) {
        final Set<String> groupBySet = new HashSet<>(Arrays.asList(groupByColumns));
        return getAggregationContext(table, sortColumns, isFirst, table.getDefinition().getColumnNames().stream().filter(col -> !groupBySet.contains(col)).map(name -> new MatchPair(name, name)).toArray(MatchPair[]::new));
    }

    @NotNull
    static AggregationContext getAggregationContext(Table table, String[] sortColumns, boolean isFirst, MatchPair[] resultNames) {
        //noinspection unchecked
        final ChunkSource.WithPrev<Values>[] inputSource = new ChunkSource.WithPrev[1];
        final IterativeChunkedAggregationOperator[] operator = new IterativeChunkedAggregationOperator[1];
        final String [][] name = new String[1][];

        if (sortColumns.length == 1) {
            final ColumnSource columnSource = table.getColumnSource(sortColumns[0]);
            //noinspection unchecked
            inputSource[0] = columnSource;
        }
        else {
            // create a tuple source, because our underlying SSA does not handle multiple sort columns
            final ColumnSource [] sortColumnSources = new ColumnSource[sortColumns.length];
            for (int ii = 0; ii < sortColumnSources.length; ++ii) {
                sortColumnSources[ii] = table.getColumnSource(sortColumns[ii]);
            }
            //noinspection unchecked
            inputSource[0] = TupleSourceFactory.makeTupleSource(sortColumnSources);
        }

        name[0] = sortColumns;
        operator[0] = new SortedFirstOrLastChunkedOperator(inputSource[0].getChunkType(), isFirst, resultNames, table);

        return new AggregationContext(operator, name, inputSource);
    }

    @Override
    public String toString() {
        return (isFirst ? "SortedFirstBy" : "SortedLastBy") + Arrays.toString(sortColumns);
    }
}
