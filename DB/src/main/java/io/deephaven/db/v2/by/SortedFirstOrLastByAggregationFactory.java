package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.StreamTableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SortedFirstOrLastByAggregationFactory implements AggregationContextFactory {

    private final boolean isFirst;
    private final boolean isCombo;
    private final String[] sortColumns;

    public SortedFirstOrLastByAggregationFactory(final boolean isFirst, final boolean isCombo,
            final String... sortColumns) {
        this.isFirst = isFirst;
        this.isCombo = isCombo;
        this.sortColumns = sortColumns;
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
            @NotNull final String... groupByColumns) {
        final Set<String> groupBySet = new HashSet<>(Arrays.asList(groupByColumns));
        return getAggregationContext(table, sortColumns, isFirst, isCombo,
                table.getDefinition().getColumnNames().stream().filter(col -> !groupBySet.contains(col))
                        .map(name -> new MatchPair(name, name)).toArray(MatchPair[]::new));
    }

    @NotNull
    static AggregationContext getAggregationContext(@NotNull final Table table,
            @NotNull final String[] sortColumns,
            final boolean isFirst,
            final boolean isCombo,
            @NotNull final MatchPair[] resultNames) {
        // noinspection unchecked
        final ChunkSource.WithPrev<Values>[] inputSource = new ChunkSource.WithPrev[1];
        final IterativeChunkedAggregationOperator[] operator = new IterativeChunkedAggregationOperator[1];
        final String[][] name = new String[1][];

        if (sortColumns.length == 1) {
            final ColumnSource columnSource = table.getColumnSource(sortColumns[0]);
            // noinspection unchecked
            inputSource[0] = columnSource;
        } else {
            // create a tuple source, because our underlying SSA does not handle multiple sort columns
            final ColumnSource[] sortColumnSources = new ColumnSource[sortColumns.length];
            for (int ii = 0; ii < sortColumnSources.length; ++ii) {
                sortColumnSources[ii] = table.getColumnSource(sortColumns[ii]);
            }
            // noinspection unchecked
            inputSource[0] = TupleSourceFactory.makeTupleSource(sortColumnSources);
        }

        name[0] = sortColumns;
        operator[0] = makeOperator(inputSource[0].getChunkType(), isFirst, isCombo, resultNames, table);

        return new AggregationContext(operator, name, inputSource);
    }

    @Override
    public String toString() {
        return (isFirst ? "SortedFirstBy" : "SortedLastBy") + Arrays.toString(sortColumns);
    }

    private static IterativeChunkedAggregationOperator makeOperator(@NotNull final ChunkType chunkType,
            final boolean isFirst,
            final boolean isCombo,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final Table sourceTable) {
        final boolean isAddOnly = ((BaseTable) sourceTable).isAddOnly();
        final boolean isStream = StreamTableTools.isStream(sourceTable);
        if (isAddOnly) {
            // @formatter:off
            switch (chunkType) {
                case Boolean: throw new UnsupportedOperationException("Columns never use boolean chunks");
                case    Char: return new CharAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case    Byte: return new ByteAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case   Short: return new ShortAddOnlySortedFirstOrLastChunkedOperator( isFirst, resultPairs, sourceTable, null);
                case     Int: return new IntAddOnlySortedFirstOrLastChunkedOperator(   isFirst, resultPairs, sourceTable, null);
                case    Long: return new LongAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case   Float: return new FloatAddOnlySortedFirstOrLastChunkedOperator( isFirst, resultPairs, sourceTable, null);
                case  Double: return new DoubleAddOnlySortedFirstOrLastChunkedOperator(isFirst, resultPairs, sourceTable, null);
                case  Object: return new ObjectAddOnlySortedFirstOrLastChunkedOperator(isFirst, resultPairs, sourceTable, null);
            }
            // @formatter:on
        }
        if (isStream) {
            // @formatter:off
            switch (chunkType) {
                case Boolean: throw new UnsupportedOperationException("Columns never use boolean chunks");
                case    Char: return new CharStreamSortedFirstOrLastChunkedOperator(  isFirst, isCombo, resultPairs, sourceTable);
                case    Byte: return new ByteStreamSortedFirstOrLastChunkedOperator(  isFirst, isCombo, resultPairs, sourceTable);
                case   Short: return new ShortStreamSortedFirstOrLastChunkedOperator( isFirst, isCombo, resultPairs, sourceTable);
                case     Int: return new IntStreamSortedFirstOrLastChunkedOperator(   isFirst, isCombo, resultPairs, sourceTable);
                case    Long: return new LongStreamSortedFirstOrLastChunkedOperator(  isFirst, isCombo, resultPairs, sourceTable);
                case   Float: return new FloatStreamSortedFirstOrLastChunkedOperator( isFirst, isCombo, resultPairs, sourceTable);
                case  Double: return new DoubleStreamSortedFirstOrLastChunkedOperator(isFirst, isCombo, resultPairs, sourceTable);
                case  Object: return new ObjectStreamSortedFirstOrLastChunkedOperator(isFirst, isCombo, resultPairs, sourceTable);
            }
            // @formatter:on
        }
        return new SortedFirstOrLastChunkedOperator(chunkType, isFirst, resultPairs, sourceTable);
    }
}
