package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.tables.dbarrays.DbArrayBase;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.UngroupableColumnSource;
import io.deephaven.engine.v2.sources.UngroupedColumnSource;
import io.deephaven.engine.v2.sources.chunk.util.SimpleTypeMap;
import io.deephaven.engine.v2.utils.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiFunction;

/**
 * {@link ColumnSource} and {@link UngroupableColumnSource} interface for aggregation result columns.
 */
public interface AggregateColumnSource<DB_ARRAY_TYPE extends DbArrayBase, COMPONENT_TYPE>
        extends UngroupableColumnSource, MutableColumnSourceGetDefaults.ForObject<DB_ARRAY_TYPE> {

    UngroupedColumnSource<COMPONENT_TYPE> ungrouped();

    static <DB_ARRAY_TYPE extends DbArrayBase, DATA_TYPE> AggregateColumnSource<DB_ARRAY_TYPE, DATA_TYPE> make(
            @NotNull final ColumnSource<DATA_TYPE> aggregatedSource,
            @NotNull final ColumnSource<RowSet> indexSource) {
        // noinspection unchecked
        return (AggregateColumnSource<DB_ARRAY_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_CONSTRUCTOR
                .get(aggregatedSource.getType()).apply(aggregatedSource, indexSource);
    }

    final class FactoryHelper {

        private FactoryHelper() {}

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<BiFunction<ColumnSource<?>, ColumnSource<RowSet>, AggregateColumnSource<?, ?>>> TYPE_TO_CONSTRUCTOR =
                SimpleTypeMap.create(
                // @formatter:off
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> {
                    throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                },
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new     CharAggregateColumnSource((ColumnSource<Character>) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new     ByteAggregateColumnSource((ColumnSource<Byte>     ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new    ShortAggregateColumnSource((ColumnSource<Short>    ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new      IntAggregateColumnSource((ColumnSource<Integer>  ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new     LongAggregateColumnSource((ColumnSource<Long>     ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new    FloatAggregateColumnSource((ColumnSource<Float>    ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new   DoubleAggregateColumnSource((ColumnSource<Double>   ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<RowSet> indexSource) -> new ObjectAggregateColumnSource<>((ColumnSource<?>        ) aggregatedSource, indexSource)
                // @formatter:on
                );
    }
}
