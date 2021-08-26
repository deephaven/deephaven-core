package io.deephaven.db.v2.sources.aggregate;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.UngroupableColumnSource;
import io.deephaven.db.v2.sources.UngroupedColumnSource;
import io.deephaven.db.v2.sources.chunk.util.SimpleTypeMap;
import io.deephaven.db.v2.utils.Index;
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
            @NotNull final ColumnSource<Index> indexSource) {
        // noinspection unchecked
        return (AggregateColumnSource<DB_ARRAY_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_CONSTRUCTOR
                .get(aggregatedSource.getType()).apply(aggregatedSource, indexSource);
    }

    final class FactoryHelper {

        private FactoryHelper() {}

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<BiFunction<ColumnSource<?>, ColumnSource<Index>, AggregateColumnSource<?, ?>>> TYPE_TO_CONSTRUCTOR =
                SimpleTypeMap.create(
                // @formatter:off
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> {
                    throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                },
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new     CharAggregateColumnSource((ColumnSource<Character>) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new     ByteAggregateColumnSource((ColumnSource<Byte>     ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new    ShortAggregateColumnSource((ColumnSource<Short>    ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new      IntAggregateColumnSource((ColumnSource<Integer>  ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new     LongAggregateColumnSource((ColumnSource<Long>     ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new    FloatAggregateColumnSource((ColumnSource<Float>    ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new   DoubleAggregateColumnSource((ColumnSource<Double>   ) aggregatedSource, indexSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<Index> indexSource) -> new ObjectAggregateColumnSource<>((ColumnSource<?>        ) aggregatedSource, indexSource)
                // @formatter:on
                );
    }
}
