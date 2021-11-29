package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.vector.Vector;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.UngroupableColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SimpleTypeMap;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiFunction;

/**
 * {@link ColumnSource} and {@link UngroupableColumnSource} interface for aggregation result columns.
 */
public interface AggregateColumnSource<DB_ARRAY_TYPE extends Vector, COMPONENT_TYPE>
        extends UngroupableColumnSource, MutableColumnSourceGetDefaults.ForObject<DB_ARRAY_TYPE> {

    UngroupedColumnSource<COMPONENT_TYPE> ungrouped();

    static <DB_ARRAY_TYPE extends Vector, DATA_TYPE> AggregateColumnSource<DB_ARRAY_TYPE, DATA_TYPE> make(
            @NotNull final ColumnSource<DATA_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        // noinspection unchecked
        return (AggregateColumnSource<DB_ARRAY_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_CONSTRUCTOR
                .get(aggregatedSource.getType()).apply(aggregatedSource, groupRowSetSource);
    }

    final class FactoryHelper {

        private FactoryHelper() {}

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<BiFunction<ColumnSource<?>, ColumnSource<? extends RowSet>, AggregateColumnSource<?, ?>>> TYPE_TO_CONSTRUCTOR =
                SimpleTypeMap.create(
                // @formatter:off
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> {
                    throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                },
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new     CharAggregateColumnSource((ColumnSource<Character>) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new     ByteAggregateColumnSource((ColumnSource<Byte>     ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new    ShortAggregateColumnSource((ColumnSource<Short>    ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new      IntAggregateColumnSource((ColumnSource<Integer>  ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new     LongAggregateColumnSource((ColumnSource<Long>     ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new    FloatAggregateColumnSource((ColumnSource<Float>    ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new   DoubleAggregateColumnSource((ColumnSource<Double>   ) aggregatedSource, groupRowSetSource),
                (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource) -> new ObjectAggregateColumnSource<>((ColumnSource<?>        ) aggregatedSource, groupRowSetSource)
                // @formatter:on
                );
    }
}
