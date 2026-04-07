//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
public interface AggregateColumnSource<VECTOR_TYPE extends Vector<VECTOR_TYPE>, COMPONENT_TYPE>
        extends UngroupableColumnSource, MutableColumnSourceGetDefaults.ForObject<VECTOR_TYPE> {

    UngroupedColumnSource<COMPONENT_TYPE> ungrouped();

    /**
     * Get the underlying source that is aggregated by this ColumnSource.
     *
     * @return the underlying source that is aggregated by this ColumnSource
     */
    ColumnSource<COMPONENT_TYPE> getAggregatedSource();

    static <VECTOR_TYPE extends Vector<VECTOR_TYPE>, DATA_TYPE> AggregateColumnSource<VECTOR_TYPE, DATA_TYPE> make(
            @NotNull final ColumnSource<DATA_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        // noinspection unchecked
        return (AggregateColumnSource<VECTOR_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_CONSTRUCTOR
                .get(aggregatedSource.getType()).apply(aggregatedSource, groupRowSetSource);
    }

    /**
     * Returns a sliced aggregate ColumnSource from the provided sources.
     *
     * @param aggregatedSource the value column source for the aggregation
     * @param groupRowSetSource the column source that maps rows to group row sets
     * @param startPosSource the column source that maps rows to starting position offsets
     * @param endPosSource the column source that maps rows to ending position offsets (exclusive)
     */
    static <VECTOR_TYPE extends Vector<VECTOR_TYPE>, DATA_TYPE> AggregateColumnSource<VECTOR_TYPE, DATA_TYPE> makeSliced(
            @NotNull final ColumnSource<DATA_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            @NotNull final ColumnSource<Long> startPosSource,
            @NotNull final ColumnSource<Long> endPosSource) {
        // noinspection unchecked
        return (AggregateColumnSource<VECTOR_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_SLICED_CONSTRUCTOR
                .get(aggregatedSource.getType())
                .apply(aggregatedSource, groupRowSetSource, startPosSource, endPosSource);
    }

    /**
     * Returns a sliced aggregate ColumnSource from the provided sources.
     *
     * @param aggregatedSource the value column source for the aggregation
     * @param groupRowSetSource the column source that maps rows to group row sets
     * @param startPosOffset the fixed starting position offset for every row
     * @param endPosOffset the fixed ending position offset for every row (exclusive)
     */
    static <VECTOR_TYPE extends Vector<VECTOR_TYPE>, DATA_TYPE> AggregateColumnSource<VECTOR_TYPE, DATA_TYPE> makeSliced(
            @NotNull final ColumnSource<DATA_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            final long startPosOffset,
            final long endPosOffset) {
        // noinspection unchecked
        return (AggregateColumnSource<VECTOR_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_SLICED_CONSTRUCTOR_FIXED
                .get(aggregatedSource.getType())
                .apply(aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset);
    }

    /**
     * Returns a range aggregated ColumnSource from the provided sources.
     *
     * @param aggregatedSource The input value ColumnSource for the aggregation, in input row key space
     * @param groupRowSetSource ColumnSource of grouped input row sets, in output row key space
     * @param startPositionInclusiveSource ColumnSource of starting positions (inclusive), in output row key space
     * @param endPositionExclusiveSource ColumnSource of ending positions (exclusive), in output row key space
     */
    static <VECTOR_TYPE extends Vector<VECTOR_TYPE>, DATA_TYPE> AggregateColumnSource<VECTOR_TYPE, DATA_TYPE> forRangeJoin(
            @NotNull final ColumnSource<?> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            @NotNull final ColumnSource<Integer> startPositionInclusiveSource,
            @NotNull final ColumnSource<Integer> endPositionExclusiveSource) {
        // noinspection unchecked
        return (AggregateColumnSource<VECTOR_TYPE, DATA_TYPE>) FactoryHelper.TYPE_TO_RANGE_CONSTRUCTOR
                .get(aggregatedSource.getType())
                .apply(aggregatedSource, groupRowSetSource, startPositionInclusiveSource, endPositionExclusiveSource);
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

        @FunctionalInterface
        private interface SlicedConstructor {
            AggregateColumnSource<?, ?> apply(
                    @NotNull ColumnSource<?> aggregatedSource,
                    @NotNull ColumnSource<? extends RowSet> groupRowSetSource,
                    @NotNull ColumnSource<Long> startPosSource,
                    @NotNull ColumnSource<Long> endPosSource);
        }

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<SlicedConstructor> TYPE_TO_SLICED_CONSTRUCTOR =
                SimpleTypeMap.create(
                // @formatter:off
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> {
                            throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                        },
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new     SlicedCharAggregateColumnSource((ColumnSource<Character>) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new     SlicedByteAggregateColumnSource((ColumnSource<Byte>     ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new    SlicedShortAggregateColumnSource((ColumnSource<Short>    ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new      SlicedIntAggregateColumnSource((ColumnSource<Integer>  ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new     SlicedLongAggregateColumnSource((ColumnSource<Long>     ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new    SlicedFloatAggregateColumnSource((ColumnSource<Float>    ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new   SlicedDoubleAggregateColumnSource((ColumnSource<Double>   ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final ColumnSource<Long> startPosSource, final ColumnSource<Long> endPosSource) -> new SlicedObjectAggregateColumnSource<>((ColumnSource<?>        ) aggregatedSource, groupRowSetSource, startPosSource, endPosSource)
                        // @formatter:on
                );

        @FunctionalInterface
        private interface SlicedConstructorFixedOffset {
            AggregateColumnSource<?, ?> apply(
                    @NotNull ColumnSource<?> aggregatedSource,
                    @NotNull ColumnSource<? extends RowSet> groupRowSetSource,
                    long startPosOffset,
                    long endPosOffset);
        }

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<SlicedConstructorFixedOffset> TYPE_TO_SLICED_CONSTRUCTOR_FIXED =
                SimpleTypeMap.create(
                // @formatter:off
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> {
                            throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                        },
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new     SlicedCharAggregateColumnSource((ColumnSource<Character>) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new     SlicedByteAggregateColumnSource((ColumnSource<Byte>     ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new    SlicedShortAggregateColumnSource((ColumnSource<Short>    ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new      SlicedIntAggregateColumnSource((ColumnSource<Integer>  ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new     SlicedLongAggregateColumnSource((ColumnSource<Long>     ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new    SlicedFloatAggregateColumnSource((ColumnSource<Float>    ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new   SlicedDoubleAggregateColumnSource((ColumnSource<Double>   ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset),
                        (final ColumnSource<?> aggregatedSource, final ColumnSource<? extends RowSet> groupRowSetSource, final long startPosOffset, final long endPosOffset) -> new SlicedObjectAggregateColumnSource<>((ColumnSource<?>        ) aggregatedSource, groupRowSetSource, startPosOffset, endPosOffset)
                        // @formatter:on
                );

        @FunctionalInterface
        private interface RangeAggregatedColumnSourceConstructor {
            AggregateColumnSource<?, ?> apply(
                    @NotNull ColumnSource<?> aggregatedSource,
                    @NotNull ColumnSource<? extends RowSet> groupRowSetSource,
                    @NotNull ColumnSource<Integer> startPositionInclusiveSource,
                    @NotNull ColumnSource<Integer> endPositionExclusiveSource);
        }

        @SuppressWarnings({"unchecked", "AutoUnboxing"})
        private static final SimpleTypeMap<RangeAggregatedColumnSourceConstructor> TYPE_TO_RANGE_CONSTRUCTOR =
                SimpleTypeMap.create(
                // @formatter:off
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> {
                            throw new UnsupportedOperationException("Cannot create a primitive boolean ColumnSource");
                        },
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new     RangeAggregateColumnSourceChar((ColumnSource<Character>) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new     RangeAggregateColumnSourceByte((ColumnSource<Byte>     ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new    RangeAggregateColumnSourceShort((ColumnSource<Short>    ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new      RangeAggregateColumnSourceInt((ColumnSource<Integer>  ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new     RangeAggregateColumnSourceLong((ColumnSource<Long>     ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new    RangeAggregateColumnSourceFloat((ColumnSource<Float>    ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new   RangeAggregateColumnSourceDouble((ColumnSource<Double>   ) as, grss, spis, epes),
                        (final ColumnSource<?> as, final ColumnSource<? extends RowSet> grss, final ColumnSource<Integer> spis, final ColumnSource<Integer> epes) -> new RangeAggregateColumnSourceObject<>((ColumnSource<?>        ) as, grss, spis, epes)
                        // @formatter:on
                );
    }
}
