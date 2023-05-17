package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.GroupingBuilder;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.KeyRangeGroupingProvider;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link KeyRangeGroupingProvider} for partitioning columns.
 */
public class PartitionColumnGroupingProvider extends MemoizingGroupingProvider implements KeyRangeGroupingProvider {
    final Map<Object, WritableRowSet> partitionMap = new LinkedHashMap<>();
    final ObjectArraySource<Object> baseValueSource =
            (ObjectArraySource<Object>) ArrayBackedColumnSource.getMemoryColumnSource(10, Object.class, null);
    final ObjectArraySource<WritableRowSet> baseWritableRowSetSource =
            (ObjectArraySource<WritableRowSet>) ArrayBackedColumnSource.getMemoryColumnSource(10, WritableRowSet.class,
                    null);
    private final String columnName;

    public static PartitionColumnGroupingProvider make(@NotNull final String name,
            @NotNull final ColumnSource<?> source) {
        if (!(source instanceof RegionedColumnSource) || !((RegionedColumnSource<?>) source).isPartitioning()) {
            throw new IllegalArgumentException("Column source is not partitioning");
        }

        return new PartitionColumnGroupingProvider(name);
    }

    public PartitionColumnGroupingProvider(final String columnName) {
        this.columnName = columnName;
    }

    @NotNull
    @Override
    public GroupingBuilder getGroupingBuilder() {
        return new PartitionedGroupingBuilder();
    }

    @Override
    public void addSource(@NotNull final ColumnLocation columnLocation, @NotNull RowSet locationRowSetInTable) {

        final Object partitionValue =
                columnLocation.getTableLocation().getKey().getPartitionValue(columnLocation.getName());

        final WritableRowSet current = partitionMap.get(partitionValue);
        if (current != null) {
            current.insert(locationRowSetInTable);
        } else {
            final WritableRowSet partitionWritableRowSet = locationRowSetInTable.copy();
            partitionMap.put(partitionValue, partitionWritableRowSet);
            baseValueSource.ensureCapacity(partitionMap.size());
            baseWritableRowSetSource.ensureCapacity(partitionMap.size());
            baseValueSource.set(partitionMap.size() - 1, partitionValue);
            baseWritableRowSetSource.set(partitionMap.size() - 1, partitionWritableRowSet);
        }

        clearMemoizedGroupings();
    }

    @Override
    public boolean hasGrouping() {
        return true;
    }

    private class PartitionedGroupingBuilder extends AbstractGroupingBuilder {
        private Set<Object> matchSet;

        private WritableRowSet collectRelevantPartitions() {
            if (rowSetOfInterest == null && groupKeys == null) {
                return RowSetFactory.flat(partitionMap.size());
            }

            // Construct the matching set if we have to
            if (groupKeys != null && groupKeys.length > 0) {
                if (!matchCase && (groupKeys[0].getClass() == String.class)) {
                    matchSet = Arrays.stream(groupKeys)
                            .map(s -> (Object) ((String) s).toLowerCase()).collect(Collectors.toSet());

                    final Stream<String> stringStream = Arrays.stream(groupKeys).map(s -> (String) s);
                    matchSet = matchCase ? stringStream.collect(Collectors.toSet())
                            : stringStream.map(String::toLowerCase).collect(Collectors.toSet());
                } else {
                    matchSet = Arrays.stream(groupKeys).collect(Collectors.toSet());
                }
            }

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (long ii = 0; ii < partitionMap.size(); ii++) {
                if (match(baseValueSource.get(ii)) && overlaps(ii)) {
                    builder.appendKey(ii);
                }
            }
            return builder.build();
        }

        private boolean overlaps(long ii) {
            return rowSetOfInterest == null || rowSetOfInterest.overlaps(baseWritableRowSetSource.getUnsafe(ii));
        }

        private boolean match(final Object o) {
            return matchSet == null || (matchSet.contains(o) ^ invert);
        }

        @NotNull
        @Override
        public GroupingBuilder sortByFirstKey() {
            // Partitions will always be sorted by first key. There's nothing more to do here.
            return this;
        }

        @Override
        public long estimateGroupingSize() {
            if (rowSetOfInterest == null) {
                return partitionMap.size();
            }

            final WritableRowSet remainingRelevance = rowSetOfInterest.copy();
            long estimatedSize = 0;
            for (long ii = 0; ii < partitionMap.size() && remainingRelevance.isNonempty(); ii++) {
                final long oldCard = remainingRelevance.size();
                remainingRelevance.remove(baseWritableRowSetSource.get(ii));
                if (oldCard > remainingRelevance.size()) {
                    estimatedSize++;
                }
            }

            return estimatedSize;
        }

        @NotNull
        @Override
        public String getValueColumnName() {
            return columnName;
        }

        @Nullable
        @Override
        public Table buildTable() {
            return memoizeGrouping(makeMemoKey(), this::doBuildTable);
        }

        private Table doBuildTable() {
            final WritableRowSet relevantPartitions = collectRelevantPartitions();
            final Map<String, ColumnSource<?>> csm = new LinkedHashMap<>();
            csm.put(columnName, baseValueSource);
            csm.put(INDEX_COL_NAME, baseWritableRowSetSource);
            Table groupingTable = new QueryTable(relevantPartitions.toTracking(), csm);
            if (regionMutators.isEmpty()) {
                return applyIntersectAndInvert(groupingTable);
            }

            Table result = groupingTable;
            for (final Function<Table, Table> mutator : regionMutators) {
                result = mutator.apply(result);
                if (result == null) {
                    return null;
                }
            }
            result = condenseGrouping(result);

            return applyIntersectAndInvert(result);
        }

        @NotNull
        @Override
        public <T> Map<T, RowSet> buildGroupingMap() {
            if (regionMutators.isEmpty()) {
                final WritableRowSet relevantPartitions = collectRelevantPartitions();

                final Map<T, RowSet> grouping = new LinkedHashMap<>();
                relevantPartitions.forAllRowKeys(key -> {
                    WritableRowSet rowSet =
                            strictIntersect ? baseWritableRowSetSource.getUnsafe(key).intersect(rowSetOfInterest)
                                    : baseWritableRowSetSource.get(key);
                    rowSet = inverter != null ? inverter.invert(rowSet) : rowSet;
                    grouping.put((T) baseValueSource.get(key), rowSet);
                });

                // noinspection unchecked
                return (Map<T, RowSet>) grouping;
            }

            return convertToMap(buildTable());
        }
    }
}
