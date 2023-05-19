package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.GroupingProvider;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class provides data indexes for merged tables. It is responsible for ensuring that the provided table accounts
 * for the relative positions of individual table locations in the provided table of indices.
 *
 * <p>
 * Much like the {@link GroupingProvider} implementations, this also attempts to defer any actual disk accesses until
 * they are absolutely necessary.
 *
 * @implNote This is an experimental feature, it is likely to change.
 */
@InternalUseOnly
public class DataIndexProviderImpl implements DataIndexProvider {
    private final LinkedHashMap<TableLocation, LocationState> locations = new LinkedHashMap<>();

    private static class LocationState {
        private final TableLocation location;
        private final long firstKey;

        private Map<List<String>, SoftReference<Table>> dataIndices;

        private LocationState(TableLocation location, long firstKey) {
            this.location = location;
            this.firstKey = firstKey;
        }

        @Nullable
        private Table getDataIndex(@NotNull final String... keyColumns) {
            // Sort the key columns so we can depend on the list as a lookup key.
            // make a copy of the collection so we don't permute the parameter array.
            final List<String> indexKey = Arrays.stream(keyColumns)
                    .sorted(String::compareTo)
                    .collect(Collectors.toList());

            synchronized (LocationState.this) {
                if (dataIndices != null) {
                    final SoftReference<Table> tableRef = dataIndices.get(indexKey);
                    if (tableRef != null) {
                        final Table result = tableRef.get();
                        if (result != null) {
                            return result;
                        } else {
                            dataIndices.remove(indexKey);
                        }
                    }
                }

                Table indexTable = location.getDataIndex(keyColumns);
                if (indexTable != null) {
                    // Shift the indices onto our range.
                    indexTable = indexTable.update("Index=Index.shift(" + firstKey + ")");

                    if (dataIndices == null) {
                        dataIndices = new HashMap<>();
                    }
                    dataIndices.put(indexKey, new SoftReference<>(indexTable));
                }
                return indexTable;
            }
        }
    }

    /**
     * Add a single location to be tracked by the dataIndexProvider. The firstKey will be used to ensure that any
     * retrieved dataIndexes are properly shifted into the key space for each location.
     *
     * @param location the location to add
     * @param firstKey the first key in the location.
     */
    public void addLocation(TableLocation location, long firstKey) {
        if (locations.put(location, new LocationState(location, firstKey)) != null) {
            throw new IllegalStateException(
                    "The location " + location + " has already been added to this dataIndexProvider");
        }
    }

    // region DataIndexProvider impl
    @Nullable
    @Override
    public Table getDataIndex(@NotNull final String... columns) {
        return QueryPerformanceRecorder.withNugget("Build Data Index [" + String.join(", ", columns) + "]", () -> {
            final Table[] locationIndexes = new Table[locations.size()];
            int tCount = 0;
            for (final LocationState ls : locations.values()) {
                final Table locationIndex = ls.getDataIndex(columns);
                // If any location is missing a data index, we must bail out because we can't guarantee a consistent
                // index.
                if (locationIndex == null) {
                    return null;
                }

                locationIndexes[tCount++] = locationIndex;
            }

            // Ensure we return the final table sorted by firstKey so we preserve encounter order of the original data.
            return TableTools.merge(locationIndexes)
                    .groupBy(columns)
                    .update("Index=io.deephaven.engine.table.impl.dataindex.DataIndexProviderImpl.combineIndices(Index)")
                    .updateView("FirstKey=Index.firstRowJey()")
                    .sort("FirstKey")
                    .dropColumns("FirstKey");
        });
    }

    @SuppressWarnings("unused")
    public static RowSet combineIndices(ObjectVector<RowSet> arr) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < arr.size(); ii++) {
            builder.appendRowSequence(arr.get(ii));
        }
        return builder.build();
    }
    // endregion
}
