package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A builder interface used to create grouping tables or maps.
 */
public interface GroupingBuilder {
    static <DATA_TYPE> Table makeNullOnlyGroupingTable(final ColumnDefinition<DATA_TYPE> columnDefinition, @NotNull final RowSet locationRowSetInTable) {
        final NullValueColumnSource<DATA_TYPE> nullOnlyColumn = NullValueColumnSource.getInstance(columnDefinition.getDataType(), null);
        //noinspection unchecked
        final SingleValueColumnSource<RowSet> indexColumn = SingleValueColumnSource.getSingleValueColumnSource(RowSet.class);
        indexColumn.set(locationRowSetInTable);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put(columnDefinition.getName(), nullOnlyColumn);
        columnSourceMap.put(GroupingProvider.INDEX_COL_NAME, indexColumn);

        return new QueryTable(RowSetFactory.flat(1).toTracking(), columnSourceMap);
    }

    /**
     * Add a method that will process tables per region.  If the mutator returns a null or empty table, it will
     * not be passed to the following mutators.
     *
     * @param mutator a function to process the input tables.
     * @return this {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder addRegionMutator(@NotNull Function<Table, Table> mutator);

    /**
     * Clamp the returned table of indices to the specified range.  In strict mode, guarantees that the indices
     * returned for each group include ony indices in the specified index.
     *
     * @param index the index of interest
     * @param strict when true, the final table will contain only indices that are part of 'index'
     * @return this {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder clampToIndex(@NotNull RowSet index, boolean strict);

    /**
     * Indicate that only groups which intersect the specified index are returned in the final table.  Note that this may include
     * indices in specified groups that are outside the index, but allows the implementation to optimize out entire regions
     * which may not be contained in the specified index.
     *
     * <p>Users may use {@link #clampToIndex(RowSet, boolean)} for more control over this behavior.
     *
     * @param index the index to constrain with
     * @return this {@link GroupingBuilder}
     */
    @NotNull
    @FinalDefault
    default GroupingBuilder clampToIndex(@NotNull RowSet index) {
        return clampToIndex(index, false);
    }

    /**
     * Sort the final grouping table by the first key within each group.  This can be useful to maintain visitation order.
     * @return this {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder sortByFirstKey();

    /**
     * Only include keys in the resultant grouping that match the specified set of grouping keys, optionally
     * ignoring case or inverting the match.
     *
     * @param matchCase if matches should consider case
     * @param invert if the match should be inverted
     * @param groupKeys the set of keys to match
     *
     * @return this {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder matching(boolean matchCase, boolean invert, Object... groupKeys);

    /**
     * Construct groupings as positions, as if the source index were flat.
     * @return this {@link GroupingBuilder}
     */
    GroupingBuilder positionally(final @NotNull RowSet inverter);

    /**
     * Build a Grouping table based on the current configuration of this builder.
     *
     * @return the data index table.
     */
    @Nullable
    Table buildTable();

    /**
     * Build a Map of the Grouping values based on the current configuration of this builder.  Note that the returned map
     * will respect the table order of the final table.
     *
     * @return a properly ordered Map of key to read only index.
     */
    @Nullable
    <T> Map<T, RowSet> buildGroupingMap();

    /**
     * Estimate the total size of the resultant grouping table based on the current builder parameters.  This method
     * is guaranteed to return a worse-case size, that is, the final grouping may be smaller than reported, but will never
     * be larger.
     *
     * @return the estimated size of the grouping provided current parameters.
     */
    long estimateGroupingSize();

    /**
     * Get the name of the column containing the grouping indices.
     * @return the name of the index column
     */
    @NotNull
    default String getIndexColumnName() {
        return GroupingProvider.INDEX_COL_NAME;
    }

    /**
     * Get the name of the column containing the grouping keys.
     * @return the name of the key column
     */
    @NotNull
    String getValueColumnName();
}
