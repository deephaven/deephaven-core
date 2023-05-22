package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.GroupingBuilder;
import io.deephaven.engine.table.GroupingProvider;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.deephaven.engine.table.impl.by.AggregationProcessor.EXPOSED_GROUP_ROW_SETS;

/**
 * A {@link GroupingProvider} implementation that constructs groupings from a statically precomputed grouping table.
 * Users should construct instances using {@link #buildFrom(ColumnSource, String, RowSet)}
 */
public class StaticGroupingProvider extends MemoizingGroupingProvider implements GroupingProvider {
    private final Table baseGrouping;
    private final String keyColumnName;

    private StaticGroupingProvider(@NotNull final Table baseGrouping, @NotNull final String keyColumnName) {
        if (baseGrouping.getColumnSourceMap().size() != 2 ||
                !baseGrouping.hasColumns(keyColumnName, INDEX_COL_NAME)) {
            throw new IllegalArgumentException("Grouping table should only have 2 columns, Index, and " + keyColumnName
                    + ", but has " + baseGrouping.getDefinition().getColumnNamesAsString());
        }
        this.keyColumnName = keyColumnName;
        this.baseGrouping = baseGrouping;
    }

    @NotNull
    @Override
    public StaticGroupingBuilder getGroupingBuilder() {
        return new StaticGroupingBuilder();
    }

    @Override
    public boolean hasGrouping() {
        return true;
    }

    /**
     * Wrap the specified grouping in a new {@link StaticGroupingProvider}.
     *
     * @param inputGrouping The input grouping table to wrap
     * @param valueColumnName The name of the value column in the provided grouping
     * @param indexColumnName The name of the index column in the provided grouping
     * 
     * @return a new {@link StaticGroupingProvider}.
     */
    @NotNull
    public static StaticGroupingProvider buildFrom(@NotNull final Table inputGrouping,
            @NotNull final String valueColumnName,
            @NotNull final String indexColumnName) {
        return new StaticGroupingProvider(
                inputGrouping.renameColumns(INDEX_COL_NAME + "=" + indexColumnName),
                valueColumnName);
    }

    /**
     * Construct a new {@link StaticGroupingProvider} from the specified column source and index of interest. Existing
     * groupings will be used if it makes sense, otherwise new groupings will be computed directly.
     *
     * @param source The column source to group
     * @param rowSetOfInterest the index to use as the whole table index
     * @param <T> The type of the column
     * @return a new {@link StaticGroupingProvider}.
     */
    @NotNull
    public static <T> StaticGroupingProvider buildFrom(@NotNull final ColumnSource<T> source,
            @NotNull final String keyColumnName,
            @NotNull final RowSet rowSetOfInterest) {
        return buildFrom(source, keyColumnName, rowSetOfInterest, false);
    }

    /**
     * Construct a new {@link StaticGroupingProvider} from the specified column source and index of interest. Existing
     * groupings will be used if it makes sense, otherwise new groupings will be computed directly.
     *
     * @param source The column source to group
     * @param rowSetOfInterest the index to use as the whole table index
     * @param useExistingGrouping if existing groups should be used to compute the static grouping.
     * @param <T> The type of the column
     * @return a new {@link StaticGroupingProvider}.
     */
    @NotNull
    public static <T> StaticGroupingProvider buildFrom(@NotNull final ColumnSource<T> source,
            @NotNull final String keyColumnName,
            @NotNull final RowSet rowSetOfInterest,
            final boolean useExistingGrouping) {
        if (useExistingGrouping && source.hasGrouping()) {
            final GroupingBuilder builder = source.getGroupingBuilder().sortByFirstKey();
            Table computedGrouping = builder.buildTable();

            // Just guarantee that the table is always keyColumnName, INDEX as required.
            if (!keyColumnName.equals(builder.getValueColumnName())) {
                computedGrouping = computedGrouping.renameColumns(keyColumnName + "=" + builder.getValueColumnName());
            }

            return new StaticGroupingProvider(computedGrouping, keyColumnName);
        }

        final QueryTable input =
                new QueryTable(rowSetOfInterest.copy().toTracking(), Collections.singletonMap(keyColumnName, source));
        final Table rowSetTable = input
                .aggNoMemo(AggregationProcessor.forExposeGroupRowSets(), false, null, ColumnName.from(keyColumnName))
                .renameColumns(new MatchPair(INDEX_COL_NAME, EXPOSED_GROUP_ROW_SETS.name()));

        return new StaticGroupingProvider(rowSetTable, keyColumnName);
    }

    /**
     * Construct a new {@link StaticGroupingProvider} from the specified grouping map. This will attempt to infer the
     * grouping column type from the values within the grouping.
     *
     * @param baseGrouping the base grouping map.
     * @param <T> the key type.
     * @return a new {@link StaticGroupingProvider}
     */
    @NotNull
    public static <T> StaticGroupingProvider buildFrom(@NotNull final Map<T, RowSet> baseGrouping,
            @NotNull final String groupingColumnName) {
        if (baseGrouping.isEmpty()) {
            throw new IllegalArgumentException("Input grouping can't be empty");
        }

        // At least try to get a real value that isn't null.
        final Iterator<T> iterator = baseGrouping.keySet().iterator();
        Object templateValue = iterator.next();
        if (templateValue == null && iterator.hasNext()) {
            templateValue = iterator.next();
        }

        // noinspection unchecked
        final Class<T> templateClass = (Class<T>) (templateValue == null ? Object.class : templateValue.getClass());
        final Class<?> templateComponentType;
        if (templateClass.isArray()) {
            templateComponentType = templateClass.getComponentType();
        } else if (Vector.class.isAssignableFrom(templateClass)) {
            templateComponentType = ((Vector) templateValue).getComponentType();
        } else {
            templateComponentType = null;
        }

        return buildFrom(baseGrouping, groupingColumnName, templateClass, templateComponentType);
    }

    /**
     * Construct a new {@link StaticGroupingProvider} from the specified grouping map.
     *
     * @param baseGrouping the base grouping map.
     * @param groupingColDef The {@link ColumnDefinition} of the grouping column
     * @param <T> the key type.
     * @return a new {@link StaticGroupingProvider}
     */
    @NotNull
    public static <T> StaticGroupingProvider buildFrom(@NotNull final Map<T, RowSet> baseGrouping,
            @NotNull final ColumnDefinition<T> groupingColDef) {
        return buildFrom(baseGrouping, groupingColDef.getName(), groupingColDef.getDataType(),
                groupingColDef.getComponentType());
    }

    /**
     * Construct a new {@link StaticGroupingProvider} from the specified grouping map.
     *
     * @param baseGrouping the base grouping map.
     * @param groupingValueType The type of the grouping column
     * @param groupingValueComponentType the component type of the grouping column, if it was an array.
     * @param <T> the key type.
     * @return a new {@link StaticGroupingProvider}
     */
    @NotNull
    public static <T> StaticGroupingProvider buildFrom(@NotNull final Map<T, RowSet> baseGrouping,
            @NotNull final String groupingColumnName,
            @NotNull final Class<T> groupingValueType,
            @Nullable final Class<?> groupingValueComponentType) {
        if (baseGrouping.isEmpty()) {
            throw new IllegalArgumentException("Input grouping can't be empty");
        }

        final int numGroups = baseGrouping.size();

        final WritableColumnSource<T> keySource =
                ArrayBackedColumnSource.getMemoryColumnSource(numGroups, groupingValueType, groupingValueComponentType);
        final ObjectArraySource<RowSet> indexSource = new ObjectArraySource<>(RowSet.class);
        indexSource.ensureCapacity(numGroups);

        long groupIndex = 0;
        for (final Map.Entry<T, RowSet> entry : baseGrouping.entrySet()) {
            keySource.set(groupIndex, entry.getKey());
            indexSource.set(groupIndex++, entry.getValue());
        }

        final Map<String, ColumnSource<?>> csm = new LinkedHashMap<>();
        csm.put(groupingColumnName, keySource);
        csm.put(INDEX_COL_NAME, indexSource);
        return new StaticGroupingProvider(new QueryTable(RowSetFactory.flat(groupIndex).toTracking(), csm),
                groupingColumnName);
    }

    public class StaticGroupingBuilder extends AbstractGroupingBuilder {
        @NotNull
        @Override
        public Table buildTable() {
            // TODO: This needs a better solution. see DiskBackedDeferredGroupingProvider#buildTable.
            try (final SafeCloseable ignored = QueryTable.disableParallelWhereForThread()) {
                return memoizeGrouping(makeMemoKey(), this::doBuildTable);
            }
        }

        @NotNull
        private Table doBuildTable() {
            Table result = maybeApplyMatch(baseGrouping);

            if (regionMutators.isEmpty()) {
                return applyIntersectAndInvert(result);
            }

            for (final Function<Table, Table> mutator : regionMutators) {
                result = mutator.apply(result);
                if (result == null || result.isEmpty()) {
                    return null;
                }
            }
            result = condenseGrouping(result);

            return maybeSortByFirsKey(applyIntersectAndInvert(result));
        }

        @Override
        public long estimateGroupingSize() {
            // TODO: We can probably do better, but we need a way to estimate index intersection cardinality
            // without computing the entire index. If we had that, and knew of any key filtering we had to do
            // we could estimate the final grouping size more accurately.
            return baseGrouping.size();
        }

        @NotNull
        @Override
        public String getValueColumnName() {
            return keyColumnName;
        }
    }
}
