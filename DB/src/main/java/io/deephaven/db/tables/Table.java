/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationOutputs;
import io.deephaven.api.agg.Array;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.Function;
import io.deephaven.base.Pair;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.remote.*;
import io.deephaven.db.tables.select.*;
import io.deephaven.db.tables.utils.*;
import io.deephaven.db.util.ColumnFormattingValues;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.util.liveness.LivenessNode;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.by.AggregationFormulaStateFactory;
import io.deephaven.db.v2.by.AggregationIndexStateFactory;
import io.deephaven.db.v2.by.AggregationStateFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory.ComboBy;
import io.deephaven.db.v2.iterators.*;
import io.deephaven.db.v2.select.ReinterpretedColumn;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.qst.table.TableSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Deephaven table.
 */
public interface Table extends LongSizedDataStructure, LivenessNode, TableOperations<Table, Table> {

    Table[] ZERO_LENGTH_TABLE_ARRAY = new Table[0];

    static Table of(TableSpec table) {
        return TableCreatorImpl.create(table);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------------------------------------------------

    @AsyncMethod
    TableDefinition getDefinition();

    /**
     * Provides column metadata in Table form.
     * Convenience method, behaves exactly the same as getDefinition().getColumnDefinitionsTable().
     *
     * @return A Table of metadata about this Table's columns.
     */
    @AsyncMethod
    default Table getMeta() {
        return getDefinition().getColumnDefinitionsTable();
    }

    @AsyncMethod
    String getDescription();

    /**
     * Determines whether this Table contains a column for each string in the specified array of {@code columnNames}.
     *
     * @param columnNames The array of column names to be checked for inclusion in this table. Must not be {@code null}.
     * @return {@code true} if this Table contains a column for each and every string in the {@code columnNames} array;
     * {@code false} if any element of {@code columnNames} is <b>not</b> the name of a column in this table
     */
    @AsyncMethod
    default boolean hasColumns(final String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return hasColumns(Arrays.asList(columnNames));
    }

    /**
     * Determines whether this Table contains a column for each string in the specified collection of {@code columnNames}.
     *
     * @param columnNames The collection of column names to be checked for inclusion in this table. Must not be {@code null}.
     * @return {@code true} if this Table contains a column for each and every string in the {@code columnNames} collection;
     * {@code false} if any element of {@code columnNames} is <b>not</b> the name of a column in this table
     */
    @AsyncMethod
    default boolean hasColumns(Collection<String> columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return getDefinition().getColumnNameMap().keySet().containsAll(columnNames);
    }

    String DO_NOT_MAKE_REMOTE_ATTRIBUTE = "DoNotMakeRemote";
    String INPUT_TABLE_ATTRIBUTE = "InputTable";
    String KEY_COLUMNS_ATTRIBUTE = "keyColumns";
    String UNIQUE_KEYS_ATTRIBUTE = "uniqueKeys";
    String SORTABLE_COLUMNS_ATTRIBUTE = "SortableColumns";
    String FILTERABLE_COLUMNS_ATTRIBUTE = "FilterableColumns";
    String LAYOUT_HINTS_ATTRIBUTE = "LayoutHints";
    String TOTALS_TABLE_ATTRIBUTE = "TotalsTable";
    String TABLE_DESCRIPTION_ATTRIBUTE = "TableDescription";
    String COLUMN_RENDERERS_ATTRIBUTE = "ColumnRenderers";
    String COLUMN_DESCRIPTIONS_ATTRIBUTE = "ColumnDescriptions";
    String ADD_ONLY_TABLE_ATTRIBUTE = "AddOnly";
    /**
     * <p>If this attribute is present with value {@code true}, this Table is a "stream table".
     * <p>A stream table is a sequence of additions that represent rows newly received from a stream; on the cycle
     * after the stream table is refreshed the rows are removed. Note that this means any particular row of data (not
     * to be confused with an index key) never exists for more than one cycle.
     * <p>Most operations are supported as normal on stream tables, but aggregation operations are treated specially,
     * producing aggregate results that are valid over the entire observed stream from the time the operation is
     * initiated. These semantics necessitate a few exclusions, i.e. unsupported operations:
     * <ol>
     *     <li>{@link #by(SelectColumn...) by()} as an index-aggregation is unsupported. This means any of the overloads
     *     for {@link #by(AggregationStateFactory, SelectColumn...)} or {@link #by(Collection, Collection)} using
     *     {@link AggregationIndexStateFactory}, {@link AggregationFormulaStateFactory}, or {@link Array}.
     *     {@link io.deephaven.db.v2.by.ComboAggregateFactory#AggArray(java.lang.String...)}, and
     *     {@link ComboAggregateFactory#AggFormula(java.lang.String, java.lang.String, java.lang.String...)} are also
     *     unsupported.
     *     <li>{@link #byExternal(boolean, String...) byExternal()} is unsupported</li>
     *     <li>{@link #rollup(ComboAggregateFactory, boolean, SelectColumn...) rollup()} is unsupported if
     *     {@code includeConstituents == true}</li>
     *     <li>{@link #treeTable(String, String) treeTable()} is unsupported</li>
     * </ol>
     * <p>To disable these semantics, a {@link #dropStream()} method is offered.
     */
    String STREAM_TABLE_ATTRIBUTE = "StreamTable";
    /**
     * The query engine may set or read this attribute to determine if a table is sorted by a particular column.
     */
    String SORTED_COLUMNS_ATTRIBUTE = "SortedColumns";
    String SYSTEMIC_TABLE_ATTRIBUTE = "SystemicTable";

    // TODO: Might be good to take a pass through these and see what we can condense into
    // TODO: TreeTableInfo and RollupInfo to reduce the attribute noise.
    String ROLLUP_LEAF_ATTRIBUTE = "RollupLeaf";
    String HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE = "HierarchicalChildrenTableMap";
    String HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE = "HierarchicalSourceTable";
    String TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE = "TreeTableFilterReverseLookup";
    String HIERARCHICAL_SOURCE_INFO_ATTRIBUTE = "HierarchicalSourceTableInfo";
    String REVERSE_LOOKUP_ATTRIBUTE = "ReverseLookup";
    String PREPARED_RLL_ATTRIBUTE = "PreparedRll";
    String PREDEFINED_ROLLUP_ATTRIBUTE = "PredefinedRollup";

    String SNAPSHOT_VIEWPORT_TYPE = "Snapshot";

    /**
     * This attribute is used internally by TableTools.merge to detect successive merges.  Its presence indicates that
     * it is safe to decompose the table into its multiple constituent parts.
     */
    String MERGED_TABLE_ATTRIBUTE = "MergedTable";

    /**
     * <p>This attribute is applied to source tables, and takes on Boolean values.
     * <ul>
     *     <li>True for post-{@link #coalesce()} source tables and their children if the source table is empty.</li>
     *     <li>False for post-{@link #coalesce()} source tables and their children if the source table is non-empty.</li>
     *     <li>Missing for all other tables.</li>
     * </ul>
     */
    String EMPTY_SOURCE_TABLE_ATTRIBUTE = "EmptySourceTable";

    /**
     * This attribute stores a reference to a table that is the parent table for a Preview Table.
     */
    String PREVIEW_PARENT_TABLE = "PreviewParentTable";

    /**
     * Set this attribute for tables that should not be displayed in the UI.
     */
    String NON_DISPLAY_TABLE = "NonDisplayTable";

    /**
     * Set this attribute to load a plugin for this table in the Web Client
     */
    String PLUGIN_NAME = "PluginName";

    /**
     * Set the value of an attribute.
     *
     * @param key the name of the attribute
     * @param object the value
     */
    @AsyncMethod
    void setAttribute(@NotNull String key, @Nullable Object object);

    /**
     * Get the value of the specified attribute.
     *
     * @param key the name of the attribute
     * @return the value, or null if there was none.
     */
    @AsyncMethod
    @Nullable
    Object getAttribute(@NotNull String key);

    /**
     * Get a set of all the attributes that have values for this table.
     * @return a set of names
     */
    @AsyncMethod
    @NotNull
    Set<String> getAttributeNames();

    /**
     * Check if the specified attribute exists in this table.
     *
     * @param name the name of the attribute
     * @return true if the attribute exists
     */
    @AsyncMethod
    boolean hasAttribute(final @NotNull String name);

    /**
     * Get all of the attributes from the table.
     *
     * @return A map containing all of the attributes.
     */
    @AsyncMethod
    default Map<String, Object> getAttributes() {
        return getAttributes(Collections.emptySet());
    }

    /**
     * Get all attributes from the desired table except the items that appear in excluded.
     *
     * @param excluded A set of attributes to exclude from the result
     * @return All of the table's attributes except the ones present in excluded
     */
    @AsyncMethod
    Map<String, Object> getAttributes(Collection<String> excluded);

    @AsyncMethod
    default boolean isLive() {
        return DynamicNode.isDynamicAndIsRefreshing(this);
    }

    /**
     * Explicitly ensure that any work needed to make a table indexable, iterable, or queryable has been done, and
     * return the coalesced child table if appropriate.
     * @return This table, or a fully-coalesced child
     */
    default Table coalesce() {
        if (DynamicNode.isDynamicAndIsRefreshing(this)) {
            LivenessScopeStack.peek().manage(this);
        }
        return this;
    }

    Index getIndex();

    default long sizeForInstrumentation() {
        return size();
    }

    /**
     * Returns {@code true} if this table has no rows (i.e. {@code size() == 0}).
     *
     * @return {@code true} if this table has no rows
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Sources - for fetching data by index key
    // -----------------------------------------------------------------------------------------------------------------

    ColumnSource getColumnSource(String sourceName);

    /**
     * Retrieves a {@code ColumnSource} and {@link ColumnSource#cast casts} is to to the target class {@code clazz}.
     *
     * @param sourceName The name of the column.
     * @param clazz      The target type.
     * @param <T>        The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return The column source for {@code sourceName}, parameterized by {@code T}.
     */
    default <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz) {
        @SuppressWarnings("rawtypes") ColumnSource rawColumnSource = getColumnSource(sourceName);
        //noinspection unchecked
        return rawColumnSource.cast(clazz);
    }

    Map<String, ? extends ColumnSource> getColumnSourceMap();

    Collection<? extends ColumnSource> getColumnSources();

    // -----------------------------------------------------------------------------------------------------------------
    // Data Columns - for fetching data by position
    // -----------------------------------------------------------------------------------------------------------------

    default DataColumn[] getColumns() {
        return getDefinition().getColumnStream().map(c -> getColumn(c.getName())).toArray(DataColumn[]::new);
    }

    default DataColumn getColumn(int columnIndex) {
        return getColumn(this.getDefinition().getColumns()[columnIndex].getName());
    }

    DataColumn getColumn(String columnName);

    // -----------------------------------------------------------------------------------------------------------------
    // Column Iterators
    // -----------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    default <TYPE> Iterator<TYPE> columnIterator(@NotNull final String columnName) {
        final Class<TYPE> type = getDefinition().getColumn(columnName).getDataType();
        if (type == byte.class || type == Byte.class) {
            return (Iterator<TYPE>)byteColumnIterator(columnName);
        }
        if (type == char.class || type == Character.class) {
            return (Iterator<TYPE>)characterColumnIterator(columnName);
        }
        if (type == double.class || type == Double.class) {
            return (Iterator<TYPE>)doubleColumnIterator(columnName);
        }
        if (type == float.class || type == Float.class) {
            return (Iterator<TYPE>)floatColumnIterator(columnName);
        }
        if (type == int.class || type == Integer.class) {
            return (Iterator<TYPE>)integerColumnIterator(columnName);
        }
        if (type == long.class || type == Long.class) {
            return (Iterator<TYPE>)longColumnIterator(columnName);
        }
        if (type == short.class || type == Short.class) {
            return (Iterator<TYPE>)shortColumnIterator(columnName);
        }
        return new ColumnIterator<>(this, columnName);
    }

    default ByteColumnIterator byteColumnIterator(@NotNull final String columnName) {
        return new ByteColumnIterator(this, columnName);
    }

    default CharacterColumnIterator characterColumnIterator(@NotNull final String columnName) {
        return new CharacterColumnIterator(this, columnName);
    }

    default DoubleColumnIterator doubleColumnIterator(@NotNull final String columnName) {
        return new DoubleColumnIterator(this, columnName);
    }

    default FloatColumnIterator floatColumnIterator(@NotNull final String columnName) {
        return new FloatColumnIterator(this, columnName);
    }

    default IntegerColumnIterator integerColumnIterator(@NotNull final String columnName) {
        return new IntegerColumnIterator(this, columnName);
    }

    default LongColumnIterator longColumnIterator(@NotNull final String columnName) {
        return new LongColumnIterator(this, columnName);
    }

    default ShortColumnIterator shortColumnIterator(@NotNull final String columnName) {
        return new ShortColumnIterator(this, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Convenience data fetching
    // -----------------------------------------------------------------------------------------------------------------

    Object[] getRecord(long rowNo, String... columnNames);

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @AsyncMethod
    Table where(SelectFilter... filters);

    @AsyncMethod
    default Table where(String... filters) {
        return where(SelectFilterFactory.getExpressions(filters));
    }

    @Override
    @AsyncMethod
    default Table where(Collection<? extends Filter> filters) {
        return where(SelectFilter.from(filters));
    }

    @AsyncMethod
    default Table where() {
        return where(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
    }

    @AsyncMethod
    default Table wouldMatch(String... expressions) {
        return wouldMatch(WouldMatchPairFactory.getExpressions(expressions));
    }

    /**
     * A table operation that applies the supplied predicate to each row in the table and produces columns containing the
     * pass/fail result of the predicate application. This is similar to {@link #where(String...)} except that instead of
     * selecting only rows that meet the criteria, new columns are added with the result of the comparison.
     *
     * @return a table with new columns containing the filter result for each row.
     */
    @AsyncMethod
    Table wouldMatch(WouldMatchPair... matchers);

    /**
     * Filters this table based on the set of values in the rightTable.  Note that when the right table ticks, all of
     * the rows in the left table are going to be re-evaluated, thus the intention is that the right table is fairly
     * slow moving compared with the left table.
     *
     * @param rightTable     the filtering table.
     * @param inclusion      whether things included in rightTable should be passed through (they are exluded if false)
     * @param columnsToMatch the columns to match between the two tables
     * @return a new table filtered on right table
     */
    Table whereIn(GroupStrategy groupStrategy, Table rightTable, boolean inclusion, MatchPair... columnsToMatch);

    default Table whereIn(Table rightTable, boolean inclusion, MatchPair... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, inclusion, columnsToMatch);
    }
    default Table whereIn(Table rightTable, boolean inclusion, String... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, inclusion, MatchPairFactory.getExpressions(columnsToMatch));
    }

    default Table whereIn(Table rightTable, String... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, true, MatchPairFactory.getExpressions(columnsToMatch));
    }
    default Table whereIn(Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, true, columnsToMatch);
    }
    default Table whereNotIn(Table rightTable, String... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, false, MatchPairFactory.getExpressions(columnsToMatch));
    }
    default Table whereNotIn(Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(GroupStrategy.DEFAULT, rightTable, false, columnsToMatch);
    }

    default Table whereIn(GroupStrategy groupStrategy, Table rightTable, String... columnsToMatch) {
        return whereIn(groupStrategy, rightTable, true, columnsToMatch);
    }
    default Table whereIn(GroupStrategy groupStrategy, Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(groupStrategy, rightTable, true, columnsToMatch);
    }
    default Table whereNotIn(GroupStrategy groupStrategy, Table rightTable, String... columnsToMatch) {
        return whereIn(groupStrategy, rightTable, false, columnsToMatch);
    }
    default Table whereNotIn(GroupStrategy groupStrategy, Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(groupStrategy, rightTable, false, columnsToMatch);
    }

    default Table whereIn(GroupStrategy groupStrategy, Table rightTable, boolean inclusion, String... columnsToMatch) {
        return whereIn(groupStrategy, rightTable, inclusion, MatchPairFactory.getExpressions(columnsToMatch));
    }

    @Override
    default Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereIn(rightTable, MatchPair.fromMatches(columnsToMatch));
    }

    @Override
    default Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereNotIn(rightTable, MatchPair.fromMatches(columnsToMatch));
    }

    /**
     * Use the whereIn method call instead.
     */
    @Deprecated
    default Table whereDynamic(Table rightTable, boolean inclusion, MatchPair... columnsToMatch) {
        return whereIn(rightTable, inclusion, columnsToMatch);
    }

    @Deprecated
    default Table whereDynamicIn(Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(rightTable, columnsToMatch);
    }

    @Deprecated
    default Table whereDynamicNotIn(Table rightTable, MatchPair... columnsToMatch) {
        return whereIn(rightTable, columnsToMatch);
    }

    @Deprecated
    default Table whereDynamicIn(Table rightTable, String... columnsToMatch) {
        return whereIn(rightTable, columnsToMatch);
    }
    @Deprecated
    default Table whereDynamicNotIn(Table rightTable, String... columnsToMatch) {
        return whereNotIn(rightTable, columnsToMatch);
    }

    /**
     * Filters according to an expression in disjunctive normal form.
     * <p>
     * The input is an array of clauses, which in turn are a collection of filters.
     *
     * @param filtersToApply each inner collection is a set of filters, all of must which match for the clause to
     *                       be true.  If any one of the collections in the array evaluates to true, the row is part
     *                       of the output table.
     * @return a new table, with the filters applied.
     */
    @SuppressWarnings("unchecked")
    @AsyncMethod
    default Table whereOneOf(Collection<SelectFilter>... filtersToApply) {
        return where(WhereClause.createDisjunctiveFilter(filtersToApply));
    }

    /**
     * Applies the provided filters to the table disjunctively.
     *
     * @param filtersToApplyStrings an Array of filters to apply
     * @return a new table, with the filters applied
     */
    @AsyncMethod
    default Table whereOneOf(String... filtersToApplyStrings) {
        //noinspection unchecked, generic array creation is not possible
        final Collection<SelectFilter>[] filtersToApplyArrayOfCollections = (Collection<SelectFilter>[]) Arrays.stream(SelectFilterFactory.getExpressions(filtersToApplyStrings)).map(Collections::singleton).toArray(Collection[]::new);
        return whereOneOf(filtersToApplyArrayOfCollections);
    }

    @AsyncMethod
    default Table whereOneOf() {
        return where(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
    }

    Table getSubTable(Index index);

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    Table select(SelectColumn... columns);

    default Table select(String... columns) {
        return select(SelectColumnFactory.getExpressions(columns));
    }

    @Override
    default Table select(Collection<? extends Selectable> columns) {
        return select(SelectColumn.from(columns));
    }

    default Table select() {
        return select(getDefinition().getColumnNamesArray());
    }

    @AsyncMethod
    Table selectDistinct(SelectColumn... columns);

    @AsyncMethod
    default Table selectDistinct(String... columns) {
        return selectDistinct(SelectColumnFactory.getExpressions(columns));
    }

    @AsyncMethod
    default Table selectDistinct(Collection<String> columns) {
        return selectDistinct(SelectColumnFactory.getExpressions(columns));
    }

    @AsyncMethod
    default Table selectDistinct() {
        return selectDistinct(getDefinition().getColumnNamesArray());
    }

    Table update(SelectColumn... newColumns);

    default Table update(String... newColumns) {
        return update(SelectColumnFactory.getExpressions(newColumns));
    }

    @Override
    default Table update(Collection<? extends Selectable> columns) {
        return update(SelectColumn.from(columns));
    }

    /**
     * DO NOT USE -- this API is in flux and may change or disappear in the future.
     */
    default SelectValidationResult validateSelect(String... columns) {
        return validateSelect(SelectColumnFactory.getExpressions(columns));
    }

    /**
     * DO NOT USE -- this API is in flux and may change or disappear in the future.
     */
    SelectValidationResult validateSelect(SelectColumn... columns);

    /**
     * Compute column formulas on demand.
     *
     * <p>Lazy update defers computation until required for a set of values, and caches the results for a set of input
     * values.  This uses less RAM than an update statement when you have a smaller set of unique values.  Less
     * computation than an updateView is needed, because the results are saved in a cache.</p>
     *
     * <p>If you have many unique values, you should instead use an update statement, which will have more memory
     * efficient structures.  Values are never removed from the lazyUpdate cache, so it should be used judiciously
     * on a ticking table.</p>
     *
     * @param newColumns the columns to add
     *
     * @return a new Table with the columns added; to be computed on demand
     */
    Table lazyUpdate(SelectColumn... newColumns);

    default Table lazyUpdate(String... newColumns) {
        return lazyUpdate(SelectColumnFactory.getExpressions(newColumns));
    }

    default Table lazyUpdate(Collection<String> newColumns) {
        return lazyUpdate(SelectColumnFactory.getExpressions(newColumns));
    }

    @AsyncMethod
    Table view(SelectColumn... columns);

    @AsyncMethod
    default Table view(String... columns) {
        return view(SelectColumnFactory.getExpressions(columns));
    }

    @Override
    @AsyncMethod
    default Table view(Collection<? extends Selectable> columns) {
        return view(SelectColumn.from(columns));
    }

    @AsyncMethod
    Table updateView(SelectColumn... newColumns);

    @AsyncMethod
    default Table updateView(String... newColumns) {
        return updateView(SelectColumnFactory.getExpressions(newColumns));
    }

    @Override
    @AsyncMethod
    default Table updateView(Collection<? extends Selectable> columns) {
        return updateView(SelectColumn.from(columns));
    }

    @AsyncMethod
    Table dropColumns(String... columnNames);

    @AsyncMethod
    default Table dropColumnFormats() {
        String[] columnAry = getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(ColumnFormattingValues::isFormattingColumn)
                .toArray(String[]::new);
        return dropColumns(columnAry);
    }

    @AsyncMethod
    default Table dropColumns(Collection<String> columnNames) {
        return dropColumns(columnNames.toArray(new String[columnNames.size()]));
    }

    Table renameColumns(MatchPair... pairs);

    default Table renameColumns(String... columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    default Table renameColumns(Collection<String> columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    @FunctionalInterface
    interface RenameFunction {
        String rename(String currentName);
    }

    default Table renameAllColumns(RenameFunction renameFunction) {
        return renameColumns(getDefinition().getColumnStream().map(ColumnDefinition::getName).map(n -> new MatchPair(renameFunction.rename(n), n)).toArray(MatchPair[]::new));
    }

    @AsyncMethod
    default Table formatColumns(String... columnFormats) {
        final SelectColumn[] selectColumns = SelectColumnFactory.getFormatExpressions(columnFormats);

        final Set<String> existingColumns = getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = Arrays.stream(selectColumns)
                .map(SelectColumnFactory::getFormatBaseColumn)
                .filter(column -> (column != null && !column.equals("*") && !existingColumns.contains(column)))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException("Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        return updateView(selectColumns);
    }

    @AsyncMethod
    default Table formatRowWhere(String condition, String formula) {
        return formatColumnWhere(ColumnFormattingValues.ROW_FORMAT_NAME , condition, formula);
    }

    @AsyncMethod
    default Table formatColumnWhere(String columnName, String condition, String formula) {
        return formatColumns(columnName + " = (" + condition + ") ? io.deephaven.db.util.DBColorUtil.toLong(" + formula + ") : io.deephaven.db.util.DBColorUtil.toLong(NO_FORMATTING)");
    }

    /**
     * Produce a new table with the specified columns moved to the leftmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param columnsToMove The columns to move to the left (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     * {@link #moveColumns(int, String...)}
     */
    @AsyncMethod
    default Table moveUpColumns(String... columnsToMove) {
        return moveColumns(0, columnsToMove);
    }

    /**
     * Produce a new table with the specified columns moved to the rightmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param columnsToMove The columns to move to the right (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     * {@link #moveColumns(int, String...)}
     */
    @AsyncMethod
    default Table moveDownColumns(String... columnsToMove) {
        return moveColumns(getDefinition().getColumns().length - columnsToMove.length, true, columnsToMove);
    }

    /**
     * Produce a new table with the specified columns moved to the specified {@code index}. Column indices begin at 0.
     * Columns can be renamed with the usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param index         The index to which the specified columns should be moved
     * @param columnsToMove The columns to move to the specified index (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     */
    @AsyncMethod
    default Table moveColumns(int index, String... columnsToMove) {
        return moveColumns(index, false, columnsToMove);
    }

    @AsyncMethod
    default Table moveColumns(int index, boolean moveToEnd, String... columnsToMove) {
        // Get the current columns
        final List<String> currentColumns = getDefinition().getColumnNames();

        // Create a Set from columnsToMove. This way, we can rename and rearrange columns at once.
        final Set<String> leftColsToMove = new HashSet<>();
        final Set<String> rightColsToMove = new HashSet<>();
        int extraCols = 0;

        for (final String columnToMove : columnsToMove) {
            final String left = MatchPairFactory.getExpression(columnToMove).leftColumn;
            final String right = MatchPairFactory.getExpression(columnToMove).rightColumn;

            if(!leftColsToMove.add(left) || !currentColumns.contains(left) || (rightColsToMove.contains(left) && !left.equals(right) && leftColsToMove.stream().anyMatch(col -> col.equals(right)))) {
                extraCols++;
            }
            if(currentColumns.stream().anyMatch(currentColumn -> currentColumn.equals(right)) && !left.equals(right) && rightColsToMove.add(right) && !rightColsToMove.contains(left)) {
                extraCols--;
            }
        }
        index += moveToEnd ? extraCols: 0;

        // vci for write, cci for currentColumns, ctmi for columnsToMove
        final SelectColumn[] viewColumns = new SelectColumn[currentColumns.size() + extraCols];
        for (int vci = 0, cci = 0, ctmi = 0; vci < viewColumns.length; ) {
            if (vci >= index && ctmi < columnsToMove.length) {
                viewColumns[vci++] = SelectColumnFactory.getExpression(columnsToMove[ctmi++]);
            } else {
                // Don't add the column if it's one of the columns we're moving or if it has been renamed.
                final String currentColumn = currentColumns.get(cci++);
                if (!leftColsToMove.contains(currentColumn) && Arrays.stream(viewColumns).noneMatch(viewCol -> viewCol != null && viewCol.getMatchPair().leftColumn.equals(currentColumn))
                                                            && Arrays.stream(columnsToMove).noneMatch(colToMove -> MatchPairFactory.getExpression(colToMove).rightColumn.equals(currentColumn))) {

                    viewColumns[vci++] = SelectColumnFactory.getExpression(currentColumn);
                }
            }
        }
        return view(viewColumns);
    }

    /**
     * Produce a new table with the same columns as this table, but with a new column presenting the specified
     * DBDateTime column as a Long column (with each DBDateTime represented instead as the corresponding number of
     * nanos since the epoch).
     * <p>
     * NOTE: This is a really just an updateView(), and behaves accordingly for column ordering and (re)placement.
     * This doesn't work on data that has been brought fully into memory (e.g. via select()).  Use a view instead.
     *
     * @param dateTimeColumnName
     * @param nanosColumnName
     * @return The new table, constructed as explained above.
     */
    @AsyncMethod
    default Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName) {
        // noinspection unchecked
        return updateView(new ReinterpretedColumn(dateTimeColumnName, DBDateTime.class, nanosColumnName, long.class));
    }

    /**
     * @param columnName
     * @return The result of dateTimeColumnAsNanos(columnName, columnName).
     */
    @AsyncMethod
    default Table dateTimeColumnAsNanos(String columnName) {
        return dateTimeColumnAsNanos(columnName, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Head / Tail Operations
    // -----------------------------------------------------------------------------------------------------------------

    @AsyncMethod
    Table head(long size);

    @AsyncMethod
    Table tail(long size);

    /**
     * Extracts a subset of a table by row position.
     *
     * If both firstPosition and lastPosition are positive, then the rows are counted from the beginning of the table.
     * The firstPosition is inclusive, and the lastPosition is exclusive.  The {@link #head}(N) call is equivalent to
     * slice(0, N).  The firstPosition must be less than or equal to the lastPosition.
     *
     * If firstPosition is positive and lastPosition is negative, then the firstRow is counted from the beginning of the
     * table, inclusively.  The lastPosition is counted from the end of the table.  For example, slice(1, -1) includes
     * all rows but the first and last.  If the lastPosition would be before the firstRow, the result is an emptyTable.
     *
     * If firstPosition is negative, and lastPosition is zero, then the firstRow is counted from the end of the table,
     * and the end of the slice is the size of the table.  slice(-N, 0) is equivalent to {@link #tail}(N).
     *
     * If the firstPosition is nega tive and the lastPosition is negative, they are both counted from the end of the
     * table. For example, slice(-2, -1) returns the second to last row of the table.
     *
     * @param firstPositionInclusive the first position to include in the result
     * @param lastPositionExclusive the last position to include in the result
     *
     * @return a new Table, which is the request subset of rows from the original table
     */
    @AsyncMethod
    Table slice(long firstPositionInclusive, long lastPositionExclusive);

    /**
     * Provides a head that selects a dynamic number of rows based on a percent.
     *
     * @param percent the fraction of the table to return (0..1), the number of rows will be rounded up.  For example
     *                if there are 3 rows, headPct(50) returns the first two rows.
     */
    @AsyncMethod
    Table headPct(double percent);

    @AsyncMethod
    Table tailPct(double percent);

    // -----------------------------------------------------------------------------------------------------------------
    // Grouping - used for various operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * GroupStrategy is used for joins and other operations that can choose one of several ways to make use of grouping
     * information.
     */
    enum GroupStrategy {
        DEFAULT,
        LINEAR,
        USE_EXISTING_GROUPS,
        CREATE_GROUPS,
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns a table that has one column for each original table's columns, and one column corresponding to each of
     * the input table (right table) columns listed in the columns to add (or all the columns whose names don't overlap
     * with the name of a column from the source table if the columnsToAdd is length zero).
     * The new columns (those corresponding to the input table) contain an aggregation of all values from the left side that
     * match the join criteria. Consequently the types of all right side columns not involved in a join criteria, is an
     * array of the original column type.
     * If the two tables have columns with matching names then the method will fail with an exception unless the columns with
     * corresponding names are found in one of the matching criteria.
     * <p>
     * <p>
     * NOTE: leftJoin operation does not involve an actual data copy, or an in-memory table creation. In order to produce
     * an actual in memory table you need to apply a select call on the join result.
     *
     * @param rightTable   input table
     * @param columnsToMatch match criteria
     * @param columnsToAdd columns to add
     * @return a table that has one column for each original table's columns, and one column corresponding to each column
     * listed in columnsToAdd.  If columnsToAdd.length==0 one column corresponding to each column of
     * the input table (right table) columns whose names don't overlap with the name of a column from the source table is added.
     * The new columns (those corresponding to the input table) contain an aggregation of all values from the left side that
     * match the join criteria.
     */
    Table leftJoin(Table rightTable, MatchPair columnsToMatch[], MatchPair[] columnsToAdd);

    default Table leftJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return leftJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    default Table leftJoin(Table rightTable, Collection<String> columnsToMatch) {
        return leftJoin(
                rightTable,
                MatchPairFactory.getExpressions(columnsToMatch),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    default Table leftJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return leftJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    default Table leftJoin(Table rightTable, String columnsToMatch) {
        return leftJoin(rightTable, StringUtils.splitToCollection(columnsToMatch));
    }

    default Table leftJoin(Table rightTable) {
        return leftJoin(rightTable, Collections.emptyList());
    }

    Table exactJoin(Table rightTable, MatchPair columnsToMatch[], MatchPair[] columnsToAdd);

    @Override
    default Table exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return exactJoin(
            rightTable,
            MatchPair.fromMatches(columnsToMatch),
            MatchPair.fromAddition(columnsToAdd));
    }

    default Table exactJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return exactJoin(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    default Table exactJoin(Table rightTable, String columnsToMatch) {
        return exactJoin(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    enum AsOfMatchRule {
        LESS_THAN_EQUAL,
        LESS_THAN,
        GREATER_THAN_EQUAL,
        GREATER_THAN;

        static AsOfMatchRule of(AsOfJoinRule rule) {
            switch (rule) {
                case LESS_THAN_EQUAL:
                    return AsOfMatchRule.LESS_THAN_EQUAL;
                case LESS_THAN:
                    return AsOfMatchRule.LESS_THAN;
            }
            throw new IllegalStateException("Unexpected rule " + rule);
        }

        static AsOfMatchRule of(ReverseAsOfJoinRule rule) {
            switch (rule) {
                case GREATER_THAN_EQUAL:
                    return AsOfMatchRule.GREATER_THAN_EQUAL;
                case GREATER_THAN:
                    return AsOfMatchRule.GREATER_THAN;
            }
            throw new IllegalStateException("Unexpected rule " + rule);
        }
    }


    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the columns from rightTable defined in
     * a comma separated list "columnsToAdd"
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the left side that need to be added to the right
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule);


    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the columns from rightTable defined in
     * a comma separated list "columnsToAdd"
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the left side that need to be added to the right
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return aj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.LESS_THAN_EQUAL);
    }

    default Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return aj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    default Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return aj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(asOfJoinRule));
    }

    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with all the columns from rightTable.
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table aj(Table rightTable, Collection<String> columnsToMatch) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(false, columnsToMatch);
        return aj(
                rightTable,
                expressions.getFirst(),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                expressions.getSecond());
    }

    default Table aj(Table rightTable, String columnsToMatch, String columnsToAdd) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(false, StringUtils.splitToCollection(columnsToMatch));
        return aj(
                rightTable,
                expressions.getFirst(),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
                expressions.getSecond());
    }

    default Table aj(Table rightTable, String columnsToMatch) {
        return aj(rightTable, StringUtils.splitToCollection(columnsToMatch));
    }

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the columns from rightTable defined in
     * a comma separated list "columnsToAdd"
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the left side that need to be added to the right
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table raj(Table rightTable, MatchPair columnsToMatch[], MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule);


    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the columns from rightTable defined in
     * a comma separated list "columnsToAdd"
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the left side that need to be added to the right
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table raj(Table rightTable, MatchPair columnsToMatch[], MatchPair[] columnsToAdd) {
        return raj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.GREATER_THAN_EQUAL);
    }

    default Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return raj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    default Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return raj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(reverseAsOfJoinRule));
    }

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the all columns from rightTable.
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table raj(Table rightTable, Collection<String> columnsToMatch) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(true, columnsToMatch);
        return raj(
                rightTable,
                expressions.getFirst(),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                expressions.getSecond());
    }

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list.
     * Matching is done exactly for the first n-1 columns and via a binary search for the last match pair.
     * The columns of the original table are returned intact, together with the columns from rightTable defined in
     * a comma separated list "columnsToAdd"
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the left side that need to be added to the right
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table raj(Table rightTable, String columnsToMatch, String columnsToAdd) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(true, StringUtils.splitToCollection(columnsToMatch));
        return raj(
                rightTable,
                expressions.getFirst(),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
                expressions.getSecond());
    }

    default Table raj(Table rightTable, String columnsToMatch) {
        return raj(rightTable, StringUtils.splitToCollection(columnsToMatch));
    }

    Table naturalJoin(Table rightTable, MatchPair columnsToMatch[], MatchPair[] columnsToAdd);

    @Override
    default Table naturalJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return naturalJoin(
            rightTable,
            MatchPair.fromMatches(columnsToMatch),
            MatchPair.fromAddition(columnsToAdd));
    }

    default Table naturalJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return naturalJoin(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    default Table naturalJoin(Table rightTable, String columnsToMatch) {
        return naturalJoin(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns. The rows are ordered first
     * by the left table then by the right table.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @return a new table joined according to the specification with zero key-columns and includes all right columns
     */
    default Table join(Table rightTable) {
        return join(
                rightTable,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns. The rows are ordered first
     * by the left table then by the right table.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable            The right side table on the join.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification with zero key-columns and includes all right columns
     */
    default Table join(Table rightTable, int numRightBitsToReserve) {
        return join(rightTable, Collections.emptyList(), Collections.emptyList(), numRightBitsToReserve);
    }

    default Table join(Table rightTable, String columnsToMatch) {
        return join(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are not key-columns.
     * The rows are ordered first by the left table then by the right table. If columnsToMatch is non-empty then the
     * product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable            The right side table on the join.
     * @param columnsToMatch        A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and includes all non-key-columns from the right table
     */
    default Table join(Table rightTable, String columnsToMatch, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                numRightBitsToReserve);
    }

    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return join(
            rightTable,
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
            MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch
     * is non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   A comma separated list with the columns from the right side that need to be added to the left
     *                       side as a result of the match.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
                numRightBitsToReserve);
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch
     * is non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable     The right side table on the join.
     * @param columnsToMatch An array of match pair conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd   An array of the columns from the right side that need to be added to the left
     *                       side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    default Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return join(rightTable, columnsToMatch, columnsToAdd, CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch
     * is non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless specified,
     * join reserves 16 bits to represent a right row. When there are too few bits to represent all of the right rows
     * for a given aggregation group the table will shift a bit from the left side to the right side. The default of 16
     * bits was carefully chosen because it results in an efficient implementation to process live updates.
     * <p>
     * An {@link io.deephaven.db.v2.utils.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     * - If the left table is sparse, consider flattening the left table.
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using {@link #naturalJoin}.
     *
     * @param rightTable            The right side table on the join.
     * @param columnsToMatch        An array of match pair conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd          An array of the columns from the right side that need to be added to the left
     *                              side as a result of the match.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, int numRightBitsToReserve);

    @Override
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd) {
        return join(
            rightTable,
            MatchPair.fromMatches(columnsToMatch),
            MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch, Collection<? extends JoinAddition> columnsToAdd, int numRightBitsToReserve) {
        return join(
            rightTable,
            MatchPair.fromMatches(columnsToMatch),
            MatchPair.fromAddition(columnsToAdd),
            numRightBitsToReserve);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @AsyncMethod
    Table by(AggregationStateFactory aggregationStateFactory, SelectColumn... groupByColumns);

    @AsyncMethod
    default Table by(AggregationStateFactory aggregationStateFactory, String... groupByColumns) {
        return by(aggregationStateFactory, SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    default Table by(AggregationStateFactory aggregationStateFactory) {
        return by(aggregationStateFactory, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    @AsyncMethod
    default Table by(SelectColumn... groupByColumns) {
        return by(new AggregationIndexStateFactory(), groupByColumns);
    }

    @AsyncMethod
    default Table by(String... groupByColumns) {
        return by(SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    default Table by() {
        return by(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table by(Collection<? extends Selectable> groupByColumns) {
        return by(SelectColumn.from(groupByColumns));
    }

    @Override
    @AsyncMethod
    default Table by(Collection<? extends Selectable> groupByColumns, Collection<? extends Aggregation> aggregations) {
        List<ComboBy> optimized = ComboBy.optimize(aggregations);
        List<ColumnName> optimizedOrder = optimized.stream()
                .map(ComboBy::getResultPairs)
                .flatMap(Stream::of)
                .map(MatchPair::left)
                .map(ColumnName::of)
                .collect(Collectors.toList());
        List<ColumnName> userOrder = AggregationOutputs.of(aggregations).collect(Collectors.toList());

        Table aggregationTable = by(
                new ComboAggregateFactory(optimized),
                SelectColumn.from(groupByColumns));

        if (userOrder.equals(optimizedOrder)) {
            return aggregationTable;
        }

        // We need to re-order the columns to match the user-provided order
        List<ColumnName> newOrder = Stream.concat(groupByColumns.stream().map(Selectable::newColumn), userOrder.stream())
                .collect(Collectors.toList());

        return aggregationTable.view(newOrder);
    }

    default Table headBy(long nRows, SelectColumn... groupByColumns) {
        throw new UnsupportedOperationException();
    }

    Table headBy(long nRows, String... groupByColumns);

    default Table headBy(long nRows, Collection<String> groupByColumns) {
        return headBy(nRows, groupByColumns.stream().toArray(String[]::new));
    }

    default Table tailBy(long nRows, SelectColumn... groupByColumns) {
        throw new UnsupportedOperationException();
    }

    Table tailBy(long nRows, String... groupByColumns);

    default Table tailBy(long nRows, Collection<String> groupByColumns) {
        return tailBy(nRows, groupByColumns.stream().toArray(String[]::new));
    }

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation.
     * <code>columnParamName</code> is used as place-holder for the name of each column inside
     * <code>formulaColumn</code>.
     *
     * @param formulaColumn   Formula applied to each column
     * @param columnParamName The parameter name used as a placeholder for each column
     * @param groupByColumns  The grouping columns {@link Table#by(SelectColumn[])}
     */
    @AsyncMethod
    Table applyToAllBy(String formulaColumn, String columnParamName, SelectColumn... groupByColumns);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping operation.
     *
     * @param formulaColumn  Formula applied to each column, uses parameter <i>each</i> to refer to each colum it being applied to
     * @param groupByColumns The grouping columns {@link Table#by(SelectColumn...)}
     */
    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, SelectColumn... groupByColumns) {
        return applyToAllBy(formulaColumn, "each", groupByColumns);
    }

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping operation.
     *
     * @param formulaColumn  Formula applied to each column, uses parameter <i>each</i> to refer to each colum it being applied to
     * @param groupByColumns The grouping columns {@link Table#by(String...)}
     */
    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, String... groupByColumns) {
        return applyToAllBy(formulaColumn, SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, String groupByColumn) {
        return applyToAllBy(formulaColumn, SelectColumnFactory.getExpression(groupByColumn));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table sumBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table sumBy(String... groupByColumns) {
        return sumBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table sumBy(Collection<String> groupByColumns) {
        return sumBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the sum of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table sumBy() {
        return sumBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table absSumBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table absSumBy(String... groupByColumns) {
        return absSumBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table absSumBy(Collection<String> groupByColumns) {
        return absSumBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the absolute sum of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table absSumBy() {
        return absSumBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table avgBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table avgBy(String... groupByColumns) {
        return avgBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table avgBy(Collection<String> groupByColumns) {
        return avgBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the average of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table avgBy() {
        return avgBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table wavgBy(String weightColumn, SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table wavgBy(String weightColumn, String... groupByColumns) {
        return wavgBy(weightColumn, SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return wavgBy(weightColumn, SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the weighted average using weightColumn for the rest of the fields
     *
     * When the input table is empty, zero output rows are produced.
     *
     * @param weightColumn the column to use for the weight
     */
    @AsyncMethod
    default Table wavgBy(String weightColumn) {
        return wavgBy(weightColumn, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using
     * weightColumn for the rest of the fields
     *
     * If the weight column is a floating point type, all result columns will be doubles.  If the weight
     * column is an integral type, all integral input columns will have long results and all floating point
     * input columns will have double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table wsumBy(String weightColumn, SelectColumn... groupByColumns);

    /**
     * Computes the weighted sum for all rows in the table using weightColumn for the rest of the fields
     *
     * If the weight column is a floating point type, all result columns will be doubles.  If the weight
     * column is an integral type, all integral input columns will have long results and all floating point
     * input columns will have double results.
     *
     * @param weightColumn the column to use for the weight
     */
    @AsyncMethod
    default Table wsumBy(String weightColumn) {
        return wsumBy(weightColumn, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using
     * weightColumn for the rest of the fields
     *
     * If the weight column is a floating point type, all result columns will be doubles.  If the weight
     * column is an integral type, all integral input columns will have long results and all floating point
     * input columns will have double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table wsumBy(String weightColumn, String... groupByColumns) {
        return wsumBy(weightColumn, SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using
     * weightColumn for the rest of the fields
     *
     * If the weight column is a floating point type, all result columns will be doubles.  If the weight
     * column is an integral type, all integral input columns will have long results and all floating point
     * input columns will have double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return wsumBy(weightColumn, SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    Table stdBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table stdBy(String... groupByColumns) {
        return stdBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table stdBy(Collection<String> groupByColumns) {
        return stdBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the standard deviation of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    default Table stdBy() {
        return stdBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table varBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table varBy(String... groupByColumns) {
        return varBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table varBy(Collection<String> groupByColumns) {
        return varBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the variance of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    default Table varBy() {
        return varBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table lastBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table lastBy(String... groupByColumns) {
        return lastBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table lastBy(Collection<String> groupByColumns) {
        return lastBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Returns the last row of the given table.
     */
    @AsyncMethod
    default Table lastBy() {
        return lastBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table firstBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table firstBy(String... groupByColumns) {
        return firstBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table firstBy(Collection<String> groupByColumns) {
        return firstBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Returns the first row of the given table.
     */
    @AsyncMethod
    default Table firstBy() {
        return firstBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    Table minBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table minBy(String... groupByColumns) {
        return minBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)}
     */
    @AsyncMethod
    default Table minBy(Collection<String> groupByColumns) {
        return minBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the minimum of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table minBy() {
        return minBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    Table maxBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    default Table maxBy(String... groupByColumns) {
        return maxBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    default Table maxBy(Collection<String> groupByColumns) {
        return maxBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the maximum of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table maxBy() {
        return maxBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    Table medianBy(SelectColumn... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    default Table medianBy(String... groupByColumns) {
        return medianBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the fields
     *
     * @param groupByColumns The grouping columns {@link io.deephaven.db.tables.Table#by(String...)} }
     */
    @AsyncMethod
    default Table medianBy(Collection<String> groupByColumns) {
        return medianBy(SelectColumnFactory.getExpressions(groupByColumns));
    }

    /**
     * Produces a single row table with the median of each column.
     *
     * When the input table is empty, zero output rows are produced.
     */
    @AsyncMethod
    default Table medianBy() {
        return medianBy(SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

     @AsyncMethod
    Table countBy(String countColumnName, SelectColumn... groupByColumns);

    @AsyncMethod
    default Table countBy(String countColumnName, String... groupByColumns) {
        return countBy(countColumnName, SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    default Table countBy(String countColumnName, Collection<String> groupByColumns) {
        return countBy(countColumnName, SelectColumnFactory.getExpressions(groupByColumns));
    }

    @AsyncMethod
    default Table countBy(String countColumnName) {
        return countBy(countColumnName, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * If this table is a stream table, i.e. it has {@link #STREAM_TABLE_ATTRIBUTE} set to {@code true}, return a child
     * without the attribute, restoring standard semantics for aggregation operations.
     *
     * @return A non-stream child table, or this table if it is not a stream table
     */
    @AsyncMethod
    Table dropStream();

    // -----------------------------------------------------------------------------------------------------------------
    // Disaggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Ungroups a table by converting arrays into columns.
     *
     * @param nullFill indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with
     *                 null values.  If set to false, then all arrays should be the same length.
     * @param columnsToUngroup the columns to ungroup
     * @return the ungrouped table
     */
    Table ungroup(boolean nullFill, String... columnsToUngroup);

    default Table ungroup(String... columnsToUngroup) {
        return ungroup(false, columnsToUngroup);
    }

    default Table ungroupAllBut(String... columnsNotToUngroup) {
        final Set<String> columnsNotToUnwrapSet = Arrays.stream(columnsNotToUngroup).collect(Collectors.toSet());
        return ungroup(getDefinition().getColumnStream().filter(c ->
                !columnsNotToUnwrapSet.contains(c.getName()) && (c.getDataType().isArray() || DBLanguageParser.isDbArray(c.getDataType()))
        ).map(ColumnDefinition::getName).toArray(String[]::new));
    }

    default Table ungroup() {
        return ungroup(getDefinition().getColumnStream().filter(c ->
                c.getDataType().isArray() || DBLanguageParser.isDbArray(c.getDataType())
        ).map(ColumnDefinition::getName).toArray(String[]::new));
    }

    default Table ungroup(boolean nullFill) {
        return ungroup(nullFill, getDefinition().getColumnStream().filter(c ->
                c.getDataType().isArray() || DBLanguageParser.isDbArray(c.getDataType())
        ).map(ColumnDefinition::getName).toArray(String[]::new));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ByExternal Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a {@link TableMap} from this table, keyed by the specified columns.
     *
     * <p>The returned TableMap contains each row in this table in exactly one of the tables within the map.  If you
     * have exactly one key column the TableMap is keyed by the value in that column.  If you have zero key columns, then
     * the TableMap is keyed by {@code io.deephaven.datastructures.util.SmartKey.EMPTY} (and will contain this table as the
     * value).  If you have multiple key columns, then
     * the TableMap is keyed by a {@code io.deephaven.datastructures.util.SmartKey}.  The SmartKey will have one value for
     * each of your column values, in the order specified by keyColumnNames.</p>
     *
     * <p>For example if you have a Table keyed by a String column named USym, and a DBDateTime column named Expiry; a
     * value could be retrieved from the TableMap with
     * {@code tableMap.get(new SmartKey("SPY";, DBTimeUtils.convertDateTime("2020-06-19T16:15:00 NY")))}.  For a table
     * with an Integer column named Bucket, you simply use the desired value as in {@code tableMap.get(1)}.</p>
     *
     * @param dropKeys       if true, drop key columns in the output Tables
     * @param keyColumnNames the name of the key columns to use.
     * @return a TableMap keyed by keyColumnNames
     */
    @AsyncMethod
    TableMap byExternal(boolean dropKeys, String... keyColumnNames);

    /**
     * Create a {@link TableMap} from this table, keyed by the specified columns.
     *
     * <p>The returned TableMap contains each row in this table in exactly one of the tables within the map.  If you
     * have exactly one key column the TableMap is keyed by the value in that column.  If you have zero key columns, then
     * the TableMap is keyed by {@code io.deephaven.datastructures.util.SmartKey.EMPTY} (and will contain this table as the
     * value).  If you have multiple key columns, then
     * the TableMap is keyed by a {@code io.deephaven.datastructures.util.SmartKey}.  The SmartKey will have one value for
     * each of your column values, in the order specified by keyColumnNames.</p>
     *
     * <p>For example if you have a Table keyed by a String column named USym, and a DBDateTime column named Expiry; a
     * value could be retrieved from the TableMap with
     * {@code tableMap.get(new SmartKey("SPY";, DBTimeUtils.convertDateTime("2020-06-19T16:15:00 NY")))}.  For a table
     * with an Integer column named Bucket, you simply use the desired value as in {@code tableMap.get(1)}.</p>
     *
     * @param keyColumnNames the name of the key columns to use.
     * @return a TableMap keyed by keyColumnNames
     */
    @AsyncMethod
    default TableMap byExternal(String... keyColumnNames) {
        return byExternal(false, keyColumnNames);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and treeTable).
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level.  The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, Collection<String> columns) {
        return rollup(comboAggregateFactory, SelectColumnFactory.getExpressions(columns));
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level.  The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @param columns the columns to group by
     *
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents, Collection<String> columns) {
        return rollup(comboAggregateFactory, includeConstituents, SelectColumnFactory.getExpressions(columns));
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level.  The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, String... columns) {
        return rollup(comboAggregateFactory, SelectColumnFactory.getExpressions(columns));
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level.  The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param columns the columns to group by
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     *
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents, String... columns) {
        return rollup(comboAggregateFactory, includeConstituents, SelectColumnFactory.getExpressions(columns));
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level.  The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, SelectColumn... columns) {
        return rollup(comboAggregateFactory, false, columns);
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates all rows of the table.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory) {
        return rollup(comboAggregateFactory, false, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    /**
     * Create a rollup table.
     *
     * A rollup table aggregates all rows of the table.
     *
     * @param comboAggregateFactory the ComboAggregateFactory describing the aggregation
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @return a hierarchical table with the rollup applied
     */
    @AsyncMethod
    default Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents) {
        return rollup(comboAggregateFactory, includeConstituents, SelectColumn.ZERO_LENGTH_SELECT_COLUMN_ARRAY);
    }

    @AsyncMethod
    Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents, SelectColumn... columns);

    /**
     * Create a hierarchical tree table.
     *
     * The structure of the table is encoded by an "id" and a "parent" column.  The id column should represent a unique
     * identifier for a given row, and the parent column indicates which row is the parent for a given row.  Rows that
     * have a null parent, are shown in the main table.  It is possible for rows to be "orphaned", if their parent
     * reference is non-null and does not exist in the table.
     *
     * @param idColumn the name of a column containing a unique identifier for a particular row in the table
     * @param parentColumn the name of a column containing the parent's identifier, null for elements that are part of
     *                     the root table
     * @return a hierarchical table grouped according to the parentColumn
     */
    @AsyncMethod
    Table treeTable(String idColumn, String parentColumn);

    // -----------------------------------------------------------------------------------------------------------------
    // Sort Operations
    // -----------------------------------------------------------------------------------------------------------------

    @AsyncMethod
    Table sort(SortPair... sortPairs);

    @AsyncMethod
    default Table sort(String... columnsToSortBy) {
        return sort(SortPair.ascendingPairs(columnsToSortBy));
    }

    @AsyncMethod
    default Table sortDescending(String... columnsToSortBy) {
        return sort(SortPair.descendingPairs(columnsToSortBy));
    }

    @Override
    @AsyncMethod
    default Table sort(Collection<SortColumn> columnsToSortBy) {
        return sort(SortPair.from(columnsToSortBy));
    }

    @AsyncMethod
    Table reverse();

    /**
     * <p>Disallow sorting on all but the specified columns.</p>
     *
     * @param allowedSortingColumns The columns on which sorting is allowed.
     *
     * @return The same table this was invoked on.
     */
    @AsyncMethod
    Table restrictSortTo(String... allowedSortingColumns);

    /**
     * <p>Clear all sorting restrictions that was applied to the current table.</p>
     *
     * <p>Note that this table operates on the table it was invoked on and does not
     *    create a new table. So in the following code
     *    <code>T1 = baseTable.where(...)
     *          T2 = T1.restrictSortTo("C1")
     *          T3 = T2.clearSortingRestrictions()
     *    </code>
     *
     *    T1 == T2 == T3 and the result has no restrictions on sorting.
     * </p>
     *
     * @return The same table this was invoked on.
     */
    @AsyncMethod
    Table clearSortingRestrictions();

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns);

    default Table snapshot(Table baseTable, String... stampColumns) {
        return snapshot(baseTable, true, stampColumns);
    }

    Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns);

    default Table snapshotIncremental(Table rightTable, String... stampColumns) {
        return snapshotIncremental(rightTable, false, stampColumns);
    }

    Table snapshotHistory(final Table rightTable);

    @Override
    default Table snapshot(Table baseTable, boolean doInitialSnapshot, Collection<ColumnName> stampColumns) {
        return snapshot(baseTable, doInitialSnapshot, stampColumns.stream().map(ColumnName::name).toArray(String[]::new));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Miscellaneous Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Applies a function to this table.
     * <p>
     * This is useful if you have a reference to a table or a proxy and want to run a series of operations against the
     * table without each individual operation resulting in an RMI.
     *
     * @implNote If the LTM is not required the {@link Function.Unary#call(Object)} method should be annotated with
     *           {@link AsyncMethod}.
     *
     * @param function the function to run, its single argument will be this table
     * @param <R>      the return type of function
     * @return the return value of function
     */
    default <R> R apply(Function.Unary<R, Table> function) {
        final QueryPerformanceNugget nugget = QueryPerformanceRecorder.getInstance().getNugget("apply(" + function + ")");

        try {
            return function.call(this);
        } finally {
            nugget.done();
        }
    }

    /**
     * Return true if this table is guaranteed to be flat.
     * The index of a flat table will be from 0...numRows-1.
     */
    @AsyncMethod
    boolean isFlat();

    /**
     * Creates a version of this table with a flat index (V2 only).
     */
    @AsyncMethod
    Table flatten();

    /**
     * Set the table's key columns.
     *
     * @return The same table this method was invoked on, with the keyColumns attribute set
     */
    @AsyncMethod
    Table withKeys(String... columns);

    /**
     * Set the table's key columns and indicate that each key set will be unique.
     *
     * @return The same table this method was invoked on, with the keyColumns
     *         and unique attributes set
     */
    @AsyncMethod
    Table withUniqueKeys(String... columns);

    @AsyncMethod
    Table layoutHints(String hints);

    @AsyncMethod
    default Table layoutHints(LayoutHintBuilder builder) {
        return layoutHints(builder.build());
    }

    @AsyncMethod
    Table withTableDescription(String description);

    /**
     * Add a description for a specific column.  You may use {@link #withColumnDescription(Map)} to set several descriptions
     * at once.
     *
     * @param column the name of the column
     * @param description the column description
     *
     * @return a copy of the source table with the description applied
     */
    @AsyncMethod
    default Table withColumnDescription(String column, String description) {
        return withColumnDescription(Collections.singletonMap(column, description));
    }

    /**
     * Add a set of column descriptions to the table.
     *
     * @param descriptions a map of Column name to Column description.
     * @return a copy of the table with the descriptions applied.
     */
    @AsyncMethod
    Table withColumnDescription(Map<String, String> descriptions);

    /**
     * Sets parameters for the default totals table display.
     *
     * @param builder a {@link TotalsTableBuilder} object
     * @return a table with the totals applied
     */
    @AsyncMethod
    Table setTotalsTable(TotalsTableBuilder builder);

    /**
     * Sets renderers for columns.
     *
     * @param builder a builder that creates the packed string for the attribute
     * @return The same table with the ColumnRenderes attribute set
     */
    @AsyncMethod
    Table setColumnRenderers(ColumnRenderersBuilder builder);

    // -----------------------------------------------------------------------------------------------------------------
    // Resource Management
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Release resources held by this table, possibly destructively. This may render the table unsuitable or unsafe for
     * further use.
     *
     * @apiNote In practice, implementations usually just invoke {@link #releaseCachedResources()}.
     */
    default void close() {
        releaseCachedResources();
    }

    /**
     * Attempt to release cached resources held by this table. Unlike {@link #close()}, this must not render the table
     * unusable for subsequent read operations. Implementations should be sure to call
     * {@code super.releaseCachedResources()}.
     */
    default void releaseCachedResources() {
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Legacy Column-Oriented Grouping
    // -----------------------------------------------------------------------------------------------------------------

    @Deprecated
    void addColumnGrouping(String columnName);
}
