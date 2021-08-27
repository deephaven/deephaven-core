/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.base.Copyable;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.HashCodeUtil;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.qst.column.header.ColumnHeader;
import java.util.Map.Entry;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Table definition for all Deephaven tables.
 */
public class TableDefinition implements Externalizable, LogOutputAppendable, Copyable<TableDefinition> {
    private static final long serialVersionUID = -120432133075760976L;

    private static final String NEW_LINE = System.getProperty("line.separator");

    public static TableDefinition of(ColumnDefinition<?>... columnDefinitions) {
        return new TableDefinition(Arrays.asList(columnDefinitions));
    }

    public static TableDefinition inferFrom(Map<String, ? extends ColumnSource<?>> sources) {
        List<ColumnDefinition<?>> definitions = new ArrayList<>(sources.size());
        for (Entry<String, ? extends ColumnSource<?>> e : sources.entrySet()) {
            final String name = e.getKey();
            final ColumnSource<?> source = e.getValue();
            final ColumnDefinition<?> inferred =
                    ColumnDefinition.fromGenericType(name, source.getType(), source.getComponentType());
            definitions.add(inferred);
        }
        return new TableDefinition(definitions);
    }

    public static TableDefinition from(Iterable<ColumnHeader<?>> headers) {
        List<ColumnDefinition<?>> definitions = new ArrayList<>();
        for (ColumnHeader<?> columnHeader : headers) {
            final ColumnDefinition<?> columnDefinition = ColumnDefinition.from(columnHeader);
            definitions.add(columnDefinition);
        }
        return new TableDefinition(definitions);
    }

    private transient Map<String, ColumnDefinition<?>> columnNameMap;

    public TableDefinition() {}

    public TableDefinition(@NotNull final List<Class<?>> types, @NotNull final List<String> columnNames) {
        this(getColumnDefinitions(types, columnNames));
    }

    public TableDefinition(@NotNull final List<ColumnDefinition<?>> columnDefs) {
        this.setColumns(columnDefs.toArray(new ColumnDefinition[0]));
    }

    public TableDefinition(@NotNull final ColumnDefinition<?>[] columnDefs) {
        this.setColumns(columnDefs);
    }

    public TableDefinition(@NotNull final TableDefinition other) {
        this.setColumns(other.columns);
        this.columnNameMap = other.columnNameMap;
    }

    public static TableDefinition tableDefinition(@NotNull final Class<?>[] types,
            @NotNull final String[] columnNames) {
        return new TableDefinition(getColumnDefinitions(types, columnNames));
    }

    @Override
    public String toString() {
        return super.toString() + "|columns=" + Arrays.deepToString(columns);
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        logOutput.append("TableDefinition");
        logOutput.append("|columns=[");
        for (final ColumnDefinition<?> column : columns) {
            logOutput.append(column);
        }
        logOutput.append(']');
        return logOutput;
    }

    public void setColumns(final ColumnDefinition<?>[] columns) {
        Require.elementsNeqNull(columns, "columns");
        final Set<String> columnNames = new HashSet<>();
        for (final ColumnDefinition<?> column : columns) {
            if (!columnNames.add(column.getName())) {
                throw new IllegalArgumentException("Duplicate definition for column \"" + column.getName() + "\"");
            }
        }
        columnNameMap = null;
        this.columns = columns;
    }

    /**
     * @return A list view of the column definition array for this table definition.
     */
    public List<ColumnDefinition<?>> getColumnList() {
        return Collections.unmodifiableList(Arrays.asList(columns));
    }

    /**
     * @return A stream of the column definition array for this table definition
     */
    public Stream<ColumnDefinition<?>> getColumnStream() {
        return Arrays.stream(columns);
    }

    /**
     * @return A freshly-allocated, unmodifiable map from column name to column definition.
     */
    public Map<String, ColumnDefinition<?>> getColumnNameMap() {
        if (columnNameMap != null) {
            return columnNameMap;
        }
        return columnNameMap = Collections.unmodifiableMap(getColumnStream().collect(Collectors
                .toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked, LinkedHashMap::new)));
    }

    /**
     * @return A freshly-allocated list of column definitions for all partitioning columns, in the same relative order
     *         as in the column definitions array.
     */
    public List<ColumnDefinition<?>> getPartitioningColumns() {
        return getColumnStream().filter(ColumnDefinition::isPartitioning).collect(Collectors.toList());
    }

    /**
     * @return A freshly-allocated list of column definitions for all grouping columns, in the same relative order as in
     *         the column definitions array.
     */
    public List<ColumnDefinition<?>> getGroupingColumns() {
        return getColumnStream().filter(ColumnDefinition::isGrouping).collect(Collectors.toList());
    }

    /**
     * @return A freshly-allocated array of column names for all grouping columns, in the same relative order as in the
     *         column definitions array.
     */
    public String[] getGroupingColumnNamesArray() {
        return getColumnStream().filter(ColumnDefinition::isGrouping).map(ColumnDefinition::getName)
                .toArray(String[]::new);
    }

    /**
     * @return A freshly-allocated list of column names in the same order as the column definitions array.
     */
    public List<String> getColumnNames() {
        return getColumnStream().map(ColumnDefinition::getName).collect(Collectors.toList());
    }

    /**
     * @return A freshly-allocated array of column names in the same order as the column definitions array.
     */
    public String[] getColumnNamesArray() {
        return getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);
    }

    /**
     * @return A freshly-allocated list of column types in the same order as the column definitions array.
     */
    public List<Class<?>> getColumnTypes() {
        return getColumnStream().map(ColumnDefinition::getDataType).collect(Collectors.toList());
    }

    /**
     * @return A freshly-allocated array of column types in the same order as the column definitions array.
     */
    public Class<?>[] getColumnTypesArray() {
        return getColumnStream().map(ColumnDefinition::getDataType).toArray(Class[]::new);
    }

    /**
     * @param columnName the column name to search for
     * @param <T> The target type, as a type parameter. Inferred from context.
     * @return The column definition for the supplied name, or null if no such column exists in this table definition.
     */
    public <T> ColumnDefinition<T> getColumn(@NotNull final String columnName) {
        // noinspection unchecked
        return (ColumnDefinition<T>) getColumnNameMap().get(columnName);
    }

    /**
     * @param column the ColumnDefinition to search for
     * @return The index of the column for the supplied name, or -1 if no such column exists in this table definition.
     *         <b>Note:</b> This is an O(columns.length) lookup.
     */
    public int getColumnIndex(@NotNull final ColumnDefinition<?> column) {
        for (int ci = 0; ci < columns.length; ++ci) {
            if (column.equals(columns[ci])) {
                return ci;
            }
        }
        return -1;
    }

    /**
     * @return A freshly-allocated String of column names joined with ','.
     */
    @SuppressWarnings("unused")
    public String getColumnNamesAsString() {
        final StringBuilder sb = new StringBuilder();
        for (final ColumnDefinition<?> column : columns) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(column.getName());
        }
        return sb.toString();
    }

    /**
     * Tests mutual-compatibility of {@code this} and {@code other}. To be mutually compatible, they must have the same
     * number of columns, each matched up with {@link ColumnDefinition#isCompatible}. As such, this method has an
     * equivalence relation, ie {@code A.checkMutualCompatibility(B) == B.checkMutualCompatibility(A)}.
     *
     * @param other the other definition
     * @return {@code this} table definition, but in the the column order of {@code other}
     */
    public TableDefinition checkMutualCompatibility(@NotNull final TableDefinition other) {
        TableDefinition result = checkCompatibility(other, false);
        other.checkCompatibility(this, false);
        return result;
    }

    /**
     * Test compatibility of this definition with another. This definition must have all columns of the other, and the
     * column definitions in common must be compatible, as defined by
     * {@link ColumnDefinition#isCompatible(ColumnDefinition)}.
     *
     * <p>
     * Note: unlike {@link ColumnDefinition#isCompatible(ColumnDefinition)}, this method does not have an equivalence
     * relation. For a stricter check, use {@link #checkMutualCompatibility(TableDefinition)}.
     *
     * @param other comparison table definition
     * @return the minimized compatible table definition, in the same order as {@code other}
     * @throws IncompatibleTableDefinitionException if the definitions are not compatible
     */
    public TableDefinition checkCompatibility(@NotNull final TableDefinition other) {
        return checkCompatibility(other, false);
    }

    /**
     * Test compatibility of this definition with another. This definition must have all columns of the other, and the
     * column definitions in common must be compatible, as defined by
     * {@link ColumnDefinition#isCompatible(ColumnDefinition)}.
     *
     * <p>
     * Partitioning columns in other will be ignored if ignorePartitioningColumns is true.
     *
     * <p>
     * Note: unlike {@link ColumnDefinition#isCompatible(ColumnDefinition)}, this method does not have an equivalence
     * relation. For a stricter check, use {@link #checkMutualCompatibility(TableDefinition)}.
     *
     * @param other comparison table definition
     * @param ignorePartitioningColumns if true, other definition may contain partitioning columns not in this
     *        definition
     * @return the minimized compatible table definition, in the same order as {@code other}
     * @throws IncompatibleTableDefinitionException if the definitions are not compatible
     */
    public TableDefinition checkCompatibility(@NotNull final TableDefinition other,
            final boolean ignorePartitioningColumns) {
        List<ColumnDefinition<?>> inOrder = new ArrayList<>();

        // TODO: need to compare in order and be less permissive with partitioning -
        final StringBuilder sb = new StringBuilder();
        final Map<String, ColumnDefinition<?>> myNamesToColumns = getColumnNameMap();
        for (final ColumnDefinition<?> otherColumn : other.columns) {
            if (ignorePartitioningColumns && otherColumn.isPartitioning())
                continue;
            final ColumnDefinition<?> myColumn = myNamesToColumns.get(otherColumn.getName());
            if (myColumn == null) {
                sb.append(NEW_LINE).append("\tMissing column definition for ").append(otherColumn.getName());
            } else if (!myColumn.isCompatible(otherColumn)) {
                sb.append(NEW_LINE)
                        .append("\tColumn definitions aren't compatible - ")
                        .append("found column ")
                        .append(myColumn.describeForCompatibility())
                        .append(", expected compatibility with ")
                        .append(otherColumn.describeForCompatibility());
            }
            inOrder.add(myColumn);
        }
        if (sb.length() > 0) {
            throw new IncompatibleTableDefinitionException("Table definition incompatibilities: " + sb.toString());
        }
        return new TableDefinition(inOrder);
    }

    /**
     * Build a description of the difference between this definition and the other. Should correspond to
     * equalsIgnoreOrder logic.
     *
     * @param other another TableDefinition to compare
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @return a list of strings representing the difference between two table definitions
     */
    public List<String> describeDifferences(@NotNull final TableDefinition other, @NotNull final String lhs,
            @NotNull final String rhs) {
        final List<String> differences = new ArrayList<>();

        final Map<String, ColumnDefinition<?>> otherColumns = other.getColumnNameMap();
        for (final ColumnDefinition<?> thisColumn : columns) {
            final ColumnDefinition<?> otherColumn = otherColumns.get(thisColumn.getName());
            if (otherColumn == null) {
                differences.add(lhs + " column '" + thisColumn.getName() + "' is missing in " + rhs);
            } else if (!thisColumn.equals(otherColumn)) {
                differences.add("column '" + thisColumn.getName() + "' is different ...");
                thisColumn.describeDifferences(differences, otherColumn, lhs, rhs,
                        "    " + thisColumn.getName() + ": ");
            }
            // else same
        }

        final Map<String, ColumnDefinition<?>> thisColumns = getColumnNameMap();
        for (final ColumnDefinition<?> otherColumn : other.getColumns()) {
            if (null == thisColumns.get(otherColumn.getName())) {
                differences.add("column '" + otherColumn.getName() + "' is missing in " + lhs);
            }
        }

        return differences;
    }

    /**
     * Build a description of the difference between this definition and the other. Should correspond to
     * equalsIgnoreOrder logic.
     *
     * @param other another TableDefinition to compare
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @param separator separate strings in the list of differences with this separator
     * @return A string in which the differences are enumerated, separated by the given separator
     */
    public String getDifferenceDescription(@NotNull final TableDefinition other, @NotNull final String lhs,
            @NotNull final String rhs, @NotNull final String separator) {
        List<String> differences = describeDifferences(other, lhs, rhs);
        return String.join(separator, differences);
    }

    /**
     * Strict comparison (column-wise only).
     *
     * @param other - The other TableDefinition to compare with.
     * @return True if other contains equal ColumnDefinitions in any order. False otherwise.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean equalsIgnoreOrder(@NotNull final TableDefinition other) {
        if (columns.length != other.columns.length) {
            return false;
        }
        final Iterator<ColumnDefinition<?>> thisColumns =
                getColumnStream().sorted(Comparator.comparing(ColumnDefinition::getName)).iterator();
        final Iterator<ColumnDefinition<?>> otherColumns =
                other.getColumnStream().sorted(Comparator.comparing(ColumnDefinition::getName)).iterator();
        while (thisColumns.hasNext()) {
            if (!thisColumns.next().equals(otherColumns.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Strict comparison (column-wise only).
     *
     * @param other - The object to compare with.
     * @return True if other is a TableDefinition and contains equal ColumnDefinitions in the same order. False
     *         otherwise.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TableDefinition)) {
            return false;
        }
        final TableDefinition otherTD = (TableDefinition) other;
        if (columns.length != otherTD.columns.length) {
            return false;
        }
        for (int cdi = 0; cdi < columns.length; ++cdi) {
            if (!columns[cdi].equals(otherTD.columns[cdi])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.combineHashCodes((Object[]) columns);
    }


    /**
     * Factory helper function for column definitions.
     *
     * @param columnTypes List of column types
     * @param columnNames List of column names, parallel to columnTypes
     * @return A new array of column definitions from the supplied lists of types and names.
     */
    private static ColumnDefinition<?>[] getColumnDefinitions(@NotNull final List<Class<?>> columnTypes,
            @NotNull final List<String> columnNames) {
        Require.eq(columnTypes.size(), "types.size()", columnNames.size(), "columnNames.size()");

        final ColumnDefinition<?>[] result = new ColumnDefinition[columnTypes.size()];
        for (int ci = 0; ci < result.length; ++ci) {
            result[ci] = ColumnDefinition.fromGenericType(columnNames.get(ci), columnTypes.get(ci));
        }

        return result;
    }

    /**
     * Factory helper function for column definitions.
     *
     * @param columnTypes Array of column types
     * @param columnNames Array of column names, parallel to columnTypes
     * @param additionalColumnDefs optional additional column definitions to add at the beginning.
     * @return A new array of column definitions from the supplied lists of types and names.
     */
    private static ColumnDefinition<?>[] getColumnDefinitions(
            @NotNull final Class<?>[] columnTypes, @NotNull final String[] columnNames,
            ColumnDefinition<?>... additionalColumnDefs) {
        Require.eq(columnTypes.length, "types.length", columnNames.length, "columnNames.length");

        final ColumnDefinition<?>[] result = new ColumnDefinition[columnTypes.length + additionalColumnDefs.length];
        int ri = 0;
        for (ColumnDefinition<?> additionalColumnDef : additionalColumnDefs) {
            result[ri++] = additionalColumnDef;
        }

        for (int ci = 0; ci < columnTypes.length; ++ci) {
            result[ri++] = ColumnDefinition.fromGenericType(columnNames[ci], columnTypes[ci]);
        }

        return result;
    }

    /**
     * @return This definition if it's writable, or a freshly-allocated definition that is identical but for the columns
     *         array, which will exclude all non-writable columns.
     */
    public TableDefinition getWritable() {
        return getWritable(false);
    }

    /**
     * @return This definition if it's writable, or a freshly-allocated definition that is identical but for the columns
     *         array, which will exclude all non-writable columns, optionally converting partitioning columns to normal
     *         columns.
     * @param partitioningToNormal Whether partitioning columns should be preserved as normal columns, or excluded
     */
    public TableDefinition getWritable(final boolean partitioningToNormal) {
        final ColumnDefinition<?>[] writableColumns = getWritableColumns(partitioningToNormal);
        if (writableColumns == columns) {
            return this;
        }
        return new TableDefinition(writableColumns);
    }

    /**
     * @return This definition's array of column definitions if they're all writable, or a freshly-allocated array of
     *         column definitions which will exclude all non-writable columns, optionally converting partitioning
     *         columns to normal columns.
     * @param partitioningToNormal Whether partitioning columns should be preserved as normal columns, or excluded
     */
    public ColumnDefinition<?>[] getWritableColumns(final boolean partitioningToNormal) {
        if (getColumnStream().anyMatch(c -> !c.isDirect())) {
            if (partitioningToNormal) {
                return getColumnStream().filter(c -> c.isDirect() || c.isPartitioning()).map(c -> {
                    if (c.isPartitioning()) {
                        return c.withNormal();
                    }
                    return c;
                }).toArray(ColumnDefinition[]::new);
            }
            return getColumnStream().filter(ColumnDefinition::isDirect).toArray(ColumnDefinition[]::new);
        }
        return columns;
    }

    // TODO: Keep cleaning up. ImmutableColumnDefinition, or ImmutableADO? Builder pattern?

    public Table getColumnDefinitionsTable() {
        List<String> columnNames = new ArrayList<>();
        List<String> columnDataTypes = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<Boolean> columnPartitioning = new ArrayList<>();
        List<Boolean> columnGrouping = new ArrayList<>();
        for (ColumnDefinition<?> cDef : columns) {
            columnNames.add(cDef.getName());
            columnDataTypes.add(cDef.getDataType().getName());
            columnTypes.add(ColumnDefinition.COLUMN_TYPE_FORMATTER.format(cDef.getColumnType()));
            columnPartitioning.add(cDef.isPartitioning());
            columnGrouping.add(cDef.isGrouping());

        }
        final String[] resultColumnNames = {"Name", "DataType", "ColumnType", "IsPartitioning", "IsGrouping"};
        final Object[] resultValues = {
                columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnDataTypes.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnTypes.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnPartitioning.toArray(new Boolean[0]),
                columnGrouping.toArray(new Boolean[0]),
        };

        return new InMemoryTable(resultColumnNames, resultValues);
    }

    /**
     * Helper method to assist with definition creation for user-namespace partitioned tables. This version
     * automatically converts grouping columns to normal columns.
     *
     * @param partitioningColumnName The name of the column to use for partitioning
     * @param baseDefinition The definition to work from
     * @return A new definition suitable for writing partitioned tables with
     */
    public static TableDefinition createUserPartitionedTableDefinition(@NotNull final String partitioningColumnName,
            @NotNull final TableDefinition baseDefinition) {
        return createUserPartitionedTableDefinition(partitioningColumnName, baseDefinition, true);
    }

    /**
     * Helper method to assist with definition creation for user-namespace partitioned tables.
     *
     * @param partitioningColumnName The name of the column to use for partitioning
     * @param baseDefinition The definition to work from
     * @param groupingColumnsAsNormal Whether grouping columns should be converted to normal columns
     * @return A new definition suitable for writing partitioned tables with
     */
    @SuppressWarnings("WeakerAccess")
    public static TableDefinition createUserPartitionedTableDefinition(@NotNull final String partitioningColumnName,
            @NotNull final TableDefinition baseDefinition,
            final boolean groupingColumnsAsNormal) {
        final List<ColumnDefinition<?>> columnDefs = new ArrayList<>();
        columnDefs.add(ColumnDefinition.ofShort(partitioningColumnName).withPartitioning());
        final List<ColumnDefinition<?>> baseDefs = new ArrayList<>(baseDefinition.getColumnList());
        for (final ListIterator<ColumnDefinition<?>> iter = baseDefs.listIterator(); iter.hasNext();) {
            final ColumnDefinition<?> current = iter.next();
            if (current.getName().equals(partitioningColumnName)) {
                iter.remove();
                continue;
            }
            if (current.getColumnType() != ColumnDefinition.COLUMNTYPE_NORMAL &&
                    (current.getColumnType() != ColumnDefinition.COLUMNTYPE_GROUPING || groupingColumnsAsNormal)) {
                iter.set(current.withNormal());
            }
        }
        columnDefs.addAll(baseDefs);

        return new TableDefinition(columnDefs);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        columns = (ColumnDefinition<?>[]) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(columns);
    }

    /**
     * Runtime exception representing an incompatibility between table definitions.
     */
    @SuppressWarnings("WeakerAccess")
    public static class IncompatibleTableDefinitionException extends IllegalStateException {
        private static final long serialVersionUID = 7668080323885707687L;

        public IncompatibleTableDefinitionException() {
            super();
        }

        public IncompatibleTableDefinitionException(String s) {
            super(s);
        }

        public IncompatibleTableDefinitionException(String message, Throwable cause) {
            super(message, cause);
        }

        public IncompatibleTableDefinitionException(Throwable cause) {
            super(cause);
        }
    }

    protected io.deephaven.db.tables.ColumnDefinition<?>[] columns;

    public io.deephaven.db.tables.ColumnDefinition<?>[] getColumns() {
        return columns;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public TableDefinition clone() {
        return new TableDefinition(this);
    }

    @Override
    public void copyValues(final TableDefinition other) {
        this.columns = other.columns;
    }

    @Override
    public TableDefinition safeClone() {
        return clone();
    }
}
