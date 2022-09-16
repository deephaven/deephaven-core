/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.base.cache.OpenAddressedCanonicalizationCache;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.qst.column.header.ColumnHeader;
import org.jetbrains.annotations.NotNull;

import java.util.Map.Entry;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Table definition for all Deephaven tables.
 */
public class TableDefinition implements LogOutputAppendable {

    public static TableDefinition EMPTY = new TableDefinition(List.of());

    private static final OpenAddressedCanonicalizationCache INTERNED_DEFINITIONS =
            new OpenAddressedCanonicalizationCache();

    public static TableDefinition of(ColumnDefinition<?>... columnDefinitions) {
        return new TableDefinition(new ArrayList<>(Arrays.asList(columnDefinitions)));
    }

    public static TableDefinition of(Collection<ColumnDefinition<?>> columnDefinitions) {
        return new TableDefinition(new ArrayList<>(columnDefinitions));
    }

    public static TableDefinition inferFrom(@NotNull final Map<String, ? extends ColumnSource<?>> sources) {
        final List<ColumnDefinition<?>> definitions = new ArrayList<>(sources.size());
        for (Entry<String, ? extends ColumnSource<?>> e : sources.entrySet()) {
            final String name = e.getKey();
            final ColumnSource<?> source = e.getValue();
            final ColumnDefinition<?> inferred =
                    ColumnDefinition.fromGenericType(name, source.getType(), source.getComponentType());
            definitions.add(inferred);
        }
        return new TableDefinition(definitions);
    }

    public static TableDefinition from(@NotNull final Iterable<ColumnHeader<?>> headers) {
        final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        for (ColumnHeader<?> columnHeader : headers) {
            final ColumnDefinition<?> columnDefinition = ColumnDefinition.from(columnHeader);
            definitions.add(columnDefinition);
        }
        return new TableDefinition(definitions);
    }

    /**
     * Convenience factory method for use with parallel arrays of column names and data types. All
     * {@link ColumnDefinition column definitions} will have default {@link ColumnDefinition#getColumnType() component
     * type} and {@link ColumnDefinition.ColumnType#Normal normal column type}.
     *
     * @param columnNames An array of column names to use, parallel to {@code columnDataTypes}
     * @param columnDataTypes An array of column data types to use, parallel to {@code columnNames}
     * @return The resulting TableDefinition
     */
    public static TableDefinition from(
            @NotNull final String[] columnNames,
            @NotNull final Class<?>[] columnDataTypes) {
        if (columnNames.length != columnDataTypes.length) {
            throw new IllegalArgumentException(String.format(
                    "Input size mismatch: columnNames is of length %d, but columnDataTypes is of length %d",
                    columnNames.length, columnDataTypes.length));
        }
        return new TableDefinition(IntStream.range(0, columnNames.length)
                .mapToObj(ci -> ColumnDefinition.fromGenericType(columnNames[ci], columnDataTypes[ci]))
                .toArray(ColumnDefinition[]::new));
    }

    /**
     * Convenience factory method for use with parallel structures of column names and data types. All
     * {@link ColumnDefinition column definitions} will have default {@link ColumnDefinition#getColumnType() component
     * type} and {@link ColumnDefinition.ColumnType#Normal normal column type}.
     *
     * @param columnNames Column names to use, parallel to {@code columnDataTypes}
     * @param columnDataTypes Column data types to use, parallel to {@code columnNames}
     * @return The resulting TableDefinition
     */
    public static TableDefinition from(
            @NotNull final Iterable<String> columnNames,
            @NotNull final Iterable<Class<?>> columnDataTypes) {
        final Iterator<String> cn = columnNames.iterator();
        final Iterator<Class<?>> cdt = columnDataTypes.iterator();
        final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>();
        while (cn.hasNext() && cdt.hasNext()) {
            columnDefinitions.add(ColumnDefinition.fromGenericType(cn.next(), cdt.next()));
        }
        if (cn.hasNext() || cdt.hasNext()) {
            throw new IllegalArgumentException(
                    "Input size mismatch: columnNames and columnDataTypes are not the same size");
        }
        return new TableDefinition(columnDefinitions);
    }

    private final List<ColumnDefinition<?>> columns;

    private int cachedHashCode;
    private Map<String, ColumnDefinition<?>> columnNameMap;

    private TableDefinition(@NotNull final ColumnDefinition<?>[] columnDefinitions) {
        this(Arrays.asList(columnDefinitions));
    }

    private TableDefinition(@NotNull final Collection<ColumnDefinition<?>> columnDefinitions) {
        final List<ColumnDefinition<?>> columns = new ArrayList<>(columnDefinitions);
        this.columns = Collections.unmodifiableList(checkForNullOrDuplicates(columns));
    }

    private static List<ColumnDefinition<?>> checkForNullOrDuplicates(
            @NotNull final List<ColumnDefinition<?>> columns) {
        if (columns.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Supplied ColumnDefinitions include one or more null values");
        }
        final Set<String> columnNames = new HashSet<>(columns.size());
        final List<String> duplicateNames = columns.stream().map(ColumnDefinition::getName)
                .filter((final String columnName) -> !columnNames.add(columnName))
                .collect(Collectors.toList());
        if (!duplicateNames.isEmpty()) {
            throw new IllegalArgumentException("Supplied ColumnDefinitions include duplicate names " + duplicateNames);
        }
        return columns;
    }

    /**
     * Intern {@code this} TableDefinition in order to avoid storing many identical instances. Useful (for example) in
     * heavily partitioned workloads.
     *
     * @return An interned TableDefinition that is equal to {@code this}
     */
    public TableDefinition intern() {
        return INTERNED_DEFINITIONS.getCachedItem(this);
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        logOutput.append("TableDefinition {");
        logOutput.append("columns=[");
        boolean first = true;
        for (final ColumnDefinition<?> column : columns) {
            if (first) {
                first = false;
            } else {
                logOutput.append(", ");
            }
            logOutput.append(column);
        }
        logOutput.append("]}");
        return logOutput;
    }

    /**
     * @return The number of columns for this table definition
     */
    public int numColumns() {
        return columns.size();
    }

    /**
     * @return An unmodifiable list of the column definitions for this table definition
     */
    public List<ColumnDefinition<?>> getColumns() {
        return columns;
    }

    /**
     * @return An array of the column definitions for this table definition
     */
    public ColumnDefinition<?>[] getColumnsArray() {
        return columns.toArray(ColumnDefinition.ZERO_LENGTH_COLUMN_DEFINITION_ARRAY);
    }

    /**
     * @return A stream of the column definitions for this table definition
     */
    public Stream<ColumnDefinition<?>> getColumnStream() {
        return columns.stream();
    }

    /**
     * @return An unmodifiable map from column name to column definition
     */
    public Map<String, ColumnDefinition<?>> getColumnNameMap() {
        if (columnNameMap != null) {
            return columnNameMap;
        }
        return columnNameMap = Collections.unmodifiableMap(getColumnStream().collect(Collectors
                .toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked, LinkedHashMap::new)));
    }

    /**
     * @return A list of {@link ColumnDefinition column definitions} for all
     *         {@link ColumnDefinition.ColumnType#Partitioning partitioning} columns in the same relative order as the
     *         column definitions list
     */
    public List<ColumnDefinition<?>> getPartitioningColumns() {
        return getColumnStream().filter(ColumnDefinition::isPartitioning).collect(Collectors.toList());
    }

    /**
     * @return A list of {@link ColumnDefinition column definitions} for all {@link ColumnDefinition.ColumnType#Grouping
     *         grouping} columns in the same relative order as the column definitions list
     */
    public List<ColumnDefinition<?>> getGroupingColumns() {
        return getColumnStream().filter(ColumnDefinition::isGrouping).collect(Collectors.toList());
    }

    /**
     * @return An array containing the names of all {@link ColumnDefinition.ColumnType#Grouping grouping} columns in the
     *         same relative order as the column definitions list
     */
    public String[] getGroupingColumnNamesArray() {
        return getColumnStream().filter(ColumnDefinition::isGrouping).map(ColumnDefinition::getName)
                .toArray(String[]::new);
    }

    /**
     * @return The column names as a list in the same order as {@link #getColumns()}
     */
    public List<String> getColumnNames() {
        return getColumnStream().map(ColumnDefinition::getName).collect(Collectors.toList());
    }

    /**
     * @return The {@link ColumnName column names} as a list in the same order as {@link #getColumns()}
     */
    public List<ColumnName> getTypedColumnNames() {
        return getColumnStream().map(ColumnDefinition::getName).map(ColumnName::of).collect(Collectors.toList());
    }

    /**
     * @return The column names as an array in the same order as {@link #getColumns()}
     */
    public String[] getColumnNamesArray() {
        return getColumnStream().map(ColumnDefinition::getName).toArray(String[]::new);
    }

    /**
     * @return The column {@link ColumnDefinition#getDataType() data types} as a list in the same order as
     *         {@link #getColumns()}
     */
    public List<Class<?>> getColumnTypes() {
        return getColumnStream().map(ColumnDefinition::getDataType).collect(Collectors.toList());
    }

    /**
     * @return The column {@link ColumnDefinition#getDataType() data types} as an array in the same order as
     *         {@link #getColumns()}
     */
    public Class<?>[] getColumnTypesArray() {
        return getColumnStream().map(ColumnDefinition::getDataType).toArray(Class[]::new);
    }

    /**
     * @param columnName The column name to search for
     * @param <T> The column {@link ColumnDefinition#getDataType() data types}, as a type parameter
     * @return The {@link ColumnDefinition} for the supplied name, or {@code null} if no such column exists in this
     *         table definition
     */
    public <T> ColumnDefinition<T> getColumn(@NotNull final String columnName) {
        // noinspection unchecked
        return (ColumnDefinition<T>) getColumnNameMap().get(columnName);
    }

    /**
     * @param column The {@link ColumnDefinition} to search for
     * @return The index of {@code column}, or {@code -1} if no such column exists in this table definition
     * @apiNote This is an O({@link #numColumns()}) lookup.
     */
    public int getColumnIndex(@NotNull final ColumnDefinition<?> column) {
        return columns.indexOf(column);
    }

    /**
     * @return A String of column names joined with {@code ','}
     */
    @SuppressWarnings("unused")
    public String getColumnNamesAsString() {
        return getColumnStream().map(ColumnDefinition::getName).collect(Collectors.joining(","));
    }

    /**
     * Tests mutual-compatibility of {@code this} and {@code other}. To be mutually compatible, they must have the same
     * number of columns, each matched up with {@link ColumnDefinition#isCompatible}. As such, this method has an
     * equivalence relation, ie {@code A.checkMutualCompatibility(B) == B.checkMutualCompatibility(A)}.
     *
     * @param other The other definition
     * @return {@code this} table definition, but in the column order of {@code other}
     * @throws IncompatibleTableDefinitionException if the definitions are not compatible
     */
    public TableDefinition checkMutualCompatibility(@NotNull final TableDefinition other) {
        return checkMutualCompatibility(other, "this", "other");
    }

    /**
     * Tests mutual-compatibility of {@code this} and {@code other}. To be mutually compatible, they must have the same
     * number of columns, each matched up with {@link ColumnDefinition#isCompatible}. As such, this method has an
     * equivalence relation, ie {@code A.checkMutualCompatibility(B) == B.checkMutualCompatibility(A)}.
     *
     * @param other The other definition
     * @param lhsName Name to use when describing {@code this} if an exception is thrown
     * @param rhsName Name to use when describing {@code other} if an exception is thrown
     * @return {@code this} table definition, but in the column order of {@code other}
     * @throws IncompatibleTableDefinitionException if the definitions are not compatible
     */
    public TableDefinition checkMutualCompatibility(
            @NotNull final TableDefinition other,
            @NotNull final String lhsName,
            @NotNull final String rhsName) {
        if (equals(other)) {
            return this;
        }
        final TableDefinition result = checkCompatibilityInternal(other, false);
        if (result == null || other.checkCompatibilityInternal(this, false) == null) {
            final List<String> differences = describeCompatibilityDifferences(other, lhsName, rhsName);
            throw new IncompatibleTableDefinitionException("Table definition incompatibilities: \n\t"
                    + String.join("\n\t", differences));
        }
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
    public TableDefinition checkCompatibility(
            @NotNull final TableDefinition other,
            final boolean ignorePartitioningColumns) {
        if (equals(other)) {
            return this;
        }
        final TableDefinition minimized = checkCompatibilityInternal(other, ignorePartitioningColumns);
        if (minimized != null) {
            return minimized;
        }
        final List<String> differences = describeCompatibilityDifferences(other, "this", "other");
        throw new IncompatibleTableDefinitionException("Table definition incompatibilities: "
                + String.join("\n\t", differences));
    }

    /**
     * Test compatibility of {@code this} with {@code other}. This definition must have all columns of the other, and
     * the column definitions in common must be compatible, as defined by
     * {@link ColumnDefinition#isCompatible(ColumnDefinition)}.
     *
     * @param other The definition to compare to
     * @param ignorePartitioningColumns If true, {@code other} may contain partitioning columns not in {@code this}
     * @return The minimized compatible table definition, in the same order as {@code other}, or {@code null} if
     *         incompatible
     */
    private TableDefinition checkCompatibilityInternal(
            @NotNull final TableDefinition other,
            final boolean ignorePartitioningColumns) {
        final List<ColumnDefinition<?>> inOrder = new ArrayList<>();

        final Map<String, ColumnDefinition<?>> myNamesToColumns = getColumnNameMap();
        for (final ColumnDefinition<?> otherColumn : other.columns) {
            if (ignorePartitioningColumns && otherColumn.isPartitioning())
                continue;
            final ColumnDefinition<?> myColumn = myNamesToColumns.get(otherColumn.getName());
            if (myColumn == null) {
                return null;
            } else if (!myColumn.isCompatible(otherColumn)) {
                return null;
            }
            inOrder.add(myColumn);
        }

        return new TableDefinition(inOrder);
    }

    /**
     * Build a description of the difference between this definition and the other. Should correspond to
     * {@link #equalsIgnoreOrder} logic.
     *
     * @param other another TableDefinition to compare
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @return a list of strings representing the difference between two table definitions
     */
    public List<String> describeDifferences(@NotNull final TableDefinition other, @NotNull final String lhs,
            @NotNull final String rhs) {
        return describeDifferences(other, lhs, rhs, ColumnDefinition::equals, true);
    }

    /**
     * Build a description of the difference between this definition and the other. Should correspond to
     * {@link #checkMutualCompatibility} logic.
     *
     * @param other another TableDefinition to compare
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @return a list of strings representing the difference between two table definitions
     */
    public List<String> describeCompatibilityDifferences(
            @NotNull final TableDefinition other,
            @NotNull final String lhs,
            @NotNull final String rhs) {
        return describeDifferences(other, lhs, rhs, ColumnDefinition::isCompatible, false);
    }

    @FunctionalInterface
    private interface ColumnDefinitionEqualityTest {
        boolean match(ColumnDefinition<?> c1, ColumnDefinition<?> c2);
    }

    private List<String> describeDifferences(
            @NotNull final TableDefinition other, @NotNull final String lhs, @NotNull final String rhs,
            @NotNull final ColumnDefinitionEqualityTest test, final boolean includeColumnType) {
        if (this == other) {
            return Collections.emptyList();
        }
        final List<String> differences = new ArrayList<>();

        final Map<String, ColumnDefinition<?>> otherColumns = other.getColumnNameMap();
        for (final ColumnDefinition<?> thisColumn : columns) {
            final ColumnDefinition<?> otherColumn = otherColumns.get(thisColumn.getName());
            if (otherColumn == null) {
                differences.add(lhs + " column '" + thisColumn.getName() + "' is missing in " + rhs);
            } else if (!test.match(thisColumn, otherColumn)) {
                differences.add("column '" + thisColumn.getName() + "' is different ...");
                thisColumn.describeDifferences(differences, otherColumn, lhs, rhs,
                        "    " + thisColumn.getName() + ": ", includeColumnType);
            }
            // else same
        }

        final Map<String, ColumnDefinition<?>> thisColumns = getColumnNameMap();
        for (final ColumnDefinition<?> otherColumn : other.getColumns()) {
            if (null == thisColumns.get(otherColumn.getName())) {
                differences.add(rhs + " column '" + otherColumn.getName() + "' is missing in " + lhs);
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
        if (this == other) {
            return true;
        }
        if (columns.size() != other.columns.size()) {
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
        return columns.equals(otherTD.columns);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode != 0) {
            return cachedHashCode;
        }
        final int columnsHashCode = columns.hashCode();
        return cachedHashCode = columnsHashCode == 0 ? 31 : columnsHashCode;
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
        final List<ColumnDefinition<?>> writableColumns = getWritableColumns(partitioningToNormal);
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
    private List<ColumnDefinition<?>> getWritableColumns(final boolean partitioningToNormal) {
        if (getColumnStream().anyMatch(c -> !c.isDirect())) {
            if (partitioningToNormal) {
                return getColumnStream()
                        .filter(c -> c.isDirect() || c.isPartitioning())
                        .map(c -> c.isPartitioning() ? c.withNormal() : c)
                        .collect(Collectors.toList());
            }
            return getColumnStream().filter(ColumnDefinition::isDirect).collect(Collectors.toList());
        }
        return columns;
    }

    /**
     * Runtime exception representing an incompatibility between table definitions.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class IncompatibleTableDefinitionException extends UncheckedDeephavenException {

        private static final long serialVersionUID = 7668080323885707687L;

        public IncompatibleTableDefinitionException() {
            super();
        }

        public IncompatibleTableDefinitionException(String message) {
            super(message);
        }

        public IncompatibleTableDefinitionException(String message, Throwable cause) {
            super(message, cause);
        }

        public IncompatibleTableDefinitionException(Throwable cause) {
            super(cause);
        }
    }
}
