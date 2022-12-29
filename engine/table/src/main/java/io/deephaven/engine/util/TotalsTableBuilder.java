/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.type.EnumValue;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.engine.table.Table.TOTALS_TABLE_ATTRIBUTE;

/**
 * Defines the default aggregations and display for a totals table.
 *
 * <p>
 * The builder is intended to be used with the {@link #applyToTable(Table)} method after the operations are applied.
 * </p>
 */
@ScriptApi
public class TotalsTableBuilder {

    /**
     * Enumeration representing valid aggregation types for {@link TotalsTableBuilder}.
     */
    public enum AggType {
        /** Return the number of rows in each group. */
        Count,
        /** Return the minimum value of each group. */
        Min,
        /** Return the maximum value of each group. */
        Max,
        /** Return the sum of values in each group. */
        Sum,
        /** Return the sum of absolute values in each group. */
        AbsSum,
        /** Return the variance of values in each group. */
        Var,
        /** Return the average of values in each group. */
        Avg,
        /** Return the standard deviation of each group. */
        Std,
        /** Return the first value of each group. */
        First,
        /** Return the last value of each group. */
        Last,
        /** Return the values of each group as a Vector. */
        Group,
        /** Return the number of unique values in each group */
        CountDistinct,
        /** Collect the distinct items from the column */
        Distinct,
        /**
         * Display the singular value from the column if it is unique, or a default value if none are present, or it is
         * not unique
         */
        Unique,
        /** Only valid in a TotalsTableBuilder to indicate we should not perform any aggregation. */
        Skip
    }

    private boolean showTotalsByDefault = false;
    private boolean showGrandTotalsByDefault = false;
    private AggType defaultOperation = AggType.Sum;
    private final Map<String, Set<AggType>> operationMap = new HashMap<>();
    private final Map<String, Map<AggType, String>> formatMap = new HashMap<>();

    /**
     * Should totals be shown by default?
     *
     * @return true if totals should be shown by default
     */
    public boolean getShowTotalsByDefault() {
        return showTotalsByDefault;
    }

    /**
     * Should grand totals be shown by default?
     *
     * @return true if grand totals should be shown by default
     */
    public boolean getShowGrandTotalsByDefault() {
        return showGrandTotalsByDefault;
    }

    /**
     * Set whether totals are shown by default.
     *
     * @param showTotalsByDefault whether totals are shown by default
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setShowTotalsByDefault(boolean showTotalsByDefault) {
        this.showTotalsByDefault = showTotalsByDefault;
        return this;
    }

    /**
     * Set whether grand totals are shown by default.
     *
     * @param showGrandTotalsByDefault whether grand totals are shown by default
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setShowGrandTotalsByDefault(boolean showGrandTotalsByDefault) {
        this.showGrandTotalsByDefault = showGrandTotalsByDefault;
        return this;
    }

    /**
     * Sets the operation for columns which are not otherwise specified.
     *
     * @param defaultOperation the default operation
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setDefaultOperation(AggType defaultOperation) {
        this.defaultOperation = defaultOperation;
        return this;
    }

    /**
     * Sets the operation for columns which are not otherwise specified.
     *
     * @param defaultOperation the default operation
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setDefaultOperation(String defaultOperation) {
        return setDefaultOperation(EnumValue.caseInsensitiveValueOf(AggType.class, defaultOperation));
    }

    /**
     * Sets the operation for a column.
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     * @param format the format string for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setOperation(String column, AggType operation, String format) {
        this.operationMap.put(column, EnumSet.of(operation));
        if (format != null && !format.isEmpty()) {
            setFormat(column, operation, format);
        }
        return this;
    }


    /**
     * Sets the operation for a column.
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setOperation(String column, AggType operation) {
        return setOperation(column, operation, "");
    }

    /**
     * Sets the operation for a column.
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setOperation(String column, String operation) {
        final AggType operationEnum = EnumValue.caseInsensitiveValueOf(AggType.class, operation);
        return setOperation(column, operationEnum);
    }

    /**
     * Sets the operation for a column.
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     * @param format the format string for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setOperation(String column, String operation, String format) {
        final AggType operationEnum = EnumValue.caseInsensitiveValueOf(AggType.class, operation);
        return setOperation(column, operationEnum, format);
    }

    /**
     * Adds an operation for a column.
     *
     * <p>
     * The add method is used instead of the {@link #setOperation(String, String)} method when more than one aggregation
     * per input column is desired.
     * </p>
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder addOperation(String column, AggType operation) {
        return addOperation(column, operation, "");
    }

    /**
     * Adds an operation for a column.
     *
     * <p>
     * The add method is used instead of the {@link #setOperation(String, AggType, String)} method when more than one
     * aggregation per input column is desired.
     * </p>
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     * @param format the format string for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder addOperation(String column, AggType operation, String format) {
        this.operationMap.computeIfAbsent(column, k -> EnumSet.of(operation)).add(operation);
        if (format != null && !format.isEmpty()) {
            setFormat(column, operation, format);
        }
        return this;
    }

    /**
     * Adds an operation for a column.
     *
     * <p>
     * The add method is used instead of the {@link #setOperation(String, String, String)} method when more than one
     * aggregation per input column is desired.
     * </p>
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     * @param format the format string for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder addOperation(String column, String operation, String format) {
        final AggType operationEnum = EnumValue.caseInsensitiveValueOf(AggType.class, operation);
        return addOperation(column, operationEnum, format);
    }

    /**
     * Adds an operation for a column.
     *
     * <p>
     * The add method is used instead of the {@link #setOperation(String, String)} method when more than one aggregation
     * per input column is desired.
     * </p>
     *
     * @param column the name of the column to operate on
     * @param operation the aggregation operation for this column
     *
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder addOperation(String column, String operation) {
        return addOperation(column, operation, "");
    }

    /**
     * Gets the operations for a given column.
     *
     * @param column the column to get the operations for
     *
     * @return a set of aggregations for the column
     */
    @NotNull
    public Set<AggType> getOperations(String column) {
        return operationMap.getOrDefault(column, EnumSet.noneOf(AggType.class));
    }

    /**
     * Gets the operation to use for columns without an operation specified.
     *
     * @return the default operation
     */
    public AggType getDefaultOperation() {
        return this.defaultOperation;
    }

    /**
     * Sets the format of a column.
     *
     * @param column the column to set the format for
     * @param agg the aggregation type the format is relevant for
     * @param format the format string
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setFormat(String column, AggType agg, String format) {
        formatMap.computeIfAbsent(column, k -> new EnumMap<>(AggType.class)).put(agg, format);
        return this;
    }

    /**
     * Sets the format of a column.
     *
     * @param column the column to set the format for
     * @param agg the aggregation type the format is relevant for, &quot;*&quot; for all aggregations
     * @param format the format string
     * @return this TotalsTableBuilder
     */
    @ScriptApi
    public TotalsTableBuilder setFormat(String column, String agg, String format) {
        if ("*".equals(agg)) {
            Arrays.stream(AggType.values())
                    .filter(op -> op != AggType.Skip && op != AggType.Group)
                    .forEach(op -> setFormat(column, op, format));

            return this;
        }

        return setFormat(column, EnumValue.caseInsensitiveValueOf(AggType.class, agg), format);
    }

    /**
     * Gets the format for an aggregated column.
     *
     * @param column the column to get the format for
     * @return a map from AggType to the corresponding format string
     */
    @NotNull
    public Map<AggType, String> getFormats(String column) {
        return formatMap.getOrDefault(column, Collections.emptyMap());
    }

    /**
     * Creates the string directive used to set the Table attribute.
     *
     * @return the attribute string representing this TotalsTableBuilder.
     */
    public String buildDirective() {
        final StringBuilder builder = new StringBuilder();
        builder.append(Boolean.toString(showTotalsByDefault)).append(',')
                .append(Boolean.toString(showGrandTotalsByDefault)).append(',').append(defaultOperation).append(';');
        operationMap.forEach((k, v) -> builder.append(k).append('=')
                .append(v.stream().map(Object::toString).collect(Collectors.joining(":"))).append(','));
        builder.append(';');
        formatMap.forEach((k, v) -> builder.append(k).append('=')
                .append(v.entrySet().stream().map(ent -> ent.getKey().toString() + ':' + encodeFormula(ent.getValue()))
                        .collect(Collectors.joining("&")))
                .append(','));

        return builder.toString();
    }

    /**
     * Helper method for building and {@link Table#setTotalsTable(String) applying} a totals table to a {@link Table}.
     *
     * @param table The source {@link Table}
     * @return {@code table.setTotalsTable(buildDirective())}
     */
    @ScriptApi
    public Table applyToTable(@NotNull final Table table) {
        return table.setTotalsTable(buildDirective());
    }

    private static String encodeFormula(String formula) {
        try {
            return URLEncoder.encode(formula, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to encode formula " + formula, e);
        }
    }

    private static String decodeFormula(String encoded) {
        try {
            return URLDecoder.decode(encoded, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to decode formula " + encoded, e);
        }
    }

    @Override
    public String toString() {
        return "{TotalsTableBuilder: " + buildDirective() + "}";
    }

    /**
     * Create a totals table from a source table.
     *
     * <p>
     * Given a source table that has had a TotalsTableBuilder applied, create a new totals table from the table. If no
     * TotalsTableBuilder has been applied, then the columns are summed.
     * </p>
     *
     * @param source the source table
     * @return an aggregated totals table
     */
    public static Table makeTotalsTable(Table source) {
        return makeTotalsTable(source, (String) source.getAttribute(TOTALS_TABLE_ATTRIBUTE));
    }

    /**
     * Produce a totals table builder from the source table.
     *
     * @param source the source table
     * @return a TotalsTableBuilder derived from the source table's totals table attribute
     */
    public static TotalsTableBuilder get(Table source) {
        return fromDirective((String) source.getAttribute(TOTALS_TABLE_ATTRIBUTE));
    }

    /**
     * Produce a TotalsTableBuilder from a directive string.
     *
     * <p>
     * The {@link #buildDirective()} method produces a String representation of a TotalsTableBuilder, this function is
     * its inverse.
     * </p>
     *
     * @param directive the directive.
     * @return a TotalsTableBuilder
     */
    public static TotalsTableBuilder fromDirective(final String directive) {
        final TotalsTableBuilder builder = new TotalsTableBuilder();
        if (directive == null || directive.isEmpty()) {
            return builder;
        }

        final String[] splitSemi = directive.split(";");
        final String[] frontMatter = splitSemi[0].split(",");

        if (frontMatter.length < 3) {
            throw new IllegalArgumentException("Invalid " + TOTALS_TABLE_ATTRIBUTE + ": " + directive);
        }
        builder.setShowTotalsByDefault(Boolean.parseBoolean(frontMatter[0]));
        builder.setShowGrandTotalsByDefault(Boolean.parseBoolean(frontMatter[1]));
        builder.setDefaultOperation(frontMatter[2]);

        if (splitSemi.length > 1) {
            final String[] columnDirectives = splitSemi[1].split(",");
            for (final String columnDirective : columnDirectives) {
                if (columnDirective.trim().isEmpty())
                    continue;
                final String[] kv = columnDirective.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(
                            "Invalid " + TOTALS_TABLE_ATTRIBUTE + ": " + directive + ", bad column " + columnDirective);
                }
                final String[] operations = kv[1].split(":");
                for (final String op : operations) {
                    builder.addOperation(kv[0], op);
                }
            }
        }

        // Formats for aggregations are encoded as a comma separated list of
        // <Column>=<Agg>:<Format>&<Agg>:<Format>...;
        // Formats are URL encoded
        if (splitSemi.length > 2) {
            final String[] formatDirectives = splitSemi[2].split(",");
            for (final String formatDirective : formatDirectives) {
                if (formatDirective.trim().isEmpty()) {
                    continue;
                }

                final String[] colAndFormats = formatDirective.split("=");
                if (colAndFormats.length != 2) {
                    throw new IllegalArgumentException(
                            "Invalid " + TOTALS_TABLE_ATTRIBUTE + ": " + directive + ", bad format " + formatDirective);
                }

                final String[] formatsByAgg = colAndFormats[1].split("&");
                for (final String formatForAgg : formatsByAgg) {
                    final String[] aggAndFormat = formatForAgg.split(":");
                    if (aggAndFormat.length != 2) {
                        throw new IllegalArgumentException("Invalid " + TOTALS_TABLE_ATTRIBUTE + ": " + directive
                                + ", bad format for agg" + formatForAgg + " in " + formatDirective);
                    }

                    builder.setFormat(colAndFormats[0],
                            EnumValue.caseInsensitiveValueOf(AggType.class, aggAndFormat[0]),
                            decodeFormula(aggAndFormat[1]));
                }
            }
        }

        return builder;
    }

    /**
     * Does a table have a totals table defined?
     *
     * @param source the source table
     *
     * @return true if source has a totals table defined
     */
    public static boolean hasDefinedTotals(Table source) {
        final String attr = (String) source.getAttribute(TOTALS_TABLE_ATTRIBUTE);
        return attr != null && !attr.isEmpty();
    }

    @SuppressWarnings("WeakerAccess")
    static Table makeTotalsTable(Table source, String aggregationDirective) {
        final TotalsTableBuilder builder = fromDirective(aggregationDirective);
        return makeTotalsTable(source, builder);
    }

    /**
     * Given a source table, builder and aggregation columns build a totals table with multiple rows.
     *
     * @param source the source table
     * @param builder the TotalsTableBuilder
     * @param groupByColumns the columns to group by
     *
     * @return an aggregated totals table
     */
    public static Table makeTotalsTable(Table source, TotalsTableBuilder builder, String... groupByColumns) {
        final Collection<? extends Aggregation> aggregations = makeAggregations(source, builder);
        final String[] formatSpecs = makeColumnFormats(source, builder);
        Table totalsTable = source.aggBy(aggregations, groupByColumns);
        if (formatSpecs.length > 0) {
            totalsTable = totalsTable.formatColumns(makeColumnFormats(source, builder));
        }
        return totalsTable;
    }

    private static void ensureColumnsExist(Table source, Set<String> columns) {
        if (!source.getColumnSourceMap().keySet().containsAll(columns)) {
            final Set<String> missing = new LinkedHashSet<>(columns);
            missing.removeAll(source.getColumnSourceMap().keySet());
            throw new IllegalArgumentException("Missing columns for totals table " + missing + ", available columns "
                    + source.getColumnSourceMap().keySet());
        }
    }

    private static String[] makeColumnFormats(Table source, TotalsTableBuilder builder) {
        ensureColumnsExist(source, builder.formatMap.keySet());

        final List<String> formatSpecs = new ArrayList<>();

        builder.formatMap.forEach((col, formats) -> {
            final Set<AggType> aggsForCol = builder.operationMap.get(col);

            // If no aggregations were specified for this column, and the default op was not skip
            // add a format for col=format
            if (aggsForCol == null || aggsForCol.isEmpty()) {
                if (builder.defaultOperation == AggType.Skip) {
                    return;
                }

                final String aggFormat = formats.get(builder.defaultOperation);
                if (aggFormat == null || aggFormat.isEmpty()) {
                    return;
                }

                formatSpecs.add(col + '=' + aggFormat);
            } else {
                for (final AggType agg : aggsForCol) {
                    final String aggFormat = formats.get(agg);
                    if (aggFormat == null || aggFormat.isEmpty()) {
                        continue;
                    }

                    final String formatSpec = (aggsForCol.size() == 1) ? col + '=' + aggFormat
                            : col + "__" + agg + '=' + aggFormat;
                    formatSpecs.add(formatSpec);
                }
            }
        });

        return formatSpecs.toArray(ZERO_LENGTH_STRING_ARRAY);
    }


    /**
     * Produce {@link Aggregation aggregations}from a source table and builder.
     *
     * @param source the source table
     * @param builder the TotalsTableBuilder
     *
     * @return the {@link Aggregation aggregations} described by source and builder.
     */
    public static Collection<? extends Aggregation> makeAggregations(Table source, TotalsTableBuilder builder) {
        ensureColumnsExist(source, builder.operationMap.keySet());

        final Set<AggType> defaultOperations = EnumSet.of(builder.defaultOperation);
        final Map<AggType, List<String>> columnsByType = new LinkedHashMap<>();
        for (final Map.Entry<String, ? extends ColumnSource> entry : source.getColumnSourceMap().entrySet()) {
            final String columnName = entry.getKey();
            if (ColumnFormattingValues.isFormattingColumn(columnName)) {
                continue;
            }

            final Set<AggType> operations = builder.operationMap.getOrDefault(columnName, defaultOperations);
            final Class type = entry.getValue().getType();

            for (final AggType op : operations) {
                if (operationApplies(type, op)) {
                    final String matchPair;
                    if (operations.size() == 1) {
                        matchPair = columnName;
                    } else {
                        if (op == AggType.Count) {
                            matchPair = columnName + "__" + op;
                        } else {
                            matchPair = columnName + "__" + op + "=" + columnName;
                        }
                    }
                    columnsByType.computeIfAbsent(op, (k) -> new ArrayList<>()).add(matchPair);
                }
            }
        }

        return columnsByType.entrySet().stream()
                .flatMap(e -> makeOperation(e.getKey(), e.getValue().toArray(ZERO_LENGTH_STRING_ARRAY)))
                .collect(Collectors.toList());
    }

    private static Stream<? extends Aggregation> makeOperation(AggType operation, String... columnNames) {
        switch (operation) {
            case Group:
                throw new IllegalArgumentException("Can not use Group aggregation in totals table.");
            case Count:
                return Arrays.stream(columnNames).map(Aggregation::AggCount);
            case Min:
                return Stream.of(AggMin(columnNames));
            case Max:
                return Stream.of(AggMax(columnNames));
            case First:
                return Stream.of(AggFirst(columnNames));
            case Last:
                return Stream.of(AggLast(columnNames));
            case Sum:
                return Stream.of(AggSum(columnNames));
            case AbsSum:
                return Stream.of(AggAbsSum(columnNames));
            case Avg:
                return Stream.of(AggAvg(columnNames));
            case Std:
                return Stream.of(AggStd(columnNames));
            case Var:
                return Stream.of(AggVar(columnNames));
            case Unique:
                return Stream.of(AggUnique(columnNames));
            case CountDistinct:
                return Stream.of(AggCountDistinct(columnNames));
            case Distinct:
                return Stream.of(AggDistinct(columnNames));
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Does the operation apply to type?
     *
     * @param type the column type
     * @param operation the aggregation operation
     *
     * @return true if the operation is applicable to columns of this type
     */
    public static boolean operationApplies(Class type, AggType operation) {
        switch (operation) {
            case Skip:
                return false;
            case Count:
            case First:
            case Last:
            case CountDistinct:
            case Distinct:
            case Unique:
                return true;
            case Min:
            case Max:
                return Comparable.class.isAssignableFrom(TypeUtils.getBoxedType(type));
            case Sum:
            case AbsSum:
            case Avg:
            case Std:
            case Var:
                return Number.class.isAssignableFrom(TypeUtils.getBoxedType(type));
            default:
                throw new IllegalStateException();
        }
    }
}
