//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.literal.Literal;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.preview.DisplayWrapper;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.datastructures.CachingSupplier;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

public class MatchFilter extends WhereFilterImpl implements ExposesChunkFilter {

    private static final long serialVersionUID = 1L;

    static MatchFilter ofLiterals(
            String columnName,
            Collection<Literal> literals,
            boolean inverted) {
        final MatchOptions options = MatchOptions.builder().inverted(inverted).build();
        return new MatchFilter(
                options,
                columnName,
                literals.stream().map(AsObject::of).toArray());
    }

    /** A fail-over WhereFilter supplier should the match filter initialization fail. */
    private final CachingSupplier<ConditionFilter> failoverFilter;
    /**
     * Whether init failed over, so that this filter delegates to the (initialized) failover. That the supplier has
     * cached a failover is no substitute: copies, renames and a failover whose own init failed all fill its cache
     * without this filter failing over.
     */
    private boolean failedOver;

    @NotNull
    private String columnName;
    private Class<?> columnType;
    private Object[] values;
    private String[] strValues;
    private final MatchOptions matchOptions;

    private boolean initialized;

    /**
     * Create a new MatchFilter with a list of values to match.
     *
     * @param matchOptions options controlling how the match is performed
     * @param columnName the column name to match against
     * @param values the values to match, any of which may be null
     */
    public MatchFilter(
            @NotNull final MatchOptions matchOptions,
            @NotNull final String columnName,
            @NotNull final Object... values) {
        this(null, matchOptions, columnName, null, values);
    }

    /**
     * Create a new MatchFilter with either string values (which may be converted to actual values) or a list of values
     * to match. Exactly one of {@code strValues} and {@code values} must be non-null.
     *
     * @param failoverFilter a fail-over WhereFilter supplier should the match filter initialization fail
     * @param matchOptions options controlling how the match is performed
     * @param columnName the column name to match against
     * @param strValues the string values to convert and match against. NOTE: conversion is deferred until init time and
     *        these may be converted depending on query-scope variables.
     * @param values the values to match
     */
    public MatchFilter(
            @Nullable final CachingSupplier<ConditionFilter> failoverFilter,
            @NotNull final MatchOptions matchOptions,
            @NotNull final String columnName,
            @Nullable final String[] strValues,
            @Nullable final Object[] values) {
        this.failoverFilter = failoverFilter;
        this.matchOptions = matchOptions;
        this.columnName = columnName;
        if ((strValues == null) == (values == null)) {
            throw new IllegalArgumentException("Exactly one of `strValues` or `values` must be specified");
        }
        this.strValues = strValues;
        this.values = values;
    }

    /**
     * @return the {@link ConditionFilter} this filter delegates to because init failed over, or null if it did not
     */
    @InternalUseOnly
    @Nullable
    public ConditionFilter getFailoverFilter() {
        return failedOver ? failoverFilter.get() : null;
    }

    public WhereFilter renameFilter(Map<String, String> renames) {
        final ConditionFilter failover = getFailoverFilter();
        if (failover != null) {
            // The failover defines this filter, and uses columns that the renames may cover without covering ours. A
            // renamed MatchFilter would re-convert its values against the renamed table, where a column this filter
            // compares with may instead resolve as a query-scope variable.
            return failover.renameFilter(renames);
        }
        final String newName = renames.get(columnName);
        Assert.neqNull(newName, "newName");
        if (strValues == null) {
            // when we're constructed with values then there is no failover filter
            return new MatchFilter(matchOptions, newName, values);
        } else {
            return new MatchFilter(
                    failoverFilter != null ? new CachingSupplier<>(
                            () -> failoverFilter.get().renameFilter(renames)) : null,
                    matchOptions, newName, strValues, null);
        }
    }

    /**
     * Returns {@code convertedValues} without the values that can never match the column, or {@code convertedValues}
     * itself when there is nothing to remove: NaN if {@code dropNaN}, and, on a {@link BigDecimal} column, any value
     * that is neither null nor a {@link BigDecimal}. The values are always post-conversion.
     *
     * <p>
     * Without {@link MatchOptions#nanMatch()} a match on a primitive column follows IEEE 754, where NaN is equal to
     * nothing at all -- itself included -- so a NaN among the values can never match a row. Removing it therefore does
     * not change what this filter selects, and it leaves a value set that means the same thing to a consumer matching
     * with NaN equal to itself, which is what consumers are permitted to do. This is what upholds the
     * {@link #getValues()} contract for primitive floating-point columns.
     *
     * <p>
     * A {@link BigDecimal} column is matched by {@link BigDecimal#compareTo(BigDecimal)}, as the query language's
     * {@code ==} matches it, and only a {@link BigDecimal} can be compared that way. The convertor passes a value that
     * is not a number through as it is, and no row of the column can match it. Null is kept: it matches a null cell.
     * Dropping the value does not change which rows the filter selects, but it protects the consumers of
     * {@link #getValues()}: the column's chunk filter ({@code BigDecimalChunkMatchFilterFactory}) skips a value of
     * another type itself, but the sorted-column pushdown ({@code SortedColumnPushdownManager} and the region binary
     * search kernels) locates every value by ordering, and {@code compareTo} between a {@link BigDecimal} and a value
     * of another type throws {@link ClassCastException}.
     */
    private Object[] dropUnmatchable(final Object[] convertedValues, final boolean dropNaN) {
        final Object[] retained = Arrays.stream(convertedValues)
                .filter(value -> !(dropNaN && isNaN(value))
                        && !(columnType == BigDecimal.class && value != null && !(value instanceof BigDecimal)))
                .toArray();
        return retained.length == convertedValues.length ? convertedValues : retained;
    }

    private static boolean isNaN(final Object value) {
        return value instanceof Double && ((Double) value).isNaN()
                || value instanceof Float && ((Float) value).isNaN();
    }

    /**
     * The values this filter matches against, normalized so that they may be matched by value equality that holds NaN
     * equal to itself -- the type's {@code *Comparisons.eq}, or {@link java.util.Objects#equals}, for instance. A
     * {@link BigDecimal} matches by {@link BigDecimal#compareTo(BigDecimal)} instead, as the query language's
     * {@code ==} does, so {@code 5.0} matches {@code 5.00}.
     *
     * <p>
     * The filter's own NaN semantics are already applied here, so a consumer does not need to consult
     * {@link MatchOptions#nanMatch()} to match correctly. On a primitive floating-point column without that option a
     * match follows IEEE 754, under which NaN matches nothing, and any NaN has been removed accordingly; anywhere NaN
     * remains, matching it is what this filter intends.
     */
    public Object[] getValues() {
        return values;
    }

    public MatchOptions getMatchOptions() {
        return matchOptions;
    }

    public Class<?> getColumnType() {
        return columnType;
    }

    @Override
    public List<String> getColumns() {
        if (!initialized) {
            throw new IllegalStateException("Filter must be initialized to invoke getColumnName");
        }
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            return failover.getColumns();
        }
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        if (!initialized) {
            throw new IllegalStateException("Filter must be initialized to invoke getColumnArrays");
        }
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            return failover.getColumnArrays();
        }
        return Collections.emptyList();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        if (!initialized) {
            throw new IllegalStateException("Filter must be initialized to invoke hasVirtualRowVariables");
        }
        final WhereFilter failover = getFailoverFilter();
        return failover != null && failover.hasVirtualRowVariables();
    }

    @Override
    public boolean canPushdown() {
        // The failover is not visible to a walk of the filter tree, so answer for it here.
        final WhereFilter failover = getFailoverFilter();
        return failover == null || failover.canPushdown();
    }

    @Override
    public void validateSafeForRefresh(final BaseTable<?> sourceTable) {
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            failover.validateSafeForRefresh(sourceTable);
        }
    }

    @Override
    public boolean permitParallelization() {
        final WhereFilter failover = getFailoverFilter();
        return failover == null || failover.permitParallelization();
    }

    @Override
    public void init(@NotNull TableDefinition tableDefinition) {
        init(tableDefinition, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public synchronized void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        if (initialized) {
            return;
        }
        try {
            ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
            if (column == null) {
                if (strValues != null && strValues.length == 1
                        && (column = tableDefinition.getColumn(strValues[0])) != null) {
                    // Fix up for the case where column name and variable name were swapped. Replace strValues rather
                    // than swapping in place: copies and renamed filters share the array.
                    final String tmp = columnName;
                    columnName = strValues[0];
                    strValues = new String[] {tmp};
                } else {
                    throw new RuntimeException("Column \"" + columnName
                            + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
                }
            }
            columnType = column.getDataType();
            final ColumnTypeConvertor convertor = ColumnTypeConvertorFactory.getConvertor(column.getDataType());
            // nanMatch only applies to a primitive floating-point column
            final boolean nanMatch =
                    matchOptions.nanMatch() && (columnType == double.class || columnType == float.class);
            // Converts query-scope variables and direct values; a literal is parsed by convertStringLiteral instead.
            final UnaryOperator<Object> convertParam = value -> {
                final Object unwrapped = ColumnTypeConvertor.maybeUnwrapPyObject(value);
                if (!nanMatch && isNaN(unwrapped)) {
                    // passed through unconverted, to be dropped later; converting it to an integral type would throw,
                    // although NaN simply matches nothing
                    return unwrapped;
                }
                return convertor.convertParamValue(value);
            };
            final Object[] converted;
            if (strValues == null) {
                // Run the user-supplied values through the convertor.
                converted = new Object[values.length];
                for (int ii = 0; ii < values.length; ++ii) {
                    final Object value = values[ii];
                    converted[ii] = value == null ? null : convertParam.apply(value);
                }
            } else {
                final List<Object> valueList = new ArrayList<>();
                final Map<String, Object> queryScopeVariables =
                        compilationProcessor.getFormulaImports().getQueryScopeVariables();
                for (String strValue : strValues) {
                    convertor.convertValue(column, tableDefinition, strValue, queryScopeVariables, convertParam,
                            valueList::add);
                }
                converted = valueList.toArray();
            }
            // Without nanMatch, no value of a primitive column matches NaN, so NaN is dropped from the search values
            // (see the getValues() contract for why). It is kept for a non-primitive column, though: a column of type
            // Object may hold NaN, which is matched by equals rather than by IEEE 754 rules.
            final boolean dropNaN = !nanMatch && columnType.isPrimitive();
            values = dropUnmatchable(converted, dropNaN);
        } catch (final RuntimeException err) {
            if (failoverFilter == null) {
                throw err;
            }
            try {
                failoverFilter.get().init(tableDefinition, compilationProcessor);
            } catch (final RuntimeException ignored) {
                throw err;
            }
            failedOver = true;
        }
        initialized = true;
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            return failover.filter(selection, fullSet, table, usePrev);
        }

        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        return columnSource.match(usePrev, matchOptions, selection, values);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            return failover.filterInverse(selection, fullSet, table, usePrev);
        }

        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        final MatchOptions options = matchOptions.withInverted(!matchOptions.inverted());
        return columnSource.match(usePrev, options, selection, values);
    }

    private ChunkFilter chunkFilter;

    @Override
    public Optional<ChunkFilter> chunkFilter() {
        if (chunkFilter == null) {
            final WhereFilter failover = getFailoverFilter();
            if (failover != null) {
                if (failover instanceof ExposesChunkFilter) {
                    return ((ExposesChunkFilter) failover).chunkFilter();
                }
                return Optional.empty();
            }
            if (values == null) {
                return Optional.empty();
            }
            chunkFilter = ChunkMatchFilterFactory.getChunkFilter(columnType, matchOptions, values);
        }
        return Optional.of(chunkFilter);
    }

    @Override
    public boolean isSimpleFilter() {
        final WhereFilter failover = getFailoverFilter();
        if (failover != null) {
            return failover.isSimpleFilter();
        }

        return true;
    }

    /**
     * Return an {@link Optional} containing the {@link MatchFilter} if the provided filter is a match filter that can
     * be pushed down (i.e. is not implemented by a ConditionFilter, and has values to match). Otherwise returns
     * {@code Optional.empty()}.
     */
    public static Optional<MatchFilter> extractMatchFilter(WhereFilter filter) {
        if (filter instanceof MatchFilter
                && ((MatchFilter) filter).getFailoverFilter() == null
                && ((MatchFilter) filter).getValues() != null) {
            return Optional.of((MatchFilter) filter);
        }
        return Optional.empty();
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    public static abstract class ColumnTypeConvertor {

        abstract Object convertStringLiteral(String str);

        /**
         * Converts a query-scope or direct value to the column's type. This default only unwraps a {@link PyObject};
         * the convertors of types that need more override it, most calling it first to unwrap. The unwrapping itself is
         * {@link #maybeUnwrapPyObject(Object)}, for callers that need the value unwrapped but not converted.
         */
        Object convertParamValue(Object paramValue) {
            return maybeUnwrapPyObject(paramValue);
        }

        /**
         * @return the Java value of a convertible {@link PyObject}, or {@code paramValue} itself
         */
        static Object maybeUnwrapPyObject(final Object paramValue) {
            if (paramValue instanceof PyObject && ((PyObject) paramValue).isConvertible()) {
                return ((PyObject) paramValue).getObjectValue();
            }
            return paramValue;
        }

        /**
         * Throws, so that a filter fails over to its {@link ConditionFilter}, unless {@code converted} -- {@code value}
         * cast to the column type -- selects the rows {@code value} would select in the query language: it must equal
         * {@code value} exactly. The convertors also reject a value that converts exactly to the column type's null
         * value: only a value of the column's own type is null there, in the query language as here.
         *
         * <p>
         * A {@link BigDecimal} or {@link BigInteger} against a float or double column is the one case where "equal" is
         * not exact equality. The query language compares the two through {@link BigDecimal#valueOf(double)}, the
         * column value's shortest decimal rather than its exact binary value, so the value must equal the converted
         * value's shortest decimal: {@code BigDecimal("0.1")} converts to {@code 0.1}, but {@code new BigDecimal(0.1)},
         * though exactly a double, equals no double's shortest decimal. As that comparison is monotonic, it then also
         * orders every column value as the converted value would, so a range bound converts the same way.
         *
         * <p>
         * A large floating-point value against an int or long column is an accepted difference: the query language
         * compares the two in floating point, where more than one integer can round to the value ({@code 2^53 + 1 ==
         * (double) 2^53}), but the exact equivalent matches only itself. Likewise, a {@link Float} range bound against
         * an int column orders as its exact equivalent, where the query language compares in float.
         *
         * @param columnType the (boxed) column type, which the error names; {@code converted} may be of another type,
         *        an {@link Integer} for a {@link Character} column
         */
        static void checkRoundTrip(final Number value, final Number converted, final Class<?> columnType) {
            if (isFloatingPoint(converted) && (value instanceof BigInteger || value instanceof BigDecimal)) {
                if (!Double.isFinite(converted.doubleValue())
                        || toBigDecimal(converted).compareTo(exactValue(value)) != 0) {
                    throw cannotConvert(value, columnType, "no " + columnType.getSimpleName()
                            + " equals it as the query language compares the two, through BigDecimal.valueOf", null);
                }
                return;
            }
            if (!exactlyEqual(value, converted)) {
                throw cannotConvert(value, columnType, "the column type cannot represent it exactly", null);
            }
        }

        static IllegalArgumentException cannotConvert(
                final Object value,
                final Class<?> columnType,
                final String problem,
                @Nullable final Throwable cause) {
            return new IllegalArgumentException(String.format("Cannot convert value <%s> of type %s to %s: %s",
                    value, value.getClass().getName(), columnType.getSimpleName(), problem), cause);
        }

        private static boolean exactlyEqual(final Number a, final Number b) {
            if (!Double.isFinite(a.doubleValue()) || !Double.isFinite(b.doubleValue())) {
                // NaN and the infinities
                return Double.compare(a.doubleValue(), b.doubleValue()) == 0;
            }
            return exactValue(a).compareTo(exactValue(b)) == 0;
        }

        private static BigDecimal exactValue(final Number number) {
            if (number instanceof BigDecimal) {
                return (BigDecimal) number;
            }
            if (number instanceof BigInteger) {
                return new BigDecimal((BigInteger) number);
            }
            if (isFloatingPoint(number)) {
                return new BigDecimal(number.doubleValue());
            }
            if (number instanceof Long || number instanceof Integer || number instanceof Short
                    || number instanceof Byte) {
                return BigDecimal.valueOf(number.longValue());
            }
            // Any other Number (DoubleAdder, AtomicLong, ...) may hold a fraction that longValue() would drop, so
            // it has no exact value to compare, and cannot be converted.
            throw new IllegalArgumentException(String.format(
                    "Cannot convert value <%s> of type %s: it is not a standard numeric type",
                    number, number.getClass().getName()));
        }

        private static boolean isFloatingPoint(final Number number) {
            return number instanceof Float || number instanceof Double;
        }

        /**
         * Whether {@code str} is a single-quoted char literal, such as {@code '5'}, which a numeric column reads as its
         * code point, as the query language and Java compare a char with a number.
         */
        static boolean isCharLiteral(final String str) {
            return str.length() == 3 && str.charAt(0) == '\'' && str.charAt(2) == '\'';
        }

        /**
         * Whether {@code value} is its own type's null value, {@code NULL_INT} for an {@link Integer} for instance.
         */
        static boolean isNullValue(final Object value) {
            // noinspection unchecked
            return ((TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(value.getClass())).get(value) == null;
        }

        /**
         * Converts {@code number} to a {@link BigDecimal} as the query language does when it compares the two: a
         * floating-point value through {@link BigDecimal#valueOf(double)}, and any other value exactly.
         */
        static BigDecimal toBigDecimal(final Number number) {
            return isFloatingPoint(number) ? BigDecimal.valueOf(number.doubleValue()) : exactValue(number);
        }

        /**
         * Whether {@code strValue} names a column, or a column array ({@code X_} for column {@code X}), of
         * {@code tableDefinition}; a column takes precedence over a query-scope variable of the same name.
         */
        static boolean isColumnReference(
                @NotNull final TableDefinition tableDefinition,
                @NotNull final String strValue) {
            return tableDefinition.getColumn(strValue) != null
                    || (strValue.endsWith("_")
                            && tableDefinition.getColumn(strValue.substring(0, strValue.length() - 1)) != null);
        }

        /**
         * Convert the string value to the appropriate type for the column.
         *
         * @param column the column definition
         * @param strValue the string value to convert
         * @param queryScopeVariables the query scope variables
         * @param valueConsumer the consumer for the converted value
         * @return whether the value was an array or collection
         */
        final boolean convertValue(
                @NotNull final ColumnDefinition<?> column,
                @NotNull final TableDefinition tableDefinition,
                @NotNull final String strValue,
                @NotNull final Map<String, Object> queryScopeVariables,
                @NotNull final Consumer<Object> valueConsumer) {
            return convertValue(column, tableDefinition, strValue, queryScopeVariables, this::convertParamValue,
                    valueConsumer);
        }

        /**
         * As {@link #convertValue(ColumnDefinition, TableDefinition, String, Map, Consumer)}, converting each
         * query-scope value with {@code paramConverter} rather than {@link #convertParamValue(Object)}.
         */
        final boolean convertValue(
                @NotNull final ColumnDefinition<?> column,
                @NotNull final TableDefinition tableDefinition,
                @NotNull final String strValue,
                @NotNull final Map<String, Object> queryScopeVariables,
                @NotNull final UnaryOperator<Object> paramConverter,
                @NotNull final Consumer<Object> valueConsumer) {
            if (tableDefinition.getColumn(strValue) != null) {
                // this is also a column name which needs to take precedence, and we can't convert it
                throw new IllegalArgumentException(String.format(
                        "Failed to convert value <%s> for column \"%s\" of type %s; it is a column name",
                        strValue, column.getName(), column.getDataType().getName()));
            }
            if (strValue.endsWith("_")
                    && tableDefinition.getColumn(strValue.substring(0, strValue.length() - 1)) != null) {
                // this also a column array name which needs to take precedence, and we can't convert it
                throw new IllegalArgumentException(String.format(
                        "Failed to convert value <%s> for column \"%s\" of type %s; it is a column array access name",
                        strValue, column.getName(), column.getDataType().getName()));
            }

            if (queryScopeVariables.containsKey(strValue)) {
                Object paramValue = queryScopeVariables.get(strValue);
                if (paramValue != null && paramValue.getClass().isArray()) {
                    ArrayTypeUtils.ArrayAccessor<?> accessor = ArrayTypeUtils.getArrayAccessor(paramValue);
                    for (int ai = 0; ai < accessor.length(); ++ai) {
                        valueConsumer.accept(paramConverter.apply(accessor.get(ai)));
                    }
                    return true;
                }
                if (paramValue != null && Collection.class.isAssignableFrom(paramValue.getClass())) {
                    for (final Object paramValueMember : (Collection<?>) paramValue) {
                        valueConsumer.accept(paramConverter.apply(paramValueMember));
                    }
                    return true;
                }
                valueConsumer.accept(paramConverter.apply(paramValue));
                return false;
            }

            try {
                valueConsumer.accept(convertStringLiteral(strValue));
            } catch (Throwable t) {
                throw new IllegalArgumentException(String.format(
                        "Failed to convert literal value <%s> for column \"%s\" of type %s",
                        strValue, column.getName(), column.getDataType().getName()), t);
            }

            return false;
        }
    }

    /**
     * Converts a numeric query-scope value to a primitive numeric column type: exactly, to the type's null value if it
     * is its own type's null value, or not at all (see {@link #checkRoundTrip(Number, Number, Class)}).
     */
    private abstract static class NumericColumnTypeConvertor extends ColumnTypeConvertor {
        private final Class<?> boxedType;
        private final Object nullValue;

        private NumericColumnTypeConvertor(final Class<?> boxedType, final Object nullValue) {
            this.boxedType = boxedType;
            this.nullValue = nullValue;
        }

        /** Casts {@code value} to the column type, boxed. */
        abstract Number narrow(Number value);

        /** Parses a literal of the column type, or its null. */
        abstract Object parseLiteral(String str);

        @Override
        final Object convertStringLiteral(final String str) {
            if (isCharLiteral(str)) {
                return convertParamValue(str.charAt(1));
            }
            return parseLiteral(str);
        }

        @Override
        Object convertParamValue(Object paramValue) {
            paramValue = super.convertParamValue(paramValue);
            if (paramValue == null || boxedType.isInstance(paramValue)) {
                return paramValue;
            }
            if (isNullValue(paramValue)) {
                return nullValue;
            }
            if (paramValue instanceof Character) {
                // the query language, as Java, compares a char with a number by its code point
                final int codePoint = (Character) paramValue;
                final Number converted = narrow(codePoint);
                if (converted.intValue() != codePoint) {
                    // beyond a byte or short
                    throw cannotConvert(paramValue, boxedType, "the column type cannot represent its code point", null);
                }
                return converted;
            }
            if (!(paramValue instanceof Number)) {
                throw cannotConvert(paramValue, boxedType, "it is not a number", null);
            }
            final Number number = (Number) paramValue;
            final Number converted = narrow(number);
            checkRoundTrip(number, converted, boxedType);
            if (converted.equals(nullValue)) {
                // The query language compares an int -128 with a byte column as a number below every byte, not as
                // null; only a value of the column's own type is null at the null value.
                throw cannotConvert(number, boxedType, "it converts to the column type's null value", null);
            }
            return converted;
        }
    }

    public static class ColumnTypeConvertorFactory {
        /** An unquoted decimal integer literal, which a char column reads as a code point. */
        private static final Pattern INTEGER_LITERAL = Pattern.compile("-?[0-9]+");

        public static ColumnTypeConvertor getConvertor(final Class<?> cls) {
            if (cls == byte.class) {
                return new NumericColumnTypeConvertor(Byte.class, QueryConstants.NULL_BYTE_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_BYTE".equals(str)) {
                            return QueryConstants.NULL_BYTE_BOXED;
                        }
                        return Byte.parseByte(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.byteValue();
                    }
                };
            }
            if (cls == short.class) {
                return new NumericColumnTypeConvertor(Short.class, QueryConstants.NULL_SHORT_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_SHORT".equals(str)) {
                            return QueryConstants.NULL_SHORT_BOXED;
                        }
                        return Short.parseShort(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.shortValue();
                    }
                };
            }
            if (cls == int.class) {
                return new NumericColumnTypeConvertor(Integer.class, QueryConstants.NULL_INT_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_INT".equals(str)) {
                            return QueryConstants.NULL_INT_BOXED;
                        }
                        return Integer.parseInt(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.intValue();
                    }
                };
            }
            if (cls == long.class) {
                return new NumericColumnTypeConvertor(Long.class, QueryConstants.NULL_LONG_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_LONG".equals(str)) {
                            return QueryConstants.NULL_LONG_BOXED;
                        }
                        return Long.parseLong(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.longValue();
                    }
                };
            }
            if (cls == float.class) {
                return new NumericColumnTypeConvertor(Float.class, QueryConstants.NULL_FLOAT_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_FLOAT".equals(str)) {
                            return QueryConstants.NULL_FLOAT_BOXED;
                        }
                        return Float.parseFloat(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.floatValue();
                    }
                };
            }
            if (cls == double.class) {
                return new NumericColumnTypeConvertor(Double.class, QueryConstants.NULL_DOUBLE_BOXED) {
                    @Override
                    Object parseLiteral(String str) {
                        if ("null".equals(str) || "NULL_DOUBLE".equals(str)) {
                            return QueryConstants.NULL_DOUBLE_BOXED;
                        }
                        return Double.parseDouble(str);
                    }

                    @Override
                    Number narrow(final Number value) {
                        return value.doubleValue();
                    }
                };
            }
            if (cls == Boolean.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        // NB: Boolean.parseBoolean(str) doesn't do what we want here - anything not true is false.
                        if ("null".equals(str) || "NULL_BOOLEAN".equals(str)) {
                            return QueryConstants.NULL_BOOLEAN;
                        }
                        if (str.equalsIgnoreCase("true")) {
                            return Boolean.TRUE;
                        }
                        if (str.equalsIgnoreCase("false")) {
                            return Boolean.FALSE;
                        }
                        throw new IllegalArgumentException("String " + str
                                + " isn't a valid boolean value (!str.equalsIgnoreCase(\"true\") && !str.equalsIgnoreCase(\"false\"))");
                    }
                };
            }
            if (cls == char.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_CHAR".equals(str)) {
                            return QueryConstants.NULL_CHAR_BOXED;
                        }
                        // TODO: #1517 Allow escaping of chars
                        if (str.length() == 3 && ((str.charAt(0) == '\'' && str.charAt(2) == '\'')
                                || (str.charAt(0) == '"' && str.charAt(2) == '"'))) {
                            return str.charAt(1);
                        }
                        if (INTEGER_LITERAL.matcher(str).matches()) {
                            // an unquoted integer is a code point, as in the query language: 5 is (char) 5, not '5'
                            final long codePoint = Long.parseLong(str);
                            if (codePoint < Character.MIN_VALUE || codePoint >= QueryConstants.NULL_CHAR) {
                                // 65535 is NULL_CHAR, which orders below every char, where the query language
                                // compares it as a number above them all
                                throw new IllegalArgumentException(
                                        "Integer " + str + " is not the code point of a char");
                            }
                            return (char) codePoint;
                        }
                        if (str.length() > 1) {
                            throw new IllegalArgumentException(
                                    "String " + str + " has length greater than one for column ");
                        }
                        return str.charAt(0);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Character || paramValue == null) {
                            return paramValue;
                        }
                        if (isNullValue(paramValue)) {
                            return QueryConstants.NULL_CHAR_BOXED;
                        }
                        if (!(paramValue instanceof Number)) {
                            throw cannotConvert(paramValue, Character.class, "it is not a number", null);
                        }
                        final Number number = (Number) paramValue;
                        final char converted = (char) number.intValue();
                        checkRoundTrip(number, (int) converted, Character.class);
                        if (converted == QueryConstants.NULL_CHAR) {
                            // As for the numeric types, a value that converts to the null value is a number in the
                            // query language, not null (NULL_INT, which is null there too, was converted above). For
                            // char this matters more: NULL_CHAR is the highest char (65535), yet it orders below
                            // every char, while the query language compares 65535 as a number above them all.
                            throw cannotConvert(number, Character.class,
                                    "it converts to NULL_CHAR, which orders below every char", null);
                        }
                        return converted;
                    }
                };
            }
            if (cls == BigDecimal.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (isCharLiteral(str)) {
                            return convertParamValue(str.charAt(1));
                        }
                        return new BigDecimal(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof BigDecimal || paramValue == null) {
                            return paramValue;
                        }
                        if (isNullValue(paramValue)) {
                            return null;
                        }
                        if (paramValue instanceof Character) {
                            // the query language compares a char with it by code point
                            return BigDecimal.valueOf((Character) paramValue);
                        }
                        if (!(paramValue instanceof Number)) {
                            // it can never match, and dropUnmatchable removes it
                            return paramValue;
                        }
                        try {
                            return toBigDecimal((Number) paramValue);
                        } catch (final NumberFormatException err) {
                            // NaN and the infinities
                            throw cannotConvert(paramValue, BigDecimal.class,
                                    "the column type cannot represent it exactly", err);
                        }
                    }
                };
            }
            if (cls == BigInteger.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (isCharLiteral(str)) {
                            return convertParamValue(str.charAt(1));
                        }
                        return new BigInteger(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof BigInteger || paramValue == null) {
                            return paramValue;
                        }
                        if (isNullValue(paramValue)) {
                            return null;
                        }
                        if (paramValue instanceof Character) {
                            // the query language compares a char with it by code point
                            return BigInteger.valueOf((Character) paramValue);
                        }
                        if (!(paramValue instanceof Number)) {
                            // it can never equal a BigInteger, so it matches nothing
                            return paramValue;
                        }
                        try {
                            // toBigIntegerExact throws for a fraction
                            return toBigDecimal((Number) paramValue).toBigIntegerExact();
                        } catch (final ArithmeticException | NumberFormatException err) {
                            throw cannotConvert(paramValue, BigInteger.class,
                                    "the column type cannot represent it exactly", err);
                        }
                    }
                };
            }
            if (cls == String.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        // TODO(web-client-ui#1243): Confusing quick filter behavior around string column "null"
                        if (str.equals("null")) {
                            return null;
                        }
                        if ((str.charAt(0) != '"' && str.charAt(0) != '\'' && str.charAt(0) != '`')
                                || (str.charAt(str.length() - 1) != '"' && str.charAt(str.length() - 1) != '\''
                                        && str.charAt(str.length() - 1) != '`')) {
                            throw new IllegalArgumentException(
                                    "String literal not enclosed in quotes (\"" + str + "\")");
                        }
                        return str.substring(1, str.length() - 1);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        if (paramValue instanceof CompressedString) {
                            return paramValue.toString();
                        }
                        if (paramValue instanceof PyObject && ((PyObject) paramValue).isString()) {
                            Object objectValue = ((PyObject) paramValue).getObjectValue();
                            if (objectValue instanceof String) {
                                return objectValue;
                            }
                        }
                        return paramValue;
                    }
                };
            }
            if (cls == CompressedString.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if (str.equals("null")) {
                            return null;
                        }
                        if ((str.charAt(0) != '"' && str.charAt(0) != '\'' && str.charAt(0) != '`')
                                || (str.charAt(str.length() - 1) != '"' && str.charAt(str.length() - 1) != '\''
                                        && str.charAt(str.length() - 1) != '`')) {
                            throw new IllegalArgumentException("String literal not enclosed in quotes");
                        }
                        return new CompressedString(str.substring(1, str.length() - 1));
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        if (paramValue instanceof String) {
                            return new CompressedString((String) paramValue);
                        }
                        if (paramValue instanceof PyObject && ((PyObject) paramValue).isString()) {
                            Object objectValue = ((PyObject) paramValue).getObjectValue();
                            if (objectValue instanceof String) {
                                return new CompressedString((String) objectValue);
                            }
                        }
                        return paramValue;
                    }
                };
            }
            if (cls == Instant.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "Instant literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseInstant(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == LocalDate.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "LocalDate literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseLocalDate(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == LocalTime.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "LocalTime literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseLocalTime(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == LocalDateTime.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "LocalDateTime literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseLocalDateTime(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == ZonedDateTime.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "ZoneDateTime literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseZonedDateTime(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == Object.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.startsWith("\"") || str.startsWith("`")) {
                            return str.substring(1, str.length() - 1);
                        } else if (str.contains(".")) {
                            return Double.parseDouble(str);
                        }
                        if (str.endsWith("L")) {
                            return Long.parseLong(str);
                        } else {
                            return Integer.parseInt(str);
                        }
                    }
                };
            }
            if (Enum.class.isAssignableFrom(cls)) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        // noinspection unchecked,rawtypes
                        return Enum.valueOf((Class) cls, str);
                    }
                };
            }
            if (cls == DisplayWrapper.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str)) {
                            return null;
                        }
                        if (str.startsWith("\"") || str.startsWith("`")) {
                            return DisplayWrapper.make(str.substring(1, str.length() - 1));
                        } else {
                            return DisplayWrapper.make(str);
                        }
                    }
                };
            }
            return new ColumnTypeConvertor() {
                @Override
                Object convertStringLiteral(String str) {
                    if ("null".equals(str)) {
                        return null;
                    }
                    throw new IllegalArgumentException(
                            "Can't create " + cls.getName() + " from String Literal for value auto-conversion");
                }
            };
        }
    }

    @Override
    public String toString() {
        return strValues == null ? toString(values) : toString(strValues);
    }

    private String toString(Object[] x) {
        return columnName
                + (matchOptions.caseInsensitive() ? " icase" : "") + (matchOptions.inverted() ? " not" : "") + " in "
                + Arrays.toString(x);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MatchFilter that = (MatchFilter) o;

        // The equality check is used for memoization, and we cannot actually determine equality of an uninitialized
        // filter, because there is too much state that has not been realized.
        if (!initialized && !that.initialized) {
            throw new UnsupportedOperationException("MatchFilter has not been initialized");
        }

        // start off with the simple things
        if (!Objects.equals(matchOptions, that.matchOptions) ||
                !Objects.equals(columnName, that.columnName)) {
            return false;
        }

        if (!Arrays.equals(values, that.values)) {
            return false;
        }

        return Objects.equals(getFailoverFilter(), that.getFailoverFilter());
    }

    @Override
    public int hashCode() {
        if (!initialized) {
            throw new UnsupportedOperationException("MatchFilter has not been initialized");
        }
        int result = Objects.hash(columnName, matchOptions);
        // we can use values because we know the filter has been initialized; the hash code should be stable and it
        // cannot be stable before we convert the values
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public boolean canMemoize() {
        // we can be memoized once our values have been initialized; but not before
        return initialized && (getFailoverFilter() == null || getFailoverFilter().canMemoize());
    }

    @Override
    public WhereFilter copy() {
        final MatchFilter copy;
        if (strValues != null) {
            // The supplier copies our failover lazily: a copy that fails over (below) gets a copy of our initialized
            // failover, and one that does not still needs it so that a renameFilter() of the copy can fail over.
            copy = new MatchFilter(
                    failoverFilter == null ? null : new CachingSupplier<>(() -> failoverFilter.get().copy()),
                    matchOptions, columnName, strValues, null);
        } else {
            // when we're constructed with values then there is no failover filter
            copy = new MatchFilter(matchOptions, columnName, values);
        }
        if (initialized) {
            copy.initialized = true;
            // a chunk filter holds no state of its own, so the copy shares ours rather than building another
            copy.chunkFilter = chunkFilter;
            copy.values = values;
            copy.columnType = columnType;
            // If we failed over, the copy must too, or it would claim to be initialized without any values to match.
            copy.failedOver = failedOver;
        }
        return copy;
    }

    private enum AsObject implements Literal.Visitor<Object> {
        INSTANCE;

        public static Object of(Literal literal) {
            return literal.walk(INSTANCE);
        }

        @Override
        public Object visit(boolean literal) {
            return literal;
        }

        @Override
        public Object visit(char literal) {
            return literal;
        }

        @Override
        public Object visit(byte literal) {
            return literal;
        }

        @Override
        public Object visit(short literal) {
            return literal;
        }

        @Override
        public Object visit(int literal) {
            return literal;
        }

        @Override
        public Object visit(long literal) {
            return literal;
        }

        @Override
        public Object visit(float literal) {
            return literal;
        }

        @Override
        public Object visit(double literal) {
            return literal;
        }

        @Override
        public Object visit(String literal) {
            return literal;
        }
    }
}
