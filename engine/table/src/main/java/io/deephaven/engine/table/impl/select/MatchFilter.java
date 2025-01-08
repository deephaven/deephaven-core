//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.literal.Literal;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.preview.DisplayWrapper;
import io.deephaven.engine.table.impl.DependencyStreamProvider;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
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
import java.util.stream.Stream;

public class MatchFilter extends WhereFilterImpl implements DependencyStreamProvider {

    private static final long serialVersionUID = 1L;

    static MatchFilter ofLiterals(
            String columnName,
            Collection<Literal> literals,
            boolean inverted) {
        return new MatchFilter(
                inverted ? MatchType.Inverted : MatchType.Regular,
                columnName,
                literals.stream().map(AsObject::of).toArray());
    }

    /** A fail-over WhereFilter supplier should the match filter initialization fail. */
    private final CachingSupplier<ConditionFilter> failoverFilter;

    @NotNull
    private String columnName;
    private Object[] values;
    private final String[] strValues;
    private final boolean invertMatch;
    private final boolean caseInsensitive;

    private boolean initialized;

    /**
     * The {@link DataIndex} for this filter, if any. Only ever non-{@code null} during operation initialization.
     */
    private DataIndex dataIndex;
    /**
     * Whether our dependencies have been gathered at least once. We expect dependencies to be gathered one time after
     * {@link #beginOperation(Table)} (when we know if we're using a {@link DataIndex}), and then again after for every
     * instantiation attempt when initializing the listener. Since we only use the DataIndex during instantiation, we
     * don't need the listener to depend on it.
     */
    private boolean initialDependenciesGathered;

    public enum MatchType {
        Regular, Inverted,
    }

    public enum CaseSensitivity {
        MatchCase, IgnoreCase
    }

    public MatchFilter(
            @NotNull final MatchType matchType,
            @NotNull final String columnName,
            @NotNull final Object... values) {
        this(null, CaseSensitivity.MatchCase, matchType, columnName, null, values);
    }

    /**
     * @deprecated this method is non-obvious in using IgnoreCase by default. Use
     *             {@link MatchFilter#MatchFilter(MatchType, String, Object...)} instead.
     */
    @Deprecated(forRemoval = true)
    public MatchFilter(
            @NotNull final String columnName,
            @NotNull final Object... values) {
        this(null, CaseSensitivity.IgnoreCase, MatchType.Regular, columnName, null, values);
    }

    public MatchFilter(
            @NotNull final CaseSensitivity sensitivity,
            @NotNull final MatchType matchType,
            @NotNull final String columnName,
            @NotNull final String... strValues) {
        this(null, sensitivity, matchType, columnName, strValues, null);
    }

    public MatchFilter(
            @Nullable final CachingSupplier<ConditionFilter> failoverFilter,
            @NotNull final CaseSensitivity sensitivity,
            @NotNull final MatchType matchType,
            @NotNull final String columnName,
            @NotNull final String... strValues) {
        this(failoverFilter, sensitivity, matchType, columnName, strValues, null);
    }

    private MatchFilter(
            @Nullable final CachingSupplier<ConditionFilter> failoverFilter,
            @NotNull final CaseSensitivity sensitivity,
            @NotNull final MatchType matchType,
            @NotNull final String columnName,
            @Nullable String[] strValues,
            @Nullable final Object[] values) {
        this.failoverFilter = failoverFilter;
        this.caseInsensitive = sensitivity == CaseSensitivity.IgnoreCase;
        this.invertMatch = (matchType == MatchType.Inverted);
        this.columnName = columnName;
        this.strValues = strValues;
        this.values = values;
    }

    private ConditionFilter getFailoverFilterIfCached() {
        return failoverFilter != null ? failoverFilter.getIfCached() : null;
    }

    public WhereFilter renameFilter(Map<String, String> renames) {
        final String newName = renames.get(columnName);
        Assert.neqNull(newName, "newName");
        if (strValues == null) {
            // when we're constructed with values then there is no failover filter
            return new MatchFilter(getMatchType(), newName, values);
        } else {
            return new MatchFilter(
                    failoverFilter != null ? new CachingSupplier<>(
                            () -> failoverFilter.get().renameFilter(renames)) : null,
                    caseInsensitive ? CaseSensitivity.IgnoreCase : CaseSensitivity.MatchCase,
                    getMatchType(), newName, strValues, null);
        }
    }

    public Object[] getValues() {
        return values;
    }

    public boolean getInvertMatch() {
        return invertMatch;
    }

    public MatchType getMatchType() {
        return invertMatch ? MatchType.Inverted : MatchType.Regular;
    }

    @Override
    public List<String> getColumns() {
        if (!initialized) {
            throw new IllegalStateException("Filter must be initialized to invoke getColumnName");
        }
        final WhereFilter failover = getFailoverFilterIfCached();
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
        final WhereFilter failover = getFailoverFilterIfCached();
        if (failover != null) {
            return failover.getColumnArrays();
        }
        return Collections.emptyList();
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
                    // fix up for the case where column name and variable name were swapped
                    String tmp = columnName;
                    columnName = strValues[0];
                    strValues[0] = tmp;
                } else {
                    throw new RuntimeException("Column \"" + columnName
                            + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
                }
            }
            if (strValues == null) {
                initialized = true;
                return;
            }
            final List<Object> valueList = new ArrayList<>();
            final Map<String, Object> queryScopeVariables =
                    compilationProcessor.getFormulaImports().getQueryScopeVariables();
            final ColumnTypeConvertor convertor = ColumnTypeConvertorFactory.getConvertor(column.getDataType());
            for (String strValue : strValues) {
                convertor.convertValue(column, tableDefinition, strValue, queryScopeVariables, valueList::add);
            }
            values = valueList.toArray();
        } catch (final RuntimeException err) {
            if (failoverFilter == null) {
                throw err;
            }
            try {
                failoverFilter.get().init(tableDefinition, compilationProcessor);
            } catch (final RuntimeException ignored) {
                throw err;
            }
        }
        initialized = true;
    }

    @Override
    public SafeCloseable beginOperation(@NotNull final Table sourceTable) {
        if (initialDependenciesGathered || dataIndex != null) {
            throw new IllegalStateException("Inputs already initialized, use copy() instead of re-using a WhereFilter");
        }
        if (!QueryTable.USE_DATA_INDEX_FOR_WHERE) {
            return () -> {
            };
        }
        try (final SafeCloseable ignored = sourceTable.isRefreshing() ? LivenessScopeStack.open() : null) {
            dataIndex = DataIndexer.getDataIndex(sourceTable, columnName);
            if (dataIndex != null && dataIndex.isRefreshing()) {
                dataIndex.retainReference();
            }
        }
        return dataIndex != null ? this::completeOperation : () -> {
        };
    }

    private void completeOperation() {
        if (dataIndex.isRefreshing()) {
            dataIndex.dropReference();
        }
        dataIndex = null;
    }

    @Override
    public Stream<NotificationQueue.Dependency> getDependencyStream() {
        if (initialDependenciesGathered) {
            return Stream.empty();
        }
        initialDependenciesGathered = true;
        if (dataIndex == null || !dataIndex.isRefreshing()) {
            return Stream.empty();
        }
        return Stream.of(dataIndex.table());
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final WhereFilter failover = getFailoverFilterIfCached();
        if (failover != null) {
            return failover.filter(selection, fullSet, table, usePrev);
        }

        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        return columnSource.match(invertMatch, usePrev, caseInsensitive, dataIndex, selection, values);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final WhereFilter failover = getFailoverFilterIfCached();
        if (failover != null) {
            return failover.filterInverse(selection, fullSet, table, usePrev);
        }

        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        return columnSource.match(!invertMatch, usePrev, caseInsensitive, dataIndex, selection, values);
    }

    @Override
    public boolean isSimpleFilter() {
        final WhereFilter failover = getFailoverFilterIfCached();
        if (failover != null) {
            return failover.isSimpleFilter();
        }

        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    public static abstract class ColumnTypeConvertor {

        abstract Object convertStringLiteral(String str);

        Object convertParamValue(Object paramValue) {
            if (paramValue instanceof PyObject) {
                if (((PyObject) paramValue).isConvertible()) {
                    return ((PyObject) paramValue).getObjectValue();
                }
            }
            return paramValue;
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
                        valueConsumer.accept(convertParamValue(accessor.get(ai)));
                    }
                    return true;
                }
                if (paramValue != null && Collection.class.isAssignableFrom(paramValue.getClass())) {
                    for (final Object paramValueMember : (Collection<?>) paramValue) {
                        valueConsumer.accept(convertParamValue(paramValueMember));
                    }
                    return true;
                }
                valueConsumer.accept(convertParamValue(paramValue));
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

    public static class ColumnTypeConvertorFactory {
        public static ColumnTypeConvertor getConvertor(final Class<?> cls) {
            if (cls == byte.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_BYTE".equals(str)) {
                            return QueryConstants.NULL_BYTE_BOXED;
                        }
                        return Byte.parseByte(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Byte || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.byteCast(boxer.get(paramValue));
                    }
                };
            }
            if (cls == short.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_SHORT".equals(str)) {
                            return QueryConstants.NULL_SHORT_BOXED;
                        }
                        return Short.parseShort(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Short || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.shortCast(boxer.get(paramValue));
                    }
                };
            }
            if (cls == int.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_INT".equals(str)) {
                            return QueryConstants.NULL_INT_BOXED;
                        }
                        return Integer.parseInt(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Integer || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.intCast(boxer.get(paramValue));
                    }
                };
            }
            if (cls == long.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_LONG".equals(str)) {
                            return QueryConstants.NULL_LONG_BOXED;
                        }
                        return Long.parseLong(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Long || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.longCast(boxer.get(paramValue));
                    }
                };
            }
            if (cls == float.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_FLOAT".equals(str)) {
                            return QueryConstants.NULL_FLOAT_BOXED;
                        }
                        return Float.parseFloat(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Float || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.floatCast(boxer.get(paramValue));
                    }
                };
            }
            if (cls == double.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if ("null".equals(str) || "NULL_DOUBLE".equals(str)) {
                            return QueryConstants.NULL_DOUBLE_BOXED;
                        }
                        return Double.parseDouble(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Double || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.doubleCast(boxer.get(paramValue));
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
                        if (str.length() > 1) {
                            // TODO: #1517 Allow escaping of chars
                            if (str.length() == 3 && ((str.charAt(0) == '\'' && str.charAt(2) == '\'')
                                    || (str.charAt(0) == '"' && str.charAt(2) == '"'))) {
                                return str.charAt(1);
                            } else {
                                throw new IllegalArgumentException(
                                        "String " + str + " has length greater than one for column ");
                            }
                        }
                        return str.charAt(0);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof Character || paramValue == null) {
                            return paramValue;
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        return QueryLanguageFunctionUtils.charCast(boxer.get(paramValue));
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
                        return new BigDecimal(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof BigDecimal || paramValue == null) {
                            return paramValue;
                        }
                        if (paramValue instanceof BigInteger) {
                            return new BigDecimal((BigInteger) paramValue);
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        final Object boxedValue = boxer.get(paramValue);
                        if (boxedValue == null) {
                            return null;
                        }
                        if (boxedValue instanceof Number) {
                            return BigDecimal.valueOf(((Number) boxedValue).doubleValue());
                        }
                        return paramValue;
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
                        return new BigInteger(str);
                    }

                    @Override
                    Object convertParamValue(Object paramValue) {
                        paramValue = super.convertParamValue(paramValue);
                        if (paramValue instanceof BigInteger || paramValue == null) {
                            return paramValue;
                        }
                        if (paramValue instanceof BigDecimal) {
                            return ((BigDecimal) paramValue).toBigInteger();
                        }
                        // noinspection unchecked
                        final TypeUtils.TypeBoxer<Object> boxer =
                                (TypeUtils.TypeBoxer<Object>) TypeUtils.getTypeBoxer(paramValue.getClass());
                        final Object boxedValue = boxer.get(paramValue);
                        if (boxedValue == null) {
                            return null;
                        }
                        if (boxedValue instanceof Number) {
                            return BigInteger.valueOf(((Number) boxedValue).longValue());
                        }
                        return paramValue;
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
                            System.out.println("MatchFilter debug: Converting " + paramValue + " to CompressedString");
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
        return columnName + (caseInsensitive ? " icase" : "") + (invertMatch ? " not" : "") + " in "
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
        if (invertMatch != that.invertMatch ||
                caseInsensitive != that.caseInsensitive ||
                !Objects.equals(columnName, that.columnName)) {
            return false;
        }

        if (!Arrays.equals(values, that.values)) {
            return false;
        }

        return Objects.equals(getFailoverFilterIfCached(), that.getFailoverFilterIfCached());
    }

    @Override
    public int hashCode() {
        if (!initialized) {
            throw new UnsupportedOperationException("MatchFilter has not been initialized");
        }
        int result = Objects.hash(columnName, invertMatch, caseInsensitive);
        // we can use values because we know the filter has been initialized; the hash code should be stable and it
        // cannot be stable before we convert the values
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public boolean canMemoize() {
        // we can be memoized once our values have been initialized; but not before
        return initialized && (getFailoverFilterIfCached() == null || getFailoverFilterIfCached().canMemoize());
    }

    @Override
    public WhereFilter copy() {
        final MatchFilter copy;
        if (strValues != null) {
            copy = new MatchFilter(
                    failoverFilter == null ? null : new CachingSupplier<>(() -> failoverFilter.get().copy()),
                    caseInsensitive ? CaseSensitivity.IgnoreCase : CaseSensitivity.MatchCase,
                    getMatchType(), columnName, strValues, null);
        } else {
            // when we're constructed with values then there is no failover filter
            copy = new MatchFilter(getMatchType(), columnName, values);
        }
        if (initialized) {
            copy.initialized = true;
            copy.values = values;
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
