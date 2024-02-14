/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.literal.Literal;
import io.deephaven.base.string.cache.CompressedString;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.preview.DisplayWrapper;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;

public class MatchFilter extends WhereFilterImpl {

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

    @NotNull
    private final String columnName;
    private Object[] values; // TODO: Does values need to be declared volatile (if we go back to the double-check)?
    private final String[] strValues;
    private final boolean invertMatch;
    private final boolean caseInsensitive;
    private boolean initialized = false;

    public enum MatchType {
        Regular, Inverted,
    }

    public enum CaseSensitivity {
        MatchCase, IgnoreCase
    }

    public MatchFilter(MatchType matchType, String columnName, Object... values) {
        this.columnName = columnName;
        this.values = values;
        this.strValues = null;
        this.invertMatch = (matchType == MatchType.Inverted);
        this.caseInsensitive = false;
    }

    public MatchFilter(String columnName, Object... values) {
        this(MatchType.Regular, columnName, values);
    }

    public MatchFilter(CaseSensitivity sensitivity, String columnName, String... strValues) {
        this(sensitivity, MatchType.Regular, columnName, strValues);
    }

    public MatchFilter(CaseSensitivity sensitivity, MatchType matchType, String columnName, String... strValues) {
        this.columnName = columnName;
        this.strValues = strValues;
        this.caseInsensitive = (sensitivity == CaseSensitivity.IgnoreCase);
        this.invertMatch = (matchType == MatchType.Inverted);
    }

    public MatchFilter renameFilter(String newName) {
        io.deephaven.engine.table.impl.select.MatchFilter.MatchType matchType =
                invertMatch ? io.deephaven.engine.table.impl.select.MatchFilter.MatchType.Inverted
                        : io.deephaven.engine.table.impl.select.MatchFilter.MatchType.Regular;
        CaseSensitivity sensitivity = (caseInsensitive) ? CaseSensitivity.IgnoreCase : CaseSensitivity.MatchCase;
        if (strValues == null) {
            return new MatchFilter(matchType, newName, values);
        } else {
            return new MatchFilter(sensitivity, matchType, newName, strValues);
        }
    }

    public String getColumnName() {
        return columnName;
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
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public synchronized void init(TableDefinition tableDefinition) {
        if (initialized) {
            return;
        }
        ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
        if (column == null) {
            throw new RuntimeException("Column \"" + columnName
                    + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
        }
        if (strValues == null) {
            initialized = true;
            return;
        }
        final List<Object> valueList = new ArrayList<>();
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        final ColumnTypeConvertor convertor =
                ColumnTypeConvertorFactory.getConvertor(column.getDataType(), column.getName());
        for (String strValue : strValues) {
            if (queryScope != null && queryScope.hasParamName(strValue)) {
                Object paramValue = queryScope.readParamValue(strValue);
                if (paramValue != null && paramValue.getClass().isArray()) {
                    ArrayTypeUtils.ArrayAccessor<?> accessor = ArrayTypeUtils.getArrayAccessor(paramValue);
                    for (int ai = 0; ai < accessor.length(); ++ai) {
                        valueList.add(convertor.convertParamValue(accessor.get(ai)));
                    }
                } else if (paramValue != null && Collection.class.isAssignableFrom(paramValue.getClass())) {
                    for (final Object paramValueMember : (Collection<?>) paramValue) {
                        valueList.add(convertor.convertParamValue(paramValueMember));
                    }
                } else {
                    valueList.add(convertor.convertParamValue(paramValue));
                }
            } else {
                Object convertedValue;
                try {
                    convertedValue = convertor.convertStringLiteral(strValue);
                } catch (Throwable t) {
                    throw new IllegalArgumentException("Failed to convert literal value <" + strValue +
                            "> for column \"" + columnName + "\" of type " + column.getDataType().getName(), t);
                }
                valueList.add(convertedValue);
            }
        }
        // values = (Object[])ArrayTypeUtils.toArray(valueList, TypeUtils.getBoxedType(theColumn.getDataType()));
        values = valueList.toArray();
        initialized = true;
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        return columnSource.match(invertMatch, usePrev, caseInsensitive, selection, values);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName);
        return columnSource.match(!invertMatch, usePrev, caseInsensitive, selection, values);
    }

    @Override
    public boolean isSimpleFilter() {
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
    }

    public static class ColumnTypeConvertorFactory {
        public static ColumnTypeConvertor getConvertor(final Class<?> cls, final String name) {
            if (cls == byte.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Byte.parseByte(str);
                    }
                };
            }
            if (cls == short.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Short.parseShort(str);
                    }
                };
            }
            if (cls == int.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Integer.parseInt(str);
                    }
                };
            }
            if (cls == long.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Long.parseLong(str);
                    }
                };
            }
            if (cls == float.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Float.parseFloat(str);
                    }
                };
            }
            if (cls == double.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return Double.parseDouble(str);
                    }
                };
            }
            if (cls == Boolean.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        // NB: Boolean.parseBoolean(str) doesn't do what we want here - anything not true is false.
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
                };
            }
            if (cls == BigDecimal.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return new BigDecimal(str);
                    }
                };
            }
            if (cls == BigInteger.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        return new BigInteger(str);
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
                        if (str.charAt(0) != '\'' || str.charAt(str.length() - 1) != '\'') {
                            throw new IllegalArgumentException(
                                    "Instant literal not enclosed in single-quotes (\"" + str + "\")");
                        }
                        return DateTimeUtils.parseInstant(str.substring(1, str.length() - 1));
                    }
                };
            }
            if (cls == Object.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
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
                        return Enum.valueOf((Class) cls, str);
                    }
                };
            }
            if (cls == DisplayWrapper.class) {
                return new ColumnTypeConvertor() {
                    @Override
                    Object convertStringLiteral(String str) {
                        if (str.startsWith("\"") || str.startsWith("`")) {
                            return DisplayWrapper.make(str.substring(1, str.length() - 1));
                        } else {
                            return DisplayWrapper.make(str);
                        }
                    }
                };
            }
            throw new IllegalArgumentException(
                    "Unknown type " + cls.getName() + " for MatchFilter value auto-conversion");
        }
    }

    @Override
    public String toString() {
        if (strValues == null) {
            return columnName + (invertMatch ? " not" : "") + " in " + Arrays.toString(values);
        }
        return columnName + (invertMatch ? " not" : "") + " in " + Arrays.toString(strValues);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final MatchFilter that = (MatchFilter) o;
        return invertMatch == that.invertMatch &&
                caseInsensitive == that.caseInsensitive &&
                Objects.equals(columnName, that.columnName) &&
                Arrays.equals(values, that.values) &&
                Arrays.equals(strValues, that.strValues);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(columnName, invertMatch, caseInsensitive);
        result = 31 * result + Arrays.hashCode(values);
        result = 31 * result + Arrays.hashCode(strValues);
        return result;
    }

    @Override
    public boolean canMemoize() {
        // we can be memoized once our values have been initialized; but not before
        return initialized;
    }

    @Override
    public WhereFilter copy() {
        final MatchFilter copy;
        if (strValues != null) {
            copy = new MatchFilter(caseInsensitive ? CaseSensitivity.IgnoreCase : CaseSensitivity.MatchCase,
                    getMatchType(), columnName, strValues);
        } else {
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
