package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.type.TypeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

/**
 * A filter for comparable types (including DBDateTime) for {@link Condition} values: <br>
 * <ul>
 * <li>LESS_THAN</li>
 * <li>LESS_THAN_OR_EQUAL</li>
 * <li>GREATER_THAN</li>
 * <li>GREATER_THAN_OR_EQUAL</li>
 * </ul>
 */
public class RangeConditionFilter extends SelectFilterImpl {
    private final String columnName;
    private final Condition condition;
    private final String value;

    // The expression prior to being parsed
    private final String expression;

    private SelectFilter filter;
    private final FormulaParserConfiguration parserConfiguration;

    /**
     * Creates a RangeConditionFilter.
     *
     * @param columnName the column to filter
     * @param condition the condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration
     */
    public RangeConditionFilter(String columnName, Condition condition, String value,
        String expression, FormulaParserConfiguration parserConfiguration) {
        this(columnName, condition, value, expression, null, parserConfiguration);
    }

    /**
     * Creates a RangeConditionFilter.
     *
     * @param columnName the column to filter
     * @param conditionString the String representation of a condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration
     */
    public RangeConditionFilter(String columnName, String conditionString, String value,
        String expression, FormulaParserConfiguration parserConfiguration) {
        this(columnName, conditionFromString(conditionString), value, expression,
            parserConfiguration);
    }

    // Used for copy method
    private RangeConditionFilter(String columnName, Condition condition, String value,
        String expression, SelectFilter filter, FormulaParserConfiguration parserConfiguration) {
        Assert.eqTrue(conditionSupported(condition),
            condition + " is not supported by RangeConditionFilter");
        this.columnName = columnName;
        this.condition = condition;
        this.value = value;
        this.expression = expression;
        this.filter = filter;
        this.parserConfiguration = parserConfiguration;
    }

    private static boolean conditionSupported(Condition condition) {
        switch (condition) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private static Condition conditionFromString(String conditionString) {
        switch (conditionString) {
            case "<":
                return Condition.LESS_THAN;
            case "<=":
                return Condition.LESS_THAN_OR_EQUAL;
            case ">":
                return Condition.GREATER_THAN;
            case ">=":
                return Condition.GREATER_THAN_OR_EQUAL;
            default:
                throw new IllegalArgumentException(
                    conditionString + " is not supported by RangeConditionFilter");
        }
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
    public void init(TableDefinition tableDefinition) {
        if (filter != null) {
            return;
        }

        final ColumnDefinition def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException(
                "Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class colClass = def.getDataType();

        if (colClass == double.class || colClass == Double.class) {
            filter = DoubleRangeFilter.makeDoubleRangeFilter(columnName, condition, value);
        } else if (colClass == float.class || colClass == Float.class) {
            filter = FloatRangeFilter.makeFloatRangeFilter(columnName, condition, value);
        } else if (colClass == char.class || colClass == Character.class) {
            filter = CharRangeFilter.makeCharRangeFilter(columnName, condition, value);
        } else if (colClass == byte.class || colClass == Byte.class) {
            filter = ByteRangeFilter.makeByteRangeFilter(columnName, condition, value);
        } else if (colClass == short.class || colClass == Short.class) {
            filter = ShortRangeFilter.makeShortRangeFilter(columnName, condition, value);
        } else if (colClass == int.class || colClass == Integer.class) {
            filter = IntRangeFilter.makeIntRangeFilter(columnName, condition, value);
        } else if (colClass == long.class || colClass == Long.class) {
            filter = LongRangeFilter.makeLongRangeFilter(columnName, condition, value);
        } else if (io.deephaven.util.type.TypeUtils.isDateTime(colClass)) {
            filter = makeDateTimeRangeFilter(columnName, condition, value);
        } else if (BigDecimal.class.isAssignableFrom(colClass)) {
            filter = makeComparableRangeFilter(columnName, condition, new BigDecimal(value));
        } else if (BigInteger.class.isAssignableFrom(colClass)) {
            filter = makeComparableRangeFilter(columnName, condition, new BigInteger(value));
        } else if (io.deephaven.util.type.TypeUtils.isString(colClass)) {
            final String stringValue = MatchFilter.ColumnTypeConvertorFactory
                .getConvertor(String.class, columnName).convertStringLiteral(value).toString();
            filter = makeComparableRangeFilter(columnName, condition, stringValue);
        } else if (TypeUtils.isBoxedBoolean(colClass) || colClass == boolean.class) {
            filter = makeComparableRangeFilter(columnName, condition, Boolean.valueOf(value));
        } else {
            // The expression looks like a comparison of number, string, or boolean
            // but the type does not match (or the column type is misconfigured)
            if (expression != null) {
                filter = ConditionFilter.createConditionFilter(expression, parserConfiguration);
            } else {
                throw new IllegalArgumentException("RangeConditionFilter does not support type "
                    + colClass.getSimpleName() + " for column " + columnName);
            }
        }

        filter.init(tableDefinition);
    }

    public static char parseCharFilter(String value) {
        if (value.startsWith("'") && value.endsWith("'") && value.length() == 3) {
            return value.charAt(1);
        }
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() == 3) {
            return value.charAt(1);
        }
        return (char) Long.parseLong(value);
    }

    public static byte parseByteFilter(String value) {
        return Byte.parseByte(value);
    }

    public static short parseShortFilter(String value) {
        return Short.parseShort(value);
    }

    public static int parseIntFilter(String value) {
        return Integer.parseInt(value);
    }

    public static long parseLongFilter(String value) {
        return Long.parseLong(value);
    }

    private static LongRangeFilter makeDateTimeRangeFilter(String columnName, Condition condition,
        String value) {
        switch (condition) {
            case LESS_THAN:
                return new DateTimeRangeFilter(columnName, parseDateTimeNanos(value),
                    Long.MIN_VALUE, true, false);
            case LESS_THAN_OR_EQUAL:
                return new DateTimeRangeFilter(columnName, parseDateTimeNanos(value),
                    Long.MIN_VALUE, true, true);
            case GREATER_THAN:
                return new DateTimeRangeFilter(columnName, parseDateTimeNanos(value),
                    Long.MAX_VALUE, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new DateTimeRangeFilter(columnName, parseDateTimeNanos(value),
                    Long.MAX_VALUE, true, true);
            default:
                throw new IllegalArgumentException(
                    "RangeConditionFilter does not support condition " + condition);
        }
    }

    private static long parseDateTimeNanos(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return DBTimeUtils.convertDateTime(value.substring(1, value.length() - 1)).getNanos();
        }
        return Long.parseLong(value);
    }

    private static SingleSidedComparableRangeFilter makeComparableRangeFilter(String columnName,
        Condition condition, Comparable<?> comparable) {
        switch (condition) {
            case LESS_THAN:
                return new SingleSidedComparableRangeFilter(columnName, comparable, false, false);
            case LESS_THAN_OR_EQUAL:
                return new SingleSidedComparableRangeFilter(columnName, comparable, true, false);
            case GREATER_THAN:
                return new SingleSidedComparableRangeFilter(columnName, comparable, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new SingleSidedComparableRangeFilter(columnName, comparable, true, true);
            default:
                throw new IllegalArgumentException(
                    "RangeConditionFilter does not support condition " + condition);
        }
    }

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        return filter.filter(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    @Override
    public SelectFilter copy() {
        return new RangeConditionFilter(columnName, condition, value, expression, filter,
            parserConfiguration);
    }

    @Override
    public String toString() {
        return "RangeConditionFilter(" + columnName + " " + condition.description + " " + value
            + ")";
    }
}
