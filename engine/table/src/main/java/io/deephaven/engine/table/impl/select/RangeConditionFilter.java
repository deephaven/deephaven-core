//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

/**
 * A filter for comparable types (including Instant) for {@link Condition} values: <br>
 * <ul>
 * <li>LESS_THAN</li>
 * <li>LESS_THAN_OR_EQUAL</li>
 * <li>GREATER_THAN</li>
 * <li>GREATER_THAN_OR_EQUAL</li>
 * </ul>
 */
public class RangeConditionFilter extends WhereFilterImpl {

    private final String columnName;
    private final Condition condition;
    private final String value;

    // The expression prior to being parsed
    private final String expression;

    private WhereFilter filter;
    private final FormulaParserConfiguration parserConfiguration;

    /**
     * Creates a RangeConditionFilter.
     *
     * @param columnName the column to filter
     * @param condition the condition for filtering
     * @param value a String representation of the numeric filter value
     */
    public RangeConditionFilter(String columnName, Condition condition, String value) {
        this(columnName, condition, value, null, null, null);
    }

    /**
     * Creates a RangeConditionFilter.
     *
     * @param columnName the column to filter
     * @param condition the condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration the parser configuration to use
     */
    public RangeConditionFilter(String columnName, Condition condition, String value, String expression,
            FormulaParserConfiguration parserConfiguration) {
        this(columnName, condition, value, expression, null, parserConfiguration);
    }

    /**
     * Creates a RangeConditionFilter.
     *
     * @param columnName the column to filter
     * @param conditionString the String representation of a condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration the parser configuration to useyy
     */
    public RangeConditionFilter(String columnName, String conditionString, String value, String expression,
            FormulaParserConfiguration parserConfiguration) {
        this(columnName, conditionFromString(conditionString), value, expression, parserConfiguration);
    }

    // Used for copy method
    private RangeConditionFilter(String columnName, Condition condition, String value, String expression,
            WhereFilter filter, FormulaParserConfiguration parserConfiguration) {
        Assert.eqTrue(conditionSupported(condition), condition + " is not supported by RangeConditionFilter");
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
                throw new IllegalArgumentException(conditionString + " is not supported by RangeConditionFilter");
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
    public void init(@NotNull TableDefinition tableDefinition) {
        init(tableDefinition, QueryCompilerRequestProcessor.immediate());
    }

    @Override
    public void init(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final QueryCompilerRequestProcessor compilationProcessor) {
        if (filter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class<?> colClass = def.getDataType();

        final MatchFilter.ColumnTypeConvertor convertor =
                MatchFilter.ColumnTypeConvertorFactory.getConvertor(def.getDataType());

        final MutableObject<Object> realValue = new MutableObject<>();
        convertor.convertValue(def, value, compilationProcessor.getQueryScopeVariables(),
                parsedValue -> {
                    if (realValue.getValue() != null) {
                        throw new IllegalArgumentException(value + " is an array type");
                    }
                    realValue.setValue(parsedValue);
                });

        if (colClass == double.class || colClass == Double.class) {
            filter = DoubleRangeFilter.makeDoubleRangeFilter(columnName, condition, (double) realValue.getValue());
        } else if (colClass == float.class || colClass == Float.class) {
            filter = FloatRangeFilter.makeFloatRangeFilter(columnName, condition, (float) realValue.getValue());
        } else if (colClass == char.class || colClass == Character.class) {
            filter = CharRangeFilter.makeCharRangeFilter(columnName, condition, (char) realValue.getValue());
        } else if (colClass == byte.class || colClass == Byte.class) {
            filter = ByteRangeFilter.makeByteRangeFilter(columnName, condition, (byte) realValue.getValue());
        } else if (colClass == short.class || colClass == Short.class) {
            filter = ShortRangeFilter.makeShortRangeFilter(columnName, condition, (short) realValue.getValue());
        } else if (colClass == int.class || colClass == Integer.class) {
            filter = IntRangeFilter.makeIntRangeFilter(columnName, condition, (int) realValue.getValue());
        } else if (colClass == long.class || colClass == Long.class) {
            filter = LongRangeFilter.makeLongRangeFilter(columnName, condition, (long) realValue.getValue());
        } else if (colClass == Instant.class) {
            filter = makeInstantRangeFilter(columnName, condition,
                    DateTimeUtils.epochNanos((Instant) realValue.getValue()));
        } else if (colClass == LocalDate.class) {
            filter = makeComparableRangeFilter(columnName, condition, (LocalDate) realValue.getValue());
        } else if (colClass == LocalTime.class) {
            filter = makeComparableRangeFilter(columnName, condition, (LocalTime) realValue.getValue());
        } else if (colClass == LocalDateTime.class) {
            filter = makeComparableRangeFilter(columnName, condition, (LocalDateTime) realValue.getValue());
        } else if (colClass == ZonedDateTime.class) {
            filter = makeInstantRangeFilter(columnName, condition,
                    DateTimeUtils.epochNanos((ZonedDateTime) realValue.getValue()));
        } else if (BigDecimal.class.isAssignableFrom(colClass)) {
            filter = makeComparableRangeFilter(columnName, condition, (BigDecimal) realValue.getValue());
        } else if (BigInteger.class.isAssignableFrom(colClass)) {
            filter = makeComparableRangeFilter(columnName, condition, (BigInteger) realValue.getValue());
        } else if (io.deephaven.util.type.TypeUtils.isString(colClass)) {
            filter = makeComparableRangeFilter(columnName, condition, (String) realValue.getValue());
        } else if (TypeUtils.isBoxedBoolean(colClass) || colClass == boolean.class) {
            filter = makeComparableRangeFilter(columnName, condition, (Boolean) realValue.getValue());
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

        filter.init(tableDefinition, compilationProcessor);
    }

    private static LongRangeFilter makeInstantRangeFilter(String columnName, Condition condition, long value) {
        switch (condition) {
            case LESS_THAN:
                return new InstantRangeFilter(columnName, value, Long.MIN_VALUE, true, false);
            case LESS_THAN_OR_EQUAL:
                return new InstantRangeFilter(columnName, value, Long.MIN_VALUE, true, true);
            case GREATER_THAN:
                return new InstantRangeFilter(columnName, value, Long.MAX_VALUE, false, true);
            case GREATER_THAN_OR_EQUAL:
                return new InstantRangeFilter(columnName, value, Long.MAX_VALUE, true, true);
            default:
                throw new IllegalArgumentException("RangeConditionFilter does not support condition " + condition);
        }
    }

    private static SingleSidedComparableRangeFilter makeComparableRangeFilter(String columnName, Condition condition,
            Comparable<?> comparable) {
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
                throw new IllegalArgumentException("RangeConditionFilter does not support condition " + condition);
        }
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filter(selection, fullSet, table, usePrev);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        return filter.filterInverse(selection, fullSet, table, usePrev);
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    @Override
    public WhereFilter copy() {
        return new RangeConditionFilter(columnName, condition, value, expression, filter, parserConfiguration);
    }

    @Override
    public String toString() {
        return "RangeConditionFilter(" + columnName + " " + condition.description + " " + value + ")";
    }
}
