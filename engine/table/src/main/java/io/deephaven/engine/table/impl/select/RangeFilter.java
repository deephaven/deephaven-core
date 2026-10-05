//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.gui.table.filters.Condition;
import io.deephaven.util.annotations.VisibleForTesting;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A filter for comparable types (including Instant) for {@link Condition} values: <br>
 * <ul>
 * <li>LESS_THAN</li>
 * <li>LESS_THAN_OR_EQUAL</li>
 * <li>GREATER_THAN</li>
 * <li>GREATER_THAN_OR_EQUAL</li>
 * </ul>
 *
 * <p>
 * A query-scope parameter is converted to the column's type as {@link MatchFilter} converts it, so that the filter
 * selects the rows a {@link ConditionFilter} would. Where the converted value would select other rows -- a value the
 * conversion rejects, a value of another type that the conversion leaves as it is (an {@link Integer} against a
 * {@link String} column, say), or {@code -0.0} against a byte, short, int or char column (the query language orders it
 * below {@code 0}, which the converted value {@code 0} is not) -- the filter fails over to a {@link ConditionFilter}.
 *
 * <p>
 * Two differences remain. Float and double columns' range filters treat {@code -0.0} and {@code 0.0} as equal, where
 * the query language orders {@code -0.0} below {@code 0.0}, so on rows holding {@code -0.0}, {@code X < 0.0} and
 * {@code X >= 0.0} select otherwise than the query language does. And a {@link Float} bound against an int column
 * converts to its exact int, where the query language compares the two in float, rounding an int beyond 2^24: there,
 * {@code X > 16777216f} excludes {@code 16777217}, which the converted bound {@code 16777216} includes. This is the
 * range counterpart of {@link MatchFilter}'s exact match of a large floating-point value.
 *
 * <p>
 * For primitive columns the endpoint is compared in Deephaven's type system, where each type's null value sorts below
 * every other value. An endpoint of the column's own type equal to its null value -- {@code -Double.MAX_VALUE} is
 * {@code NULL_DOUBLE}, and {@code Long.MIN_VALUE} is {@code NULL_LONG} -- is therefore null, as it is in the query
 * language, so {@code X < -Double.MAX_VALUE} matches no rows at all, {@code -Infinity} included. An endpoint of another
 * type that converts to the null value, a query-scope int {@code v = -128} against a byte column for instance, is a
 * number in the query language, which the conversion rejects: {@code X < v} selects the null rows only. A literal,
 * though, is read in the column's type, so the literal {@code -128} against a byte column is {@code NULL_BYTE}, and
 * null.
 */
public class RangeFilter extends WhereFilterImpl implements ExposesChunkFilter {

    private String columnName;
    private String value;
    private Condition condition;

    // The expression prior to being parsed
    private final String expression;

    private WhereFilter filter;
    private final FormulaParserConfiguration parserConfiguration;

    /**
     * Creates a RangeFilter.
     *
     * @param columnName the column to filter
     * @param condition the condition for filtering
     * @param value a String representation of the numeric filter value
     */
    public RangeFilter(String columnName, Condition condition, String value) {
        this(columnName, condition, value, null, null, null);
    }

    /**
     * Creates a RangeFilter.
     *
     * @param columnName the column to filter
     * @param condition the condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration the parser configuration to use
     */
    public RangeFilter(String columnName, Condition condition, String value, String expression,
            FormulaParserConfiguration parserConfiguration) {
        this(columnName, condition, value, expression, null, parserConfiguration);
    }

    /**
     * Creates a RangeFilter.
     *
     * @param columnName the column to filter
     * @param conditionString the String representation of a condition for filtering
     * @param value a String representation of the numeric filter value
     * @param expression the original expression prior to being parsed
     * @param parserConfiguration the parser configuration to use
     */
    public RangeFilter(String columnName, String conditionString, String value, String expression,
            FormulaParserConfiguration parserConfiguration) {
        this(columnName, conditionFromString(conditionString), value, expression, parserConfiguration);
    }

    // Used for copy method
    private RangeFilter(String columnName, Condition condition, String value, String expression,
            WhereFilter filter, FormulaParserConfiguration parserConfiguration) {
        Assert.eqTrue(conditionSupported(condition), condition + " is not supported by RangeFilter");
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
                throw new IllegalArgumentException(conditionString + " is not supported by RangeFilter");
        }
    }

    /**
     * Whether the query language compares a column of this integral type with a floating-point value through
     * {@link Double#compare} or {@link Float#compare}, which order {@code -0.0} below {@code 0}. It compares a
     * {@code long} column exactly, where {@code -0.0} is {@code 0}. (A float or double column keeps the sign of
     * {@code -0.0}, but its range filters do not tell it from {@code 0.0}; see the class documentation.)
     */
    private static boolean ordersNegativeZeroBelowZero(final Class<?> colClass) {
        final Class<?> type = TypeUtils.getUnboxedTypeIfBoxed(colClass);
        return type == byte.class || type == short.class || type == int.class || type == char.class;
    }

    private static boolean isNegativeZero(final Object value) {
        return (value instanceof Double && Double.doubleToRawLongBits((Double) value) == Long.MIN_VALUE)
                || (value instanceof Float && Float.floatToRawIntBits((Float) value) == Integer.MIN_VALUE);
    }

    @Override
    public List<String> getColumns() {
        if (filter == null) {
            throw new IllegalStateException("Filter must be initialized to invoke getColumnName");
        }
        return filter.getColumns();
    }

    @Override
    public List<String> getColumnArrays() {
        if (filter == null) {
            throw new IllegalStateException("Filter must be initialized to invoke getColumnArrays");
        }
        return filter.getColumnArrays();
    }

    @Override
    public boolean hasVirtualRowVariables() {
        if (filter == null) {
            throw new IllegalStateException("Filter must be initialized to invoke hasVirtualRowVariables");
        }
        return filter.hasVirtualRowVariables();
    }

    @Override
    public boolean canPushdown() {
        // The real filter is not visible to a walk of the filter tree, so answer for it here.
        return filter == null || filter.canPushdown();
    }

    @Override
    public void validateSafeForRefresh(final BaseTable<?> sourceTable) {
        if (filter == null) {
            super.validateSafeForRefresh(sourceTable);
        } else {
            filter.validateSafeForRefresh(sourceTable);
        }
    }

    @Override
    public boolean permitParallelization() {
        // A failover ConditionFilter may not permit parallelization, so answer for the real filter.
        return filter == null ? super.permitParallelization() : filter.permitParallelization();
    }

    @VisibleForTesting
    public WhereFilter getRealFilter() {
        return filter;
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

        // Why the converted value cannot be used, if it cannot: it does not convert exactly, or would not select the
        // rows the query language selects, or there is no such column. The filter then fails over to a
        // ConditionFilter; this is thrown only if it cannot.
        RuntimeException potentialConversionError = null;
        ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            if ((def = tableDefinition.getColumn(value)) != null) {
                // fix up for the case where column name and variable name were swapped
                String tmp = columnName;
                columnName = value;
                value = tmp;
                condition = condition.mirror();
            } else {
                potentialConversionError = new RuntimeException("Column \"" + columnName
                        + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
            }
        }

        final Class<?> colClass = def == null ? null : def.getDataType();
        final MutableObject<Object> realValue = new MutableObject<>();
        Object queryScopeValue = null;

        if (def != null) {
            final MatchFilter.ColumnTypeConvertor convertor =
                    MatchFilter.ColumnTypeConvertorFactory.getConvertor(def.getDataType());

            try {
                final Map<String, Object> queryScopeVariables =
                        compilationProcessor.getFormulaImports().getQueryScopeVariables();
                // consulted only if convertValue succeeds, which it does not when a column of this name takes
                // precedence
                queryScopeValue = MatchFilter.ColumnTypeConvertor.maybeUnwrapPyObject(queryScopeVariables.get(value));
                boolean wasAnArrayType = convertor.convertValue(
                        def, tableDefinition, value, queryScopeVariables, realValue::setValue);
                if (wasAnArrayType) {
                    potentialConversionError =
                            new IllegalArgumentException("RangeFilter does not support array types for column "
                                    + columnName + " with value <" + value + ">");
                } else if (ordersNegativeZeroBelowZero(colClass) && isNegativeZero(queryScopeValue)) {
                    // Failover to match the query language, which widens the column value to double and compares with
                    // Double.compare, ordering -0.0 below 0: X <= -0.0 excludes 0 there, though -0.0 converts to 0.
                    potentialConversionError = new IllegalArgumentException("RangeFilter cannot compare column "
                            + columnName + " with -0.0 as the query language does");
                } else if (realValue.getValue() != null
                        && !TypeUtils.getBoxedType(colClass).isInstance(realValue.getValue())) {
                    // a value the convertor passed through unconverted, which the range filters below would cast
                    potentialConversionError = MatchFilter.ColumnTypeConvertor.cannotConvert(realValue.getValue(),
                            TypeUtils.getBoxedType(colClass), "it is not of the column's type", null);
                }
            } catch (final RuntimeException err) {
                potentialConversionError = err;
            }
        }

        if (potentialConversionError != null) {
            if (expression != null) {
                try {
                    filter = ConditionFilter.createConditionFilter(expression, parserConfiguration);
                } catch (final RuntimeException ignored) {
                    throw potentialConversionError;
                }
            } else {
                throw potentialConversionError;
            }
        } else if (colClass == double.class || colClass == Double.class) {
            filter = DoubleRangeFilter.makeDoubleRangeFilter(columnName, condition,
                    TypeUtils.unbox((Double) realValue.getValue()));
        } else if (colClass == float.class || colClass == Float.class) {
            filter = FloatRangeFilter.makeFloatRangeFilter(columnName, condition,
                    TypeUtils.unbox((Float) realValue.getValue()));
        } else if (colClass == char.class || colClass == Character.class) {
            filter = CharRangeFilter.makeCharRangeFilter(columnName, condition,
                    TypeUtils.unbox((Character) realValue.getValue()));
        } else if (colClass == byte.class || colClass == Byte.class) {
            filter = ByteRangeFilter.makeByteRangeFilter(columnName, condition,
                    TypeUtils.unbox((Byte) realValue.getValue()));
        } else if (colClass == short.class || colClass == Short.class) {
            filter = ShortRangeFilter.makeShortRangeFilter(columnName, condition,
                    TypeUtils.unbox((Short) realValue.getValue()));
        } else if (colClass == int.class || colClass == Integer.class) {
            filter = IntRangeFilter.makeIntRangeFilter(columnName, condition,
                    TypeUtils.unbox((Integer) realValue.getValue()));
        } else if (colClass == long.class || colClass == Long.class) {
            filter = LongRangeFilter.makeLongRangeFilter(columnName, condition,
                    TypeUtils.unbox((Long) realValue.getValue()));
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
            filter = makeComparableRangeFilter(columnName, condition, (ZonedDateTime) realValue.getValue());
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
                try {
                    filter = ConditionFilter.createConditionFilter(expression, parserConfiguration);
                } catch (final RuntimeException ignored) {
                    throw new IllegalArgumentException("RangeFilter does not support type "
                            + colClass.getSimpleName() + " for column " + columnName);
                }
            } else {
                throw new IllegalArgumentException("RangeFilter does not support type "
                        + colClass.getSimpleName() + " for column " + columnName);
            }
        }

        filter.init(tableDefinition, compilationProcessor);
    }

    @Override
    public Optional<ChunkFilter> chunkFilter() {
        // The underlying filter may be a ConditionFilter
        if (filter instanceof ExposesChunkFilter) {
            return ((ExposesChunkFilter) filter).chunkFilter();
        }
        return Optional.empty();
    }

    /**
     * Return an {@link Optional} containing the underlying {@link AbstractRangeFilter} if the provided filter is a
     * range filter that can be pushed down (i.e. is not implemented by a ConditionFilter). Otherwise returns
     * {@code Optional.empty()}.
     */
    public static Optional<AbstractRangeFilter> extractRangeFilter(WhereFilter filter) {
        if (filter instanceof RangeFilter
                && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter) {
            return Optional.of((AbstractRangeFilter) ((RangeFilter) filter).getRealFilter());
        }
        if (filter instanceof AbstractRangeFilter) {
            return Optional.of((AbstractRangeFilter) filter);
        }
        return Optional.empty();
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
                throw new IllegalArgumentException("RangeFilter does not support condition " + condition);
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
                throw new IllegalArgumentException("RangeFilter does not support condition " + condition);
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
        return filter.isSimpleFilter();
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

    @Override
    public WhereFilter copy() {
        final WhereFilter innerCopy = filter == null ? null : filter.copy();
        return new RangeFilter(columnName, condition, value, expression, innerCopy, parserConfiguration);
    }

    @Override
    public String toString() {
        return "RangeFilter(" + columnName + " " + condition.description + " " + value + ")";
    }
}
