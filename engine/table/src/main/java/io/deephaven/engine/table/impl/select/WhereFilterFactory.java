/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.api.expression.AbstractExpressionFactory;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.gui.table.QuickFilterMode;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.text.SplitIgnoreQuotes;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.api.expression.SelectFactoryConstants.*;

/**
 * Given a user's filter string produce an appropriate WhereFilter instance.
 */
public class WhereFilterFactory {

    private static final Logger log = LoggerFactory.getLogger(WhereFilterFactory.class);

    private static final ExpressionParser<WhereFilter> parser = new ExpressionParser<>();

    static {
        // <ColumnName>==<Number|Boolean|"String">
        // <ColumnName>=<Number|Boolean|"String">
        // <ColumnName>!=<Number|Boolean|"String">
        parser.registerFactory(new AbstractExpressionFactory<>(
                START_PTRN + "(" + ID_PTRN + ")\\s*(?:(?:={1,2})|(!=))\\s*(" + LITERAL_PTRN + ")" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final boolean inverted = matcher.group(2) != null;
                final String value = matcher.group(3);

                final FormulaParserConfiguration parserConfiguration = (FormulaParserConfiguration) args[0];
                if (isRowVariable(columnName)) {
                    log.debug().append("WhereFilterFactory creating ConditionFilter for expression: ")
                            .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                log.debug().append("WhereFilterFactory creating MatchFilter for expression: ").append(expression)
                        .endl();
                return new MatchFilter(
                        MatchFilter.CaseSensitivity.MatchCase,
                        inverted ? MatchFilter.MatchType.Inverted : MatchFilter.MatchType.Regular,
                        columnName,
                        value);
            }
        });
        // <ColumnName>==<User QueryScopeParam>
        // <ColumnName>=<User QueryScopeParam>
        // <ColumnName>!=<User QueryScopeParam>
        parser.registerFactory(new AbstractExpressionFactory<>(
                START_PTRN + "(" + ID_PTRN + ")\\s*(?:(?:={1,2})|(!=))\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final boolean inverted = matcher.group(2) != null;
                final String paramName = matcher.group(3);

                final FormulaParserConfiguration parserConfiguration = (FormulaParserConfiguration) args[0];

                if (isRowVariable(columnName)) {
                    log.debug().append("WhereFilterFactory creating ConditionFilter for expression: ")
                            .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                try {
                    QueryScope.getParamValue(paramName);
                } catch (QueryScope.MissingVariableException e) {
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                log.debug().append("WhereFilterFactory creating MatchFilter for expression: ").append(expression)
                        .endl();
                return new MatchFilter(
                        MatchFilter.CaseSensitivity.MatchCase,
                        inverted ? MatchFilter.MatchType.Inverted : MatchFilter.MatchType.Regular,
                        columnName,
                        paramName);
            }
        });

        // <ColumnName> < <Number|Boolean|"String">
        // <ColumnName> <= <Number|Boolean|"String">
        // <ColumnName> > <Number|Boolean|"String">
        // <ColumnName> >= <Number|Boolean|"String">
        parser.registerFactory(new AbstractExpressionFactory<>(
                START_PTRN + "(" + ID_PTRN + ")\\s*([<>]=?)\\s*(" + LITERAL_PTRN + ")" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                final FormulaParserConfiguration parserConfiguration = (FormulaParserConfiguration) args[0];
                final String columnName = matcher.group(1);
                final String conditionString = matcher.group(2);
                final String value = matcher.group(3);
                if (isRowVariable(columnName)) {
                    log.debug().append("WhereFilterFactory creating ConditionFilter for expression: ")
                            .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                try {
                    log.debug().append("WhereFilterFactory creating RangeConditionFilter for expression: ")
                            .append(expression).endl();
                    return new RangeConditionFilter(columnName, conditionString, value, expression,
                            parserConfiguration);
                } catch (Exception e) {
                    log.warn().append("WhereFilterFactory could not make RangeFilter for expression: ")
                            .append(expression).append(" due to ").append(e)
                            .append(" Creating ConditionFilter instead.").endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
            }
        });

        // <ColumnName> [icase] [not] in <value 1>, <value 2>, ... , <value n>
        parser.registerFactory(new AbstractExpressionFactory<>("(?s)" + START_PTRN + "(" + ID_PTRN
                + ")\\s+(" + ICASE + "\\s+)?(" + NOT + "\\s+)?" + IN + "\\s+(.+?)" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final boolean icase = matcher.group(2) != null;
                final boolean inverted = matcher.group(3) != null;
                final String[] values = new SplitIgnoreQuotes().split(matcher.group(4), ',');

                log.debug().append("WhereFilterFactory creating MatchFilter for expression: ").append(expression)
                        .endl();
                return new MatchFilter(
                        icase ? MatchFilter.CaseSensitivity.IgnoreCase : MatchFilter.CaseSensitivity.MatchCase,
                        inverted ? MatchFilter.MatchType.Inverted : MatchFilter.MatchType.Regular,
                        columnName,
                        values);
            }
        });

        // <ColumnName> [icase] [not] includes [any|all]<"String">
        parser.registerFactory(new AbstractExpressionFactory<>(START_PTRN + "(" + ID_PTRN + ")\\s+(" + ICASE
                + "\\s+)?(" + NOT + "\\s+)?" + INCLUDES +
                "(?:\\s+(" + ANY + "|" + ALL + ")\\s+)?" + "\\s*((?:(?:" + STR_PTRN + ")(?:,\\s*)?)+)" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final boolean icase = matcher.group(2) != null;
                final boolean inverted = matcher.group(3) != null;
                final String anyAllPart = matcher.group(4);
                final String[] values = new SplitIgnoreQuotes().split(matcher.group(5), ',');
                final boolean internalDisjunctive = values.length == 1
                        || StringUtils.isNullOrEmpty(anyAllPart)
                        || "any".equalsIgnoreCase(anyAllPart);

                log.debug().append("WhereFilterFactory creating StringContainsFilter for expression: ")
                        .append(expression).endl();
                return new StringContainsFilter(
                        icase ? MatchFilter.CaseSensitivity.IgnoreCase : MatchFilter.CaseSensitivity.MatchCase,
                        inverted ? MatchFilter.MatchType.Inverted : MatchFilter.MatchType.Regular,
                        columnName,
                        internalDisjunctive,
                        true,
                        values);
            }
        });

        // Anything else is assumed to be a condition formula.
        parser.registerFactory(
                new AbstractExpressionFactory<>(START_PTRN + "(" + ANYTHING + ")" + END_PTRN) {
                    @Override
                    public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                        final String condition = matcher.group(1);

                        final FormulaParserConfiguration parserConfiguration = (FormulaParserConfiguration) args[0];

                        log.debug().append("WhereFilterFactory creating ConditionFilter for expression: ")
                                .append(expression).endl();
                        return ConditionFilter.createConditionFilter(condition, parserConfiguration);
                    }
                });
    }

    private static boolean isRowVariable(String columnName) {
        return columnName.equals("i") || columnName.equals("ii") || columnName.equals("k");
    }

    public static WhereFilter getExpression(String match) {
        Pair<FormulaParserConfiguration, String> parserAndExpression =
                FormulaParserConfiguration.extractParserAndExpression(match);
        return parser.parse(parserAndExpression.second, parserAndExpression.first);
    }

    public static WhereFilter[] getExpressions(String... expressions) {
        return Arrays.stream(expressions).map(WhereFilterFactory::getExpression).toArray(WhereFilter[]::new);
    }

    public static WhereFilter[] getExpressions(Collection<String> expressions) {
        return expressions.stream().map(WhereFilterFactory::getExpression).toArray(WhereFilter[]::new);
    }

    public static WhereFilter[] expandQuickFilter(Table t, String quickFilter, Set<String> columnNames) {
        return expandQuickFilter(t, quickFilter, QuickFilterMode.NORMAL, columnNames);
    }

    public static WhereFilter[] expandQuickFilter(Table t, String quickFilter, QuickFilterMode filterMode) {
        return expandQuickFilter(t, quickFilter, filterMode, Collections.emptySet());
    }

    public static WhereFilter[] expandQuickFilter(Table t, String quickFilter, QuickFilterMode filterMode,
            @NotNull Set<String> columnNames) {
        // Do some type inference
        if (quickFilter != null && !quickFilter.isEmpty()) {
            if (filterMode == QuickFilterMode.MULTI) {
                return expandMultiColumnQuickFilter(t, quickFilter);
            }

            return t.getColumnSourceMap().entrySet().stream()
                    .filter(entry -> !ColumnFormattingValues.isFormattingColumn(entry.getKey()) &&
                            !RollupInfo.ROLLUP_COLUMN.equals(entry.getKey()) &&
                            (columnNames.isEmpty() || columnNames.contains(entry.getKey())))
                    .map(entry -> {
                        final Class<?> colClass = entry.getValue().getType();
                        final String colName = entry.getKey();
                        if (filterMode == QuickFilterMode.REGEX) {
                            if (colClass.isAssignableFrom(String.class)) {
                                return new RegexFilter(MatchFilter.CaseSensitivity.IgnoreCase,
                                        MatchFilter.MatchType.Regular, colName, quickFilter);
                            }
                            return null;
                        } else if (filterMode == QuickFilterMode.AND) {
                            final String[] parts = quickFilter.split("\\s+");
                            final List<WhereFilter> filters =
                                    Arrays.stream(parts).map(part -> getSelectFilterForAnd(colName, part, colClass))
                                            .filter(Objects::nonNull).collect(Collectors.toList());
                            if (filters.isEmpty()) {
                                return null;
                            }
                            return ConjunctiveFilter.makeConjunctiveFilter(
                                    filters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
                        } else if (filterMode == QuickFilterMode.OR) {
                            final String[] parts = quickFilter.split("\\s+");
                            final List<WhereFilter> filters = Arrays.stream(parts)
                                    .map(part -> getSelectFilter(colName, part, filterMode, colClass))
                                    .filter(Objects::nonNull).collect(Collectors.toList());
                            if (filters.isEmpty()) {
                                return null;
                            }
                            return DisjunctiveFilter.makeDisjunctiveFilter(
                                    filters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
                        } else {
                            return getSelectFilter(colName, quickFilter, filterMode, colClass);
                        }

                    }).filter(Objects::nonNull).toArray(WhereFilter[]::new);
        }

        return WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY;
    }

    private static WhereFilter[] expandMultiColumnQuickFilter(Table t, String quickFilter) {
        final String[] parts = quickFilter.split("\\s+");
        final List<WhereFilter> filters = new ArrayList<>(parts.length);

        for (String part : parts) {
            final WhereFilter[] filterArray = t.getColumnSourceMap().entrySet().stream()
                    .filter(entry -> !ColumnFormattingValues.isFormattingColumn(entry.getKey())
                            && !RollupInfo.ROLLUP_COLUMN.equals(entry.getKey()))
                    .map(entry -> {
                        final Class<?> colClass = entry.getValue().getType();
                        final String colName = entry.getKey();
                        return getSelectFilter(colName, part, QuickFilterMode.MULTI, colClass);
                    }).filter(Objects::nonNull).toArray(WhereFilter[]::new);
            if (filterArray.length > 0) {
                filters.add(DisjunctiveFilter.makeDisjunctiveFilter(filterArray));
            }
        }

        return filters.toArray(WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
    }

    private static WhereFilter getSelectFilter(String colName, String quickFilter, QuickFilterMode filterMode,
            Class<?> colClass) {
        final InferenceResult typeData = new InferenceResult(quickFilter);
        if ((colClass == Double.class || colClass == double.class) && (!Double.isNaN(typeData.doubleVal))) {
            try {
                return DoubleRangeFilter.makeRange(colName, quickFilter);
            } catch (NumberFormatException ignored) {
                return new MatchFilter(colName, typeData.doubleVal);
            }
        } else if (colClass == Float.class || colClass == float.class && (!Float.isNaN(typeData.floatVal))) {
            try {
                return FloatRangeFilter.makeRange(colName, quickFilter);
            } catch (NumberFormatException ignored) {
                return new MatchFilter(colName, typeData.floatVal);
            }
        } else if ((colClass == Integer.class || colClass == int.class) && typeData.isInt) {
            return new MatchFilter(colName, typeData.intVal);
        } else if ((colClass == long.class || colClass == Long.class) && typeData.isLong) {
            return new MatchFilter(colName, typeData.longVal);
        } else if ((colClass == short.class || colClass == Short.class) && typeData.isShort) {
            return new MatchFilter(colName, typeData.shortVal);
        } else if ((colClass == byte.class || colClass == Byte.class) && typeData.isByte) {
            return new MatchFilter(colName, typeData.byteVal);
        } else if (colClass == BigInteger.class && typeData.isBigInt) {
            return new MatchFilter(colName, typeData.bigIntVal);
        } else if (colClass == BigDecimal.class && typeData.isBigDecimal) {
            return ComparableRangeFilter.makeBigDecimalRange(colName, quickFilter);
        } else if (filterMode != QuickFilterMode.NUMERIC) {
            if (colClass == String.class) {
                return new StringContainsFilter(MatchFilter.CaseSensitivity.IgnoreCase, MatchFilter.MatchType.Regular,
                        colName, quickFilter);
            } else if ((colClass == boolean.class || colClass == Boolean.class) && typeData.isBool) {
                return new MatchFilter(colName, Boolean.parseBoolean(quickFilter));
            } else if (colClass == DateTime.class && typeData.dateLower != null && typeData.dateUpper != null) {
                return new DateTimeRangeFilter(colName, typeData.dateLower, typeData.dateUpper, true, false);
            } else if ((colClass == char.class || colClass == Character.class) && typeData.isChar) {
                return new MatchFilter(colName, typeData.charVal);
            }
        }
        return null;
    }

    private static WhereFilter getSelectFilterForAnd(String colName, String quickFilter, Class<?> colClass) {
        // AND mode only supports String types
        if (colClass.isAssignableFrom(String.class)) {
            return new StringContainsFilter(MatchFilter.CaseSensitivity.IgnoreCase, MatchFilter.MatchType.Regular,
                    colName, quickFilter);
        }
        return null;
    }

    public static WhereFilter[] getExpressionsWithQuickFilter(String[] expressions, Table t, String quickFilter,
            QuickFilterMode filterMode) {
        if (quickFilter != null && !quickFilter.isEmpty()) {
            return Stream.concat(
                    Arrays.stream(getExpressions(expressions)),
                    Stream.of(filterMode == QuickFilterMode.MULTI
                            ? ConjunctiveFilter.makeConjunctiveFilter(
                                    WhereFilterFactory.expandQuickFilter(t, quickFilter, filterMode))
                            : DisjunctiveFilter.makeDisjunctiveFilter(
                                    WhereFilterFactory.expandQuickFilter(t, quickFilter, filterMode))))
                    .toArray(WhereFilter[]::new);
        }
        return getExpressions(expressions);
    }

    static class InferenceResult {
        boolean isChar;
        char charVal;
        boolean isBool;
        double doubleVal;
        float floatVal;
        boolean isInt;
        int intVal;
        boolean isByte;
        byte byteVal;
        boolean isShort;
        short shortVal;
        boolean isLong;
        long longVal;
        boolean isBigInt;
        BigInteger bigIntVal;
        boolean isBigDecimal;
        BigDecimal bigDecVal;

        DateTime dateUpper;
        DateTime dateLower;

        InferenceResult(String valString) {
            isBool = (valString.equalsIgnoreCase("false") || valString.equalsIgnoreCase("true"));

            if (valString.length() == 1) {
                charVal = valString.charAt(0);
                isChar = true;
            }

            try {
                intVal = Integer.parseInt(valString);
                isInt = true;
            } catch (NumberFormatException ignored) {
            }

            try {
                longVal = Long.parseLong(valString);
                isLong = true;
            } catch (NumberFormatException ignored) {
            }

            try {
                shortVal = Short.parseShort(valString);
                isShort = true;
            } catch (NumberFormatException ignored) {
            }

            try {
                bigIntVal = new BigInteger(valString);
                isBigInt = true;
            } catch (NumberFormatException ignored) {
            }

            try {
                byteVal = Byte.parseByte(valString);
                isByte = true;
            } catch (NumberFormatException ignored) {
            }

            doubleVal = Double.NaN;
            try {
                doubleVal = Double.parseDouble(valString);
            } catch (NumberFormatException ignored) {
            }

            floatVal = Float.NaN;
            try {
                floatVal = Float.parseFloat(valString);
            } catch (NumberFormatException ignored) {
            }

            try {
                bigDecVal = new BigDecimal(valString);
                isBigDecimal = true;
            } catch (NumberFormatException ignored) {
            }

            ZonedDateTime dateLower = null;
            ZonedDateTime dateUpper = null;
            try {
                // Was it a full date?
                dateLower = DateTimeUtils.getZonedDateTime(DateTimeUtils.convertDateTime(valString));
            } catch (RuntimeException ignored) {
                try {
                    // Maybe it was just a TOD?
                    long time = DateTimeUtils.convertTime(valString);
                    dateLower =
                            DateTimeUtils.getZonedDateTime(DateTime.nowMillis()).truncatedTo(ChronoUnit.DAYS).plus(time,
                                    ChronoUnit.NANOS);
                } catch (RuntimeException ignored1) {
                }
            }

            if (dateLower != null) {
                final ChronoField finestUnit = DateTimeUtils.getFinestDefinedUnit(valString);
                dateUpper = finestUnit == null ? dateLower : dateLower.plus(1, finestUnit.getBaseUnit());
            }

            this.dateUpper =
                    dateUpper == null ? null : DateTimeUtils.millisToTime(dateUpper.toInstant().toEpochMilli());
            this.dateLower =
                    dateLower == null ? null : DateTimeUtils.millisToTime(dateLower.toInstant().toEpochMilli());
        }
    }
}
