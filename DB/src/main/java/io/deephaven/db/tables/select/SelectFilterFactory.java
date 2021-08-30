/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.base.Pair;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.AbstractExpressionFactory;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.ExpressionParser;
import io.deephaven.db.util.ColumnFormattingValues;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.select.*;
import io.deephaven.gui.table.QuickFilterMode;
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

import static io.deephaven.db.tables.select.SelectFactoryConstants.*;

/**
 * Given a user's filter string produce an appropriate SelectFilter instance.
 */
public class SelectFilterFactory {

    private static final Logger log = ProcessEnvironment.getDefaultLog(SelectFilterFactory.class);

    private static final ExpressionParser<SelectFilter> parser = new ExpressionParser<>();

    static {
        // <ColumnName>==<Number|Boolean|"String">
        parser.registerFactory(new AbstractExpressionFactory<SelectFilter>(
            START_PTRN + "(" + ID_PTRN + ")\\s*={1,2}\\s*(" + LITERAL_PTRN + ")" + END_PTRN) {
            @Override
            public SelectFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final FormulaParserConfiguration parserConfiguration =
                    (FormulaParserConfiguration) args[0];
                if (isRowVariable(columnName)) {
                    log.debug()
                        .append("SelectFilterFactory creating ConditionFilter for expression: ")
                        .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                log.debug().append("SelectFilterFactory creating MatchFilter for expression: ")
                    .append(expression).endl();
                return new MatchFilter(MatchFilter.CaseSensitivity.MatchCase, columnName,
                    matcher.group(2));
            }
        });
        // <ColumnName>==<User Param>
        parser.registerFactory(new AbstractExpressionFactory<SelectFilter>(
            START_PTRN + "(" + ID_PTRN + ")\\s*={1,2}\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public SelectFilter getExpression(String expression, Matcher matcher, Object... args) {
                final String columnName = matcher.group(1);
                final FormulaParserConfiguration parserConfiguration =
                    (FormulaParserConfiguration) args[0];

                if (isRowVariable(columnName)) {
                    log.debug()
                        .append("SelectFilterFactory creating ConditionFilter for expression: ")
                        .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                try {
                    QueryScope.getParamValue(matcher.group(2));
                } catch (QueryScope.MissingVariableException e) {
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                log.debug().append("SelectFilterFactory creating MatchFilter for expression: ")
                    .append(expression).endl();
                return new MatchFilter(MatchFilter.CaseSensitivity.MatchCase, columnName,
                    matcher.group(2));
            }
        });

        // <ColumnName> < <Number|Boolean|"String">
        // <ColumnName> <= <Number|Boolean|"String">
        // <ColumnName> > <Number|Boolean|"String">
        // <ColumnName> >= <Number|Boolean|"String">
        parser.registerFactory(new AbstractExpressionFactory<SelectFilter>(
            START_PTRN + "(" + ID_PTRN + ")\\s*([<>]=?)\\s*(" + LITERAL_PTRN + ")" + END_PTRN) {
            @Override
            public SelectFilter getExpression(String expression, Matcher matcher, Object... args) {
                final FormulaParserConfiguration parserConfiguration =
                    (FormulaParserConfiguration) args[0];
                final String columnName = matcher.group(1);
                final String conditionString = matcher.group(2);
                final String value = matcher.group(3);
                if (isRowVariable(columnName)) {
                    log.debug()
                        .append("SelectFilterFactory creating ConditionFilter for expression: ")
                        .append(expression).endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
                try {
                    log.debug()
                        .append(
                            "SelectFilterFactory creating RangeConditionFilter for expression: ")
                        .append(expression).endl();
                    return new RangeConditionFilter(columnName, conditionString, value, expression,
                        parserConfiguration);
                } catch (Exception e) {
                    log.warn()
                        .append("SelectFilterFactory could not make RangeFilter for expression: ")
                        .append(expression).append(" due to ").append(e)
                        .append(" Creating ConditionFilter instead.").endl();
                    return ConditionFilter.createConditionFilter(expression, parserConfiguration);
                }
            }
        });

        // <ColumnName> [icase] [not] in <value 1>, <value 2>, ... , <value n>
        parser.registerFactory(
            new AbstractExpressionFactory<SelectFilter>("(?s)" + START_PTRN + "(" + ID_PTRN
                + ")\\s+(" + ICASE + "\\s+)?(" + NOT + "\\s+)?" + IN + "\\s+(.+?)" + END_PTRN) {
                @Override
                public SelectFilter getExpression(String expression, Matcher matcher,
                    Object... args) {
                    final SplitIgnoreQuotes splitter = new SplitIgnoreQuotes();
                    log.debug().append("SelectFilterFactory creating MatchFilter for expression: ")
                        .append(expression).endl();
                    return new MatchFilter(
                        matcher.group(2) == null ? MatchFilter.CaseSensitivity.MatchCase
                            : MatchFilter.CaseSensitivity.IgnoreCase,
                        matcher.group(3) == null ? MatchFilter.MatchType.Regular
                            : MatchFilter.MatchType.Inverted,
                        matcher.group(1), splitter.split(matcher.group(4), ','));
                }
            });

        // <ColumnName> [icase] [not] includes [any|all]<"String">
        parser.registerFactory(new AbstractExpressionFactory<SelectFilter>(
            START_PTRN + "(" + ID_PTRN + ")\\s+(" + ICASE + "\\s+)?(" + NOT + "\\s+)?" + INCLUDES +
                "(?:\\s+(" + ANY + "|" + ALL + ")\\s+)?" + "\\s*((?:(?:" + STR_PTRN
                + ")(?:,\\s*)?)+)" + END_PTRN) {
            @Override
            public SelectFilter getExpression(String expression, Matcher matcher, Object... args) {
                final SplitIgnoreQuotes splitter = new SplitIgnoreQuotes();
                log.debug()
                    .append("SelectFilterFactory creating StringContainsFilter for expression: ")
                    .append(expression).endl();
                final String[] values = splitter.split(matcher.group(5), ',');
                final String anyAllPart = matcher.group(4);
                return new StringContainsFilter(
                    matcher.group(2) == null ? MatchFilter.CaseSensitivity.MatchCase
                        : MatchFilter.CaseSensitivity.IgnoreCase,
                    matcher.group(3) == null ? MatchFilter.MatchType.Regular
                        : MatchFilter.MatchType.Inverted,
                    matcher.group(1),
                    values.length == 1 ||
                        StringUtils.isNullOrEmpty(anyAllPart) || "any".equalsIgnoreCase(anyAllPart),
                    true, values);
            }
        });

        // Anything else is assumed to be a condition formula.
        parser.registerFactory(new AbstractExpressionFactory<SelectFilter>(
            START_PTRN + "(" + ANYTHING + ")" + END_PTRN) {
            @Override
            public SelectFilter getExpression(String expression, Matcher matcher, Object... args) {
                final FormulaParserConfiguration parserConfiguration =
                    (FormulaParserConfiguration) args[0];

                log.debug().append("SelectFilterFactory creating ConditionFilter for expression: ")
                    .append(expression).endl();
                return ConditionFilter.createConditionFilter(matcher.group(1), parserConfiguration);
            }
        });
    }

    private static boolean isRowVariable(String columnName) {
        return columnName.equals("i") || columnName.equals("ii") || columnName.equals("k");
    }

    public static SelectFilter getExpression(String match) {
        Pair<FormulaParserConfiguration, String> parserAndExpression =
            FormulaParserConfiguration.extractParserAndExpression(match);
        return parser.parse(parserAndExpression.second, parserAndExpression.first);
    }

    public static SelectFilter[] getExpressions(String... expressions) {
        return Arrays.stream(expressions).map(SelectFilterFactory::getExpression)
            .toArray(SelectFilter[]::new);
    }

    public static SelectFilter[] getExpressions(Collection<String> expressions) {
        return expressions.stream().map(SelectFilterFactory::getExpression)
            .toArray(SelectFilter[]::new);
    }

    public static SelectFilter[] expandQuickFilter(Table t, String quickFilter,
        Set<String> columnNames) {
        return expandQuickFilter(t, quickFilter, QuickFilterMode.NORMAL, columnNames);
    }

    public static SelectFilter[] expandQuickFilter(Table t, String quickFilter,
        QuickFilterMode filterMode) {
        return expandQuickFilter(t, quickFilter, filterMode, Collections.emptySet());
    }

    public static SelectFilter[] expandQuickFilter(Table t, String quickFilter,
        QuickFilterMode filterMode, @NotNull Set<String> columnNames) {
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
                        final List<SelectFilter> filters = Arrays.stream(parts)
                            .map(part -> getSelectFilterForAnd(colName, part, colClass))
                            .filter(Objects::nonNull).collect(Collectors.toList());
                        if (filters.isEmpty()) {
                            return null;
                        }
                        return ConjunctiveFilter.makeConjunctiveFilter(
                            filters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
                    } else if (filterMode == QuickFilterMode.OR) {
                        final String[] parts = quickFilter.split("\\s+");
                        final List<SelectFilter> filters = Arrays.stream(parts)
                            .map(part -> getSelectFilter(colName, part, filterMode, colClass))
                            .filter(Objects::nonNull).collect(Collectors.toList());
                        if (filters.isEmpty()) {
                            return null;
                        }
                        return DisjunctiveFilter.makeDisjunctiveFilter(
                            filters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY));
                    } else {
                        return getSelectFilter(colName, quickFilter, filterMode, colClass);
                    }

                }).filter(Objects::nonNull).toArray(SelectFilter[]::new);
        }

        return SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY;
    }

    private static SelectFilter[] expandMultiColumnQuickFilter(Table t, String quickFilter) {
        final String[] parts = quickFilter.split("\\s+");
        final List<SelectFilter> filters = new ArrayList<>(parts.length);

        for (String part : parts) {
            final SelectFilter[] filterArray = t.getColumnSourceMap().entrySet().stream()
                .filter(entry -> !ColumnFormattingValues.isFormattingColumn(entry.getKey())
                    && !RollupInfo.ROLLUP_COLUMN.equals(entry.getKey()))
                .map(entry -> {
                    final Class<?> colClass = entry.getValue().getType();
                    final String colName = entry.getKey();
                    return getSelectFilter(colName, part, QuickFilterMode.MULTI, colClass);
                }).filter(Objects::nonNull).toArray(SelectFilter[]::new);
            if (filterArray.length > 0) {
                filters.add(DisjunctiveFilter.makeDisjunctiveFilter(filterArray));
            }
        }

        return filters.toArray(SelectFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY);
    }

    private static SelectFilter getSelectFilter(String colName, String quickFilter,
        QuickFilterMode filterMode, Class<?> colClass) {
        final InferenceResult typeData = new InferenceResult(quickFilter);
        if ((colClass == Double.class || colClass == double.class)
            && (!Double.isNaN(typeData.doubleVal))) {
            try {
                return DoubleRangeFilter.makeRange(colName, quickFilter);
            } catch (NumberFormatException ignored) {
                return new MatchFilter(colName, typeData.doubleVal);
            }
        } else if (colClass == Float.class
            || colClass == float.class && (!Float.isNaN(typeData.floatVal))) {
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
                return new StringContainsFilter(MatchFilter.CaseSensitivity.IgnoreCase,
                    MatchFilter.MatchType.Regular, colName, quickFilter);
            } else if ((colClass == boolean.class || colClass == Boolean.class)
                && typeData.isBool) {
                return new MatchFilter(colName, Boolean.parseBoolean(quickFilter));
            } else if (colClass == DBDateTime.class && typeData.dateLower != null
                && typeData.dateUpper != null) {
                return new DateTimeRangeFilter(colName, typeData.dateLower, typeData.dateUpper,
                    true, false);
            } else if ((colClass == char.class || colClass == Character.class) && typeData.isChar) {
                return new MatchFilter(colName, typeData.charVal);
            }
        }
        return null;
    }

    private static SelectFilter getSelectFilterForAnd(String colName, String quickFilter,
        Class<?> colClass) {
        // AND mode only supports String types
        if (colClass.isAssignableFrom(String.class)) {
            return new StringContainsFilter(MatchFilter.CaseSensitivity.IgnoreCase,
                MatchFilter.MatchType.Regular, colName, quickFilter);
        }
        return null;
    }

    public static SelectFilter[] getExpressionsWithQuickFilter(String[] expressions, Table t,
        String quickFilter, QuickFilterMode filterMode) {
        if (quickFilter != null && !quickFilter.isEmpty()) {
            return Stream.concat(
                Arrays.stream(getExpressions(expressions)),
                Stream.of(filterMode == QuickFilterMode.MULTI
                    ? ConjunctiveFilter.makeConjunctiveFilter(
                        SelectFilterFactory.expandQuickFilter(t, quickFilter, filterMode))
                    : DisjunctiveFilter.makeDisjunctiveFilter(
                        SelectFilterFactory.expandQuickFilter(t, quickFilter, filterMode))))
                .toArray(SelectFilter[]::new);
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

        DBDateTime dateUpper;
        DBDateTime dateLower;

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
                dateLower = DBTimeUtils.getZonedDateTime(DBTimeUtils.convertDateTime(valString));
            } catch (RuntimeException ignored) {
                try {
                    // Maybe it was just a TOD?
                    long time = DBTimeUtils.convertTime(valString);
                    dateLower = DBTimeUtils.getZonedDateTime(DBDateTime.now())
                        .truncatedTo(ChronoUnit.DAYS).plus(time, ChronoUnit.NANOS);
                } catch (RuntimeException stillIgnored) {

                }
            }

            if (dateLower != null) {
                final ChronoField finestUnit = DBTimeUtils.getFinestDefinedUnit(valString);
                dateUpper =
                    finestUnit == null ? dateLower : dateLower.plus(1, finestUnit.getBaseUnit());
            }

            this.dateUpper = dateUpper == null ? null
                : DBTimeUtils.millisToTime(dateUpper.toInstant().toEpochMilli());
            this.dateLower = dateLower == null ? null
                : DBTimeUtils.millisToTime(dateLower.toInstant().toEpochMilli());
        }
    }
}
