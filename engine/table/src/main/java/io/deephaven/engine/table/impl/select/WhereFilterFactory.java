//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterPattern.Mode;
import io.deephaven.base.Pair;
import io.deephaven.api.expression.AbstractExpressionFactory;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.MatchFilter.CaseSensitivity;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.gui.table.QuickFilterMode;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.CachingSupplier;
import io.deephaven.util.text.SplitIgnoreQuotes;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        // Each side may fit: (<ColumnName>|<QueryScopeParam>|<Number|Boolean|"String"|Literal>)
        // Supported ops: ==, =, !=, <, <=, >, >=
        parser.registerFactory(new AbstractExpressionFactory<>(
                START_PTRN + "(?:(" + ID_PTRN + ")|(" + LITERAL_PTRN + "))\\s*((?:=|!|<|>)=?)\\s*(?:(" + ID_PTRN + ")|("
                        + LITERAL_PTRN + "))" + END_PTRN) {
            @Override
            public WhereFilter getExpression(String expression, Matcher matcher, Object... args) {
                // LITERAL_PTRN has 5 groups; mostly non-capturing
                final boolean leftIsId = matcher.group(1) != null;
                final boolean rightIsId = matcher.group(8) != null;

                if (!leftIsId && !rightIsId) {
                    return ConditionFilter.createConditionFilter(expression, (FormulaParserConfiguration) args[0]);
                }
                final boolean mirrored = !leftIsId;

                final String columnName = leftIsId ? matcher.group(1) : matcher.group(8);
                final String op = matcher.group(7);
                final String value = leftIsId ? rightIsId ? matcher.group(8) : matcher.group(9) : matcher.group(2);

                return getWhereFilterOneSideColumn(
                        expression, (FormulaParserConfiguration) args[0], columnName, op, value, mirrored);
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
                log.debug()
                        .append("WhereFilterFactory creating stringContainsFilter for expression: ")
                        .append(expression)
                        .endl();
                return stringContainsFilter(
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

    private static @NotNull WhereFilter getWhereFilterOneSideColumn(
            final String expression,
            final FormulaParserConfiguration parserConfiguration,
            final String columnName,
            String op,
            final String value,
            boolean mirrored) {

        if (isRowVariable(columnName)) {
            log.debug().append("WhereFilterFactory creating ConditionFilter for expression: ")
                    .append(expression).endl();
            return ConditionFilter.createConditionFilter(expression, parserConfiguration);
        }

        boolean inverted = false;
        switch (op) {
            case "!=":
                inverted = true;
            case "=":
            case "==":
                log.debug().append("WhereFilterFactory creating MatchFilter for expression: ").append(expression)
                        .endl();
                return new MatchFilter(
                        new CachingSupplier<>(() -> (ConditionFilter) ConditionFilter.createConditionFilter(expression,
                                parserConfiguration)),
                        CaseSensitivity.MatchCase,
                        inverted ? MatchType.Inverted : MatchType.Regular,
                        columnName,
                        value);

            case "<":
            case ">":
            case "<=":
            case ">=":
                if (mirrored) {
                    switch (op) {
                        case "<":
                            op = ">";
                            break;
                        case "<=":
                            op = ">=";
                            break;
                        case ">":
                            op = "<";
                            break;
                        case ">=":
                            op = "<=";
                            break;
                        default:
                            throw new IllegalStateException("Unexpected operator: " + op);
                    }
                }
                log.debug().append("WhereFilterFactory creating RangeFilter for expression: ")
                        .append(expression).endl();
                return new RangeFilter(columnName, op, value, expression, parserConfiguration);

            default:
                throw new IllegalStateException("Unexpected operator: " + op);
        }
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

    public static WhereFilter[] expandQuickFilter(
            @NotNull final TableDefinition tableDefinition,
            final String quickFilter,
            @NotNull final Set<String> columnNames) {
        return expandQuickFilter(tableDefinition, quickFilter, QuickFilterMode.NORMAL, columnNames);
    }

    public static WhereFilter[] expandQuickFilter(
            @NotNull final TableDefinition tableDefinition,
            final String quickFilter,
            final QuickFilterMode filterMode) {
        return expandQuickFilter(tableDefinition, quickFilter, filterMode, Collections.emptySet());
    }

    public static WhereFilter[] expandQuickFilter(
            @NotNull final TableDefinition tableDefinition,
            final String quickFilter,
            final QuickFilterMode filterMode,
            @NotNull final Set<String> columnNames) {
        // Do some type inference
        if (quickFilter != null && !quickFilter.isEmpty()) {
            if (filterMode == QuickFilterMode.MULTI) {
                return expandMultiColumnQuickFilter(tableDefinition, quickFilter);
            }

            return tableDefinition.getColumnStream()
                    .filter(cd -> !ColumnFormatting.isFormattingColumn(cd.getName()) &&
                            (columnNames.isEmpty() || columnNames.contains(cd.getName())))
                    .map(cd -> {
                        final Class<?> colClass = cd.getDataType();
                        final String colName = cd.getName();
                        if (filterMode == QuickFilterMode.REGEX) {
                            if (colClass.isAssignableFrom(String.class)) {
                                return WhereFilterAdapter.of(FilterPattern.of(
                                        ColumnName.of(colName),
                                        Pattern.compile(quickFilter, Pattern.CASE_INSENSITIVE | Pattern.DOTALL),
                                        Mode.MATCHES,
                                        false), false);
                            }
                            return null;
                        } else if (filterMode == QuickFilterMode.AND) {
                            final String[] parts = quickFilter.split("\\s+");
                            final List<WhereFilter> filters = Arrays.stream(parts)
                                    .map(part -> getSelectFilterForAnd(colName, part, colClass))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                            if (filters.isEmpty()) {
                                return null;
                            }
                            return ConjunctiveFilter.makeConjunctiveFilter(
                                    filters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
                        } else if (filterMode == QuickFilterMode.OR) {
                            final String[] parts = quickFilter.split("\\s+");
                            final List<WhereFilter> filters = Arrays.stream(parts)
                                    .map(part -> createQuickFilter(cd, part, filterMode))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                            if (filters.isEmpty()) {
                                return null;
                            }
                            return DisjunctiveFilter.makeDisjunctiveFilter(
                                    filters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY));
                        } else {
                            return createQuickFilter(cd, quickFilter, filterMode);
                        }

                    }).filter(Objects::nonNull).toArray(WhereFilter[]::new);
        }

        return WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY;
    }

    private static WhereFilter[] expandMultiColumnQuickFilter(TableDefinition tableDefinition, String quickFilter) {
        final String[] parts = quickFilter.split("\\s+");
        final List<WhereFilter> filters = new ArrayList<>(parts.length);

        for (String part : parts) {
            final WhereFilter[] filterArray = tableDefinition.getColumnStream()
                    .filter(cd -> !ColumnFormatting.isFormattingColumn(cd.getName()))
                    .map(cd -> createQuickFilter(cd, part, QuickFilterMode.MULTI))
                    .filter(Objects::nonNull)
                    .toArray(WhereFilter[]::new);
            if (filterArray.length > 0) {
                filters.add(DisjunctiveFilter.makeDisjunctiveFilter(filterArray));
            }
        }

        return filters.toArray(WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);
    }

    private static WhereFilter createQuickFilter(ColumnDefinition<?> colDef, String quickFilter,
            QuickFilterMode filterMode) {
        final String colName = colDef.getName();
        final Class<?> colClass = colDef.getDataType();
        final InferenceResult typeData = new InferenceResult(quickFilter);
        if ((colClass == Double.class || colClass == double.class) && (!Double.isNaN(typeData.doubleVal))) {
            try {
                return DoubleRangeFilter.makeRange(colName, quickFilter);
            } catch (NumberFormatException ignored) {
                return new MatchFilter(MatchType.Regular, colName, typeData.doubleVal);
            }
        } else if (colClass == Float.class || colClass == float.class && (!Float.isNaN(typeData.floatVal))) {
            try {
                return FloatRangeFilter.makeRange(colName, quickFilter);
            } catch (NumberFormatException ignored) {
                return new MatchFilter(MatchType.Regular, colName, typeData.floatVal);
            }
        } else if ((colClass == Integer.class || colClass == int.class) && typeData.isInt) {
            return new MatchFilter(MatchType.Regular, colName, typeData.intVal);
        } else if ((colClass == long.class || colClass == Long.class) && typeData.isLong) {
            return new MatchFilter(MatchType.Regular, colName, typeData.longVal);
        } else if ((colClass == short.class || colClass == Short.class) && typeData.isShort) {
            return new MatchFilter(MatchType.Regular, colName, typeData.shortVal);
        } else if ((colClass == byte.class || colClass == Byte.class) && typeData.isByte) {
            return new MatchFilter(MatchType.Regular, colName, typeData.byteVal);
        } else if (colClass == BigInteger.class && typeData.isBigInt) {
            return new MatchFilter(MatchType.Regular, colName, typeData.bigIntVal);
        } else if (colClass == BigDecimal.class && typeData.isBigDecimal) {
            return ComparableRangeFilter.makeBigDecimalRange(colName, quickFilter);
        } else if (filterMode != QuickFilterMode.NUMERIC) {
            if (colClass == String.class) {
                return WhereFilterAdapter.of(FilterPattern.of(
                        ColumnName.of(colName),
                        Pattern.compile(Pattern.quote(quickFilter), Pattern.CASE_INSENSITIVE),
                        Mode.FIND,
                        false), false);
            } else if ((colClass == boolean.class || colClass == Boolean.class) && typeData.isBool) {
                return new MatchFilter(MatchType.Regular, colName, Boolean.parseBoolean(quickFilter));
            } else if (colClass == Instant.class && typeData.dateLower != null && typeData.dateUpper != null) {
                return new InstantRangeFilter(colName, typeData.dateLower, typeData.dateUpper, true, false);
            } else if ((colClass == char.class || colClass == Character.class) && typeData.isChar) {
                return new MatchFilter(MatchType.Regular, colName, typeData.charVal);
            }
        }
        return null;
    }

    private static WhereFilter getSelectFilterForAnd(String colName, String quickFilter, Class<?> colClass) {
        // AND mode only supports String types
        if (colClass.isAssignableFrom(String.class)) {
            return WhereFilterAdapter.of(FilterPattern.of(
                    ColumnName.of(colName),
                    Pattern.compile(Pattern.quote(quickFilter), Pattern.CASE_INSENSITIVE),
                    Mode.FIND,
                    false), false);
        }
        return null;
    }

    public static WhereFilter[] getExpressionsWithQuickFilter(
            @NotNull final String[] expressions,
            @NotNull final TableDefinition tableDefinition,
            final String quickFilter,
            final QuickFilterMode filterMode) {
        if (quickFilter != null && !quickFilter.isEmpty()) {
            return Stream.concat(
                    Arrays.stream(getExpressions(expressions)),
                    Stream.of(filterMode == QuickFilterMode.MULTI
                            ? ConjunctiveFilter.makeConjunctiveFilter(
                                    WhereFilterFactory.expandQuickFilter(tableDefinition, quickFilter, filterMode))
                            : DisjunctiveFilter.makeDisjunctiveFilter(
                                    WhereFilterFactory.expandQuickFilter(tableDefinition, quickFilter, filterMode))))
                    .toArray(WhereFilter[]::new);
        }
        return getExpressions(expressions);
    }

    @VisibleForTesting
    public static WhereFilter stringContainsFilter(
            CaseSensitivity sensitivity,
            MatchType matchType,
            @NotNull String columnName,
            boolean internalDisjunctive,
            boolean removeQuotes,
            String... values) {
        final String value =
                constructStringContainsRegex(values, matchType, internalDisjunctive, removeQuotes);
        return WhereFilterAdapter.of(FilterPattern.of(
                ColumnName.of(columnName),
                Pattern.compile(value, sensitivity == CaseSensitivity.IgnoreCase ? Pattern.CASE_INSENSITIVE : 0),
                Mode.FIND,
                matchType == MatchType.Inverted), false);
    }

    private static String constructStringContainsRegex(
            String[] values,
            MatchType matchType,
            boolean internalDisjunctive,
            boolean removeQuotes) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException(
                    "constructStringContainsRegex must be called with at least one value parameter");
        }
        final MatchFilter.ColumnTypeConvertor converter = removeQuotes
                ? MatchFilter.ColumnTypeConvertorFactory.getConvertor(String.class)
                : null;
        final String regex;
        final Stream<String> valueStream = Arrays.stream(values)
                .map(val -> {
                    if (StringUtils.isNullOrEmpty(val)) {
                        throw new IllegalArgumentException(
                                "Parameters to constructStringContainsRegex must not be null or empty");
                    }
                    return Pattern.quote(converter == null ? val : converter.convertStringLiteral(val).toString());
                });
        // If the match is simple, includes -any- or includes -none- we can just use a simple
        // regex of or'd values
        if ((matchType == MatchType.Regular && internalDisjunctive) ||
                (matchType == MatchType.Inverted && !internalDisjunctive)) {
            regex = valueStream.collect(Collectors.joining("|"));
        } else {
            // If we need to match -all of- or -not one of- then we must use forward matching
            regex = valueStream.map(item -> "(?=.*" + item + ")")
                    .collect(Collectors.joining()) + ".*";
        }
        return regex;
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

        Instant dateUpper;
        Instant dateLower;

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
                dateLower = DateTimeUtils.toZonedDateTime(
                        DateTimeUtils.parseInstant(valString), DateTimeUtils.timeZone());
            } catch (RuntimeException ignored) {
                try {
                    // Maybe it was just a TOD?
                    long time = DateTimeUtils.parseDurationNanos(valString);
                    dateLower = DateTimeUtils.toZonedDateTime(DateTimeUtils.now(), DateTimeUtils.timeZone())
                            .truncatedTo(ChronoUnit.DAYS).plus(time, ChronoUnit.NANOS);
                } catch (RuntimeException ignored1) {
                }
            }

            if (dateLower != null) {
                final ChronoField finestUnit = DateTimeUtils.parseTimePrecisionQuiet(valString);
                dateUpper = finestUnit == null ? dateLower : dateLower.plus(1, finestUnit.getBaseUnit());
            }

            this.dateUpper = dateUpper == null ? null : dateUpper.toInstant();
            this.dateLower = dateLower == null ? null : dateLower.toInstant();
        }
    }
}
