//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.Pair;
import io.deephaven.api.expression.AbstractExpressionFactory;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.engine.util.ColumnFormatting;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.api.expression.SelectFactoryConstants.*;

/**
 * A factory to create {@link SelectColumn}s from an input expression.
 */
public class SelectColumnFactory {

    private static final ExpressionParser<SelectColumn> parser = new ExpressionParser<>();

    static {

        // If you add more logic here, please kindly update
        // io.deephaven.web.shared.data.CustomColumnDescriptor#extractColumnName

        /*
         * SwitchColumn will explicitly check if <expression> is a column in the source table first, and use
         * FormulaColumn#createFormulaColumn(String, String, FormulaParserConfiguration) where appropriate.
         */
        // <ColumnName>=<expression>
        parser.registerFactory(new AbstractExpressionFactory<>(
                START_PTRN + "(" + ID_PTRN + ")\\s*=\\s*(" + ANYTHING + ")" + END_PTRN) {
            @Override
            public SelectColumn getExpression(String expression, Matcher matcher, Object... args) {
                return new SwitchColumn(matcher.group(1), matcher.group(2), (FormulaParserConfiguration) args[0]);
            }
        });

        // <ColumnName>
        parser.registerFactory(
                new AbstractExpressionFactory<>(START_PTRN + "(" + ID_PTRN + ")" + END_PTRN) {
                    @Override
                    public SelectColumn getExpression(String expression, Matcher matcher, Object... args) {
                        return new SourceColumn(matcher.group(1));
                    }
                });

        // If you add more logic here, please kindly update
        // io.deephaven.web.shared.data.CustomColumnDescriptor#extractColumnName
    }

    public static SelectColumn getExpression(String expression) {
        Pair<FormulaParserConfiguration, String> parserAndExpression =
                FormulaParserConfiguration.extractParserAndExpression(expression);
        return parser.parse(parserAndExpression.second, parserAndExpression.first);
    }

    public static SelectColumn[] getExpressions(String... expressions) {
        return Arrays.stream(expressions).map(SelectColumnFactory::getExpression).toArray(SelectColumn[]::new);
    }

    public static SelectColumn[] getExpressions(Collection<String> expressions) {
        return expressions.stream().map(SelectColumnFactory::getExpression).toArray(SelectColumn[]::new);
    }

    private static final Pattern formatPattern =
            Pattern.compile(START_PTRN + "(" + ID_PTRN + "|\\*)\\s*=\\s*(.*\\S+)" + END_PTRN);
    private static final Pattern coloringPattern = Pattern.compile(START_PTRN + "Color\\((.*\\S+)\\)" + END_PTRN);
    private static final Pattern numberFormatPattern = Pattern.compile(START_PTRN + "Decimal\\((.*\\S+)\\)" + END_PTRN);
    private static final Pattern dateFormatPattern = Pattern.compile(START_PTRN + "Date\\((.*\\S+)\\)" + END_PTRN);


    @SuppressWarnings("WeakerAccess")
    public static SelectColumn getFormatExpression(String expression) {
        final Matcher topMatcher = formatPattern.matcher(expression);

        if (!topMatcher.matches()) {
            throw new IllegalArgumentException("Illegal format specification: " + expression);
        }

        final Matcher colorMatcher = coloringPattern.matcher(topMatcher.group(2));
        final Matcher numberMatcher = numberFormatPattern.matcher(topMatcher.group(2));
        final Matcher dateMatcher = dateFormatPattern.matcher(topMatcher.group(2));

        final String columnName = topMatcher.group(1);
        if (numberMatcher.matches()) {
            return FormulaColumn.createFormulaColumn(ColumnFormatting.getNumberFormatColumn(columnName),
                    numberMatcher.group(1),
                    FormulaParserConfiguration.Deephaven);
        } else if (dateMatcher.matches()) {
            return FormulaColumn.createFormulaColumn(ColumnFormatting.getDateFormatColumn(columnName),
                    dateMatcher.group(1), FormulaParserConfiguration.Deephaven);
        } else {
            return FormulaColumn.createFormulaColumn(ColumnFormatting.getStyleFormatColumn(columnName),
                    "io.deephaven.engine.util.ColorUtil.toLong("
                            + (colorMatcher.matches() ? colorMatcher.group(1) : topMatcher.group(2)) + ")",
                    FormulaParserConfiguration.Deephaven);
        }
    }

    public static DhFormulaColumn[] getFormatExpressions(String... expressions) {
        return Arrays.stream(expressions).map(SelectColumnFactory::getFormatExpression).toArray(DhFormulaColumn[]::new);
    }
}
