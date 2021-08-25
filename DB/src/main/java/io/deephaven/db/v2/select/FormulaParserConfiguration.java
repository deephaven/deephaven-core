package io.deephaven.db.v2.select;

import io.deephaven.base.Pair;
import io.deephaven.configuration.Configuration;

public enum FormulaParserConfiguration {
    Deephaven(), Numba();

    public static final String OBFUSCATED_PARSER_ANNOTATION = "<ObfuscatedParserAnnotation>";

    public static FormulaParserConfiguration parser = FormulaParserConfiguration.valueOf(
        Configuration.getInstance().getStringWithDefault("default.parser", Deephaven.name()));

    public static void setParser(FormulaParserConfiguration parser) {
        FormulaParserConfiguration.parser = parser;
    }

    public static Pair<FormulaParserConfiguration, String> extractParserAndExpression(
        String expression) {
        if (expression.startsWith(FormulaParserConfiguration.OBFUSCATED_PARSER_ANNOTATION)) {
            expression = expression.substring(OBFUSCATED_PARSER_ANNOTATION.length());
            int endOfTag = expression.indexOf(':');
            FormulaParserConfiguration parserConfig =
                FormulaParserConfiguration.valueOf(expression.substring(0, endOfTag));
            return new Pair<>(parserConfig, expression.substring(endOfTag + 1));
        }

        return new Pair<>(parser, expression);
    }

    public static String nb(String expression) {
        return new StringBuilder().append(OBFUSCATED_PARSER_ANNOTATION).append(Numba.name())
            .append(":").append(expression).toString();
    }

    public static String dh(String expression) {
        return new StringBuilder().append(OBFUSCATED_PARSER_ANNOTATION).append(Deephaven.name())
            .append(":").append(expression).toString();
    }

}
