//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;

import java.util.Properties;
import java.util.Set;

/**
 * By default, calcite operates with the equivalent of {@link Lex#ORACLE} (via the defaults exposed through
 * {@link SqlParser#config()}, and the default value for {@link CalciteConnectionProperty#LEX}). This has the
 * side-effect of tokenizing unquoted identifiers into {@link Casing#TO_UPPER upper-case}, which is not what one might
 * expect by default. Additionally, there is no {@link Lex} that has all of the defaults we want ({@link Lex#JAVA} comes
 * close, but uses {@link Quoting#BACK_TICK} instead of {@link Quoting#DOUBLE_QUOTE}). We can be explicit with
 * {@link Config#withQuoting(Quoting)}, {@link Config#withUnquotedCasing(Casing)},
 * {@link Config#withQuotedCasing(Casing)}, {@link Config#withCaseSensitive(boolean)},
 * {@link Config#withCharLiteralStyles(Iterable)}, {@link CalciteConnectionProperty#QUOTING},
 * {@link CalciteConnectionProperty#UNQUOTED_CASING}, {@link CalciteConnectionProperty#QUOTED_CASING}, and
 * {@link CalciteConnectionProperty#CASE_SENSITIVE}.
 *
 * <p>
 * If a new {@link Lex} that has our defaults is introduced, or an interface is added, we'd be able to simplify how
 * these configurations get built via {@link Config#withLex(Lex)} and {@link CalciteConnectionProperty#LEX}.
 *
 * @see #withLexConfig(Config)
 * @see #setLexProperties(Properties)
 */
class LexConfig {

    // No such Lex exists that has our desired defaults.
    // private static final Lex LEX = null;
    private static final Quoting QUOTING = Quoting.DOUBLE_QUOTE;
    private static final Casing UNQUOTED_CASING = Casing.UNCHANGED;
    private static final Casing QUOTED_CASING = Casing.UNCHANGED;
    private static final boolean CASE_SENSITIVE = true;
    private static final Set<CharLiteralStyle> CHAR_LITERAL_STYLES = Set.of(CharLiteralStyle.STANDARD);

    static Config withLexConfig(SqlParser.Config config) {
        // return config.withLex(LEX);
        return config
                .withQuoting(QUOTING)
                .withUnquotedCasing(UNQUOTED_CASING)
                .withQuotedCasing(QUOTED_CASING)
                .withCaseSensitive(CASE_SENSITIVE)
                .withCharLiteralStyles(CHAR_LITERAL_STYLES);
    }

    static void setLexProperties(Properties props) {
        // props.setProperty(CalciteConnectionProperty.LEX.camelName(), LEX.name());
        props.setProperty(CalciteConnectionProperty.QUOTING.camelName(), QUOTING.name());
        props.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), UNQUOTED_CASING.name());
        props.setProperty(CalciteConnectionProperty.QUOTED_CASING.camelName(), QUOTED_CASING.name());
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(CASE_SENSITIVE));
        // Note: there is no CalciteConnectionProperty.CHAR_LITERAL_STYLES or equivalent
    }
}
