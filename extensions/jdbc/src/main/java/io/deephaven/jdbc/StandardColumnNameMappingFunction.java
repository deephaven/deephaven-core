package io.deephaven.jdbc;

import com.google.common.base.CaseFormat;
import io.deephaven.api.util.NameValidator;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;


public final class StandardColumnNameMappingFunction implements Function<String, String> {

    private static final CaseFormat FROM_FORMAT = CaseFormat.LOWER_HYPHEN;
    private static final Map<CasingStyle, CaseFormat> CASING_STYLE_TO_FORMAT;

    static {
        final Map<CasingStyle, CaseFormat> tmpCasingStyleToFormat = new HashMap<>();
        tmpCasingStyleToFormat.put(CasingStyle.lowerCamel, CaseFormat.LOWER_CAMEL);
        tmpCasingStyleToFormat.put(CasingStyle.UpperCamel, CaseFormat.UPPER_CAMEL);
        tmpCasingStyleToFormat.put(CasingStyle.lowercase, CaseFormat.LOWER_UNDERSCORE);
        tmpCasingStyleToFormat.put(CasingStyle.UPPERCASE, CaseFormat.UPPER_UNDERSCORE);
        tmpCasingStyleToFormat.put(CasingStyle.None, null);
        CASING_STYLE_TO_FORMAT = Collections.unmodifiableMap(tmpCasingStyleToFormat);
    }

    private final JdbcReadInstructions instructions;

    private final HashSet<String> usedNames = new HashSet<>();

    StandardColumnNameMappingFunction(@NotNull final JdbcReadInstructions instructions) {
        this.instructions = instructions;
    }

    /**
     * Ensures that columns names are valid for use in Deephaven tables and applies optional casing rules.
     *
     * @param originalColumnName Column name to be checked for validity and uniqueness
     * @param usedNames List of names already used in the table
     * @param casing Optional {@link CasingStyle} to use when processing source names, if null or
     *        {@link CasingStyle#None} the source name's casing is not modified
     * @param replacement A String to use as a replacement for invalid characters in the source name
     * @return Legalized, uniquified, column name, with specified casing applied
     */
    private static String fixColumnName(
            final String originalColumnName,
            @NotNull final Set<String> usedNames,
            final CasingStyle casing,
            @NotNull final String replacement) {
        if (casing == null || CASING_STYLE_TO_FORMAT.get(casing) == null) {
            return NameValidator.legalizeColumnName(originalColumnName, (s) -> s.replaceAll("[- ]", replacement),
                    usedNames);
        }

        // Run through the legalization and casing process twice, in case legalization returns a name that doesn't
        // conform with casing.
        // During casing adjustment, we'll allow hyphen, space, backslash, forward slash, and period as word separators.
        // noinspection unchecked
        final String intermediateLegalName =
                NameValidator.legalizeColumnName(
                        originalColumnName.replaceAll("[- .\\\\/]", "_"),
                        (s) -> s,
                        Collections.EMPTY_SET)
                        .replaceAll("_", "-").toLowerCase();

        // There should be no reason for the casing options to return a String with hyphen or space in them, but, in
        // case we add other CasingStyles later, we'll check.
        return NameValidator.legalizeColumnName(
                FROM_FORMAT.to(
                        CASING_STYLE_TO_FORMAT.get(casing),
                        intermediateLegalName),
                (s) -> s.replaceAll("[- _]", replacement),
                usedNames);
    }

    @Override
    public String apply(final String inputColumnName) {
        final String outputColumnName = fixColumnName(
                inputColumnName,
                usedNames,
                instructions.columnNameCasingStyle(),
                instructions.columnNameInvalidCharacterReplacement());
        usedNames.add(outputColumnName);
        return outputColumnName;
    }
}
