//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

import com.google.common.base.CaseFormat;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;


public final class StandardColumnNameMappingFunction implements Function<String, String> {

    /**
     * Immutable parameters for column name mapping.
     */
    @Immutable
    @BuildableStyle
    public interface NamingParams {
        /**
         * The casing style to use for column names.
         *
         * @return the casing style, defaults to {@link CasingStyle#None}
         */
        @Default
        default CasingStyle casingStyle() {
            return CasingStyle.None;
        }

        /**
         * The replacement string for invalid characters in column names.
         *
         * @return the replacement string, defaults to "_"
         */
        @Default
        default String invalidCharacterReplacement() {
            return "_";
        }

        /**
         * Returns a new builder for NamingParams.
         *
         * @return a new builder
         */
        static Builder builder() {
            return ImmutableNamingParams.builder();
        }

        /**
         * Returns default NamingParams.
         *
         * @return default parameters
         */
        static NamingParams defaults() {
            return ImmutableNamingParams.builder().build();
        }

        /**
         * Builder for {@link NamingParams}.
         */
        interface Builder {
            /**
             * Sets the casing style for column names.
             *
             * @param casingStyle the casing style
             * @return this builder
             */
            Builder casingStyle(CasingStyle casingStyle);

            /**
             * Sets the replacement string for invalid characters in column names.
             *
             * @param invalidCharacterReplacement the replacement string
             * @return this builder
             */
            Builder invalidCharacterReplacement(String invalidCharacterReplacement);

            /**
             * Builds the immutable NamingParams instance.
             *
             * @return the built instance
             */
            NamingParams build();
        }
    }

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

    private final NamingParams namingParams;
    private final CaseFormat caseFormat;

    private final HashSet<String> usedNames = new HashSet<>();

    public StandardColumnNameMappingFunction(@NotNull final NamingParams namingParams) {
        this.namingParams = namingParams;
        caseFormat = CASING_STYLE_TO_FORMAT.get(namingParams.casingStyle());
    }

    /**
     * Ensures that columns names are valid for use in Deephaven tables and applies optional casing rules.
     *
     * @param originalColumnName Column name to be checked for validity and uniqueness
     * @param usedNames List of names already used in the table
     * @return Legalized, uniquified, column name, with specified casing applied
     */
    private String fixColumnName(
            final String originalColumnName,
            @NotNull final Set<String> usedNames) {
        if (caseFormat == null) {
            return NameValidator.legalizeColumnName(originalColumnName,
                    (s) -> s.replaceAll("[- ]", namingParams.invalidCharacterReplacement()),
                    usedNames);
        }

        // Run through the legalization and casing process twice, in case legalization returns a name that doesn't
        // conform with casing.
        // During casing adjustment, we'll allow hyphen, space, backslash, forward slash, and period as word separators.
        // noinspection unchecked
        final String intermediateLegalName =
                NameValidator.legalizeColumnName(
                        originalColumnName.replaceAll("[- .\\\\/]", "_"),
                        Function.identity(),
                        Collections.EMPTY_SET)
                        .replaceAll("_", "-").toLowerCase();

        // There should be no reason for the casing options to return a String with hyphen or space in them, but, in
        // case we add other CasingStyles later, we'll check.
        return NameValidator.legalizeColumnName(
                FROM_FORMAT.to(caseFormat, intermediateLegalName),
                (s) -> s.replaceAll("[- _]", namingParams.invalidCharacterReplacement()),
                usedNames);
    }

    @Override
    public String apply(final String inputColumnName) {
        final String outputColumnName = fixColumnName(inputColumnName, usedNames);
        usedNames.add(outputColumnName);
        return outputColumnName;
    }
}
