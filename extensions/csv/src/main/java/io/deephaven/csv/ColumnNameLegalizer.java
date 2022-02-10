package io.deephaven.csv;

import io.deephaven.api.util.NameValidator;
import io.deephaven.csv.CsvSpecs.Builder;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A {@link Builder#headerLegalizer(Function)} that replaces {@code '-'} and {@code ' '} with {@code '_'}. Also
 * implements {@link Builder#headerValidator(Predicate)}.
 */
public enum ColumnNameLegalizer implements Function<String[], String[]>, Predicate<String> {
    INSTANCE;

    private final Pattern pattern;

    ColumnNameLegalizer() {
        this.pattern = Pattern.compile("[- ]");
    }

    private String replace(String columnName) {
        return pattern.matcher(columnName).replaceAll("_");
    }

    @Override
    public String[] apply(String[] columnNames) {
        return NameValidator.legalizeColumnNames(columnNames, this::replace, true);
    }


    @Override
    public boolean test(String columnName) {
        return NameValidator.isValidColumnName(columnName);
    }
}
