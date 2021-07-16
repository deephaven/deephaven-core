/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import javax.lang.model.SourceVersion;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NameValidator {
    private static final String COLUMN_PREFIX = "column_";
    private static final String QUERY_PREFIX = "var_";
    private static final String TABLE_PREFIX = "table_";
    private static final String STERILE_COLUMN_AND_QUERY_REGEX = "[^A-Za-z0-9_$]";
    private static final String STERILE_TABLE_AND_NAMESPACE_REGEX = "[^a-zA-Z0-9_$\\-\\+@]";

    private enum ValidationCode {
        // @formatter:off
        OK(""),
        RESERVED("Invalid <type> name \"<name>\": \"<name>\" is a reserved keyword"),
        INVALID("Invalid <type> name \"<name>\""),
        NULL_NAME("Invalid <type> name: name can not be null or empty");
        // @formatter:on

        private final String message;

        ValidationCode(String message) {
            this.message = message;
        }

        private String getErrorMessage(String name, String type) {
            return message.replace("<name>", name == null ? "null" : name).replaceAll("<type>",
                type);
        }

        private boolean isValid() {
            return this == OK;
        }
    }

    // table names should not start with numbers. Partition names should be able to
    // TODO(deephaven-core#822): Allow more table names
    private final static Pattern TABLE_NAME_PATTERN =
        Pattern.compile("([a-zA-Z_$])[a-zA-Z0-9_$[-][+]@]*");
    private final static Pattern PARTITION_NAME_PATTERN =
        Pattern.compile("([a-zA-Z0-9$])[a-zA-Z0-9_$[-][+]@\\.]*");

    public enum Type {
        // @formatter:off
        COLUMN(true, true, "column", null),
        QUERY_PARAM(true, true, "query variable", null),
        TABLE(false, false, "table", TABLE_NAME_PATTERN),
        NAMESPACE(false, false, "namespace", TABLE_NAME_PATTERN),
        PARTITION(false, false, "partition", PARTITION_NAME_PATTERN);
        // @formatter:on

        private final boolean checkReservedVariableNames;
        private final boolean checkValidJavaWord;
        private final String type;
        private final Pattern pattern;

        @Override
        public String toString() {
            return type;
        }

        Type(boolean checkReservedVariableNames, boolean checkValidJavaWord, String type,
            Pattern pattern) {
            this.checkReservedVariableNames = checkReservedVariableNames;
            this.checkValidJavaWord = checkValidJavaWord;
            this.type = type;
            this.pattern = pattern;
        }

        private String validate(String name) {
            ValidationCode code =
                getCode(name, pattern, checkReservedVariableNames, checkValidJavaWord);
            if (!code.isValid()) {
                throw new InvalidNameException(code.getErrorMessage(name, type));
            }

            return name;
        }
    }

    private static final Set<String> DB_RESERVED_VARIABLE_NAMES =
        Stream.of("in", "not", "i", "ii", "k").collect(
            Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));

    public static String validateTableName(String name) {
        return Type.TABLE.validate(name);
    }

    public static String validateNamespaceName(String name) {
        return Type.NAMESPACE.validate(name);
    }

    public static String validatePartitionName(String name) {
        return Type.PARTITION.validate(name);
    }

    public static String validateQueryParameterName(String name) {
        return Type.QUERY_PARAM.validate(name);
    }

    public static String validateColumnName(String name) {
        return Type.COLUMN.validate(name);
    }

    private static ValidationCode getCode(String name, Pattern pattern,
        boolean checkReservedVariableNames, boolean checkValidJavaWord) {
        if (name == null || name.isEmpty()) {
            return ValidationCode.NULL_NAME;
        }

        if (checkReservedVariableNames) {
            // isReserved checks if name is a DB literal, or Java reserved word or literal
            if (isReserved(name)) {
                return ValidationCode.RESERVED;
            }
        }

        if (checkValidJavaWord) {
            if (!SourceVersion.isIdentifier(name)) {
                return ValidationCode.INVALID;
            }
        }

        if (pattern == null) {
            return ValidationCode.OK;
        }

        if (pattern.matcher(name).matches()) {
            return ValidationCode.OK;
        }

        return ValidationCode.INVALID;
    }

    public static class InvalidNameException extends IllegalArgumentException {
        /**
         * Constructs a new InvalidNameException with the specified detail message.
         *
         * @param message the detail message
         * @see IllegalArgumentException#IllegalArgumentException(String)
         */
        InvalidNameException(String message) {
            super(message);
        }
    }

    public static class LegalizeNameException extends IllegalArgumentException {
        /**
         * Constructs a new LegalizeNameException with the specified detail message.
         *
         * @param message the detail message
         * @see IllegalArgumentException#IllegalArgumentException(String)
         */
        LegalizeNameException(String message) {
            super(message);
        }
    }

    public static String legalizeColumnName(String name) {
        return legalizeColumnName(name, Collections.emptySet());
    }

    public static String legalizeColumnName(String name, Set<String> takenNames) {
        return legalizeColumnName(name, Function.identity(), takenNames);
    }

    public static String legalizeColumnName(String name, Function<String, String> replaceCustom) {
        return legalizeColumnName(name, replaceCustom, Collections.emptySet());
    }

    /**
     * Attempts to return a legal name based on the passed in {@code name}.
     *
     * <p>
     * Illegal characters are simply removed. Custom replacement is possible through
     * {@code customReplace}
     *
     * <p>
     * To avoid duplicated names, anything in the set {@code takenNames} will not be returned. These
     * duplicates are resolved by adding sequential digits at the end of the variable name.
     *
     * <p>
     * Column names A variable's name can be any legal identifier - an unlimited-length sequence of
     * Unicode letters and digits, beginning with a letter, the dollar sign "$", or the underscore
     * character "_". Subsequent characters may be letters, digits, dollar signs, or underscore
     * characters.
     *
     * @param name, customReplace, takenNames can not be null
     * @return
     */
    public static String legalizeColumnName(String name, Function<String, String> customReplace,
        Set<String> takenNames) {
        return legalizeName(name, customReplace, takenNames, "Can not legalize column name " + name,
            COLUMN_PREFIX, STERILE_COLUMN_AND_QUERY_REGEX, true, true,
            NameValidator::validateColumnName);
    }

    public static String[] legalizeColumnNames(String[] names) {
        return legalizeColumnNames(names, Function.identity());
    }

    public static String[] legalizeColumnNames(String[] names,
        Function<String, String> customReplace) {
        return legalizeColumnNames(names, customReplace, false);
    }

    public static String[] legalizeColumnNames(String[] names, boolean resolveConflicts) {
        return legalizeColumnNames(names, Function.identity(), resolveConflicts);
    }

    public static String[] legalizeColumnNames(String[] names,
        Function<String, String> customReplace, boolean resolveConflicts) {
        return legalizeNames(names, customReplace, resolveConflicts,
            NameValidator::legalizeColumnName);
    }

    public static String legalizeQueryParameterName(String name) {
        return legalizeQueryParameterName(name, Function.identity());
    }

    public static String legalizeQueryParameterName(String name,
        Function<String, String> replaceCustom) {
        return legalizeQueryParameterName(name, replaceCustom, Collections.emptySet());
    }

    public static String legalizeQueryParameterName(String name, Set<String> takenNames) {
        return legalizeQueryParameterName(name, Function.identity(), takenNames);
    }

    /**
     * Attempts to return a legal name based on the passed in {@code name}.
     *
     * <p>
     * Illegal characters are simply removed. Custom replacement is possible through
     * {@code customReplace}
     *
     * <p>
     * To avoid duplicated names, anything in the set {@code takenNames} will not be returned. These
     * duplicates are resolved by adding sequential digits at the end of the variable name.
     *
     * <p>
     * Query parameters follow the same rules as column names
     *
     * @param name, customReplace, takenNames can not be null
     * @return
     */
    public static String legalizeQueryParameterName(String name,
        Function<String, String> customReplace, Set<String> takenNames) {
        return legalizeName(name, customReplace, takenNames, "Can not legalize table name " + name,
            QUERY_PREFIX, STERILE_COLUMN_AND_QUERY_REGEX, true, true,
            NameValidator::validateQueryParameterName);
    }

    public static String[] legalizeQueryParameterNames(String[] names) {
        return legalizeQueryParameterNames(names, Function.identity());
    }

    public static String[] legalizeQueryParameterNames(String[] names,
        Function<String, String> customReplace) {
        return legalizeQueryParameterNames(names, customReplace, false);
    }

    public static String[] legalizeQueryParameterNames(String[] names, boolean resolveConflicts) {
        return legalizeQueryParameterNames(names, Function.identity(), resolveConflicts);
    }

    public static String[] legalizeQueryParameterNames(String[] names,
        Function<String, String> customReplace, boolean resolveConflicts) {
        return legalizeNames(names, customReplace, resolveConflicts,
            NameValidator::legalizeQueryParameterName);
    }

    public static String legalizeTableName(String name) {
        return legalizeTableName(name, Collections.emptySet());
    }

    public static String legalizeTableName(String name, Set<String> takenNames) {
        return legalizeTableName(name, Function.identity(), takenNames);
    }

    public static String legalizeTableName(String name, Function<String, String> replaceCustom) {
        return legalizeTableName(name, replaceCustom, Collections.emptySet());
    }

    /**
     * Attempts to return a legal name based on the passed in {@code name}.
     *
     * <p>
     * Illegal characters are simply removed. Custom replacement is possible through
     * {@code customReplace}
     *
     * <p>
     * To avoid duplicated names, anything in the set {@code takenNames} will not be returned. These
     * duplicates are resolved by adding sequential digits at the end of the variable name.
     *
     * <p>
     * Table Names- check the regex {@code TABLE_NAME_PATTERN}
     *
     * @param name, customReplace, takenNames can not be null
     * @return
     */
    public static String legalizeTableName(String name, Function<String, String> customReplace,
        Set<String> takenNames) {
        return legalizeName(name, customReplace, takenNames, "Can not legalize table name " + name,
            TABLE_PREFIX, STERILE_TABLE_AND_NAMESPACE_REGEX, false, true,
            NameValidator::validateTableName);
    }

    public static boolean isLegalTableName(String name) {
        return isLegalTableName(name, Collections.emptySet());
    }

    public static boolean isLegalTableName(String name, Set<String> takenNames) {
        return isLegalTableName(name, Function.identity(), takenNames);
    }

    public static boolean isLegalTableName(String name, Function<String, String> replaceCustom) {
        return isLegalTableName(name, replaceCustom, Collections.emptySet());
    }

    /**
     * Validates whether a given name is a legal table name.
     *
     * @param name the name to validate
     * @param customReplace a function that is applied to the name before processing legality
     * @param takenNames the list of names that are already taken
     * @return whether the name is valid for a new table
     * 
     */
    public static boolean isLegalTableName(String name, Function<String, String> customReplace,
        Set<String> takenNames) {
        return isLegal(name, customReplace, takenNames, STERILE_TABLE_AND_NAMESPACE_REGEX, false,
            true, NameValidator::validateTableName);
    }

    public static String[] legalizeTableNames(String[] names) {
        return legalizeTableNames(names, Function.identity());
    }

    public static String[] legalizeTableNames(String[] names,
        Function<String, String> customReplace) {
        return legalizeTableNames(names, customReplace, false);
    }

    public static String[] legalizeTableNames(String[] names, boolean resolveConflicts) {
        return legalizeTableNames(names, Function.identity(), resolveConflicts);
    }

    public static String[] legalizeTableNames(String[] names,
        Function<String, String> customReplace, boolean resolveConflicts) {
        return legalizeNames(names, customReplace, resolveConflicts,
            NameValidator::legalizeTableName);
    }

    public static String legalizeNamespaceName(String name) {
        return legalizeNamespaceName(name, Collections.emptySet());
    }

    public static String legalizeNamespaceName(String name, Set<String> takenNames) {
        return legalizeNamespaceName(name, Function.identity(), takenNames);
    }

    public static String legalizeNamespaceName(String name,
        Function<String, String> replaceCustom) {
        return legalizeNamespaceName(name, replaceCustom, Collections.emptySet());
    }

    /**
     * Attempts to return a legal name based on the passed in {@code name}.
     *
     * <p>
     * Illegal characters are simply removed. Custom replacement is possible through
     * {@code customReplace}
     *
     * <p>
     * To avoid duplicated names, anything in the set {@code takenNames} will not be returned. These
     * duplicates are resolved by adding sequential digits at the end of the variable name.
     *
     * <p>
     * Namespace Names- check the regex {@code TABLE_NAME_PATTERN}
     *
     * @param name, customReplace, takenNames can not be null
     * @return
     */
    public static String legalizeNamespaceName(String name, Function<String, String> customReplace,
        Set<String> takenNames) {
        return legalizeName(name, customReplace, takenNames,
            "Can not legalize namespace name " + name, null, STERILE_TABLE_AND_NAMESPACE_REGEX,
            false, false, NameValidator::validateNamespaceName);
    }

    /**
     * Validates whether a given {@code name} is a valid namespace.
     *
     * @param name the name to validate
     * @param customReplace a function that is applied to the name before processing legality
     * @param takenNames the list of names that are already taken
     * @return whether the name is valid for a new namespace
     */
    public static boolean isLegalNamespaceName(String name, Function<String, String> customReplace,
        Set<String> takenNames) {
        return isLegal(name, customReplace, takenNames, STERILE_TABLE_AND_NAMESPACE_REGEX, false,
            false, NameValidator::validateNamespaceName);
    }

    public static boolean isLegalNamespaceName(String name) {
        return isLegalNamespaceName(name, Collections.emptySet());
    }

    public static boolean isLegalNamespaceName(String name, Set<String> takenNames) {
        return isLegalNamespaceName(name, Function.identity(), takenNames);
    }

    public static boolean isLegalNamespaceName(String name,
        Function<String, String> replaceCustom) {
        return isLegalNamespaceName(name, replaceCustom, Collections.emptySet());
    }

    public static String[] legalizeNamespaceNames(String[] names) {
        return legalizeNamespaceNames(names, Function.identity());
    }

    public static String[] legalizeNamespaceNames(String[] names,
        Function<String, String> customReplace) {
        return legalizeNamespaceNames(names, customReplace, false);
    }

    public static String[] legalizeNamespaceNames(String[] names, boolean resolveConflicts) {
        return legalizeNamespaceNames(names, Function.identity(), resolveConflicts);
    }

    public static String[] legalizeNamespaceNames(String[] names,
        Function<String, String> customReplace, boolean resolveConflicts) {
        return legalizeNames(names, customReplace, resolveConflicts,
            NameValidator::legalizeNamespaceName);
    }

    private static String legalizeName(String name, Function<String, String> customReplace,
        Set<String> takenNames, String error, String prefix, String regex, boolean checkReserved,
        boolean checkFirstIsNumber, Consumer<String> validation) {
        // if null, throw an exception
        if (name == null) {
            throw new LegalizeNameException("Can not legalize a null name");
        }
        Objects.requireNonNull(customReplace, "customReplace");
        Objects.requireNonNull(takenNames, "takenNames");


        String replacedName = customReplace.apply(name);

        // if this is a reserved word, append the prefix to the front
        if (checkReserved && isReserved(replacedName)) {
            replacedName = prefix + replacedName;
        }

        // remove illegal characters
        String sanitizedName = replacedName.replaceAll(regex, "");

        if (sanitizedName.isEmpty()) {
            throw new LegalizeNameException(error);
        }

        // if name starts with a number, append prefix to the front
        if (Character.isDigit(sanitizedName.charAt(0)) && checkFirstIsNumber) {
            sanitizedName = prefix + sanitizedName;
        }

        int i = 2;
        String tempName = sanitizedName;
        while (takenNames.contains(tempName)) {
            tempName = sanitizedName + i++;
        }

        try {
            validation.accept(sanitizedName);
        } catch (NameValidator.InvalidNameException e) {
            throw new LegalizeNameException(error);
        }

        return tempName;
    }

    private static boolean isLegal(String name, Function<String, String> customReplace,
        Set<String> takenNames, String regex, boolean checkReserved, boolean checkFirstIsNumber,
        Consumer<String> validation) {
        // if null, throw an exception
        if (name == null || name.isEmpty()) {
            return false;
        }
        Objects.requireNonNull(customReplace, "customReplace");
        Objects.requireNonNull(takenNames, "takenNames");


        final String replacedName = customReplace.apply(name);

        // if this is a reserved word, append the prefix to the front
        if (checkReserved && isReserved(replacedName)) {
            return false;
        }

        // remove illegal characters
        final String sanitizedName = replacedName.replaceAll(regex, "");
        if (!Objects.equals(sanitizedName, replacedName)) {
            return false;
        }

        // if name starts with a number, append prefix to the front
        if (checkFirstIsNumber && Character.isDigit(sanitizedName.charAt(0))) {
            return false;
        }

        if (takenNames.contains(replacedName)) {
            return false;
        }

        try {
            validation.accept(sanitizedName);
        } catch (NameValidator.InvalidNameException e) {
            return false;
        }

        return true;
    }

    private static boolean isReserved(String replacedName) {
        return DB_RESERVED_VARIABLE_NAMES.contains(replacedName)
            || SourceVersion.isKeyword(replacedName);
    }

    private static String[] legalizeNames(final String[] names,
        final Function<String, String> customReplace, final boolean resolveConflicts,
        final Legalizer legalizer) {
        // if null, throw an exception
        if (names == null) {
            throw new LegalizeNameException("Can not legalize a null name array");
        }
        Objects.requireNonNull(customReplace, "customReplace");
        Objects.requireNonNull(legalizer, "legalizer");

        Set<String> result = new LinkedHashSet<>();
        for (String name : names) {
            name = legalizer.apply(name, customReplace,
                resolveConflicts ? result : Collections.emptySet());
            if (!resolveConflicts && result.contains(name)) {
                throw new LegalizeNameException("Duplicate names during legalization: " + name);
            }
            result.add(name);
        }
        return result.toArray(new String[result.size()]);
    }

    @FunctionalInterface
    private interface Legalizer {
        String apply(String name, Function<String, String> customReplace, Set<String> takenNames);
    }
}
