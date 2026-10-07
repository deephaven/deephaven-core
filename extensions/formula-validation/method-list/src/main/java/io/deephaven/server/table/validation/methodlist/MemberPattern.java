//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The declaring class and argument list of a <code>#declaring class# #method name#(#argument list#)</code> pattern,
 * matched against reflective members. The syntax is described by {@link MethodListInvocationValidator}; use
 * {@link #parse(String)} to create the {@link MethodPattern} and {@link ConstructorPattern} that a pattern denotes.
 */
abstract sealed class MemberPattern permits MethodPattern, ConstructorPattern {
    private static final String CONSTRUCTOR_NAME = "<constructor>";
    private static final String ANY_ARGUMENTS = "..";
    /** A package, class or method name, in which "*" matches any run of characters. */
    private static final String SEGMENT = "[\\p{javaJavaIdentifierStart}*][\\p{javaJavaIdentifierPart}*]*";
    /** A dotted name, in which ".." matches any number of intermediate names. */
    private static final String TYPE = SEGMENT + "(?:\\.\\.?" + SEGMENT + ")*";
    private static final String ARGUMENT = TYPE + "(?:\\[])*(?:\\.\\.\\.)?";
    private static final String COMMA = " *, *";
    /** Arguments without "..", or with exactly one ".." among them. */
    private static final String ARGUMENT_LIST = "(?:" + ARGUMENT + "(?:" + COMMA + ARGUMENT + ")*"
            + "|(?:" + ARGUMENT + COMMA + ")*\\.\\.(?:" + COMMA + ARGUMENT + ")*)";
    /**
     * The whole pattern; the declaring class is separated from the name by spaces or "#". Only spaces may separate the
     * elements of a pattern.
     */
    private static final Pattern PATTERN = Pattern.compile("(?<type>" + TYPE + ")(?: +| *# *)"
            + "(?<name><constructor>|" + SEGMENT + ") *\\( *(?<arguments>" + ARGUMENT_LIST + ")? *\\)");
    private static final Set<String> PRIMITIVE_NAMES =
            Set.of("boolean", "byte", "char", "short", "int", "long", "float", "double", "void");

    private final Pattern declaringType;
    /**
     * One entry per element of the argument list: a {@link Pattern} for a single argument, or {@link #ANY_ARGUMENTS}
     * for zero or more arguments of any type.
     */
    private final List<Object> arguments;

    MemberPattern(final Pattern declaringType, final List<Object> arguments) {
        this.declaringType = declaringType;
        this.arguments = arguments;
    }

    /**
     * Parse a pattern into the member patterns it denotes: a {@link ConstructorPattern} for the name
     * {@code <constructor>}, a {@link MethodPattern} for any other name, and both for a name of only wildcards.
     *
     * @param pattern the pattern text
     * @return one or two member patterns
     * @throws IllegalArgumentException if the pattern is malformed
     */
    static List<MemberPattern> parse(final String pattern) {
        final Matcher matcher = PATTERN.matcher(pattern.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Expected '<declaring class> <method name>(<argument list>)', but got '" + pattern + "'");
        }
        final String declaringTypeText = matcher.group("type");
        if (declaringTypeText.equals("*")) {
            throw new IllegalArgumentException("Use '*..*' rather than '*' to match every declaring class: '"
                    + pattern + "'");
        }
        final Pattern declaringType = typePattern(declaringTypeText, false);
        final String name = matcher.group("name");

        final String argumentList = matcher.group("arguments");
        final List<Object> parsedArguments = new ArrayList<>();
        if (argumentList != null) {
            final String[] split = argumentList.split(COMMA);
            for (int ai = 0; ai < split.length; ++ai) {
                final String argument = split[ai];
                if (argument.equals(ANY_ARGUMENTS)) {
                    parsedArguments.add(ANY_ARGUMENTS);
                } else if (argument.endsWith("...")) {
                    if (ai != split.length - 1) {
                        throw new IllegalArgumentException("Only the last argument may be variable arity: '"
                                + pattern + "'");
                    }
                    parsedArguments.add(typePattern(argument.substring(0, argument.length() - 3) + "[]", true));
                } else {
                    parsedArguments.add(typePattern(argument, true));
                }
            }
        }
        final List<Object> arguments = List.copyOf(parsedArguments);

        if (name.equals(CONSTRUCTOR_NAME)) {
            return List.of(new ConstructorPattern(declaringType, arguments));
        }
        final MethodPattern methodPattern = new MethodPattern(declaringType, name, arguments);
        if (name.replace("*", "").isEmpty()) {
            // a name of only wildcards also matches constructors
            return List.of(methodPattern, new ConstructorPattern(declaringType, arguments));
        }
        return List.of(methodPattern);
    }

    boolean declaringTypeMatches(final Class<?> type) {
        return typeMatches(declaringType, type);
    }

    boolean argumentsMatch(final Class<?>[] parameterTypes) {
        return argumentsMatch(0, parameterTypes, 0);
    }

    private boolean argumentsMatch(final int argumentIndex, final Class<?>[] parameterTypes,
            final int parameterIndex) {
        if (argumentIndex == arguments.size()) {
            return parameterIndex == parameterTypes.length;
        }
        final Object argument = arguments.get(argumentIndex);
        if (argument == ANY_ARGUMENTS) {
            for (int pi = parameterIndex; pi <= parameterTypes.length; ++pi) {
                if (argumentsMatch(argumentIndex + 1, parameterTypes, pi)) {
                    return true;
                }
            }
            return false;
        }
        return parameterIndex < parameterTypes.length
                && typeMatches((Pattern) argument, parameterTypes[parameterIndex])
                && argumentsMatch(argumentIndex + 1, parameterTypes, parameterIndex + 1);
    }

    private static boolean typeMatches(final Pattern pattern, final Class<?> type) {
        final String canonicalName = type.getCanonicalName();
        if (canonicalName != null && pattern.matcher(canonicalName).matches()) {
            return true;
        }
        // local and anonymous classes, and arrays of them, have no canonical name; "$" in a pattern also matches a
        // nested class
        return pattern.matcher(type.getTypeName()).matches();
    }

    /**
     * Translate a type pattern to a regular expression over canonical or binary class names. A {@code *} alone matches
     * any argument type; otherwise it matches within one segment of a name, which may be the binary name of a nested
     * class. {@code ..} matches any number of intermediate package or enclosing class names, and an unqualified name
     * that is not a primitive matches only the class of that name in {@code java.lang}. In an argument, each trailing
     * {@code []} matches one array dimension.
     */
    private static Pattern typePattern(final String text, final boolean argument) {
        String element = text;
        final StringBuilder arraySuffix = new StringBuilder();
        while (argument && element.endsWith("[]")) {
            element = element.substring(0, element.length() - 2);
            arraySuffix.append("\\[\\]");
        }
        if (argument && element.equals("*") && arraySuffix.length() == 0) {
            // a lone "*" matches any single argument, arrays included
            return Pattern.compile(".*");
        }
        if ((argument && element.equals("*")) || element.equals("*..*")) {
            // otherwise the wildcard must not match brackets, so that each [] matches exactly one dimension and
            // "*..*" alone matches only types that are not arrays
            return Pattern.compile("[^\\[\\]]*" + arraySuffix);
        }

        final StringBuilder regex = new StringBuilder();
        int start = 0;
        while (true) {
            final int dot = element.indexOf('.', start);
            final int end = dot < 0 ? element.length() : dot;
            final String segment = element.substring(start, end);
            // a wildcard within a name does not match the brackets of an array type
            regex.append(globToRegex(segment, "[^.\\[\\]]*"));
            if (dot < 0) {
                break;
            }
            if (element.startsWith("..", dot)) {
                regex.append("\\.(?:.*\\.)?");
                start = dot + 2;
            } else {
                regex.append("\\.");
                start = dot + 1;
            }
        }

        String result = regex.toString();
        if (element.indexOf('.') < 0 && element.indexOf('*') < 0 && !PRIMITIVE_NAMES.contains(element)) {
            result = "java\\.lang\\." + result;
        }
        return Pattern.compile(result + arraySuffix);
    }

    static String globToRegex(final String glob, final String wildcard) {
        final StringBuilder regex = new StringBuilder();
        int start = 0;
        for (int star = glob.indexOf('*'); star >= 0; star = glob.indexOf('*', start)) {
            regex.append(literal(glob.substring(start, star))).append(wildcard);
            start = star + 1;
        }
        return regex.append(literal(glob.substring(start))).toString();
    }

    private static String literal(final String text) {
        // "$" separates a nested class from its enclosing class in a binary name, and "." in a canonical name
        return text.isEmpty() ? "" : Pattern.quote(text).replace("$", "\\E[.$]\\Q");
    }
}
