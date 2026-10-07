//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.util.*;
import java.util.regex.Pattern;

/**
 * The declaring class and argument list of a <code>#declaring class# #method name#(#argument list#)</code> pattern,
 * matched against reflective members. The syntax is described by {@link MethodListInvocationValidator}; use
 * {@link #parse(String)} to create the {@link MethodPattern} and {@link ConstructorPattern} that a pattern denotes.
 */
abstract sealed class MemberPattern permits MethodPattern, ConstructorPattern {
    private static final String CONSTRUCTOR_NAME = "<constructor>";
    private static final String ANY_ARGUMENTS = "..";
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[\\p{javaJavaIdentifierPart}*]+");
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
        final String trimmed = pattern.trim();
        final int space = trimmed.indexOf(' ');
        final int open = trimmed.indexOf('(');
        if (space <= 0 || open < space || !trimmed.endsWith(")")) {
            throw new IllegalArgumentException(
                    "Expected '<declaring class> <method name>(<argument list>)', but got '" + pattern + "'");
        }
        final String declaringTypeText = trimmed.substring(0, space);
        if (declaringTypeText.equals("*")) {
            throw new IllegalArgumentException("Use '*..*' rather than '*' to match every declaring class: '"
                    + pattern + "'");
        }
        final Pattern declaringType = typePattern(declaringTypeText, false);
        final String name = trimmed.substring(space + 1, open).trim();
        if (!name.equals(CONSTRUCTOR_NAME) && !IDENTIFIER_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid method name pattern: '" + name + "'");
        }

        final String argumentList = trimmed.substring(open + 1, trimmed.length() - 1).trim();
        final List<Object> parsedArguments = new ArrayList<>();
        if (!argumentList.isEmpty()) {
            final String[] split = argumentList.split(",", -1);
            for (int ai = 0; ai < split.length; ++ai) {
                final String argument = split[ai].trim();
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
        String element = text.trim();
        final StringBuilder arraySuffix = new StringBuilder();
        while (argument && element.endsWith("[]")) {
            element = element.substring(0, element.length() - 2).trim();
            arraySuffix.append("\\[\\]");
        }
        if (element.isEmpty() || element.startsWith(".") || element.endsWith(".") || element.contains("...")) {
            throw new IllegalArgumentException("Invalid type pattern: '" + text + "'");
        }
        if ((argument && element.equals("*")) || element.equals("*..*")) {
            // with an array suffix, the wildcard must not match brackets, so that each [] matches one dimension
            return Pattern.compile((arraySuffix.length() == 0 ? ".*" : "[^\\[\\]]*") + arraySuffix);
        }

        final StringBuilder regex = new StringBuilder();
        int start = 0;
        while (true) {
            final int dot = element.indexOf('.', start);
            final int end = dot < 0 ? element.length() : dot;
            final String segment = element.substring(start, end);
            if (!IDENTIFIER_PATTERN.matcher(segment).matches()) {
                throw new IllegalArgumentException("Invalid type pattern: '" + text + "'");
            }
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
