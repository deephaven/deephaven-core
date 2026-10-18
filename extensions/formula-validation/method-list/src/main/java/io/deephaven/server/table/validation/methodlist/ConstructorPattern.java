//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A pattern that matches constructors, from a method pattern with the name {@code <constructor>} or a name of only
 * wildcards.
 */
final class ConstructorPattern extends MemberPattern {
    ConstructorPattern(final Pattern declaringType, final List<Object> arguments) {
        super(declaringType, arguments);
    }

    /**
     * Does this pattern match the given constructor?
     *
     * @param constructor the constructor to test
     * @return true if the constructor's declaring class and parameter types match
     */
    boolean matches(final Constructor<?> constructor) {
        return declaringTypeMatches(constructor.getDeclaringClass())
                && argumentsMatch(constructor.getParameterTypes());
    }
}
