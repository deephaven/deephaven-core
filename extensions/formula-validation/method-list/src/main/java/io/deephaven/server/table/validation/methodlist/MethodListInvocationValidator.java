//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.validation.MethodInvocationValidator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

/**
 * An invocation validator that has a hardcoded list of classes and methods to permit.
 *
 * <p>
 * The methods to permit are encoded using a method pattern adapted from AspectJ:
 * </p>
 *
 * <P>
 * <B> #declaring class# #method name#(#argument list#) </B>
 * <ul>
 * <li>The declaring class must be fully qualified, except that a class in the {@code java.lang} package may be
 * unqualified. A nested class may be written as {@code java.util.Map.Entry} or {@code java.util.Map$Entry}.</li>
 * <li>A wildcard character, "*", may be used in the declaring class or method name. In the declaring class it matches
 * within a single package or class name; "..", as in {@code java..*}, matches any number of intermediate packages, and
 * {@code *..*} matches every class. A declaring class of only "*" is rejected.</li>
 * <li>The argument list is expressed as a comma-separated list of the argument types. A type in the {@code java.lang}
 * package may be unqualified, "*" matches any single argument, and the last argument may be written as either
 * {@code T[]} or {@code T...}.</li>
 * <li>".." can be used in the argument list to match zero or more arguments of any type.</li>
 * <li>"&lt;constructor&gt;" can be used as the method name to match a constructor. A method name of only wildcards also
 * matches constructors.</li>
 * <li>An instance method also matches when it overrides a method declared by a matching class or interface. For
 * example, {@code java.lang.Object toString()} matches {@link Integer#toString()} and
 * {@code java.lang.CharSequence length()} matches {@link String#length()}. Static methods only match their own
 * declaring class.</li>
 * </ul>
 * 
 * <table>
 * <tr>
 * <th>Pattern</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>*..* *(..)</td>
 * <td>Every method and constructor of every class</td>
 * </tr>
 * <tr>
 * <td>java.util.* *(..)</td>
 * <td>Every method and constructor of the classes in java.util and their nested classes, but not of sub-packages, and
 * every instance method, in any class, that overrides one of those methods</td>
 * </tr>
 * <tr>
 * <td>java.util..* *(..)</td>
 * <td>Every method and constructor of the classes in java.util, its sub-packages, and their nested classes, and every
 * instance method, in any class, that overrides one of those methods</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections *(..)</td>
 * <td>Every method and constructor declared by java.util.Collections, and every instance method that overrides one of
 * its methods</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections unmodifiable*(..)</td>
 * <td>The methods declared by java.util.Collections whose names start with "unmodifiable"</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections min(..)</td>
 * <td>Every overload of java.util.Collections.min</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections emptyList()</td>
 * <td>java.util.Collections.emptyList()</td>
 * </tr>
 * <tr>
 * <td>my.org.MyClass *(boolean, ..)</td>
 * <td>The methods and constructors of my.org.MyClass whose first parameter is a boolean, and every instance method that
 * overrides one of those methods</td>
 * </tr>
 * <tr>
 * <td>java.lang.Number intValue()</td>
 * <td>{@link Number#intValue()} and every implementation of it, such as {@link Integer#intValue()}</td>
 * </tr>
 * </table>
 */
public class MethodListInvocationValidator implements MethodInvocationValidator {
    private final List<MethodPattern> methodPatterns;

    /**
     * Create a new MethodInvocationValidator that permits any of the provided pointcut patterns.
     * 
     * @param pointCuts the patterns to permit
     */
    public MethodListInvocationValidator(final Collection<String> pointCuts) {
        final List<MethodPattern> list = new ArrayList<>();
        for (final String pointCut : pointCuts) {
            try {
                list.add(new MethodPattern(pointCut));
            } catch (Exception e) {
                throw new UncheckedDeephavenException("Could not parse method pattern: '" + pointCut + "'", e);
            }
        }
        methodPatterns = Collections.unmodifiableList(list);
    }

    @Override
    public Boolean permitConstructor(final Constructor<?> constructor) {
        if (methodPatterns.stream().anyMatch(mp -> mp.matches(constructor))) {
            return true;
        }
        return null;
    }

    @Override
    public Boolean permitMethod(final Method method) {
        if (methodPatterns.stream().anyMatch(mp -> mp.matches(method))) {
            return true;
        }
        return null;
    }
}
