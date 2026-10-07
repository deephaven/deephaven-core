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
 * <li>The declaring class is separated from the method name by spaces or by "#", as in
 * {@code java.lang.String#length()}.</li>
 * <li>The declaring class must be fully qualified, except that a class in the {@code java.lang} package may be
 * unqualified. A nested class may be written as {@code java.util.Map.Entry} or {@code java.util.Map$Entry}. Only a name
 * without dots or wildcards is taken to be in {@code java.lang}, so an unqualified nested class must use the binary
 * form, as in {@code Thread$State}; {@code Thread.State} matches nothing.</li>
 * <li>A wildcard character, "*", may be used in the declaring class or method name. In the declaring class it matches
 * within a single package or class name, which includes the binary name of a nested class, so {@code java.util.*}
 * matches {@code java.util.Map$Entry}. "..", as in {@code java..*}, matches any number of intermediate package or
 * enclosing class names, so {@code java.util..Entry} matches {@code java.util.Map.Entry}, and {@code *..*} matches
 * every class. A declaring class of only "*" is rejected.</li>
 * <li>The argument list is expressed as a comma-separated list of the argument types. A type in the {@code java.lang}
 * package may be unqualified, and "*" matches any single argument. An array argument is written {@code T[]}, in any
 * position; only the last argument may instead be written {@code T...}, which is the same as {@code T[]}.</li>
 * <li>".." can be used once in the argument list to match zero or more arguments of any type.</li>
 * <li>"&lt;constructor&gt;" can be used as the method name to match a constructor. A method name of only wildcards also
 * matches constructors.</li>
 * <li>An instance method also matches when it overrides a method declared by a matching class or interface. For
 * example, {@code java.lang.Object toString()} matches {@link Integer#toString()} and
 * {@code java.lang.CharSequence length()} matches {@link String#length()}. Static methods only match their own
 * declaring class, and a bridge method declared by a matching class is never the method overridden.</li>
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
    private final List<ConstructorPattern> constructorPatterns;

    /**
     * Create a new MethodInvocationValidator that permits any of the provided pointcut patterns.
     * 
     * @param pointCuts the patterns to permit
     */
    public MethodListInvocationValidator(final Collection<String> pointCuts) {
        final List<MethodPattern> methods = new ArrayList<>();
        final List<ConstructorPattern> constructors = new ArrayList<>();
        for (final String pointCut : pointCuts) {
            final List<MemberPattern> parsed;
            try {
                parsed = MemberPattern.parse(pointCut);
            } catch (Exception e) {
                throw new UncheckedDeephavenException("Could not parse method pattern: '" + pointCut + "'", e);
            }
            for (final MemberPattern memberPattern : parsed) {
                if (memberPattern instanceof MethodPattern) {
                    methods.add((MethodPattern) memberPattern);
                } else {
                    constructors.add((ConstructorPattern) memberPattern);
                }
            }
        }
        methodPatterns = Collections.unmodifiableList(methods);
        constructorPatterns = Collections.unmodifiableList(constructors);
    }

    @Override
    public Boolean permitConstructor(final Constructor<?> constructor) {
        if (constructorPatterns.stream().anyMatch(cp -> cp.matches(constructor))) {
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
