//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.validation.MethodInvocationValidator;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.JavaType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;

/**
 * An invocation validator that has a hardcoded list of classes and methods to permit.
 *
 * <p>
 * The methods to permit are encoded using an AspectJ method pattern, as implemented by the <a href=
 * "https://javadoc.io/static/org.openrewrite/rewrite-java/8.6.2/org/openrewrite/java/MethodMatcher.html"></a>OpenRewrite
 * MethodMatcher</a>, which are documented as follows:
 * </p>
 *
 * <P>
 * <B> #declaring class# #method name#(#argument list#) </B>
 * <ul>
 * <li>The declaring class must be fully qualified.</li>
 * <li>A wildcard character, "*", may be used in either the declaring class or method name.</li>
 * <li>The argument list is expressed as a comma-separated list of the argument types</li>
 * <li>".." can be used in the argument list to match zero or more arguments of any type.</li>
 * <li>"&lt;constructor&gt;" can be used as the method name to match a constructor.</li>
 * </ul>
 * 
 * <table>
 * <tr>
 * <th>Pattern</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>*..* *(..)</td>
 * <td>All method invocations</td>
 * </tr>
 * <tr>
 * <td>java.util.* *(..)</td>
 * <td>All method invocations to classes belonging to java.util (including sub-packages)</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections *(..)</td>
 * <td>All method invocations on java.util.Collections class</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections unmodifiable*(..)</td>
 * <td>All method invocations starting with "unmodifiable" on java.util.Collections</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections min(..)</td>
 * <td>All method invocations for all overloads of "min"</td>
 * </tr>
 * <tr>
 * <td>java.util.Collections emptyList()</td>
 * <td>All method invocations on java.util.Collections.emptyList()</td>
 * </tr>
 * <tr>
 * <td>my.org.MyClass *(boolean, ..)</td>
 * <td>All method invocations where the first arg is a boolean in my.org.MyClass</td>
 * </tr>
 * </table>
 */
public class MethodListInvocationValidator implements MethodInvocationValidator {
    private final List<MethodMatcher> methodMatchers;

    /**
     * Create a new MethodInvocationValidator that permits any of the provided pointcut patterns.
     * 
     * @param pointCuts the patterns to permit
     */
    public MethodListInvocationValidator(final Collection<String> pointCuts) {
        final List<MethodMatcher> list = new ArrayList<>();
        for (final String pointCut : pointCuts) {
            final MethodMatcher methodMatcher;
            try {
                methodMatcher = new MethodMatcher(pointCut, true);
            } catch (Exception e) {
                throw new UncheckedDeephavenException("Could not parse method pattern: '" + pointCut + "'", e);
            }
            list.add(methodMatcher);
        }
        methodMatchers = Collections.unmodifiableList(list);
    }

    @Override
    public Boolean permitConstructor(final Constructor<?> constructor) {
        final JavaType.Method jtm = toJavaType(constructor);
        if (methodMatchers.stream().anyMatch(mm -> mm.matches(jtm))) {
            return true;
        }
        return null;
    }

    @Override
    public Boolean permitMethod(final Method method) {
        final JavaType.Method jtm = toJavaType(method);
        if (methodMatchers.stream().anyMatch(mm -> mm.matches(jtm))) {
            return true;
        }
        return null;
    }

    /**
     * We need to convert the method we have from reflection to an open rewrite JavaType. There is no direct conversion
     * available in the API, but internally the <a href=
     * "https://github.com/openrewrite/rewrite/blob/main/rewrite-java/src/main/java/org/openrewrite/java/internal/JavaReflectionTypeMapping.java">org.openrewrite.java.internal.JavaReflectionTypeMapping</a>
     * class gives us clues on how to make this work. (Note the openrewrite project is Apache licensed.)
     *
     * @param method the method to convert
     * @return the converted method
     */
    static JavaType.Method toJavaType(final Method method) {
        final JavaType declaringClass = classType(method.getDeclaringClass());

        final String[] parameterNames =
                Arrays.stream(method.getParameters()).map(Parameter::getName).toArray(String[]::new);
        final JavaType[] parameterTypes = Arrays.stream(method.getParameters()).map(Parameter::getType)
                .map(MethodListInvocationValidator::classType)
                .toArray(JavaType[]::new);
        // exceptions, annotations, defaultValue, declaredFormalTypes ignored
        return new JavaType.Method(null, method.getModifiers(), (JavaType.FullyQualified) declaringClass,
                method.getName(), classType(method.getReturnType()), parameterNames, parameterTypes, null, null, null,
                null);
    }

    /**
     * We need to convert the constructor we have from reflection to an open rewrite JavaType. There is no direct
     * conversion available in the API, but internally the <a href=
     * "https://github.com/openrewrite/rewrite/blob/main/rewrite-java/src/main/java/org/openrewrite/java/internal/JavaReflectionTypeMapping.java">org.openrewrite.java.internal.JavaReflectionTypeMapping</a>
     * class gives us clues on how to make this work. (Note the openrewrite project is Apache licensed.)
     *
     * @param method the method to convert
     * @return the converted method
     */
    static JavaType.Method toJavaType(final Constructor<?> method) {
        final JavaType declaringClass = classType(method.getDeclaringClass());

        final String[] parameterNames =
                Arrays.stream(method.getParameters()).map(Parameter::getName).toArray(String[]::new);
        final JavaType[] parameterTypes = Arrays.stream(method.getParameters()).map(Parameter::getType)
                .map(MethodListInvocationValidator::classType)
                .toArray(JavaType[]::new);
        // exceptions, annotations, defaultValue, declaredFormalTypes ignored
        return new JavaType.Method(null, method.getModifiers(), (JavaType.FullyQualified) declaringClass,
                "<constructor>", null, parameterNames, parameterTypes, null, null, null,
                null);
    }

    private static JavaType classType(Class<?> declaringClass) {
        if (declaringClass.isArray()) {
            final Class<?> elementType = declaringClass.getComponentType();
            return new JavaType.Array(null, classType(elementType), null);
        }
        if (declaringClass.isPrimitive()) {
            return JavaType.Primitive.fromKeyword(declaringClass.getSimpleName());
        }

        JavaType.Class.Kind kind;
        final int modifiers = declaringClass.getModifiers();
        if (declaringClass.isEnum()) {
            kind = JavaType.Class.Kind.Enum;
        } else if (declaringClass.isAnnotation()) {
            kind = JavaType.Class.Kind.Annotation;
        } else if (declaringClass.isInterface()) {
            kind = JavaType.Class.Kind.Interface;
        } else {
            kind = JavaType.Class.Kind.Class;
        }

        return new JavaType.Class(null, modifiers, declaringClass.getCanonicalName(), kind, null, null, null, null,
                null, null, null);
    }
}
