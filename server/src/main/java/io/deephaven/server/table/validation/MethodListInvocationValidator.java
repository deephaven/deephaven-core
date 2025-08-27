//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.UncheckedDeephavenException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * An invocation validator that has a hardcoded list of classes and methods to permit.
 */
public class MethodListInvocationValidator implements MethodInvocationValidator {
    private final Set<Class<?>> allowedInstanceTargets;
    private final Set<Class<?>> allowedStaticTargets;

    /**
     * These methods are permitted. We key by name so that the search does not need to examine every permitted method.
     */
    private final Map<String, Set<Method>> allowedInstanceMethods;
    private final Map<String, Set<Method>> allowedStaticMethods;

    public MethodListInvocationValidator(final MethodList methodList) {
        this.allowedInstanceTargets = methodList.instanceTargets();
        this.allowedStaticTargets = methodList.staticTargets();

        final Collector<Method, ?, Map<String, Set<Method>>> setToMapCollector =
                Collectors.toUnmodifiableMap(Method::getName, x -> {
                    final HashSet<Method> hs = new HashSet<>();
                    hs.add(x);
                    return hs;
                }, (hs1, hs2) -> {
                    hs1.addAll(hs2);
                    return hs1;
                });

        this.allowedInstanceMethods = methodList.instanceMethods().stream().collect(setToMapCollector);
        this.allowedStaticMethods = methodList.staticMethods().stream().collect(setToMapCollector);
    }

    public static Method toMethod(final String userTarget, final boolean isStatic) {
        final String target = userTarget.trim();
        final int hashIdx = target.indexOf('#');
        if (hashIdx == -1) {
            throw new IllegalArgumentException("Invalid method target: " + target);
        }
        final String className = target.substring(0, hashIdx).trim();
        final String methodSignature = target.substring(hashIdx + 1).trim();
        final int openParen = methodSignature.indexOf("(");
        final int closeParen = methodSignature.lastIndexOf(")");
        if (openParen < 0) {
            throw new IllegalArgumentException(
                    "Invalid method target, must have opening parenthesis for parameter types: " + target);
        }
        if (closeParen != methodSignature.length() - 1) {
            throw new IllegalArgumentException("Invalid method target, must end with closing parenthesis: " + target);
        }
        final String methodName = methodSignature.substring(0, openParen).trim();
        final String paramSpec = methodSignature.substring(openParen + 1, closeParen).trim();
        final String[] params;
        if (paramSpec.isEmpty()) {
            params = new String[0];
        } else {
            params = paramSpec.split(",\\s*");
            for (int pp = 0; pp < params.length; pp++) {
                params[pp] = params[pp].trim();
            }
        }
        try {
            final Class<?> clazz = findMaybeInnerClass(className);
            final List<Method> foundMethods = Arrays.stream(clazz.getDeclaredMethods())
                    .filter(m -> m.getName().equals(methodName) && paramsMatch(params, m.getParameterTypes()))
                    .collect(Collectors.toList());
            if (foundMethods.isEmpty()) {
                throw new IllegalArgumentException("Could not find method " + target + " in " + className);
            }
            if (foundMethods.size() > 1) {
                throw new IllegalArgumentException(
                        "Could not find unambiguous method " + target + " in " + className + ", found " + foundMethods);
            }
            if (isStatic) {
                if (!Modifier.isStatic(foundMethods.get(0).getModifiers())) {
                    throw new IllegalArgumentException("Expected static method for " + target + " in " + className);
                }
            } else {
                if (Modifier.isStatic(foundMethods.get(0).getModifiers())) {
                    throw new IllegalArgumentException(
                            "Expected instance method for " + target + " in " + className + " but was static.");
                }
            }
            return foundMethods.get(0);
        } catch (ClassNotFoundException e) {
            throw new UncheckedDeephavenException(e);
        }
    }

    /**
     * If the className is a String representing a Java primitive, then use the primitive class. Otherwise, search using
     * our inner class rules
     *
     * @param className the classname to find
     * @return the Class represented by className
     * @throws ClassNotFoundException if the class was not found
     */
    private static Class<?> findPrimitiveOrMaybeInnerClass(final String className) throws ClassNotFoundException {
        switch (className.trim()) {
            case "boolean":
                return boolean.class;
            case "byte":
                return byte.class;
            case "char":
                return char.class;
            case "short":
                return short.class;
            case "int":
                return int.class;
            case "long":
                return long.class;
            case "float":
                return float.class;
            case "double":
                return double.class;
            default:
                return findMaybeInnerClass(className);
        }
    }

    private static Class<?> findMaybeInnerClass(final String className) throws ClassNotFoundException {
        if (className.isEmpty()) {
            throw new IllegalArgumentException("Class name may not be empty.");
        }

        ClassNotFoundException originalException = null;
        int innerLevels = 0;
        boolean replaced = false;

        String toFind = className;

        while (true) {
            try {
                final Class<?> clazz =
                        Class.forName(toFind, false, MethodListInvocationValidator.class.getClassLoader());
                if (innerLevels == 0) {
                    return clazz;
                } else {
                    // we need to put back our inner class markers as dollar signs
                    toFind = toFind + className.substring(toFind.length()).replace('.', '$');
                    replaced = true;
                    break;
                }
            } catch (ClassNotFoundException e) {
                if (originalException == null) {
                    originalException = e;
                }
                innerLevels++;
                final int lastSeparator = toFind.lastIndexOf('.');
                if (lastSeparator == -1) {
                    break;
                }
                toFind = toFind.substring(0, lastSeparator);
            }
        }

        if (replaced) {
            return Class.forName(toFind, false, MethodListInvocationValidator.class.getClassLoader());
        }

        throw originalException;
    }

    private static boolean paramsMatch(final String[] configuredParameters,
            final Class<?>[] candidateMethodParameters) {
        if (candidateMethodParameters.length != configuredParameters.length) {
            return false;
        }
        for (int pp = 0; pp < candidateMethodParameters.length; pp++) {
            final Class<?> configuredClass;
            try {
                configuredClass = findPrimitiveOrMaybeInnerClass(configuredParameters[pp]);
            } catch (ClassNotFoundException e) {
                throw new UncheckedDeephavenException(e);
            }
            if (!candidateMethodParameters[pp].equals(configuredClass)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean permitConstructor(final Constructor<?> constructor) {
        return null;
    }

    @Override
    public Boolean permitMethod(final Method method) {
        final Class<?> declaringClass = method.getDeclaringClass();
        if (Modifier.isStatic(method.getModifiers())) {
            if (allowedStaticTargets.contains(declaringClass)) {
                // all methods on these classes are permitted
                return true;
            }
            final Set<Method> allowedMethods = allowedStaticMethods.get(method.getName());
            if (allowedMethods != null && allowedMethods.contains(method)) {
                return true;
            }
        } else {
            if (allowedInstanceTargets.contains(declaringClass)) {
                return true;
            }
            if (allowedInstanceTargets.stream().anyMatch(t -> t.isAssignableFrom(declaringClass))) {
                return true;
            }
            final Set<Method> methodsToMatch = allowedInstanceMethods.get(method.getName());
            if (methodsToMatch != null
                    && methodsToMatch.stream().anyMatch(allowed -> checkOneMethod(allowed, method))) {
                return true;
            }
        }
        return null;
    }

    private static boolean checkOneMethod(final Method allowed, final Method methodToCheck) {
        if (!allowed.getDeclaringClass().isAssignableFrom(methodToCheck.getDeclaringClass())) {
            return false;
        }
        if (!allowed.getName().equals(methodToCheck.getName())) {
            return false;
        }
        if (allowed.getParameterCount() != methodToCheck.getParameterCount()) {
            return false;
        }
        // Now check the parameter types
        for (int pp = 0; pp < allowed.getParameterCount(); ++pp) {
            final Class<?>[] allowedParameterTypes = allowed.getParameterTypes();
            final Class<?>[] checkParameterTypes = methodToCheck.getParameterTypes();
            if (!checkParameter(allowedParameterTypes[pp], checkParameterTypes[pp])) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkParameter(final Class<?> allowedParameterType, final Class<?> checkParameterType) {
        // Permit assignability of the base method's parameter to the checked methods type (i.e. the overriding method
        // can be less restrictive as to the type)
        return allowedParameterType.isAssignableFrom(checkParameterType);
    }
}
