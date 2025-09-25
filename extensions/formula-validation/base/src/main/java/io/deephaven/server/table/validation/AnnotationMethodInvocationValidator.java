//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.engine.validation.MethodInvocationValidator;
import io.deephaven.util.annotations.UserInvocationPermitted;
import io.deephaven.util.annotations.UserInvocationPermitted.ScopeType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An invocation validator that implements the rules documented in the {@link UserInvocationPermitted} annotation.
 */
public class AnnotationMethodInvocationValidator implements MethodInvocationValidator {
    @NotNull
    private final Set<String> permittedAnnotationSets;

    /**
     * Instead of reading annotations on each check, we can remember the answer when a class is fully permitted; for
     * method-level caching see the {@link CachingMethodInvocationValidator}.
     */
    private final Set<Class<?>> cachedPermittedStaticTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Class<?>> cachedPermittedInstanceTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public AnnotationMethodInvocationValidator(@NotNull final Set<String> permittedAnnotationSets) {
        this.permittedAnnotationSets = permittedAnnotationSets;
    }

    @Override
    public Boolean permitConstructor(final Constructor<?> constructor) {
        if (isPermittedSet(constructor.getAnnotation(UserInvocationPermitted.class))) {
            return true;
        }
        return null;
    }

    @Override
    public Boolean permitMethod(final Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            return checkStaticAnnotations(method) ? true : null;
        } else {
            return checkInstanceAnnotations(method) ? true : null;
        }
    }

    private boolean checkStaticAnnotations(final Method methodToCheck) {
        final Class<?> declaringClass = methodToCheck.getDeclaringClass();
        if (cachedPermittedStaticTargets.contains(declaringClass)) {
            return true;
        }

        final UserInvocationPermitted methodAnnotation = methodToCheck.getAnnotation(UserInvocationPermitted.class);
        if (isPermittedSet(methodAnnotation)) {
            return true;
        }

        final UserInvocationPermitted classAnnotation = declaringClass.getAnnotation(UserInvocationPermitted.class);
        if (isStaticPermitted(classAnnotation) && isPermittedSet(classAnnotation)) {
            cachedPermittedStaticTargets.add(declaringClass);
            return true;
        }

        return false;
    }

    private boolean isPermittedSet(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final String[] sets = annotation.value();
        return sets != null && Arrays.stream(sets).anyMatch(permittedAnnotationSets::contains);
    }

    private boolean isStaticPermitted(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final ScopeType scope = annotation.classScope();
        return scope == null || scope.equals(ScopeType.StaticAndInstance) || scope.equals(ScopeType.Static);
    }

    private boolean isInstancePermitted(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final ScopeType scope = annotation.classScope();
        return scope == null || scope.equals(ScopeType.StaticAndInstance) || scope.equals(ScopeType.Instance);
    }

    private boolean checkInstanceAnnotations(final Method methodToCheck) {
        final Class<?> declaringClass = methodToCheck.getDeclaringClass();
        if (cachedPermittedInstanceTargets.contains(declaringClass)) {
            return true;
        }

        final UserInvocationPermitted methodAnnotation = methodToCheck.getAnnotation(UserInvocationPermitted.class);
        if (isPermittedSet(methodAnnotation)) {
            return true;
        }

        final UserInvocationPermitted classAnnotation = declaringClass.getAnnotation(UserInvocationPermitted.class);
        if (isInstancePermitted(classAnnotation) && isPermittedSet(classAnnotation)) {
            cachedPermittedInstanceTargets.add(declaringClass);
            return true;
        }

        // now check if the declaring class has any interfaces; and if so then we need to examine the interfaces for
        // this method/annotation
        final ArrayDeque<Type> interfaceQueue = new ArrayDeque<>();
        Class<?> superclass = declaringClass;
        while (superclass != null && superclass != java.lang.Object.class) {
            interfaceQueue.addAll(Arrays.asList(superclass.getGenericInterfaces()));
            superclass = superclass.getSuperclass();
        }
        while (!interfaceQueue.isEmpty()) {
            final Type interfaceType = interfaceQueue.removeFirst();

            final ParameterizedType parameterizedType;
            final Class<?> interfaceClass;

            if (interfaceType instanceof ParameterizedType) {
                parameterizedType = (ParameterizedType) interfaceType;
                interfaceClass = (Class<?>) parameterizedType.getRawType();
            } else if (interfaceType instanceof Class) {
                parameterizedType = null;
                interfaceClass = (Class<?>) interfaceType;
            } else {
                throw new UnsupportedOperationException("Cannot handle interface of type: " + interfaceType + ", "
                        + interfaceType.getClass().getCanonicalName());
            }

            // first determine if this method exists in the interface
            Method interfaceMethod;
            try {
                interfaceMethod =
                        interfaceClass.getMethod(methodToCheck.getName(), methodToCheck.getParameterTypes());
            } catch (NoSuchMethodException e) {
                // let's do the more exhaustive matching logic, because there could be something compatible but
                // with a slightly more permissive signature than the exact match
                interfaceMethod = findMatchingMethod(interfaceClass, methodToCheck, parameterizedType);
            }

            if (interfaceMethod != null) {
                final UserInvocationPermitted interfaceMethodAnnotation =
                        interfaceMethod.getAnnotation(UserInvocationPermitted.class);
                if (isPermittedSet(interfaceMethodAnnotation)) {
                    return true;
                }
                final UserInvocationPermitted interfaceClassAnnotation =
                        interfaceClass.getAnnotation(UserInvocationPermitted.class);
                if (isInstancePermitted(interfaceClassAnnotation) && isPermittedSet(interfaceClassAnnotation)) {
                    return true;
                }
            }

            // if we were unsuccessful, then we should examine any interfaces we extend from
            interfaceQueue.addAll(Arrays.asList(interfaceClass.getGenericInterfaces()));
        }

        return false;
    }

    private Method findMatchingMethod(final Class<?> interfaceClass, final Method methodToCheck,
            final ParameterizedType parameterizedInterface) {
        final List<Method> candidates = Arrays.stream(interfaceClass.getMethods())
                .filter(interfaceMethod -> interfaceMethod.getName().equals(methodToCheck.getName())
                        && parametersMatch(interfaceMethod, methodToCheck, parameterizedInterface, interfaceClass)
                        && interfaceMethod.getReturnType().isAssignableFrom(methodToCheck.getReturnType()))
                .collect(Collectors.toList());

        if (candidates.size() == 1) {
            // if there is nothing found, or any ambiguity we cannot permit this method
            return candidates.get(0);
        }

        return null;
    }

    private boolean parametersMatch(final Method interfaceMethod,
            final Method methodToCheck,
            @Nullable final ParameterizedType parameterizedInterface,
            final Class<?> interfaceClass) {
        if (interfaceMethod.getParameterCount() != methodToCheck.getParameterCount()) {
            return false;
        }

        final Class<?>[] interfaceTypes = interfaceMethod.getParameterTypes();
        final Class<?>[] checkTypes = methodToCheck.getParameterTypes();

        final java.lang.reflect.Type[] interfaceGenerics = interfaceMethod.getGenericParameterTypes();
        final java.lang.reflect.Type[] checkGenerics = methodToCheck.getGenericParameterTypes();

        final Map<String, Type> interfaceTypeVariables = new HashMap<>();
        if (parameterizedInterface != null) {
            // if we do have a parameterized interface, then we fill in the type parameters; without the parameterized
            // interface we just leave the map empty and the overriding methods must exactly match the declared class
            for (int ii = 0; ii < interfaceClass.getTypeParameters().length; ii++) {
                interfaceTypeVariables.put(interfaceClass.getTypeParameters()[ii].getName(),
                        parameterizedInterface.getActualTypeArguments()[ii]);
            }
        }

        for (int pp = 0; pp < interfaceMethod.getParameterCount(); pp++) {
            if (!checkTypes[pp].equals(interfaceTypes[pp])) {
                if (interfaceGenerics[pp] instanceof TypeVariable && checkGenerics[pp] instanceof Class) {
                    final TypeVariable<?> typeVar = (TypeVariable<?>) interfaceGenerics[pp];
                    final Type expectedType = interfaceTypeVariables.get(typeVar.getName());
                    if (checkGenerics[pp].equals(expectedType)) {
                        continue;
                    }
                }
                return false;
            }
        }
        return true;
    }
}
