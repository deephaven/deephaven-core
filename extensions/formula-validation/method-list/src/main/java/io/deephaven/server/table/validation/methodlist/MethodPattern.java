//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.lang.reflect.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A pattern that matches methods, by name as well as by declaring class and argument list.
 */
final class MethodPattern extends MemberPattern {
    private final Predicate<String> name;

    MethodPattern(final Pattern declaringType, final String name, final List<Object> arguments) {
        super(declaringType, arguments);
        if (name.indexOf('*') < 0) {
            this.name = name::equals;
        } else if (name.replace("*", "").isEmpty()) {
            this.name = methodName -> true;
        } else {
            this.name = Pattern.compile(globToRegex(name, ".*")).asMatchPredicate();
        }
    }

    /**
     * Does this pattern match the given method?
     *
     * <p>
     * A method matches when its name and parameter types match and either its declaring class matches, or it is an
     * instance method that overrides a method declared by a class or interface that matches. A bridge method declared
     * by the matching class or interface is never the overridden method, though a bridge may itself be the overriding
     * method. For an override of a generic method, the argument list may match either the overriding or the overridden
     * parameter types.
     * </p>
     *
     * @param method the method to test
     * @return true if the method matches
     */
    boolean matches(final Method method) {
        if (!name.test(method.getName())) {
            return false;
        }
        final Class<?> declaringClass = method.getDeclaringClass();
        if (declaringTypeMatches(declaringClass) && argumentsMatch(method.getParameterTypes())) {
            return true;
        }
        if (Modifier.isStatic(method.getModifiers()) || Modifier.isPrivate(method.getModifiers())) {
            return false;
        }

        final Deque<Class<?>> pending = new ArrayDeque<>(directSupertypes(declaringClass));
        final Set<Class<?>> visited = new HashSet<>();
        while (!pending.isEmpty()) {
            final Class<?> supertype = pending.pop();
            if (!visited.add(supertype)) {
                continue;
            }
            if (declaringTypeMatches(supertype)) {
                for (final Method candidate : supertype.getDeclaredMethods()) {
                    if (isOverriddenBy(candidate, method)
                            && (argumentsMatch(method.getParameterTypes())
                                    || argumentsMatch(candidate.getParameterTypes()))) {
                        return true;
                    }
                }
            }
            pending.addAll(directSupertypes(supertype));
        }
        return false;
    }

    private static List<Class<?>> directSupertypes(final Class<?> type) {
        final List<Class<?>> result = new ArrayList<>(Arrays.asList(type.getInterfaces()));
        if (type.getSuperclass() != null) {
            result.add(type.getSuperclass());
        } else if (type.isInterface()) {
            // an interface implicitly declares the public methods of Object
            result.add(Object.class);
        }
        return result;
    }

    /**
     * Is {@code candidate}, declared by a supertype of {@code method}'s declaring class, overridden by {@code method}?
     * Type variables in the candidate's parameters are resolved along the inheritance path from the overriding class,
     * so that {@code Integer.compareTo(Integer)} overrides {@code Comparable.compareTo(T)}. A bridge candidate is never
     * overridden, and a bridge {@code method} is compared with the erasure of the candidate, as its signature is.
     */
    private static boolean isOverriddenBy(final Method candidate, final Method method) {
        final int modifiers = candidate.getModifiers();
        if (!candidate.getName().equals(method.getName())
                || candidate.getParameterCount() != method.getParameterCount()
                || Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers) || candidate.isBridge()) {
            return false;
        }
        if (method.getDeclaringClass().isInterface() && !Modifier.isPublic(modifiers)) {
            // an interface overrides only public methods, as it implicitly declares only the public methods of Object
            return false;
        }
        if (!Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers)
                && !samePackage(candidate.getDeclaringClass(), method.getDeclaringClass())
                && !overriddenThroughSuperclass(candidate, method)) {
            // a package-private method is only overridden within its own runtime package, or transitively through an
            // override declared there
            return false;
        }
        final Map<TypeVariable<?>, Class<?>> bindings;
        if (method.isBridge()) {
            // a bridge has the erased signature of the method it overrides, so compare it with the candidate's erasure
            bindings = Map.of();
        } else {
            // the implicit Object supertype of an interface is not among its generic supertypes, and binds nothing
            final Map<TypeVariable<?>, Class<?>> found = supertypeBindings(candidate.getDeclaringClass(),
                    method.getDeclaringClass(), Map.of(), false, new HashSet<>());
            bindings = found == null ? Map.of() : found;
        }
        if (!Arrays.equals(candidate.getParameterTypes(), method.getParameterTypes())) {
            final Type[] candidateParameters = candidate.getGenericParameterTypes();
            final Class<?>[] methodParameters = method.getParameterTypes();
            for (int pi = 0; pi < methodParameters.length; ++pi) {
                if (erase(candidateParameters[pi], bindings) != methodParameters[pi]) {
                    return false;
                }
            }
        }
        // generated bytecode may declare a same-signature method whose return type is not substitutable
        final Class<?> candidateReturn = erase(candidate.getGenericReturnType(), bindings);
        final Class<?> methodReturn = method.getReturnType();
        if (candidateReturn == null) {
            return false;
        }
        if (candidateReturn.isPrimitive() || methodReturn.isPrimitive()) {
            return candidateReturn == methodReturn;
        }
        return candidateReturn.isAssignableFrom(methodReturn);
    }

    /**
     * Does {@code method} override {@code candidate} through a method declared by a class between them, such as a
     * public override in the candidate's package of a package-private candidate?
     */
    private static boolean overriddenThroughSuperclass(final Method candidate, final Method method) {
        for (Class<?> type = method.getDeclaringClass().getSuperclass(); type != null
                && type != candidate.getDeclaringClass(); type = type.getSuperclass()) {
            for (final Method intermediate : type.getDeclaredMethods()) {
                if (isOverriddenBy(candidate, intermediate) && isOverriddenBy(intermediate, method)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean samePackage(final Class<?> first, final Class<?> second) {
        return first.getClassLoader() == second.getClassLoader()
                && first.getPackageName().equals(second.getPackageName());
    }

    /**
     * Find the erased type arguments of {@code target} as a supertype of {@code type}, following the generic supertypes
     * of each class on the way so that every class is resolved in its own context.
     *
     * @param target the supertype whose type variables to bind
     * @param type the class to search from
     * @param context the erasures of the type variables of {@code type}, and of its enclosing classes
     * @param raw whether {@code type} is used as a raw type, in which case its supertypes are erased
     * @param visited the classes already searched
     * @return the erasures of the type variables of {@code target} and its enclosing classes, empty when it is reached
     *         through a raw type, or null when {@code target} is not a generic supertype of {@code type}
     */
    private static Map<TypeVariable<?>, Class<?>> supertypeBindings(final Class<?> target, final Class<?> type,
            final Map<TypeVariable<?>, Class<?>> context, final boolean raw, final Set<Class<?>> visited) {
        if (type == target) {
            return context;
        }
        if (!visited.add(type)) {
            return null;
        }
        final List<Type> genericSupertypes = new ArrayList<>(Arrays.asList(type.getGenericInterfaces()));
        if (type.getGenericSuperclass() != null) {
            genericSupertypes.add(type.getGenericSuperclass());
        }
        for (final Type supertype : genericSupertypes) {
            final Class<?> supertypeClass;
            final Map<TypeVariable<?>, Class<?>> supertypeContext = new HashMap<>();
            final boolean supertypeRaw;
            if (supertype instanceof ParameterizedType) {
                final ParameterizedType parameterized = (ParameterizedType) supertype;
                supertypeClass = (Class<?>) parameterized.getRawType();
                // the supertype of a raw type is the erasure of its declared supertype, so it is raw too
                supertypeRaw = raw;
                if (!raw) {
                    // the owner of an inner class, as in Outer<String>.Inner, binds the type variables of Outer
                    Type owner = parameterized;
                    while (owner instanceof ParameterizedType) {
                        final ParameterizedType ownerType = (ParameterizedType) owner;
                        final TypeVariable<?>[] variables = ((Class<?>) ownerType.getRawType()).getTypeParameters();
                        final Type[] typeArguments = ownerType.getActualTypeArguments();
                        for (int vi = 0; vi < variables.length; ++vi) {
                            supertypeContext.put(variables[vi], erase(typeArguments[vi], context));
                        }
                        owner = ownerType.getOwnerType();
                    }
                }
            } else {
                // a generic supertype is either a ParameterizedType or a Class, which is raw if the class is generic
                supertypeClass = (Class<?>) supertype;
                supertypeRaw = isGeneric(supertypeClass);
            }
            final Map<TypeVariable<?>, Class<?>> result =
                    supertypeBindings(target, supertypeClass, supertypeContext, supertypeRaw, visited);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Is {@code type} generic, so that a reference to it without type arguments is raw? An inner member class is
     * generic when a class declaring it, up to the first static class, declares type parameters. A local or anonymous
     * class has no declaring class, so the type parameters of the class around it do not make it raw.
     */
    private static boolean isGeneric(final Class<?> type) {
        for (Class<?> enclosing = type; enclosing != null; enclosing =
                Modifier.isStatic(enclosing.getModifiers()) ? null : enclosing.getDeclaringClass()) {
            if (enclosing.getTypeParameters().length > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Erase a type, using {@code bindings} for the erasures of type variables bound by the inheritance path and the
     * erasure of its leftmost bound for any other type variable.
     *
     * @return the erasure, or null for a type that a parameter or return type cannot have, so that it matches nothing
     */
    private static Class<?> erase(final Type type, final Map<TypeVariable<?>, Class<?>> bindings) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        if (type instanceof GenericArrayType) {
            final Class<?> component = erase(((GenericArrayType) type).getGenericComponentType(), bindings);
            return component == null ? null : Array.newInstance(component, 0).getClass();
        }
        if (type instanceof TypeVariable) {
            if (bindings.containsKey(type)) {
                // null for a type argument that cannot be erased, which matches nothing
                return bindings.get(type);
            }
            return erase(((TypeVariable<?>) type).getBounds()[0], bindings);
        }
        return null;
    }
}
