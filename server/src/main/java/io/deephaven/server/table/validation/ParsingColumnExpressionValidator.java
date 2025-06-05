//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.annotations.UserInvocationPermitted;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.lang.FormulaMethodInvocations;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Validates a column expression coming from the web api, to ensure that the included code will use the customer defined
 * list of allowed methods.
 *
 * <p>
 * The formula is compiled as part of this validation, and the
 * {@link io.deephaven.engine.table.impl.lang.QueryLanguageParser} produces a list of the methods that the formula would
 * invoke if executed. The set of used methods is compared against our allowlist.
 * </p>
 *
 * <p>
 * This validator is stricter than the {@link MethodNameColumnExpressionValidator}, which does not take the class
 * instances into account.
 * </p>
 *
 * <p>
 * The {@link ExpressionValidatorModule#getParsingColumnExpressionValidatorFromConfiguration(Configuration)} method is
 * used to create a validator based on configuration properties.
 * </p>
 */
public class ParsingColumnExpressionValidator implements ColumnExpressionValidator {
    private final Set<Class<?>> allowedInstanceTargets;
    private final Set<Class<?>> allowedStaticTargets;

    /**
     * These methods are permitted. We key by name so that the search does not need to examine every permitted method.
     */
    private final Map<String, Set<Method>> allowedInstanceMethods;

    private final Map<String, Set<Method>> allowedStaticMethods;

    @NotNull
    private final Set<String> permittedAnnotationSets;

    // instead of reading annotations on each check, we can remember the answer
    private final Set<Class<?>> cachedPermittedStaticTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Class<?>> cachedPermittedInstanceTargets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Method> cachedPermittedInstanceMethods = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Method> cachedPermittedStaticMethods = Collections.newSetFromMap(new ConcurrentHashMap<>());


    @NotNull
    public static Set<Method> buildInstanceMethodList(final List<String> targetStrings) {
        return targetStrings.stream().map(s -> toMethod(s, false)).collect(Collectors.toUnmodifiableSet());
    }

    public ParsingColumnExpressionValidator(final Set<Class<?>> allowedInstanceTargets,
            @NotNull final Set<Class<?>> allowedStaticTargets,
            @NotNull final Set<Method> allowedInstanceMethods,
            @NotNull final Set<Method> allowedStaticMethods,
            @NotNull final Set<String> permittedAnnotationSets) {
        this.allowedInstanceTargets = allowedInstanceTargets;
        this.allowedStaticTargets = allowedStaticTargets;
        this.permittedAnnotationSets = permittedAnnotationSets;

        final Collector<Method, ?, Map<String, Set<Method>>> setToMapCollector =
                Collectors.toUnmodifiableMap(Method::getName, x -> {
                    final HashSet<Method> hs = new HashSet<>();
                    hs.add(x);
                    return hs;
                }, (hs1, hs2) -> {
                    hs1.addAll(hs2);
                    return hs1;
                });
        this.allowedStaticMethods = allowedStaticMethods.stream().collect(setToMapCollector);
        this.allowedInstanceMethods = allowedInstanceMethods.stream().collect(setToMapCollector);
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
            final Class<?> clazz = Class.forName(className);
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

    private static boolean paramsMatch(final String[] configuredParameters,
            final Class<?>[] candidateMethodParameters) {
        if (candidateMethodParameters.length != configuredParameters.length) {
            return false;
        }
        for (int pp = 0; pp < candidateMethodParameters.length; pp++) {
            final Class<?> configuredClass;
            try {
                configuredClass = Class.forName(configuredParameters[pp]);
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
    public WhereFilter[] validateSelectFilters(final String[] conditionalExpressions, final Table table) {
        final WhereFilter[] whereFilters = WhereFilterFactory.getExpressions(conditionalExpressions);
        return doValidateWhereFilters(conditionalExpressions, table, whereFilters);
    }

    @Override
    public void validateConditionFilters(final List<ConditionFilter> conditionFilters, final Table table) {
        for (final ConditionFilter conditionFilter : conditionFilters) {
            doValidateWhereFilters(new String[] {conditionFilter.toString()}, table,
                    new WhereFilter[] {conditionFilter});
        }
    }

    private WhereFilter @NotNull [] doValidateWhereFilters(final String[] conditionalExpressions,
            final Table table,
            final WhereFilter[] whereFilters) {
        final List<String> dummyAssignments = new ArrayList<>();
        for (int ii = 0; ii < whereFilters.length; ++ii) {
            final WhereFilter sf = whereFilters[ii];
            if (sf instanceof ConditionFilter) {
                dummyAssignments
                        .add(String.format("__boolean_placeholder_%d__ = (%s)", ii, conditionalExpressions[ii]));
            }
        }
        if (!dummyAssignments.isEmpty()) {
            final String[] daArray = dummyAssignments.toArray(String[]::new);
            final SelectColumn[] selectColumns = SelectColumnFactory.getExpressions(daArray);
            validateColumnExpressions(selectColumns, daArray, table);
        }
        return whereFilters;
    }

    @Override
    public void validateColumnExpressions(final SelectColumn[] selectColumns, final String[] originalExpressions,
            final Table table) {
        // It's unfortunate that we have to validateSelect which does a bunch of analysis, just to get throw-away cloned
        // columns back, so we can check here for disallowed methods. (We need to make sure SwitchColumns get
        // initialized.)
        final QueryTable prototype = (QueryTable) TableTools.newTable(table.getDefinition());
        final SelectColumn[] clonedColumns = prototype.validateSelect(selectColumns).getClonedColumns();
        validateColumnExpressions(clonedColumns, originalExpressions);
    }


    private void validateColumnExpressions(final SelectColumn[] selectColumns,
            final String[] originalExpressions) {
        assert (selectColumns.length == originalExpressions.length);
        for (int ii = 0; ii < selectColumns.length; ++ii) {
            validateSelectColumn(selectColumns[ii], originalExpressions[ii]);
        }
    }

    private void validateSelectColumn(SelectColumn selectColumn, final String originalExpression) {
        while (selectColumn instanceof SwitchColumn) {
            selectColumn = ((SwitchColumn) selectColumn).getRealColumn();
        }

        if (!(selectColumn instanceof FormulaColumn)) {
            // other variants should be safe, only test DhFormulaColumn
            return;
        }

        if (selectColumn instanceof DhFormulaColumn) {
            // we can properly validate this; anything else cannot be properly validated
            final DhFormulaColumn dhFormulaColumn = (DhFormulaColumn) selectColumn;
            validateInvocations(dhFormulaColumn.getFormulaMethodInvocations());
        } else {
            throw new IllegalStateException("Cannot validate user expression: " + originalExpression);
        }
    }

    private void validateInvocations(@NotNull final FormulaMethodInvocations formulaMethodInvocations) {
        if (formulaMethodInvocations.hasImplicitCalls()) {
            throw new IllegalStateException("User expression may not use implicit method calls.");
        }
        if (!formulaMethodInvocations.getUsedConstructors().isEmpty()) {
            final String types = formulaMethodInvocations.getUsedConstructors().stream()
                    .map(c -> c.getAnnotatedReturnType().toString()).collect(Collectors.joining(", "));
            throw new IllegalStateException("User expressions are not permitted to instantiate " + types);
        }
        for (final Method m : formulaMethodInvocations.getUsedMethods()) {
            validateMethod(m);
        }
    }

    private void validateMethod(final Method m) {
        final String name = m.getName();
        final Class<?> declaringClass = m.getDeclaringClass();
        if (Modifier.isStatic(m.getModifiers())) {
            if (allowedStaticTargets.contains(declaringClass)) {
                // all methods on these classes are permitted
                return;
            }
            final Set<Method> allowedMethods = allowedStaticMethods.get(m.getName());
            if (allowedMethods != null && allowedMethods.contains(m)) {
                return;
            }
            if (checkStaticAnnotations(m)) {
                return;
            }
            throw new IllegalStateException("User expressions are not permitted to use static method " + name + "("
                    + Arrays
                            .stream(m.getParameterTypes()).map(Class::getCanonicalName)
                            .collect(Collectors.joining(", "))
                    + ") on " + declaringClass);
        } else {
            if (allowedInstanceTargets.contains(declaringClass)) {
                return;
            }
            if (allowedInstanceTargets.stream().anyMatch(t -> t.isAssignableFrom(declaringClass))) {
                return;
            }
            final Set<Method> methodsToMatch = allowedInstanceMethods.get(m.getName());
            if (methodsToMatch != null && methodsToMatch.stream().anyMatch(allowed -> checkOneMethod(allowed, m))) {
                return;
            }
            if (checkInstanceAnnotations(m)) {
                return;
            }
            throw new IllegalStateException(
                    "User expressions are not permitted to use method " + name + "(" + Arrays
                            .stream(m.getParameterTypes()).map(Class::getCanonicalName)
                            .collect(Collectors.joining(", "))
                            + ")" + " on " + declaringClass);
        }
    }

    private boolean checkStaticAnnotations(final Method methodToCheck) {
        if (cachedPermittedStaticMethods.contains(methodToCheck)) {
            return true;
        }
        final Class<?> declaringClass = methodToCheck.getDeclaringClass();
        if (cachedPermittedStaticTargets.contains(declaringClass)) {
            return true;
        }

        final UserInvocationPermitted methodAnnotation = methodToCheck.getAnnotation(UserInvocationPermitted.class);
        if (methodAnnotation != null && isPermittedSet(methodAnnotation)) {
            cachedPermittedStaticMethods.add(methodToCheck);
            return true;
        }

        final UserInvocationPermitted classAnnotation = declaringClass.getAnnotation(UserInvocationPermitted.class);
        if (classAnnotation != null && isStaticPermitted(classAnnotation) && isPermittedSet(classAnnotation)) {
            cachedPermittedStaticTargets.add(declaringClass);
            return true;
        }

        return false;
    }

    private boolean isPermittedSet(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final String[] sets = annotation.sets();
        return sets != null && Arrays.stream(sets).anyMatch(permittedAnnotationSets::contains);
    }

    private boolean isStaticPermitted(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final String scope = annotation.classScope();
        return scope == null || scope.isEmpty() || scope.equals(UserInvocationPermitted.STATIC_CLASS_SCOPE);
    }

    private boolean isInstancePermitted(final UserInvocationPermitted annotation) {
        if (annotation == null) {
            return false;
        }
        final String scope = annotation.classScope();
        return scope == null || scope.isEmpty() || scope.equals(UserInvocationPermitted.INSTANCE_CLASS_SCOPE);
    }

    private boolean checkInstanceAnnotations(final Method methodToCheck) {
        if (cachedPermittedInstanceMethods.contains(methodToCheck)) {
            return true;
        }
        final Class<?> declaringClass = methodToCheck.getDeclaringClass();
        if (cachedPermittedInstanceTargets.contains(declaringClass)) {
            return true;
        }

        final UserInvocationPermitted methodAnnotation = methodToCheck.getAnnotation(UserInvocationPermitted.class);
        if (isPermittedSet(methodAnnotation)) {
            cachedPermittedInstanceMethods.add(methodToCheck);
            return true;
        }

        final UserInvocationPermitted classAnnotation = declaringClass.getAnnotation(UserInvocationPermitted.class);
        if (isInstancePermitted(classAnnotation) && isPermittedSet(classAnnotation)) {
            cachedPermittedInstanceTargets.add(declaringClass);
            return true;
        }

        // now check if the declaring class has any interfaces; and if so then we need to examine the interfaces for
        // this method/annotation
        final ArrayDeque<Class<?>> interfaceQueue = new ArrayDeque<>();
        Class<?> superclass = declaringClass;
        while (superclass != null && superclass != java.lang.Object.class) {
            interfaceQueue.addAll(Arrays.asList(superclass.getInterfaces()));
            superclass = superclass.getSuperclass();
        }
        while (!interfaceQueue.isEmpty()) {
            Class<?> interfaceClass = interfaceQueue.removeFirst();

            Method interfaceMethod = null;
            while (interfaceClass != null) {
                // first determine if this method exists in the interface
                try {
                    interfaceMethod =
                            interfaceClass.getMethod(methodToCheck.getName(), methodToCheck.getParameterTypes());
                    break;
                } catch (NoSuchMethodException e) {
                    // this interface is useless, but it's parent could be OK
                    interfaceClass = interfaceClass.getSuperclass();
                }
            }

            if (interfaceMethod == null) {
                continue;
            }

            final UserInvocationPermitted interfaceMethodAnnotation =
                    interfaceMethod.getAnnotation(UserInvocationPermitted.class);
            if (isPermittedSet(interfaceMethodAnnotation)) {
                cachedPermittedInstanceMethods.add(methodToCheck);
                return true;
            }
            final UserInvocationPermitted interfaceClassAnnotation =
                    interfaceClass.getAnnotation(UserInvocationPermitted.class);
            if (isInstancePermitted(interfaceClassAnnotation) && isPermittedSet(interfaceClassAnnotation)) {
                cachedPermittedInstanceMethods.add(methodToCheck);
                return true;
            }
        }

        return false;
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

    public ColumnExpressionValidator withInstanceMethods(final Set<Method> instanceMethods) {
        return new ParsingColumnExpressionValidator(this.allowedInstanceTargets,
                this.allowedStaticTargets,
                instanceMethods,
                this.allowedStaticMethods.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                this.permittedAnnotationSets);
    }

    public ColumnExpressionValidator withStaticMethods(final Set<Method> staticMethods) {
        return new ParsingColumnExpressionValidator(this.allowedInstanceTargets,
                this.allowedStaticTargets,
                this.allowedInstanceMethods.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
                staticMethods,
                this.permittedAnnotationSets);
    }
}
