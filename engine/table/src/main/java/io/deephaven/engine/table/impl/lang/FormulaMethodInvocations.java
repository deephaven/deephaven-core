//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.lang;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of methods that were used by a formula column.
 *
 * <p>
 * <b>Note: this class is not part of the public Deephaven API, and may change without notice.</b>
 * </p>
 */
public class FormulaMethodInvocations {
    private final List<Method> usedMethods = new ArrayList<>();
    private final List<Constructor<?>> usedConstructors = new ArrayList<>();
    private boolean usedImplicitCalls = false;

    public List<Method> getUsedMethods() {
        return usedMethods;
    }

    public List<Constructor<?>> getUsedConstructors() {
        return usedConstructors;
    }

    public boolean hasImplicitCalls() {
        return usedImplicitCalls;
    }

    void add(final Method method) {
        usedMethods.add(method);
    }

    void add(final Constructor<?> constructor) {
        usedConstructors.add(constructor);
    }

    void setUsedImplicitCall() {
        this.usedImplicitCalls = true;
    }
}
