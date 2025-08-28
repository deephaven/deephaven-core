//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.csv.util.MutableObject;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A MethodInvocationValidator that caches all results of an underlying MethodInvocationValidator.
 *
 * <p>
 * This caching layer is not suitable for use with a method validator that produces different results based on context
 * (e.g., permitting some users to execute methods that others may not).
 * </p>
 */
public class CachingMethodInvocationValidator implements MethodInvocationValidator {
    private final static MutableObject<Boolean> CACHED_FALSE = new MutableObject<>(false);
    private final static MutableObject<Boolean> CACHED_TRUE = new MutableObject<>(true);
    private final static MutableObject<Boolean> CACHED_NULL = new MutableObject<>(null);

    final MethodInvocationValidator delegate;

    private final Map<Constructor<?>, MutableObject<Boolean>> cachedConstructors = new ConcurrentHashMap<>();
    private final Map<Method, MutableObject<Boolean>> cachedMethods = new ConcurrentHashMap<>();

    public CachingMethodInvocationValidator(MethodInvocationValidator delegate) {
        this.delegate = delegate;
    }

    @Override
    public Boolean permitConstructor(Constructor<?> constructor) {
        final MutableObject<Boolean> cachedResult = cachedConstructors.get(constructor);
        if (cachedResult != null) {
            return cachedResult.getValue();
        }
        Boolean result = delegate.permitConstructor(constructor);
        cachedConstructors.put(constructor, convertToCached(result));
        return result;
    }

    @Override
    public Boolean permitMethod(Method method) {
        final MutableObject<Boolean> cachedResult = cachedMethods.get(method);
        if (cachedResult != null) {
            return cachedResult.getValue();
        }
        Boolean result = delegate.permitMethod(method);
        cachedMethods.put(method, convertToCached(result));
        return result;
    }

    private MutableObject<Boolean> convertToCached(Boolean result) {
        if (result == null) {
            return CACHED_NULL;
        } else if (result) {
            return CACHED_TRUE;
        } else {
            return CACHED_FALSE;
        }
    }
}
