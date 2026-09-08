//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation;

import io.deephaven.engine.validation.MethodInvocationValidator;

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
    private enum CachedResult {
        FALSE(false), TRUE(true), NULL(null);

        final Boolean value;

        CachedResult(Boolean value) {
            this.value = value;
        }

        public Boolean getValue() {
            return value;
        }
    }

    final MethodInvocationValidator delegate;

    private final Map<Constructor<?>, CachedResult> cachedConstructors = new ConcurrentHashMap<>();
    private final Map<Method, CachedResult> cachedMethods = new ConcurrentHashMap<>();

    public CachingMethodInvocationValidator(MethodInvocationValidator delegate) {
        this.delegate = delegate;
    }

    @Override
    public Boolean permitConstructor(Constructor<?> constructor) {
        final CachedResult cachedResult = cachedConstructors.get(constructor);
        if (cachedResult != null) {
            return cachedResult.getValue();
        }
        Boolean result = delegate.permitConstructor(constructor);
        cachedConstructors.put(constructor, convertToCached(result));
        return result;
    }

    @Override
    public Boolean permitMethod(Method method) {
        final CachedResult cachedResult = cachedMethods.get(method);
        if (cachedResult != null) {
            return cachedResult.getValue();
        }
        Boolean result = delegate.permitMethod(method);
        cachedMethods.put(method, convertToCached(result));
        return result;
    }

    private CachedResult convertToCached(Boolean result) {
        if (result == null) {
            return CachedResult.NULL;
        } else if (result) {
            return CachedResult.TRUE;
        } else {
            return CachedResult.FALSE;
        }
    }
}
