//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.server.table.validation.CachingMethodInvocationValidator;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.JavaType;

public class TestMethodListCaching {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testMethodCachingBehavior() throws NoSuchMethodException {
        final List<String> disallowedMethods = List.of("java.lang.StringBuilder toString()");
        final List<String> allowedMethods = List.of("java.lang.Object toString()");

        final MutableInt allowedMethodCount = new MutableInt(0);
        final MutableInt disallowedMethodCount = new MutableInt(0);
        final MutableInt nullMethodCount = new MutableInt(0);

        final MethodListInvocationValidator validator = new MethodListInvocationValidator(allowedMethods) {
            final List<MethodMatcher> disallowedMatchers =
                    disallowedMethods.stream().map(MethodMatcher::new).collect(Collectors.toUnmodifiableList());

            @Override
            public Boolean permitMethod(Method method) {
                final JavaType.Method jtm = toJavaType(method);

                if (disallowedMatchers.stream().anyMatch(mm -> mm.matches(jtm))) {
                    disallowedMethodCount.increment();
                    return false;
                }
                Boolean result = super.permitMethod(method);
                if (result == null) {
                    nullMethodCount.increment();
                } else if (!result) {
                    throw new IllegalStateException();
                } else {
                    allowedMethodCount.increment();
                }
                return result;
            }
        };
        final CachingMethodInvocationValidator cachingValidator = new CachingMethodInvocationValidator(validator);

        final Method integerToString = Integer.class.getMethod("toString");
        final Method toUnsignedLong = Integer.class.getMethod("toUnsignedLong", int.class);
        final Method stringBuilderToString = StringBuilder.class.getMethod("toString");

        Assert.assertTrue(cachingValidator.permitMethod(integerToString));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(0, disallowedMethodCount.get());
        Assert.assertEquals(0, nullMethodCount.get());
        Assert.assertFalse(cachingValidator.permitMethod(stringBuilderToString));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(0, nullMethodCount.get());
        Assert.assertNull(cachingValidator.permitMethod(toUnsignedLong));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(1, nullMethodCount.get());

        // verify we did not need to ask the validator a second time
        Assert.assertTrue(cachingValidator.permitMethod(integerToString));
        Assert.assertFalse(cachingValidator.permitMethod(stringBuilderToString));
        Assert.assertNull(cachingValidator.permitMethod(toUnsignedLong));
        Assert.assertEquals(1, allowedMethodCount.get());
        Assert.assertEquals(1, disallowedMethodCount.get());
        Assert.assertEquals(1, nullMethodCount.get());
    }
}
