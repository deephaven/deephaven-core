/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility for generating a table that counts the operations performed on its ColumnSource's. Used by the grouping tests
 * to verify that we are not doing unnecessary work.
 */
class CountingTable {
    @NotNull
    static QueryTable getCountingTable(QueryTable nonCountingTable) {
        Map<String, ColumnSource<?>> countingSources = new LinkedHashMap<>();
        nonCountingTable.getColumnSourceMap()
                .forEach((key, value) -> countingSources.put(key, getCountingColumnSource(value)));
        return new QueryTable(nonCountingTable.getIndex(), countingSources);
    }

    private static <T> ColumnSource<T> getCountingColumnSource(final ColumnSource<T> inputColumnSource) {
        // noinspection unchecked
        return (ColumnSource<T>) Proxy.newProxyInstance(IndexGroupingTest.class.getClassLoader(),
                new Class[] {MethodCounter.class, ColumnSource.class},
                new CountingColumnSourceInvocationHandler(inputColumnSource));
    }

    interface MethodCounter {
        int getMethodCount(String name);

        void clear();

        @SuppressWarnings("unused")
        int getMethodCount(Method method);

        @SuppressWarnings("unused")
        void dumpMethodCounts();
    }

    static private class CountingColumnSourceInvocationHandler implements InvocationHandler {
        ColumnSource<?> wrappedColumnSource;
        Map<Method, Integer> methodCounts = new HashMap<>();

        CountingColumnSourceInvocationHandler(ColumnSource<?> wrappedColumnSource) {
            this.wrappedColumnSource = wrappedColumnSource;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final Integer count = methodCounts.get(method);
            if (count == null) {
                methodCounts.put(method, 1);
            } else {
                methodCounts.put(method, count + 1);
            }

            if (method.getDeclaringClass().equals(MethodCounter.class)) {
                switch (method.getName()) {
                    case "clear":
                        methodCounts.clear();
                        return null;
                    case "dumpMethodCounts":
                        methodCounts.forEach((k, v) -> System.out.println(k + " -> " + v));
                        return null;
                    case "getMethodCount":
                        Require.eq(args.length, "args.length", 1, "1");
                        Require.eq(method.getParameterCount(), "method.getParameterCount()", 1, "1");


                        if (method.getParameterTypes()[0].equals(Method.class)) {
                            // noinspection SuspiciousMethodCalls
                            return methodCounts.get(args[0]);
                        } else if (method.getParameterTypes()[0].equals(String.class)) {
                            return methodCounts.entrySet().stream().filter(x -> x.getKey().getName().equals(args[0]))
                                    .mapToInt(Map.Entry::getValue).sum();
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            return wrappedColumnSource.getClass().getMethod(method.getName(), method.getParameterTypes())
                    .invoke(wrappedColumnSource, args);
        }
    }
}
