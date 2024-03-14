//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.uri.resolver;

import java.util.Objects;

public class UriResolversInstance {
    private static UriResolvers resolvers;

    public static void init(UriResolvers instance) {
        synchronized (UriResolversInstance.class) {
            if (resolvers != null) {
                throw new IllegalStateException("Can only initialize UriResolversInstance once");
            }
            resolvers = instance;
        }
    }

    public static UriResolvers get() {
        return Objects.requireNonNull(resolvers);
    }
}
