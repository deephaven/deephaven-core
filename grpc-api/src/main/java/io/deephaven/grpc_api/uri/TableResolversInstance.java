package io.deephaven.grpc_api.uri;

import java.util.Objects;

public class TableResolversInstance {
    private static TableResolvers resolvers;

    public static void init(TableResolvers instance) {
        synchronized (TableResolversInstance.class) {
            if (resolvers != null) {
                throw new IllegalStateException("Can only initialize TableResolversInstance once");
            }
            resolvers = instance;
        }
    }

    public static TableResolvers get() {
        return Objects.requireNonNull(resolvers);
    }
}
