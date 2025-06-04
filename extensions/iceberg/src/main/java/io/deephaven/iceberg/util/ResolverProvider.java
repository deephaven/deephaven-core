//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

public interface ResolverProvider {
    /**
     * An explicit resolver provider.
     *
     * @param resolver the resolver
     * @return the provider for {@code resolver}
     * @deprecated callers can simple use {@code resolver}
     */
    @Deprecated
    static Resolver of(Resolver resolver) {
        return resolver;
    }

    /**
     * The default inference resolver.
     *
     * <p>
     * Equivalent to {@code InferenceResolver.builder().build()}.
     *
     * @return the default inference resolver
     * @see InferenceResolver
     */
    static InferenceResolver infer() {
        return InferenceResolver.builder().build();
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Resolver resolver);

        T visit(UnboundResolver unboundResolver);

        T visit(InferenceResolver inferenceResolver);
    }
}
