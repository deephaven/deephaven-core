//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;

public interface ResolverProvider {
    /**
     * An explicit resolver provider.
     *
     * @param resolver the resolver
     * @return the provider for {@code resolver}
     */
    static ResolverProvider of(Resolver resolver) {
        return new ResolverProviderImpl() {
            @Override
            Resolver resolver(Table table) {
                return resolver;
            }
        };
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
}
