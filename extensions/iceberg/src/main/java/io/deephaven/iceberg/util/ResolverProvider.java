//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;

public interface ResolverProvider {
    static ResolverProvider of(Resolver resolver) {
        return new ResolverProviderImpl() {
            @Override
            Resolver resolver(Table table) {
                return resolver;
            }
        };
    }

    static ResolverProvider infer() {
        return inferenceBuilder().build();
    }

    static ResolverProviderInference.Builder inferenceBuilder() {
        return ImmutableResolverProviderInference.builder();
    }
}
