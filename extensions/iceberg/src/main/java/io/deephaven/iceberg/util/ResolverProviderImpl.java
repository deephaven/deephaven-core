//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;

import java.util.Objects;

final class ResolverProviderImpl implements ResolverProvider.Visitor<Resolver> {

    public static Resolver of(ResolverProvider provider, Table table) {
        return provider.walk(new ResolverProviderImpl(table));
    }

    private final Table table;

    ResolverProviderImpl(Table table) {
        this.table = Objects.requireNonNull(table);
    }

    @Override
    public Resolver visit(Resolver resolver) {
        return resolver;
    }

    @Override
    public Resolver visit(UnboundResolver unboundResolver) {
        return unboundResolver.resolver(table);
    }

    @Override
    public Resolver visit(InferenceResolver inferenceResolver) {
        try {
            return inferenceResolver.resolver(table);
        } catch (TypeInference.UnsupportedType e) {
            throw new RuntimeException(e);
        }
    }
}
