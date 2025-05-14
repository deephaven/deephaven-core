//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.apache.iceberg.Table;

import java.util.Objects;

abstract class ResolverProviderImpl implements ResolverProvider {

    abstract Resolver resolver(Table table) throws TypeInference.UnsupportedType;

    static final class Explicit extends ResolverProviderImpl {
        private final Resolver resolver;

        public Explicit(Resolver resolver) {
            this.resolver = Objects.requireNonNull(resolver);
        }

        @Override
        Resolver resolver(Table table) throws TypeInference.UnsupportedType {
            return resolver;
        }
    }
}
