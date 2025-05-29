//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import java.util.Objects;

public interface ResolverProvider {
    /**
     * An explicit resolver provider.
     *
     * @param resolver the resolver
     * @return the provider for {@code resolver}
     */
    static DirectResolver of(Resolver resolver) {
        return new DirectResolver(resolver);
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
        T visit(DirectResolver directResolver);

        T visit(UnboundResolver unboundResolver);

        T visit(InferenceResolver inferenceResolver);
    }

    final class DirectResolver implements ResolverProvider {
        private final Resolver resolver;

        private DirectResolver(Resolver resolver) {
            this.resolver = Objects.requireNonNull(resolver);
        }

        public Resolver resolver() {
            return resolver;
        }

        @Override
        public <T> T walk(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DirectResolver))
                return false;
            DirectResolver that = (DirectResolver) o;
            return resolver.equals(that.resolver);
        }

        @Override
        public int hashCode() {
            return resolver.hashCode();
        }

        @Override
        public String toString() {
            return "DirectResolver{" +
                    "resolver=" + resolver +
                    '}';
        }
    }
}
