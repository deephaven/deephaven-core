//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResolverProviderTest {

    @Test
    void unbound() {
        final UnboundResolver actual = newUnboundExample();
        assertVisitSelf(actual)
                .isEqualTo(newUnboundExample())
                .isNotEqualTo(ResolverProvider.infer());
    }

    @Test
    void infer() {
        final InferenceResolver actual = ResolverProvider.infer();
        assertVisitSelf(actual)
                .isEqualTo(ResolverProvider.infer())
                .isNotEqualTo(newUnboundExample());
    }

    private static Resolver newResolver() {
        return Resolver.builder()
                .definition(TableDefinition.of(ColumnDefinition.ofInt("Foo")))
                .schema(new Schema(Types.NestedField.optional(42, "Foo", Types.IntegerType.get())))
                .putColumnInstructions("Foo", ColumnInstructions.schemaField(42))
                .build();
    }

    private static UnboundResolver newUnboundExample() {
        return UnboundResolver.builder()
                .schema(SchemaProvider.fromSchemaId(42))
                .definition(TableDefinition.of(ColumnDefinition.ofInt("Foo")))
                .build();
    }

    private static ObjectAssert<ResolverProvider> assertVisitSelf(ResolverProvider resolverProvider) {
        return assertThat(ReturnSelf.of(resolverProvider));
    }

    enum ReturnSelf implements ResolverProvider.Visitor<ResolverProvider> {
        RETURN_SELF;

        public static ResolverProvider of(ResolverProvider provider) {
            // may seem a bit silly, but makes sure the implementations correctly implement walk
            return provider.walk(RETURN_SELF);
        }

        @Override
        public ResolverProvider visit(Resolver resolver) {
            return resolver;
        }

        @Override
        public ResolverProvider visit(UnboundResolver unboundResolver) {
            return unboundResolver;
        }

        @Override
        public ResolverProvider visit(InferenceResolver inferenceResolver) {
            return inferenceResolver;
        }
    }
}
