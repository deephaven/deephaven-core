//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.util.ColumnInstructions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;


/**
 * This test specifics around {@link Resolver#from(TableDefinition)}; more general validations around {@link Resolver}
 * should be in {@link ResolverTest}.
 */
class ResolverFromTest {

    public static boolean equalsModuloSchemaId(Resolver resolver, Resolver other) {
        // Schema does not implement equals; this is _ok_ when we are doing tests that have an existing Schema, but when
        // we are building one ourselves, we need to use Schema#sameSchema.
        return resolver.definition().equals(other.definition())
                && resolver.schema().sameSchema(other.schema())
                && resolver.spec().equals(other.spec())
                && resolver.columnInstructions().equals(other.columnInstructions());
    }

    private static ObjectAssert<Resolver> assertResolverFrom(TableDefinition definition) {
        return assertThat(Resolver.from(definition)).usingEquals(ResolverFromTest::equalsModuloSchemaId);
    }

    // todo: fill this out more generally

    @Test
    void shortType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.shortType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `io.deephaven.qst.type.ShortType`");
        }
    }

    @Test
    void intType() {
        final TableDefinition definition = TableDefinition.of(ColumnDefinition.of("Foo", Type.intType()));
        final Resolver expected = Resolver.builder()
                .definition(definition)
                .schema(new Schema(List.of(Types.NestedField.optional(1, "Foo", Types.IntegerType.get()))))
                .putColumnInstructions("Foo", schemaField(1))
                .build();
        assertResolverFrom(definition).isEqualTo(expected);
    }

    @Test
    void byteArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.byteType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [B, componentType=io.deephaven.qst.type.ByteType}`");
        }
    }

    @Test
    void shortArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.shortType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [S, componentType=io.deephaven.qst.type.ShortType}`");
        }
    }

    @Test
    void intArrayType() {
        try {
            Resolver.from(TableDefinition.of(ColumnDefinition.of("Foo", Type.intType().arrayType())));
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining(
                    "Unable to infer the best Iceberg type for Deephaven column type `NativeArrayType{clazz=class [I, componentType=io.deephaven.qst.type.IntType}`");
        }
    }
}
