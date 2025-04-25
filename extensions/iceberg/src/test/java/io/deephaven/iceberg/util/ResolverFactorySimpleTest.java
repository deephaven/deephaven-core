//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.optionalGroup;
import static org.apache.parquet.schema.Types.required;
import static org.assertj.core.api.Assertions.assertThat;

class ResolverFactorySimpleTest {

    private static final String DH1 = "dh_col1";
    private static final String DH2 = "dh_col2";

    private static final String PQ1 = "parquet_col1";
    private static final String PQ2 = "parquet_col2";

    private static final String I1 = "iceberg_col1";
    private static final String I2 = "iceberg_col2";

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofInt(DH1),
            ColumnDefinition.ofInt(DH2));

    private static final int I1_ID = 42;
    private static final int I2_ID = 43;

    private static final Schema SCHEMA = new Schema(
            NestedField.optional(I1_ID, I1, IntegerType.get()),
            NestedField.required(I2_ID, I2, IntegerType.get()));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

    private static final Map<String, ColumnInstructions> COLUMN_INSTRUCTIONS = Map.of(
            DH1, schemaField(I1_ID),
            DH2, schemaField(I2_ID));

    private static final LogicalTypeAnnotation.IntLogicalTypeAnnotation INT32_TYPE = intType(32, true);

    private static void check(ParquetColumnResolver resolver, boolean hasColumn1, boolean hasColumn2) {
        if (hasColumn1) {
            assertThat(resolver.of(DH1)).hasValue(List.of(PQ1));
        } else {
            try {
                resolver.of(DH1);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col1` for file `test`");
            }
        }
        if (hasColumn2) {
            assertThat(resolver.of(DH2)).hasValue(List.of(PQ2));
        } else {
            try {
                resolver.of(DH2);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col2` for file `test`");
            }
        }
        nonDhNamesAlwaysEmpty(resolver);
    }

    private static void nonDhNamesAlwaysEmpty(ParquetColumnResolver resolver) {
        // We will never resolve for column names that aren't DH column names.
        assertThat(resolver.of(I1)).isEmpty();
        assertThat(resolver.of(I2)).isEmpty();
        assertThat(resolver.of(PQ1)).isEmpty();
        assertThat(resolver.of(PQ2)).isEmpty();
        assertThat(resolver.of("RandomColumnName")).isEmpty();
    }

    private static Resolver resolver() {
        return Resolver.builder()
                .definition(TABLE_DEFINITION)
                .schema(SCHEMA)
                .putAllColumnInstructions(COLUMN_INSTRUCTIONS)
                .build();
    }

    @Test
    void normal() {
        final ResolverFactory f1 = factory(resolver(), NameMapping.empty());
        // An illustration that the name mapping is about the parquet column names, not the Deephaven nor Iceberg names,
        // so this name mapping should have no effect
        final ResolverFactory f2 = factory(resolver(), NameMapping.of(
                MappedField.of(I1_ID, List.of(I1, DH1)),
                MappedField.of(I2_ID, List.of(I2, DH2))));
        for (final ResolverFactory factory : List.of(f1, f2)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage().named("root"));
                check(resolver, false, false);
            }

            // Parquet schema without field ids does not resolve with normal resolver
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                required(INT32).as(INT32_TYPE).named(PQ2))
                        .named("root"));
                check(resolver, false, false);
            }

            // Parquet schema with field ids, resolves based on the column instructions
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2))
                        .named("root"));
                check(resolver, true, true);
            }

            // Parquet schema with duplicate field ids does not resolve the duplicated field (but does resolve correct
            // ones)
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2),
                                optional(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2 + "Dupe"))
                        .named("root"));
                check(resolver, true, false);
            }

            // Duplication detection is at the _current_ level; duplicate field ids at a different level will still
            // "work"
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2),
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1 + "Dupe"),
                                                optional(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2 + "Dupe"))
                                        .named("parquet_group_1"))
                        .named("root"));
                check(resolver, true, true);
            }
        }
    }

    @Test
    void nameMapping() {
        final ResolverFactory f1 = factory(resolver(), NameMapping.of(
                MappedField.of(I1_ID, PQ1),
                MappedField.of(I2_ID, PQ2)));
        // Unrelated mappings should have no effect
        final ResolverFactory extraNames = factory(resolver(), NameMapping.of(
                MappedField.of(I1_ID, List.of(PQ1, I1, DH1)),
                MappedField.of(I2_ID, List.of(PQ2, I2, DH2))));
        for (ResolverFactory factory : List.of(f1, extraNames)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage().named("root"));
                check(resolver, false, false);
            }

            // Parquet schema without field ids, resolves based on fallback
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                required(INT32).as(INT32_TYPE).named(PQ2))
                        .named("root"));
                check(resolver, true, true);
            }

            // Parquet schema with duplicate names does not resolve the duplicated field (but does resolve correct ones)
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                required(INT32).as(INT32_TYPE).named(PQ2),
                                optional(INT32).as(INT32_TYPE).named(PQ2))
                        .named("root"));
                check(resolver, true, false);
            }

            // Duplication detection is at the _current_ level; duplicate names at a different level will still "work"
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                required(INT32).as(INT32_TYPE).named(PQ2),
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                optional(INT32).as(INT32_TYPE).named(PQ2))
                                        .named("parquet_group_1"))
                        .named("root"));
                check(resolver, true, true);
            }
        }
    }

    @Test
    void resolvePartitionField() {
        Resolver.builder()
                .definition(TABLE_DEFINITION)
                .schema(SCHEMA)
                .putAllColumnInstructions(COLUMN_INSTRUCTIONS);
    }

    private static ResolverFactory factory(Resolver resolver, NameMapping nameMapping) {
        return new ResolverFactory(resolver, nameMapping, false);
    }
}
