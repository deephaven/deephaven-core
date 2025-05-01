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
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
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
import static org.apache.parquet.schema.Types.requiredGroup;
import static org.assertj.core.api.Assertions.assertThat;

class ResolverFactoryNestedTest {

    private static final String DH1 = "dh_col1";
    private static final String DH2 = "dh_col2";
    private static final String DH3 = "dh_col3";
    private static final String DH4 = "dh_col4";

    private static final String PQG1 = "parquet_group1";
    private static final String PQG2 = "parquet_group2";
    private static final String PQ1 = "parquet_col1";
    private static final String PQ2 = "parquet_col2";
    private static final String PQ3 = "parquet_col3";
    private static final String PQ4 = "parquet_col4";

    private static final String IS1 = "iceberg_struct1";
    private static final String IS2 = "iceberg_struct2";
    private static final String I1 = "iceberg_col1";
    private static final String I2 = "iceberg_col2";
    private static final String I3 = "iceberg_col3";
    private static final String I4 = "iceberg_col4";

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofInt(DH1),
            ColumnDefinition.ofInt(DH2),
            ColumnDefinition.ofInt(DH3),
            ColumnDefinition.ofInt(DH4));

    private static final int IS1_ID = 40;
    private static final int IS2_ID = 41;
    private static final int I1_ID = 42;
    private static final int I2_ID = 43;
    private static final int I3_ID = 44;
    private static final int I4_ID = 45;

    private static final Schema SCHEMA = new Schema(
            NestedField.optional(IS1_ID, IS1, StructType.of(
                    NestedField.optional(I1_ID, I1, IntegerType.get()),
                    NestedField.required(I2_ID, I2, IntegerType.get()))),
            NestedField.required(IS2_ID, IS2, StructType.of(
                    NestedField.optional(I3_ID, I3, IntegerType.get()),
                    NestedField.required(I4_ID, I4, IntegerType.get()))));

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

    private static final Map<String, ColumnInstructions> COLUMN_INSTRUCTIONS = Map.of(
            DH1, schemaField(I1_ID),
            DH2, schemaField(I2_ID),
            DH3, schemaField(I3_ID),
            DH4, schemaField(I4_ID));

    private static MappedFields mapping(
            List<String> s1Names, List<String> f1Names, List<String> f2Names,
            List<String> s2Names, List<String> f3Names, List<String> f4Names) {
        return mapping(IS1_ID, s1Names, I1_ID, f1Names, I2_ID, f2Names, IS2_ID, s2Names, I3_ID, f3Names, I4_ID,
                f4Names);
    }

    private static MappedFields mapping(
            int is1id, List<String> s1Names, int i1id, List<String> f1Names, int i2id, List<String> f2Names,
            int is2id, List<String> s2Names, int i3id, List<String> f3Names, int i4id, List<String> f4Names) {
        return MappedFields.of(
                MappedField.of(is1id, s1Names, MappedFields.of(
                        MappedField.of(i1id, f1Names),
                        MappedField.of(i2id, f2Names))),
                MappedField.of(is2id, s2Names, MappedFields.of(
                        MappedField.of(i3id, f3Names),
                        MappedField.of(i4id, f4Names))));
    }

    public static final LogicalTypeAnnotation.IntLogicalTypeAnnotation INT32_TYPE = intType(32, true);

    private static void check(ParquetColumnResolver resolver, boolean hasColumn1, boolean hasColumn2,
            boolean hasColumn3, boolean hasColumn4) {
        if (hasColumn1) {
            assertThat(resolver.of(DH1)).hasValue(List.of(PQG1, PQ1));
        } else {
            try {
                resolver.of(DH1);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col1` for file `test`");
            }
        }
        if (hasColumn2) {
            assertThat(resolver.of(DH2)).hasValue(List.of(PQG1, PQ2));
        } else {
            try {
                resolver.of(DH2);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col2` for file `test`");
            }
        }
        if (hasColumn3) {
            assertThat(resolver.of(DH3)).hasValue(List.of(PQG2, PQ3));
        } else {
            try {
                resolver.of(DH3);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col3` for file `test`");
            }
        }
        if (hasColumn4) {
            assertThat(resolver.of(DH4)).hasValue(List.of(PQG2, PQ4));
        } else {
            try {
                resolver.of(DH4);
            } catch (TableDataException e) {
                assertThat(e).hasMessageContaining("Unable to resolve column `dh_col4` for file `test`");
            }
        }
        nonDhNamesAlwaysEmpty(resolver);
    }

    private static void nonDhNamesAlwaysEmpty(ParquetColumnResolver resolver) {
        // We will never resolve for column names that aren't DH column names.
        assertThat(resolver.of(IS1)).isEmpty();
        assertThat(resolver.of(IS2)).isEmpty();
        assertThat(resolver.of(I1)).isEmpty();
        assertThat(resolver.of(I2)).isEmpty();
        assertThat(resolver.of(I3)).isEmpty();
        assertThat(resolver.of(I4)).isEmpty();

        assertThat(resolver.of(PQG1)).isEmpty();
        assertThat(resolver.of(PQG2)).isEmpty();
        assertThat(resolver.of(PQ1)).isEmpty();
        assertThat(resolver.of(PQ2)).isEmpty();
        assertThat(resolver.of(PQ3)).isEmpty();
        assertThat(resolver.of(PQ4)).isEmpty();

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

        // This is functionally equivalent to NameMapping.empty() when no actual names are provided
        final ResolverFactory noFallbacks =
                factory(mapping(List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));

        // No nesting, an illustration that even if you have the inner mappings correct, it still needs to be nested
        // appropriately; it won't have any effect
        final ResolverFactory noNesting = factory(resolver(), NameMapping.of(
                MappedField.of(I1_ID, PQ1),
                MappedField.of(I2_ID, PQ2),
                MappedField.of(I3_ID, PQ3),
                MappedField.of(I4_ID, PQ4)));

        // The structure is too deeply nested; it won't have any effect
        final ResolverFactory overlyNested = factory(NameMapping.of(MappedField.of(99, List.of(), mapping(
                List.of(PQG1),
                List.of(PQ1),
                List.of(PQ2),
                List.of(PQG2),
                List.of(PQ3),
                List.of(PQ4)))));

        // No outer fallback, inner has bad names; it won't have any effect
        final ResolverFactory badInnerNames = factory(mapping(
                List.of(),
                List.of(DH1, I1),
                List.of(DH2, I2),
                List.of(),
                List.of(DH3, I3),
                List.of(DH4, I4)));

        // No outer fallback, inner has bad ids; it won't have any effect
        final ResolverFactory badInnerIds = factory(mapping(
                IS1_ID, List.of(),
                I1_ID + 100, List.of(PQ1),
                I2_ID + 100, List.of(PQ2),
                IS2_ID, List.of(),
                I3_ID + 100, List.of(PQ3),
                I4_ID + 100, List.of(PQ4)));

        // Outer fallback is incorrect; it won't have any effect
        final ResolverFactory badOuterIds = factory(mapping(
                IS1_ID + 100, List.of(PQG1),
                I1_ID, List.of(PQ1),
                I2_ID, List.of(PQ2),
                IS2_ID + 100, List.of(PQG2),
                I3_ID, List.of(PQ3),
                I4_ID, List.of(PQ4)));

        for (final ResolverFactory factory : List.of(f1, noFallbacks, noNesting, overlyNested, badInnerNames,
                badInnerIds, badOuterIds)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage().named("root"));
                check(resolver, false, false, false, false);
            }

            // Parquet schema without field ids does not resolve with normal resolver
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, false, false, false, false);
            }

            // Parquet schema without outer field ids does not resolve with normal resolver
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, false, false, false, false);
            }

            // Parquet schema without inner field ids does not resolve with normal resolver
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .id(IS2_ID)
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, false, false, false, false);
            }

            // Parquet schema with field ids, resolves based on the column instructions
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .id(IS2_ID)
                                        .addFields(
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, true, true, true);
            }

            // Parquet schema with duplicate field ids does not resolve the duplicated field (but does resolve correct
            // ones)
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2 + "Dupe"))
                                        .named(PQG1),
                                requiredGroup()
                                        .id(IS2_ID)
                                        .addFields(
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, false, true, true);
            }

            // Duplicate group will not recurse
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1 + "Dupe"),
                                requiredGroup()
                                        .id(IS2_ID)
                                        .addFields(
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, false, false, true, true);
            }

            // Duplication detection is at the _current_ level; duplicate field ids at a different level will still
            // "work"
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .id(IS1_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2),
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG1),
                                requiredGroup()
                                        .id(IS2_ID)
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(INT32_TYPE).named(PQ1),
                                                required(INT32).id(I2_ID).as(INT32_TYPE).named(PQ2),
                                                optional(INT32).id(I3_ID).as(INT32_TYPE).named(PQ3),
                                                required(INT32).id(I4_ID).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, true, true, true);
            }
        }
    }

    @Test
    void nameMapping() {
        final ResolverFactory f1 = factory(mapping(
                List.of(PQG1),
                List.of(PQ1),
                List.of(PQ2),
                List.of(PQG2),
                List.of(PQ3),
                List.of(PQ4)));

        // Unrelated mappings should have no effect
        final ResolverFactory extraNames = factory(mapping(
                List.of(PQG1, IS1),
                List.of(PQ1, I1, DH1),
                List.of(PQ2, I2, DH2),
                List.of(PQG2, IS2),
                List.of(PQ3, I3, DH3),
                List.of(PQ4, I4, DH4)));

        for (final ResolverFactory factory : List.of(f1, extraNames)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage().named("root"));
                check(resolver, false, false, false, false);
            }

            // Parquet schema without field ids, resolves based on fallback
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, true, true, true);
            }

            // Parquet schema with duplicate names does not resolve the duplicated field (but does resolve correct ones)
            // PQ2 field is repeated, so will not resolve.
            // PQG2 group is repeated, so will not recurse.
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2),
                                                required(INT32).as(INT32_TYPE).named(PQ2))
                                        .named(PQG1),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, false, false, false);
            }

            // Duplication detection is at the _current_ level; duplicate names at a different level will still "work"
            {
                final ParquetColumnResolver resolver = factory.of(SPEC, buildMessage()
                        .addFields(
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2),
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG1),
                                requiredGroup()
                                        .addFields(
                                                optional(INT32).as(INT32_TYPE).named(PQ1),
                                                required(INT32).as(INT32_TYPE).named(PQ2),
                                                optional(INT32).as(INT32_TYPE).named(PQ3),
                                                required(INT32).as(INT32_TYPE).named(PQ4))
                                        .named(PQG2))
                        .named("root"));
                check(resolver, true, true, true, true);
            }
        }
    }

    private static ResolverFactory factory(NameMapping nameMapping) {
        return factory(resolver(), nameMapping);
    }

    private static ResolverFactory factory(MappedFields nameMapping) {
        return factory(resolver(), NameMapping.of(nameMapping));
    }

    private static ResolverFactory factory(Resolver resolver, NameMapping nameMapping) {
        return new ResolverFactory(resolver, nameMapping, false);
    }
}
