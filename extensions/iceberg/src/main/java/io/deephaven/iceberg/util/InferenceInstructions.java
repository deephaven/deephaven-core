//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Value.Immutable
@BuildableStyle
public abstract class InferenceInstructions {

    public static Builder builder() {
        return ImmutableInferenceInstructions.builder();
    }

    public static InferenceInstructions of(Schema schema, PartitionSpec partitionSpec) {
        return builder().schema(schema).spec(partitionSpec).build();
    }

    // public static InferenceInstructions fromSchema(Schema schema) {
    // return builder()
    // .schema(schema)
    // .spec(PartitionSpec.unpartitioned())
    // .build();
    // }
    //
    // public static InferenceInstructions fromLatest(Table table) {
    // return builder()
    // .schema(table.schema())
    // .spec(table.spec())
    //// .nameMapping(NameMappingUtil.readNameMappingDefault(table).orElse(NameMapping.empty()))
    // .build();
    // }

    /**
     * The schema to use for inference.
     */
    public abstract Schema schema();

    /**
     * The partition spec to use for inference.
     */
    public abstract PartitionSpec spec();

    // /**
    // * The name mapping to use as fallback if field-ids are not present in the data file paths. By default, is {@link
    // NameMapping#empty()}.
    // */
    // @Value.Default
    // public NameMapping nameMapping() {
    // return NameMapping.empty();
    // }

    /**
     * The namer factory. Defaults to {@link Namer.Factory#ofDefault()}.
     */
    @Value.Default
    public Namer.Factory namerFactory() {
        return Namer.Factory.ofDefault();
    }

    /**
     * If inference should fail if any of the Iceberg fields fail to map to Deephaven columns. By default, is
     * {@code false}.
     */
    @Value.Default
    public boolean failOnUnsupportedTypes() {
        return false;
    }

    /**
     * The Deephaven column namer.
     */
    public interface Namer {

        interface Factory {

            /**
             * The default namer constructs a Deephaven column name by joining together the
             * {@link Types.NestedField#name() field names} with an underscore ({@code _}) and calling
             * {@link NameValidator#legalizeColumnName(String, Set)} with de-duplication logic.
             */
            static Factory ofDefault() {
                return DefaultNamer.FactoryImpl.DEFAULT_NAMER_FACTORY;
            }

            /**
             * Creates a new namer instance.
             */
            Namer create();
        }

        /**
         * Called for each field path that Deephaven is inferring. Implementations must ensure they return a valid,
         * unique column name.
         *
         * @param path the nested field path
         * @param type the type
         * @return the Deephaven column name
         */
        String of(Collection<? extends Types.NestedField> path, Type<?> type);
    }

    public interface Builder {
        Builder schema(Schema schema);

        Builder spec(PartitionSpec spec);

        // Builder nameMapping(NameMapping nameMapping);

        Builder failOnUnsupportedTypes(boolean failOnUnsupportedTypes);

        Builder namerFactory(Namer.Factory namerFactory);

        InferenceInstructions build();
    }

    @Value.Check
    final void checkSpecSchema() {
        if (spec() == PartitionSpec.unpartitioned()) {
            return;
        }
        if (!schema().sameSchema(spec().schema())) {
            throw new IllegalArgumentException("schema and spec schema are not the same");
        }
    }

    private static final class DefaultNamer implements Namer {

        private enum FactoryImpl implements Factory {
            DEFAULT_NAMER_FACTORY;

            @Override
            public Namer create() {
                return new DefaultNamer();
            }
        }

        private final Set<String> usedNames = new HashSet<>();

        @Override
        public String of(Collection<? extends Types.NestedField> path, Type<?> type) {
            final String joinedNames = path.stream().map(Types.NestedField::name).collect(Collectors.joining("_"));
            final String columnName = NameValidator.legalizeColumnName(joinedNames, usedNames);
            usedNames.add(columnName);
            return columnName;
        }
    }
}
