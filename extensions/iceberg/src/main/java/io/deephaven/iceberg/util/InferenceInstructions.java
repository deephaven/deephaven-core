//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Value.Immutable
@BuildableStyle
public abstract class InferenceInstructions {

    static Namer.Factory defaultNamerFactory() {
        return Namer.Factory.fieldName("_");
    }

    public static Builder builder() {
        return ImmutableInferenceInstructions.builder();
    }

    /**
     * Creates a default inference instructions for {@code schema}.
     *
     * @param schema the schema
     * @return the inference instructions
     */
    public static InferenceInstructions of(Schema schema) {
        return builder().schema(schema).build();
    }

    /**
     * The schema to use for {@link Resolver#infer(InferenceInstructions) inference}. The resulting
     * {@link Resolver#definition() definition} will have columns in the same order as defined by this {@link Schema}.
     */
    public abstract Schema schema();

    /**
     * The partition spec to use for {@link Resolver#infer(InferenceInstructions) inference}. The
     * {@link Transforms#identity() identity} transforms of this {@link PartitionSpec} will be used to make the
     * resulting {@link Resolver#definition() definition} have the relevant
     * {@link ColumnDefinition.ColumnType#Partitioning Partitioning} columns.
     *
     * <p>
     * <b>Warning</b>: inferring using a partition spec for general-purpose use is dangerous. This is only meant to be
     * applied in situations where callers are working with a fixed set of data files that have this spec (or a superset
     * of this spec); or, when the caller is able to guarantee that all current and future data files will have this
     * spec (or a superset of this spec).
     */
    public abstract Optional<PartitionSpec> spec();

    /**
     * The namer factory. Defaults to {@code fieldName("_")}, which will create Deephaven column name by joining
     * together the {@link Types.NestedField#name() field names} with an underscore and
     * {@link NameValidator#legalizeColumnName(String, Set) legalize} the name if necessary.
     *
     * @see Namer.Factory#fieldName(String)
     */
    @Value.Default
    public Namer.Factory namerFactory() {
        return defaultNamerFactory();
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
             * The field name {@link Namer} constructs a Deephaven column name by joining together the
             * {@link Types.NestedField#name() field names} with a {@code delimiter} and calling
             * {@link NameValidator#legalizeColumnName(String, Set)} with de-duplication logic.
             * 
             * @param delimiter the delimiter to use to join names
             */
            static Factory fieldName(String delimiter) {
                return new FieldNameNamerFactory(delimiter);
            }

            /**
             * The field name {@link Namer} constructs a Deephaven column name of the form
             * {@value FieldIdNamer#FIELD_ID} with the last {@link Types.NestedField#fieldId() field-id} in the path
             * appended.
             */
            static Factory fieldId() {
                return FieldIdNamer.FIELD_ID_NAMER;
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

        Builder failOnUnsupportedTypes(boolean failOnUnsupportedTypes);

        Builder namerFactory(Namer.Factory namerFactory);

        InferenceInstructions build();
    }

    private static final class FieldNameNamerFactory implements Namer.Factory {
        private final String delimiter;

        FieldNameNamerFactory(String delimiter) {
            this.delimiter = Objects.requireNonNull(delimiter);
        }

        @Override
        public Namer create() {
            return new FieldNameNamer();
        }

        private final class FieldNameNamer implements Namer {

            private final Set<String> usedNames = new HashSet<>();

            @Override
            public String of(Collection<? extends Types.NestedField> path, Type<?> type) {
                final String joinedNames =
                        path.stream().map(Types.NestedField::name).collect(Collectors.joining(delimiter));
                final String columnName = NameValidator.legalizeColumnName(joinedNames, usedNames);
                usedNames.add(columnName);
                return columnName;
            }
        }
    }

    private enum FieldIdNamer implements Namer.Factory, Namer {
        FIELD_ID_NAMER;

        private static final String FIELD_ID = "FieldId_";

        @Override
        public Namer create() {
            return this;
        }

        @Override
        public String of(Collection<? extends Types.NestedField> path, Type<?> type) {
            Types.NestedField lastField = null;
            for (Types.NestedField nestedField : path) {
                lastField = nestedField;
            }
            if (lastField == null) {
                throw new IllegalStateException();
            }
            return FIELD_ID + lastField.fieldId();
        }
    }
}
