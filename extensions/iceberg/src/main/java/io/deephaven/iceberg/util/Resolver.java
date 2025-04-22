//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.internal.SchemaHelper;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * This is the necessary structure to map an Iceberg {@link Schema} to a Deephaven {@link TableDefinition}.
 */
@Value.Immutable
@BuildableStyle
public abstract class Resolver {

    // org.apache.iceberg.TableMetadata.INITIAL_SPEC_ID
    private static final int INITIAL_SPEC_ID = 0;

    // org.apache.iceberg.TableMetadata.INITIAL_SCHEMA_ID
    private static final int INITIAL_SCHEMA_ID = 0;

    /**
     * Java Iceberg field IDs currently start from 1, but this seems to be an <b>implementation</b> detail that is not
     * explicitly called out by the spec. See org.apache.iceberg.TableMetadata.newTableMetadata,
     * org.apache.iceberg.types.TypeUtil.assignFreshIds. As of now, the Iceberg APIs does not expose this information to
     * us, so we need to make the assumption here.
     */
    private static final int INITIAL_FIELD_ID = 1;

    public static Builder builder() {
        return ImmutableResolver.builder();
    }

    /**
     * Creates a {@link Resolver} from the given Table {@code definition}, <b>only applicable in contexts where an
     * {@link Table Iceberg Table} does not already exist</b>. In cases where the {@link Table Iceberg Table} already
     * exist, callers must create a resolver in relationship to an existing {@link Schema} (for example, via
     * {@link #infer(Schema)}, or manually via {@link #builder()}).
     *
     * <p>
     * All {@link ColumnDefinition.ColumnType#Partitioning partitioning columns} will be used to create a partition spec
     * for the table. Callers should take note of the documentation on {@link Resolver#definition()} when deciding to
     * create an Iceberg Table with partitioning columns.
     *
     * @param definition the Table definition
     * @return the resolver
     */
    public static Resolver from(@NotNull final TableDefinition definition) {
        final Resolver.Builder builder = Resolver.builder().definition(definition);
        final Collection<String> partitioningColumnNames = new ArrayList<>();
        final List<Types.NestedField> fields = new ArrayList<>();
        int fieldID = INITIAL_FIELD_ID;
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            final Type<?> type = Type.find(columnDefinition.getDataType(), columnDefinition.getComponentType());
            final org.apache.iceberg.types.Type icebergType = TypeInference.of(type).orElse(null);
            if (icebergType == null) {
                throw new MappingException("Unsupported Deephaven column type " + type);
            }
            fields.add(Types.NestedField.optional(fieldID, dhColumnName, icebergType));
            if (columnDefinition.isPartitioning()) {
                partitioningColumnNames.add(dhColumnName);
            }
            builder.putColumnInstructions(dhColumnName, ColumnInstructions.schemaField(fieldID));
            fieldID++;
        }
        final Schema schema = new Schema(INITIAL_SCHEMA_ID, fields);
        final PartitionSpec spec = createPartitionSpec(schema, partitioningColumnNames, INITIAL_SPEC_ID);
        if (spec.isPartitioned()) {
            builder.spec(spec);
        }
        return builder.schema(schema).build();
    }

    private static PartitionSpec createPartitionSpec(
            @NotNull final Schema schema,
            @NotNull final Iterable<String> partitionColumnNames,
            int newSpecId) {
        final PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema)
                .withSpecId(newSpecId);
        for (final String partitioningColumnName : partitionColumnNames) {
            partitionSpecBuilder.identity(partitioningColumnName);
        }
        return partitionSpecBuilder.build();
    }

    /**
     * Infer a resolver based on the default inference instructions for {@code schema}.
     *
     * <p>
     * Equivalent to {@code infer(InferenceInstructions.of(schema))}.
     *
     * @param schema the schema to use for inference
     * @return the resolver
     * @throws TypeInference.UnsupportedType if an unsupported type is encountered
     * @see #infer(InferenceInstructions)
     */
    public static Resolver infer(Schema schema) throws TypeInference.UnsupportedType {
        return infer(InferenceInstructions.of(schema));
    }

    /**
     * Infer a resolver based on the {@code inferenceInstructions}.
     *
     * @param inferenceInstructions the inference instructions
     * @return the resolver
     * @throws TypeInference.UnsupportedType if an unsupported type is encountered
     */
    public static Resolver infer(final InferenceInstructions inferenceInstructions)
            throws TypeInference.UnsupportedType {
        return InferenceImpl.of(inferenceInstructions);
    }

    /**
     * Builds a simple resolver based on the matching of {@link Schema} and {@link TableDefinition} names. In most
     * cases, callers that have both a {@code schema} and {@code definition} should prefer to build the resolver with
     * explicit {@link NestedField#fieldId() field ids}, but this is provided for simple cases as a convenience.
     *
     * <p>
     * The provided {@code definition} must not have any partitioning columns. In that case, this method will throw a
     * {@link IllegalArgumentException}. For that case, you should use the {@link #builder()} with appropriate
     * {@link #spec()} to build a resolver.
     */
    public static Resolver simple(final Schema schema, final TableDefinition definition) {
        final Resolver.Builder builder = Resolver.builder()
                .schema(schema)
                .definition(definition);
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            if (columnDefinition.isPartitioning()) {
                throw new IllegalArgumentException(
                        String.format("Column `%s` is a partitioning column, use the builder with appropriate" +
                                " partition spec to build a Resolver ", dhColumnName));
            }
            final NestedField icebergField = schema.findField(dhColumnName);
            if (icebergField == null) {
                throw new IllegalArgumentException(
                        String.format("Column `%s` from deephaven table definition not found in Iceberg schema",
                                dhColumnName));
            }
            builder.putColumnInstructions(dhColumnName, ColumnInstructions.schemaField(icebergField.fieldId()));
        }
        return builder.build();
    }

    /**
     * The Deephaven table definition. Every {@link TableDefinition#getColumns() column} of this definition must be
     * mapped via {@link #columnInstructions()}.
     *
     * <p>
     * Callers should take care and only use {@link ColumnDefinition.ColumnType#Partitioning Partitioning} columns when
     * they know the Iceberg table will always have {@link Transform#isIdentity() identity} partitions for said columns.
     * In the general case, Iceberg partitions may evolve over time, which can break the assumptions Deephaven makes
     * about partitioning columns.
     */
    public abstract TableDefinition definition();

    /**
     * The Iceberg schema.
     */
    public abstract Schema schema();

    /**
     * The Iceberg partition specification. Only necessary to set when the {@link #definition()} has
     * {@link ColumnDefinition.ColumnType#Partitioning} columns, or {@link #columnInstructions()} references
     * {@link ColumnInstructions#partitionField(int) partition fields}.
     *
     * @see InferenceInstructions#spec() for related warnings
     */
    public abstract Optional<PartitionSpec> spec();

    /**
     * The column instructions keyed by Deephaven column name from the {@link #definition() definition}.
     * {@link ColumnDefinition.ColumnType#Normal Normal} columns must reference a
     * {@link ColumnInstructions#schemaField(int) schema field} (or be {@link ColumnInstructions#unmapped() unmapped}),
     * and {@link ColumnDefinition.ColumnType#Partitioning partitioning} columns must reference a
     * {@link ColumnInstructions#schemaField(int) schema field} or {@link ColumnInstructions#partitionField(int)
     * partition field}.
     */
    public abstract Map<String, ColumnInstructions> columnInstructions();

    /**
     * Get the field path associated with the Deephaven {@code columnName}. Will return empty when the column name is
     * not in {@link #columnInstructions()}, and a result otherwise.
     *
     * @param columnName the column name
     * @return the field path
     */
    public final Optional<List<NestedField>> resolve(String columnName) {
        // noinspection unchecked
        final List<NestedField>[] out = new List[1];
        if (!resolve2(columnName, (x, y) -> out[0] = x)) {
            return Optional.empty();
        }
        return Optional.of(out[0]);
    }

    public interface Builder {

        Builder definition(TableDefinition definition);

        Builder schema(Schema schema);

        Builder spec(PartitionSpec spec);

        Builder putColumnInstructions(String columnName, ColumnInstructions columnInstructions);

        Builder putAllColumnInstructions(Map<String, ? extends ColumnInstructions> entries);

        Resolver build();
    }

    // May want to expose this in the future, or a new class instead of a BiConsumer.
    final boolean resolve2(String columnName, BiConsumer<List<NestedField>, PartitionField> consumer) {
        final ColumnInstructions ci = columnInstructions().get(columnName);
        if (ci == null || ci.isUnmapped()) {
            return false;
        }
        final ColumnDefinition<?> column = Objects.requireNonNull(definition().getColumn(columnName));
        try {
            consume2(column.isPartitioning(), ci, consumer);
        } catch (SchemaHelper.PathException e) {
            // should have been caught during constructions
            throw new IllegalStateException(e);
        }
        return true;
    }

    final PartitionSpec specOrUnpartitioned() {
        return spec().orElse(PartitionSpec.unpartitioned());
    }

    @Value.Check
    final void checkUnmappedColumns() {
        for (String column : definition().getColumnNameSet()) {
            if (!columnInstructions().containsKey(column)) {
                throw new MappingException(String.format("Column `%s` is not mapped", column));
            }
        }
    }

    @Value.Check
    final void checkColumnNames() {
        for (String columnName : definition().getColumnNames()) {
            NameValidator.validateColumnName(columnName);
        }
    }

    @Value.Check
    final void checkCompatibility() {
        for (Map.Entry<String, ColumnInstructions> e : columnInstructions().entrySet()) {
            checkColumnInstructions(e.getKey(), e.getValue());
        }
    }

    private void checkColumnInstructions(String columnName, ColumnInstructions ci) {
        definition().checkHasColumn(columnName);
        final ColumnDefinition<?> column = definition().getColumn(columnName);
        final Type<?> type = Type.find(column.getDataType(), column.getComponentType());
        if (ci.isUnmapped()) {
            return;
        }
        try {
            consume2(column.isPartitioning(), ci, (x, y) -> validate(type, x, y));
        } catch (SchemaHelper.PathException | MappingException e) {
            throw new MappingException(String.format("Unable to map Deephaven column %s", column.getName()), e);
        }
    }

    private void consume2(
            boolean isPartitioningColumn,
            ColumnInstructions ci,
            BiConsumer<List<NestedField>, PartitionField> consumer) throws SchemaHelper.PathException {
        if (ci.isUnmapped()) {
            throw new IllegalArgumentException("Expected a mapped ColumnInstructions");
        }
        final List<NestedField> fieldPath;
        final PartitionField partitionField;
        if (ci.schemaFieldId().isPresent()) {
            fieldPath = ci.schemaFieldPath(schema());
            partitionField = isPartitioningColumn ? ci.partitionFieldFromSchemaFieldId(specOrUnpartitioned()) : null;
        } else {
            if (!isPartitioningColumn) {
                throw new MappingException(
                        "Should only specify Iceberg partitionField in combination with a Deephaven partitioning column");
            }
            partitionField = ci.partitionField(specOrUnpartitioned());
            fieldPath = SchemaHelper.fieldPath(schema(), partitionField);
        }
        consumer.accept(fieldPath, partitionField);
    }

    private void validate(
            final Type<?> type,
            final List<NestedField> fieldPath,
            @Nullable final PartitionField partitionField) {
        if (partitionField != null) {
            // https://iceberg.apache.org/spec/#partitioning
            // The source columns, selected by ids, must be a primitive type and cannot be contained in a map or list,
            // but may be nested in a struct.
            for (NestedField nestedField : fieldPath) {
                // org.apache.iceberg.PartitionSpec.checkCompatibility does not currently catch this case
                if (nestedField.type().isListType()) {
                    throw new MappingException("Partition fields may not be contained in a list");
                }
                if (nestedField.type().isMapType()) {
                    throw new MappingException("Partition fields may not be contained in a map");
                }
            }
            {
                // org.apache.iceberg.PartitionSpec.checkCompatibility should typically catch this case, but in certain
                // cases (for example, a Catalog / metadata error), we can check for this ourselves.
                final NestedField field = fieldPath.get(fieldPath.size() - 1);
                if (!field.type().isPrimitiveType()) {
                    throw new MappingException(
                            String.format("Cannot partition by non-primitive source field: %s", field.type()));
                }
                final org.apache.iceberg.types.Type.PrimitiveType inputType = field.type().asPrimitiveType();
                IcebergPartitionedLayout.validateSupported(partitionField.transform(), inputType, type);
            }
        }
        checkCompatible(fieldPath, type);
    }

    final PartitionField partitionField(ColumnDefinition<?> column) {
        if (!column.isPartitioning()) {
            throw new IllegalArgumentException();
        }
        final ColumnInstructions ci = columnInstructions().get(column.getName());
        final PartitionField[] pf = new PartitionField[1];
        try {
            consume2(true, ci, (nestedFields, partitionField) -> {
                pf[0] = partitionField;
            });
        } catch (SchemaHelper.PathException e) {
            // should have been caught during constructions
            throw new IllegalStateException(e);
        }
        return Objects.requireNonNull(pf[0]);
    }

    @Value.Lazy
    Map<String, PartitionField> partitionFieldMap() {
        return Collections.unmodifiableMap(definition()
                .getColumnStream()
                .filter(ColumnDefinition::isPartitioning)
                .collect(Collectors.toMap(
                        ColumnDefinition::getName,
                        this::partitionField,
                        Assert::neverInvoked,
                        LinkedHashMap::new)));
    }

    static void checkCompatible(List<? extends NestedField> path, Type<?> type) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        checkCompatible(path);
        // todo: compare against DH type(s)
        final NestedField lastField = path.get(path.size() - 1);
        if (!type.walk(new IcebergPrimitiveCompat(lastField.type().asPrimitiveType()))) {
            throw new MappingException(
                    String.format("Unable to map Iceberg type `%s` to Deephaven type `%s`", lastField.type(), type));
        }
    }

    static void checkCompatible(Collection<? extends NestedField> fieldPath) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        if (fieldPath.isEmpty()) {
            throw new MappingException("Can't map an empty field path");
        }

        // We should make sure that we standardize on the correct level of mapping. For example, if we eventually
        // support List<Primitive>, we should make sure the fieldPath points to the *List* instead of the primitive, as
        // it's a better representation of the desired DH type. Although, we should arguably be able to support
        // List<Struct<A, ..., Z>> materialized into column types A[], ..., Z[], and that likely needs the more specific
        // field path into the actual primitive type... TBD.
        //
        // We also need to determine with how much precision we can or want to preserve optional semantics. For example,
        // a required struct with an optional field, or an optional struct with a required field are easy to represent
        // as a DH column; null implies that the optional aspect of that value was not present. But, if there is an
        // optional struct with an optional field, we are unable to whether null means the struct was not present, or
        // the field was not present. We could consider a special mapping (likely a `byte` column) that could be relayed
        // that lets the user know at which level the value resolved to:
        //
        // "MyFoo" -> Struct/Foo
        // "MyFooLevel" -> level(Struct/Foo)
        //
        // level 0 means the struct was not present
        // level 1 means the value was not present
        //
        // or, we could have a Boolean that represents whether path was present, and user would be build up logic as
        // necessary:
        //
        // "MyFoo" -> Struct1/Struct2/Foo
        // "MyBar" -> Struct1/Struct2/Bar
        // "Struct1Present" -> present(Struct1)
        // "Struct2Present" -> present(Struct1/Struct2) (would be null if Struct1 is not present)

        NestedField lastField = null;
        for (final NestedField nestedField : fieldPath) {
            if (nestedField.type().isListType()) {
                throw new MappingException("List type not supported"); // todo better error
            }
            if (nestedField.type().isMapType()) {
                throw new MappingException("Map type not supported"); // todo better error
            }
            // We *do* support struct mapping implicitly.
            lastField = nestedField;
        }
        if (!lastField.type().isPrimitiveType()) {
            // This could be extended in the future with support for:
            // * List<Primitive>
            // * List<List<...<Primitive>...>
            // * Map<Primitive, Primitive>
            // * Map<List<Primitive>, List<Primitive>>
            // * Struct<...> (if DH theoretically allowed struct columns, unlikely but possible)
            // * Other combinations of above.
            throw new MappingException(String.format("Only support mapping to primitive types, field=[%s]", lastField));
        }
        checkCompatible(lastField.type().asPrimitiveType());
    }

    static void checkCompatible(org.apache.iceberg.types.Type.PrimitiveType type) {
        // If we can't infer a type, we can't support it more generally at this time (this may not be true in the future
        // depending on things like additional options on ColumnInstruction guiding compatibility).
        Type<?> inferredType = TypeInference.of(type).orElse(null);
        if (inferredType == null) {
            throw new MappingException(String.format("Unsupported type `%s`", type));
        }
    }

    public static boolean isCompatible(Type<?> type, org.apache.iceberg.types.Type icebergType) {
        // TODO
        return true;
    }

    public static void checkCompatible(Type<?> type, org.apache.iceberg.types.Type icebergType) {
        // TODO
    }

    public static class MappingException extends RuntimeException {

        public MappingException(String message) {
            super(message);
        }

        public MappingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
