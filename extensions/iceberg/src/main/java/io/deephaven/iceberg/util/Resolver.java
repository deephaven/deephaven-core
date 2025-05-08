//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.Type;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.TypeUtil;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * The relationship between a Deephaven {@link TableDefinition} and an Iceberg {@link Schema}.
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

    // Implementation note: it's important that all the fields to construct a Resolver be publicly accessible so that
    // callers who use inference can understand precisely how the Resolver was constructed. This implies that the
    // various build setters should have public getters for the respective fields. This applies transitively as well,
    // so ColumnInstructions needs to adhere to this as well.

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
     * {@link ColumnInstructions#schemaField(int) schema field}, {@link ColumnInstructions#schemaFieldName(String)
     * schema field name}, or be {@link ColumnInstructions#unmapped() unmapped}; and
     * {@link ColumnDefinition.ColumnType#Partitioning partitioning} columns must reference a
     * {@link ColumnInstructions#schemaField(int) schema field}, {@link ColumnInstructions#schemaFieldName(String)
     * schema field name}, or {@link ColumnInstructions#partitionField(int) partition field}.
     *
     * <p>
     * The {@link #definition()} and {@link #schema()} types must be
     * {@link TypeCompatibility#isCompatible(Type, org.apache.iceberg.types.Type)}.
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
            fieldPath = ci.schemaFieldPathById(schema());
            partitionField = isPartitioningColumn
                    ? ci.partitionFieldFromSchemaFieldId(specOrUnpartitioned())
                    : null;
        } else if (ci.schemaFieldName().isPresent()) {
            fieldPath = ci.schemaFieldPathByName(schema());
            partitionField = isPartitioningColumn
                    ? ci.partitionFieldFromSchemaFieldName(schema(), specOrUnpartitioned())
                    : null;
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
                // https://github.com/apache/iceberg/issues/12870
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
        checkPath(path);
        final NestedField lastField = path.get(path.size() - 1);
        if (!TypeCompatibility.isCompatible(type, lastField.type())) {
            throw new MappingException(
                    String.format("Incompatible types @ `%s`, icebergType=`%s`, type=`%s`",
                            SchemaHelper.toFieldName(path), lastField.type(), type));
        }
    }

    static void checkPath(List<? extends NestedField> path) {
        if (path.isEmpty()) {
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
        final List<NestedField> subPath = new ArrayList<>(path.size());
        for (final NestedField field : path.subList(0, path.size() - 1)) {
            subPath.add(field);
            if (field.type().isListType()) {
                throw new MappingException(String.format("List subpath @ `%s` (in `%s`) is not supported",
                        SchemaHelper.toFieldName(subPath), SchemaHelper.toFieldName(path)));
            }
            if (field.type().isMapType()) {
                throw new MappingException(String.format("Map subpath @ `%s` (in `%s`) is not supported",
                        SchemaHelper.toFieldName(subPath), SchemaHelper.toFieldName(path)));
            }
        }
    }

    public static class MappingException extends RuntimeException {

        public MappingException(String message) {
            super(message);
        }

        public MappingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Creates a {@link Resolver} from the given Table {@code definition}, <b>only applicable in contexts where an
     * {@link Table Iceberg Table} does not already exist</b>. In cases where the {@link Table Iceberg Table} already
     * exist, callers must create a resolver in relationship to an existing {@link Schema} (for example, via
     * {@link #infer(Schema)}, or manually via {@link #builder()}). Column type inference is done via
     * {@link TypeInference#of(Type, Type.Visitor)}.
     *
     * <p>
     * All {@link ColumnDefinition.ColumnType#Partitioning partitioning columns} will be used to create a partition spec
     * for the table. Callers should take note of the documentation on {@link Resolver#definition()} when deciding to
     * create an Iceberg Table with partitioning columns.
     *
     * @param definition the Table definition
     * @return the resolver
     */
    @VisibleForTesting
    static Resolver from(@NotNull final TableDefinition definition) {
        final Resolver.Builder builder = Resolver.builder().definition(definition);
        final Collection<String> partitioningColumnNames = new ArrayList<>();
        final List<Types.NestedField> fields = new ArrayList<>();
        final TypeUtil.NextID nextID = new AtomicInteger(INITIAL_FIELD_ID)::getAndIncrement;
        final Type.Visitor<org.apache.iceberg.types.Type> inferenceVisitor = new TypeInference.BestIcebergType(nextID);
        for (final ColumnDefinition<?> columnDefinition : definition.getColumns()) {
            final String dhColumnName = columnDefinition.getName();
            final Type<?> type = Type.find(columnDefinition.getDataType(), columnDefinition.getComponentType());
            final org.apache.iceberg.types.Type icebergType = TypeInference.of(type, inferenceVisitor).orElse(null);
            if (icebergType == null) {
                throw new MappingException(
                        String.format("Unable to infer the best Iceberg type for Deephaven column type `%s`", type));
            }
            final int fieldId = nextID.get();
            fields.add(Types.NestedField.optional(fieldId, dhColumnName, icebergType));
            if (columnDefinition.isPartitioning()) {
                partitioningColumnNames.add(dhColumnName);
            }
            builder.putColumnInstructions(dhColumnName, ColumnInstructions.schemaField(fieldId));
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

    @VisibleForTesting
    static Resolver refreshIds(
            final Resolver internalResolver,
            final Schema freshSchema,
            final PartitionSpec freshSpec) {
        final Builder builder = builder()
                .definition(internalResolver.definition())
                .schema(freshSchema);
        if (internalResolver.spec().isPresent()) {
            builder.spec(freshSpec);
        }
        final Map<Integer, String> internalById = TypeUtil.indexNameById(internalResolver.schema().asStruct());
        final Map<String, Integer> freshByName = TypeUtil.indexByName(freshSchema.asStruct());
        for (final Map.Entry<String, ColumnInstructions> e : internalResolver.columnInstructions().entrySet()) {
            final ColumnInstructions internalCi = e.getValue();
            final ColumnInstructions freshCi;
            if (internalCi.isUnmapped()) {
                freshCi = internalCi;
            } else if (internalCi.schemaFieldId().isPresent()) {
                final String name = internalById.get(internalCi.schemaFieldId().getAsInt());
                final int freshId = freshByName.get(name);
                freshCi = internalCi.reassignWithSchemaField(freshId);
            } else if (internalCi.partitionFieldId().isPresent()) {
                final PartitionField internalPf = PartitionSpecHelper
                        .find(internalResolver.spec().orElseThrow(), internalCi.partitionFieldId().getAsInt())
                        .orElseThrow();
                final PartitionField freshPf = PartitionSpecHelper.find(freshSpec, internalPf.name()).orElseThrow();
                freshCi = internalCi.reassignWithPartitionField(freshPf.fieldId());
            } else {
                throw new IllegalStateException();
            }
            builder.putColumnInstructions(e.getKey(), freshCi);
        }
        return builder.build();
    }
}
