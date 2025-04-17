//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.SchemaHelper;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

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

    public static Builder builder() {
        return ImmutableResolver.builder();
    }

    // todo: a version that takes Schema and TableDef and does a name-based-only guess? this is not really the same as
    // inference (which is based solely on a Schema)

    /**
     * Infer a resolver based on the {@link Schema} and {@link TableDefinition}. This uses column names to map the
     * Deephaven columns to Iceberg fields.
     * <p>
     * The provided {@code tableDefinition} must not have any partitioning columns. In that case, this method will throw
     * a {@link IllegalArgumentException}. For that case, you should use the {@link #builder()} with appropriate
     * {@link #spec()} to build a resolver.
     *
     * @see #definition()
     */
    public static Resolver infer(final Schema schema, final TableDefinition tableDefinition) {
        final Resolver.Builder builder = Resolver.builder()
                .schema(schema)
                .definition(tableDefinition);
        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
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

            final int fieldID = icebergField.fieldId();
            builder.putColumnInstructions(dhColumnName, ColumnInstructions.schemaField(fieldID));
        }
        return builder.build();
    }

    /**
     * Infer a resolver based on the {@code inferenceInstructions}.
     *
     * @param inferenceInstructions the inference instructions
     * @return the resolver
     * @throws Inference.UnsupportedType if an unsupported type is encountered
     */
    public static Resolver infer(final InferenceInstructions inferenceInstructions) throws Inference.UnsupportedType {
        return inferBuilder(inferenceInstructions).build();
    }

    /**
     * Infer a resolver-builder based on the {@code inferenceInstructions}. This sets everything necessary, except
     * leaves {@link Resolver.Builder#nameMapping(NameMapping) name-mapping} unset, allowing the caller to provide it if
     * necessary.
     *
     * @param inferenceInstructions the inference instructions
     * @return the resolver-builder
     * @throws Inference.UnsupportedType if an unsupported type is encountered
     */
    public static Resolver.Builder inferBuilder(final InferenceInstructions inferenceInstructions)
            throws Inference.UnsupportedType {
        return InferenceImpl.of(inferenceInstructions);
    }

    /**
     * The Deephaven table definition.
     *
     * <p>
     * By default, it is expected that every column in this definition be mapped to a corresponding Iceberg field (via
     * {@link #columnInstructions()}. In the case where a column is defined here, but not in Iceberg,
     *
     * <p>
     * Callers should take care and only use {@link ColumnDefinition.ColumnType#Partitioning} columns when they know the
     * Iceberg table will always have {@link Transform#isIdentity() identity} partitions for said columns. In the
     * general case, Iceberg partitions may evolve over time, which can break the assumptions Deephaven makes about
     * partitioning columns.
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
     * The column instructions keyed by Deephaven column name.
     */
    public abstract Map<String, ColumnInstructions> columnInstructions();

    /**
     * The name mapping. This provides a fallback for resolving columns from data files that are written without
     * field-ids. This is typically provided from the Iceberg {@link Table#properties() Table property}
     * {@value TableProperties#DEFAULT_NAME_MAPPING}.
     *
     * @see NameMappingParser#fromJson(String)
     * @see <a href="https://iceberg.apache.org/spec/#column-projection">schema.name-mapping.default</a>
     */
    public abstract Optional<NameMapping> nameMapping();

    // @Value.Default
    // boolean allowUnmappedColumns() {
    // return false;
    // }

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

        Builder nameMapping(NameMapping nameMapping);

        // Builder allowUnmappedColumns(boolean allowUnmappedColumns);

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
        // This is not a hard limitation; could be improved in the future
        if (partitionField != null && !partitionField.transform().isIdentity()) {
            throw new MappingException(String
                    .format("Unable to map partition field `%s`, only identity transform is supported",
                            partitionField));
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

    // @Value.Derived
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

    // @Value.Derived
    // List<PartitionField> partitionFields() {
    // return definition()
    // .getColumnStream()
    // .filter(ColumnDefinition::isPartitioning)
    // .map(this::partitionField)
    // .collect(Collectors.toUnmodifiableList());
    // }

    static void checkCompatible(Collection<? extends NestedField> path, Type<?> type) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        checkCompatible(path);
        // todo: compare against DH type(s)
        final NestedField lastField = path.stream().reduce((p, n) -> n).orElseThrow();
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
        // do we even support this type? note: it's _possible_ there are cases where there is a primitive type where
        // we don't support inference, but do support compatibility with DH type... TODO
        Inference.of(type).orElseThrow(() -> new MappingException("todo"));
        // try {
        //
        // } catch (Inference.UnsupportedType e) {
        // throw new MappingException(e.getMessage());
        // }
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
