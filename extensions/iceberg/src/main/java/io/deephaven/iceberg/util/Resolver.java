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
import io.deephaven.iceberg.internal.PartitionSpecHelper;
import io.deephaven.iceberg.internal.SchemaHelper;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;


// This is a mapping for _read_

/**
 * This is the necessary structure to resolve data files.
 */
@Value.Immutable
@BuildableStyle
public abstract class Resolver {

    public static Builder builder() {
        return ImmutableResolver.builder();
    }

    public static Resolver empty(Schema schema) {
        return builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                .definition(TableDefinition.of(List.of()))
                .build();
    }

    public static Resolver infer(InferenceInstructions i) throws Inference.UnsupportedType {
        final Inf inf = new Inf(i);
        TypeUtil.visit(i.schema(), inf);
        // TODO: visit the PartitionSpec to build io.deephaven.iceberg.util.ColumnInstructions.partitionField as well
        // i.spec();
        if (i.failOnUnsupportedTypes() && !inf.unsupportedTypes.isEmpty()) {
            throw inf.unsupportedTypes.get(0);
        }
        return inf.build();
    }

    // TODO: infer update
    // public static Resolver inferUpdate(Resolver resolver, InferenceInstructions instructions) {
    // return null;
    // }

    /**
     * The Iceberg schema. This schema is set at the time these instructions were originally made - it is often
     * <b>not</b> the most recent schema for an Iceberg table.
     */
    public abstract Schema schema();

    // todo: should this really be UnboundPartitionSpec ?
    public abstract PartitionSpec spec();

    /**
     * The Deephaven table definition.
     */
    public abstract TableDefinition definition();

    /**
     * The column instructions keyed by Deephaven column name.
     */
    public abstract Map<String, ColumnInstructions> columnInstructions();

    // todo: should this be here, or somewhere else?
    // todo: this doesn't implement equals
    // MappedFields
    public abstract Optional<NameMapping> nameMapping();

    // We need to store as List so we can get test out the mapping wrt equals
    // todo: verify this is pointing to a schema leaf; arguably, doesn't
    // have to be a leaf if it's a list? b/c really, the list type is
    // probably better than a path to the elemnt

    // todo: this should be schema columns?
    @Value.Default
    boolean allowUnmappedColumns() {
        return false;
    }

    // todo: should we have similar setting for partition spec columns?


    // todo: need to specify any special transformations that happen

    public interface Builder {

        Builder schema(Schema schema);

        Builder spec(PartitionSpec spec);

        Builder definition(TableDefinition definition);

        Builder putColumnInstructions(String columnName, ColumnInstructions columnInstructions);

        Builder nameMapping(NameMapping nameMapping);

        Builder allowUnmappedColumns(boolean allowUnmappedColumns);

        Resolver build();
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

    // todo: check various Partitioning stuff; a column can refer to a parititionfieldId

    @Value.Check
    final void checkUnmappedColumns() {
        if (allowUnmappedColumns()) {
            return;
        }
        for (String column : definition().getColumnNameSet()) {
            if (!columnInstructions().containsKey(column)) {
                throw new MappingException(String.format("Column `%s` is not mapped", column));
            }
        }
    }

    @Value.Check
    final void checkColumnNames() {
        // TODO: TableDefinition does not do this :(
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
        try {
            validate(column.isPartitioning(), type, ci);
        } catch (SchemaHelper.PathException | MappingException e) {
            throw new MappingException(String.format("Unable to map Deephaven column %s", column.getName()), e);
        }
    }

    private void validate(boolean isPartitioningColumn, Type<?> type, ColumnInstructions ci)
            throws SchemaHelper.PathException {
        // This are not all hard technical limitations, but just narrowing the set of acceptable combinations. We could
        // search through the partition spec to see if any corresponding w/ identity to the column, but right now,
        // we'll just do that as part of inference.
        if (ci.schemaFieldId().isPresent()) {
            if (isPartitioningColumn) {
                throw new MappingException("Must use normal column with schema field");
            }
            final List<NestedField> fieldPath = ci.schemaFieldPath(schema());
            checkCompatible(fieldPath, type);
        } else {
            if (!isPartitioningColumn) {
                throw new MappingException("Must use partitioning column with partition field");
            }
            final PartitionField partitionField = ci.partitionField(spec());
            checkCompatible(schema(), partitionField, type);
        }
    }

    // TODO
    public final ResolverFactory factory() {
        return new ResolverFactory(this);
    }

    final Optional<List<NestedField>> resolveSchemaFieldViaReadersSchema(String columnName) {
        // todo: mixin spec?
        try {
            return resolveSchemaFieldVia(columnName, schema());
        } catch (SchemaHelper.PathException e) {
            // should already be accounted for in checkCompatibility
            throw new IllegalStateException(e);
        }
    }

    final Optional<List<NestedField>> resolveSchemaFieldVia(String columnName, Schema schema)
            throws SchemaHelper.PathException {
        final ColumnInstructions ci = columnInstructions().get(columnName);
        if (ci == null) {
            return Optional.empty();
        }

        return Optional.of(SchemaHelper.fieldPath(schema, ci.schemaFieldId().orElseThrow()));


        // todo: mixin spec?
        // return Optional.of(ci.schemaFieldPath().orElseThrow().resolve(schema));
    }

    // We could change result type to a List or something more complex in future if necessary.
    private static class Inf extends TypeUtil.SchemaVisitor<Void> {

        private final InferenceInstructions ii;
        private final InferenceInstructions.Namer namer;

        // build results
        private final Builder builder;
        private final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        private final List<Inference.UnsupportedType> unsupportedTypes = new ArrayList<>();

        // state
        private int skipDepth = 0; // if skipDepth > 0, we should skip inference on any types we visit
        private final List<Types.NestedField> fieldPath = new ArrayList<>();


        public Inf(InferenceInstructions ii) {
            this.ii = Objects.requireNonNull(ii);
            this.namer = ii.namerFactory().create();
            this.builder = builder()
                    .schema(ii.schema())
                    .spec(ii.spec())
                    .allowUnmappedColumns(false);
        }

        Resolver build() {
            return builder
                    .definition(TableDefinition.of(definitions))
                    .build();
        }

        private boolean isSkip() {
            if (ii.skip().isEmpty()) {
                return false;
            }
            // Not the most efficient check, but should not matter given this is only done during inference.
            final FieldPath fp = FieldPath.of(fieldPath.stream().mapToInt(NestedField::fieldId).toArray());
            return ii.skip().contains(fp);
        }

        private void push(NestedField field) {
            fieldPath.add(field);
            if (isSkip()) {
                ++skipDepth;
            }
        }

        private void pop(NestedField field) {
            if (isSkip()) {
                --skipDepth;
            }
            Assert.eq(field, "field", fieldPath.remove(fieldPath.size() - 1));
        }

        @Override
        public Void primitive(org.apache.iceberg.types.Type.PrimitiveType primitive) {
            if (skipDepth != 0) {
                return null;
            }
            final Type<?> type = Inference.of(primitive).orElse(null);
            if (type == null) {
                unsupportedTypes.add(new Inference.UnsupportedType(ii.schema(), fieldPath));
                return null;
            }
            final String columnName = namer.of(fieldPath, type);
            NameValidator.validateColumnName(columnName);
            // final int[] idPath = fieldPath.stream().mapToInt(NestedField::fieldId).toArray();
            // builder.putColumnInstructions(columnName, ColumnInstructions.schemaFieldPath(FieldPath.of(idPath)));
            builder.putColumnInstructions(columnName, schemaField(currentFieldId()));
            definitions.add(ColumnDefinition.of(columnName, type));
            return null;
        }

        private int currentFieldId() {
            return fieldPath.get(fieldPath.size() - 1).fieldId();
        }

        @Override
        public Void struct(Types.StructType struct, List<Void> fieldResults) {
            if (skipDepth != 0) {
                return null;
            }
            return null;
        }

        @Override
        public Void schema(Schema schema, Void structResult) {
            if (skipDepth != 0) {
                return null;
            }
            return null;
        }

        @Override
        public Void map(Types.MapType map, Void keyResult, Void valueResult) {
            if (skipDepth != 0) {
                return null;
            }
            unsupportedTypes.add(new Inference.UnsupportedType(ii.schema(), fieldPath));
            return null;
        }

        @Override
        public Void list(Types.ListType list, Void elementResult) {
            if (skipDepth != 0) {
                return null;
            }
            unsupportedTypes.add(new Inference.UnsupportedType(ii.schema(), fieldPath));
            return null;
        }

        @Override
        public Void variant() {
            if (skipDepth != 0) {
                return null;
            }
            unsupportedTypes.add(new Inference.UnsupportedType(ii.schema(), fieldPath));
            return null;
        }

        @Override
        public Void field(NestedField field, Void fieldResult) {
            if (skipDepth != 0) {
                return null;
            }
            return null;
        }

        @Override
        public void beforeField(NestedField field) {
            push(field);
        }

        @Override
        public void afterField(NestedField field) {
            pop(field);
        }

        @Override
        public void beforeListElement(NestedField elementField) {
            push(elementField);
            ++skipDepth;
        }

        @Override
        public void afterListElement(NestedField elementField) {
            --skipDepth;
            pop(elementField);
        }

        @Override
        public void beforeMapKey(NestedField keyField) {
            push(keyField);
            ++skipDepth;
        }

        @Override
        public void afterMapKey(NestedField keyField) {
            --skipDepth;
            pop(keyField);
        }

        @Override
        public void beforeMapValue(NestedField valueField) {
            push(valueField);
            ++skipDepth;
        }

        @Override
        public void afterMapValue(NestedField valueField) {
            --skipDepth;
            pop(valueField);
        }
    }



    static boolean isCompatible(Collection<? extends NestedField> path, Type<?> type) {
        try {
            checkCompatible(path, type);
            return true;
        } catch (MappingException e) {
            return false;
        }
    }

    static void checkCompatible(Schema schema, PartitionField partitionField, Type<?> type) {
        if (!partitionField.transform().isIdentity()) {
            throw new MappingException(String
                    .format("Unable to map partitionField=[%s], only identity transform is supported", partitionField));
        }
        final Optional<FieldPath> fieldPath = FieldPath.find(schema, partitionField.sourceId());
        if (fieldPath.isEmpty()) {
            throw new MappingException(
                    String.format("Unable to map partitionField=[%s], sourceId not found in schema", partitionField));
        }
        final List<NestedField> fields;
        try {
            fields = fieldPath.get().resolve(schema);
        } catch (SchemaHelper.PathException e) {
            // Should not happen since we just found it via this schema
            throw new IllegalStateException(e);
        }
        try {
            checkCompatible(fields, type);
        } catch (MappingException e) {
            throw new MappingException(String.format("Unable to map partitionField=[%s]", partitionField), e);
        }
    }

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

    private static RuntimeException extract(RuntimeException e) throws Inference.Exception {
        if (e.getCause() instanceof Inference.Exception) {
            throw ((Inference.Exception) e.getCause());
        }
        throw e;
    }
}
