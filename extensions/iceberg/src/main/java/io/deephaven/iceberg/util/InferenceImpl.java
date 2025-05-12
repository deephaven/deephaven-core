//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class InferenceImpl extends TypeUtil.SchemaVisitor<Void> {

    static Resolver of(InferenceInstructions inferenceInstructions) throws TypeInference.UnsupportedType {
        final InferenceImpl visitor = new InferenceImpl(inferenceInstructions);
        TypeUtil.visit(inferenceInstructions.schema(), visitor);
        return visitor.build();
    }

    private final InferenceInstructions ii;
    private final InferenceInstructions.Namer namer;

    // build results
    private final Resolver.Builder builder;
    private final List<ColumnDefinition<?>> definitions = new ArrayList<>();
    private final List<TypeInference.UnsupportedType> unsupportedTypes = new ArrayList<>();

    // state
    private int skipDepth = 0; // if skipDepth > 0, we should skip inference on any types we visit
    private final List<Types.NestedField> fieldPath = new ArrayList<>();

    private InferenceImpl(InferenceInstructions ii) {
        this.ii = Objects.requireNonNull(ii);
        this.namer = ii.namerFactory().create();
        this.builder = Resolver.builder()
                .schema(ii.schema());
    }

    Resolver build() throws TypeInference.UnsupportedType {
        if (ii.failOnUnsupportedTypes() && !unsupportedTypes.isEmpty()) {
            throw unsupportedTypes.get(0);
        }
        if (ii.spec().isPresent() && definitions.stream().anyMatch(ColumnDefinition::isPartitioning)) {
            // Only pass along the spec if it has actually been used
            builder.spec(ii.spec().get());
        }
        return builder
                .definition(TableDefinition.of(definitions))
                .build();
    }

    private boolean isSkip() {
        return false;
        // Note: this was a proposed implementation for skip. If we need this feature, we can re-add it in the future.
        // if (ii.skip().isEmpty()) {
        // return false;
        // }
        // // Not the most efficient check, but should not matter given this is only done during inference.
        // final FieldPath fp = FieldPath.of(fieldPath.stream().mapToInt(Types.NestedField::fieldId).toArray());
        // return ii.skip().contains(fp);
    }

    private void push(Types.NestedField field) {
        fieldPath.add(field);
        if (isSkip()) {
            ++skipDepth;
        }
    }

    private void pop(Types.NestedField field) {
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
        final Type<?> type = TypeInference.of(primitive).orElse(null);
        if (type == null) {
            unsupportedTypes.add(new TypeInference.UnsupportedType(ii.schema(), fieldPath));
            return null;
        }

        final String columnName = namer.of(fieldPath, type);
        NameValidator.validateColumnName(columnName);

        final int fieldId = currentFieldId();
        final ColumnDefinition<?> columnDefinition;
        final ColumnInstructions columnInstructions;
        {
            final ColumnDefinition<?> cd = ColumnDefinition.of(columnName, type);
            final PartitionField pf = ii.spec().isPresent()
                    ? PartitionSpecHelper.findIdentityForSchemaFieldId(ii.spec().get(), fieldId).orElse(null)
                    : null;
            if (pf == null) {
                columnDefinition = cd;
                columnInstructions = ColumnInstructions.schemaField(fieldId);
            } else {
                columnDefinition = cd.withPartitioning();
                columnInstructions = ColumnInstructions.partitionField(pf.fieldId());
            }
        }
        builder.putColumnInstructions(columnName, columnInstructions);
        definitions.add(columnDefinition);
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
        unsupportedTypes.add(new TypeInference.UnsupportedType(ii.schema(), fieldPath));
        return null;
    }

    @Override
    public Void list(Types.ListType list, Void elementResult) {
        if (skipDepth != 0) {
            return null;
        }
        unsupportedTypes.add(new TypeInference.UnsupportedType(ii.schema(), fieldPath));
        return null;
    }

    @Override
    public Void variant(Types.VariantType variant) {
        if (skipDepth != 0) {
            return null;
        }
        unsupportedTypes.add(new TypeInference.UnsupportedType(ii.schema(), fieldPath));
        return null;
    }

    @Override
    public Void field(Types.NestedField field, Void fieldResult) {
        if (skipDepth != 0) {
            return null;
        }
        return null;
    }

    @Override
    public void beforeField(Types.NestedField field) {
        push(field);
    }

    @Override
    public void afterField(Types.NestedField field) {
        pop(field);
    }

    @Override
    public void beforeListElement(Types.NestedField elementField) {
        push(elementField);
        ++skipDepth;
    }

    @Override
    public void afterListElement(Types.NestedField elementField) {
        --skipDepth;
        pop(elementField);
    }

    @Override
    public void beforeMapKey(Types.NestedField keyField) {
        push(keyField);
        ++skipDepth;
    }

    @Override
    public void afterMapKey(Types.NestedField keyField) {
        --skipDepth;
        pop(keyField);
    }

    @Override
    public void beforeMapValue(Types.NestedField valueField) {
        push(valueField);
        ++skipDepth;
    }

    @Override
    public void afterMapValue(Types.NestedField valueField) {
        --skipDepth;
        pop(valueField);
    }
}
