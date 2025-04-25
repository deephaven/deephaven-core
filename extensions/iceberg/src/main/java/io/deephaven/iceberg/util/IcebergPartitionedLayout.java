//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.iceberg.layout.IcebergBaseLayout;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.qst.type.Type;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class IcebergPartitionedLayout extends IcebergBaseLayout {

    static void validateSupported(
            Transform<?, ?> transform,
            org.apache.iceberg.types.Type.PrimitiveType inputType,
            Type<?> columnType) {
        if (!transform.isIdentity()) {
            throw new Resolver.MappingException(String.format("Transform `%s` is not supported", transform));
        }
        validateIdentity(inputType, columnType);
    }

    private final Map<String, PartitionField> partitionFields;

    IcebergPartitionedLayout(
            @NotNull IcebergTableAdapter tableAdapter,
            @NotNull ParquetInstructions parquetInstructions,
            @NotNull SeekableChannelsProvider seekableChannelsProvider,
            @Nullable Snapshot snapshot,
            @NotNull Resolver resolver) {
        super(tableAdapter, parquetInstructions, seekableChannelsProvider, snapshot);
        this.partitionFields = resolver.partitionFieldMap();
        // This sort of check should be redundant; the resolver should be doing all these checks itself. This is an
        // extra layer of safety though, co-located closer to the where the usage actually occurs.
        for (Map.Entry<String, PartitionField> e : partitionFields.entrySet()) {
            final String columnName = e.getKey();
            final ColumnDefinition<?> column = resolver.definition().getColumn(columnName);
            final Type<?> type = Type.find(column.getDataType(), column.getComponentType());
            final PartitionField partitionField = e.getValue();
            final List<Types.NestedField> fieldPath = resolver.resolve(columnName).orElseThrow();
            final org.apache.iceberg.types.Type.PrimitiveType inputType =
                    fieldPath.get(fieldPath.size() - 1).type().asPrimitiveType();
            try {
                validateSupported(partitionField.transform(), inputType, type);
            } catch (Resolver.MappingException mappingException) {
                throw new IllegalStateException(mappingException);
            }
        }
    }

    private static Object getPartitionValue(PartitionField partitionField, PartitionData data) {
        // Note: we could compute this mapping once per ManifestFile (give they are supposed to have the same partition
        // spec) but right now we are just doing it on-demand per data file.
        final List<Types.NestedField> fields = data.getPartitionType().fields();
        final int size = fields.size();
        int ix;
        for (ix = 0; ix < size; ix++) {
            if (fields.get(ix).fieldId() == partitionField.fieldId()) {
                break;
            }
        }
        if (ix == size) {
            throw new IllegalStateException(String.format(
                    "Unable to find partition field id %d. This likely means that the underlying Iceberg Table's partition spec has evolved in a way incompatible with Deephaven partitioning columns. See the warning on `%s.definition` for more information.",
                    partitionField.fieldId(), Resolver.class.getSimpleName()));
        }
        final Object rawValue = data.get(ix);
        if (!partitionField.transform().isIdentity()) {
            return rawValue;
        }
        return IdentityPartitionConverters.convertConstant(data.getType(ix), rawValue);
    }

    @Override
    protected IcebergTableLocationKey keyFromDataFile(
            @NotNull final PartitionSpec manifestPartitionSpec,
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        final PartitionData partitionData = (PartitionData) dataFile.partition();
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>(partitionFields.size());
        for (final Map.Entry<String, PartitionField> e : partitionFields.entrySet()) {
            final Object partitionValue = getPartitionValue(e.getValue(), partitionData);
            partitions.put(e.getKey(), (Comparable<?>) partitionValue);
        }
        return locationKey(manifestPartitionSpec, manifestFile, dataFile, fileUri, partitions, channelsProvider);
    }

    @Override
    public String toString() {
        return IcebergPartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    private static void validateIdentity(org.apache.iceberg.types.Type.PrimitiveType inputType, Type<?> columnType) {
        // There is a dance between the different types here. We should aim to be explicit for every partition type we
        // support, as well as any coercions, as there may be implicit assumptions between the various code paths
        // (org.apache.iceberg.data.IdentityPartitionConverters.convertConstant, Inference, etc). For example,
        // convertConstant with org.apache.iceberg.types.Types.TimestampType.shouldAdjustToUTC returns an OffsetDateTime
        // whereas we prefer to infer this as Instant. Now, this example may not be relevant (because while you _can_
        // have an identity on a Timestamp, it seems unlikely). Ideally, all of these cases would also be tested against
        // the actual reading code (as opposed to initial type checking).
        switch (inputType.typeId()) {
            case STRING:
                validateStringIdentity(columnType);
                break;
            case BOOLEAN:
                validateBooleanIdentity(columnType);
                break;
            case INTEGER:
                validateIntegerIdentity(columnType);
                break;
            case LONG:
                validateLongIdentity(columnType);
                break;
            case FLOAT:
                // it is questionable whether we actually want to support this
                validateFloatIdentity(columnType);
                break;
            case DOUBLE:
                // it is questionable whether we actually want to support this
                validateDoubleIdentity(columnType);
                break;
            case DATE:
                validateDateIdentity(columnType);
                break;
            default:
                throw new Resolver.MappingException(
                        String.format("Identity transform of type `%s` is not supported", inputType));
        }
    }

    private static void validateStringIdentity(Type<?> columnType) {
        if (Type.stringType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.StringType.get(), columnType);
    }

    private static void validateBooleanIdentity(Type<?> columnType) {
        if (Type.booleanType().equals(columnType) || Type.booleanType().boxedType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.BooleanType.get(), columnType);
    }

    private static void validateIntegerIdentity(Type<?> columnType) {
        if (Type.intType().equals(columnType) || Type.intType().boxedType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.IntegerType.get(), columnType);
    }

    private static void validateLongIdentity(Type<?> columnType) {
        if (Type.longType().equals(columnType) || Type.longType().boxedType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.LongType.get(), columnType);
    }

    private static void validateFloatIdentity(Type<?> columnType) {
        if (Type.floatType().equals(columnType) || Type.floatType().boxedType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.FloatType.get(), columnType);
    }

    private static void validateDoubleIdentity(Type<?> columnType) {
        if (Type.doubleType().equals(columnType) || Type.doubleType().boxedType().equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.DoubleType.get(), columnType);
    }

    private static void validateDateIdentity(Type<?> columnType) {
        if (Type.find(LocalDate.class).equals(columnType)) {
            return;
        }
        throw unsupportedIdentityCoercion(Types.DateType.get(), columnType);
    }

    private static Resolver.MappingException unsupportedIdentityCoercion(
            org.apache.iceberg.types.Type.PrimitiveType partitionType, Type<?> columnType) {
        return new Resolver.MappingException(String
                .format("Identity transform of type `%s` does not support coercion to %s", partitionType, columnType));
    }
}
