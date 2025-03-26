//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

final class ResolverFactory implements ParquetColumnResolver.Factory {

    private final Resolver resolver;

    ResolverFactory(Resolver resolver) {
        this.resolver = Objects.requireNonNull(resolver);
    }

    @Override
    public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
        return new ResolverImpl((IcebergTableParquetLocationKey) tableLocationKey);
    }

    private class ResolverImpl implements ParquetColumnResolver {

        private final IcebergTableParquetLocationKey key;

        public ResolverImpl(IcebergTableParquetLocationKey key) {
            this.key = Objects.requireNonNull(key);
        }

        @Override
        public Optional<List<String>> of(String columnName) {
            final List<Types.NestedField> readersPath =
                    resolver.resolveSchemaFieldViaReadersSchema(columnName).orElse(null);
            if (readersPath == null) {
                // DH did not map this column name
                return Optional.empty();
            }
            // Note: if Iceberg had a way to relay the writer's schema, we could use it to check whether the parquet
            // file even has this field and could potentially save ourselves from needing te read the file itself to
            // check. As it stands now, we need to read the schema and physically check if it's been written out or not.
            // See https://lists.apache.org/thread/98m6d7b08fzxkbxlm78c5tnx5zp93mgc
            // final List<Types.NestedField> writersFields;
            // try {
            // writersFields = instructions.resolveVia(columnName, key.writersSchema()).orElse(null);
            // } catch (SchemaHelper.PathException e) {
            // // Writer did not write this column
            // return Optional.empty();
            // }
            // Note: intentionally delaying the reading of the Parquet schema as late as possible.
            final MessageType parquetSchema = key.getSchema();
            try {
                return Optional.of(resolve(parquetSchema, readersPath, resolver.nameMapping().orElse(null)));
            } catch (MappingException e) {
                // TODO: we don't have enough info to know whether this is expected or not. log?
                return Optional.empty();
            }
        }
    }

    private static List<String> resolve(
            final MessageType schema,
            final List<Types.NestedField> readersPath,
            @Nullable final NameMapping nameMapping) throws MappingException {
        Type current = schema;
        MappedFields fallbackFields = nameMapping == null ? null : nameMapping.asMappedFields();
        final List<String> out = new ArrayList<>();
        for (final Types.NestedField readerField : readersPath) {
            final MappedField fallback = fallbackFields == null ? null : fallbackFields.field(readerField.fieldId());
            final List<Type> types = find(current.asGroupType(), readerField.fieldId(), readerField.type(), fallback);
            for (Type type : types) {
                out.add(type.getName());
            }
            current = types.get(types.size() - 1);
            fallbackFields = fallback == null ? null : fallback.nestedMapping();
        }
        return out;
    }

    private static List<Type> find(
            final GroupType type,
            final int fieldId,
            final org.apache.iceberg.types.Type readerType,
            @Nullable final MappedField fallback) throws MappingException {
        if (readerType.isPrimitiveType()) {
            return List.of(findPrimitive(fieldId, type, readerType.asPrimitiveType(), fallback));
        }
        if (readerType.isStructType()) {
            return List.of(findStruct(fieldId, type, readerType.asStructType(), fallback));
        }
        if (readerType.isMapType()) {
            return findMap(fieldId, type, readerType.asMapType(), fallback);
        }
        if (readerType.isListType()) {
            return findList(fieldId, type, readerType.asListType(), fallback);
        }
        throw new IllegalStateException();
    }

    private static Type findField(
            final int fieldId,
            final GroupType type,
            @Nullable final MappedField fallback) throws MappingException {
        try {
            return findField(fieldId, type);
        } catch (MappingException e) {
            if (fallback == null) {
                throw e;
            }
            try {
                return findField(fallback, type);
            } catch (MappingException e2) {
                e.addSuppressed(e2);
                throw e;
            }
        }
    }

    private static Type findField(final int fieldId, final GroupType type) throws MappingException {
        Type found = null;
        for (Type field : type.getFields()) {
            if (field.getId() != null && field.getId().intValue() == fieldId) {
                if (found != null) {
                    throw new Duplicate(String.format("Duplicate field-id %d found", fieldId));
                }
                found = field;
            }
        }
        if (found == null) {
            throw new NotFound("not found " + fieldId);
        }
        return found;
    }

    private static Type findField(MappedField fallback, GroupType type) throws MappingException {
        Type found = null;
        for (Type field : type.getFields()) {
            if (fallback.names().contains(field.getName())) {
                if (found != null) {
                    throw new Duplicate(String.format("Duplicate field names %s, %s found for field-id %d",
                            found.getName(), field.getName(), fallback.id()));
                }
                found = field;
            }
        }
        if (found == null) {
            throw new NotFound("not found " + fallback);
        }
        return found;
    }

    private static Type findPrimitive(
            final int fieldId,
            final GroupType type,
            final org.apache.iceberg.types.Type.PrimitiveType readerPrimitiveType,
            @Nullable final MappedField fallback) throws MappingException {
        final Type found = findField(fieldId, type, fallback);
        checkCompatible(found, readerPrimitiveType);
        return found;
    }

    private static Type findStruct(
            final int fieldId,
            final GroupType type,
            final Types.StructType readerStructType,
            @Nullable final MappedField fallback) throws MappingException {
        final Type found = findField(fieldId, type, fallback);
        checkCompatible(found, readerStructType);
        return found;
    }

    private static List<Type> findMap(
            final int fieldId,
            final GroupType type,
            final Types.MapType readerMapType,
            @Nullable final MappedField fallback) throws MappingException {
        throw new MapUnsupported();
    }

    private static List<Type> findList(
            final int fieldId,
            final GroupType type,
            final Types.ListType readerListType,
            @Nullable final MappedField fallback) throws MappingException {
        throw new ListUnsupported();
    }

    private static void checkCompatible(Type ptype, org.apache.iceberg.types.Type.PrimitiveType readerPrimitiveType) {
        // TODO
    }

    private static void checkCompatible(Type ptype, Types.StructType readerStructType) {
        // TODO
    }

    private static void checkCompatible(List<Type> ptypes, Types.ListType readerListType) {

    }

    private static void checkCompatible(List<Type> ptypes, Types.MapType readerMapType) {

    }

    private static abstract class MappingException extends Exception {

        public MappingException() {}

        public MappingException(String message) {
            super(message);
        }
    }

    private static class NotFound extends MappingException {

        public NotFound(String message) {
            super(message);
        }
    }

    private static class Duplicate extends MappingException {

        public Duplicate(String message) {
            super(message);
        }
    }

    private static class MapUnsupported extends MappingException {

    }

    private static class ListUnsupported extends MappingException {

    }
}
