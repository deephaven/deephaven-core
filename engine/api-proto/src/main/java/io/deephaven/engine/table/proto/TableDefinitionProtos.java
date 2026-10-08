//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.proto.gen.ColumnDefinitionProto;
import io.deephaven.engine.table.proto.gen.ColumnTypeProto;
import io.deephaven.engine.table.proto.gen.JavaClassProto;
import io.deephaven.engine.table.proto.gen.PersistedTableDefinitionProto;
import io.deephaven.engine.table.proto.gen.PrimitiveTypeProto;
import io.deephaven.engine.table.proto.gen.TableDefinitionProto;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Persists {@link TableDefinition table definitions} to files, and reads them back, so that a definition written by one
 * process (for example, the producer of a table) can be shared with others (for example, its consumers).
 *
 * <p>
 * The file format is the binary protobuf encoding of an internal (non-RPC) message,
 * {@link PersistedTableDefinitionProto}. Reading is exact: {@code read(path).equals(definition)} after
 * {@code write(path, definition, ...)}, provided every class the definition references can be loaded by the reader.
 * Non-primitive classes are recorded by {@link Class#getName() name}, so the reader needs them on its classpath.
 * Reading is also strict: a file containing fields this version does not know about is rejected rather than partially
 * understood.
 */
public final class TableDefinitionProtos {

    /**
     * Writes {@code definition} to a new file at {@code path}. By default this fails if {@code path} already exists;
     * pass {@link StandardCopyOption#REPLACE_EXISTING} to replace it.
     *
     * <p>
     * The file is written to a temporary sibling first and then published at {@code path} in a single atomic step, so
     * concurrent readers see either no file (or the previous one, when replacing) or the complete new definition, never
     * a partial write. Publishing always creates a new file: when replacing, the previous file is not modified in
     * place, so its ownership, permissions, and other links are not carried over, and readers that already have it open
     * keep seeing the previous definition. A symbolic link at {@code path} is replaced, not followed.
     *
     * <p>
     * Like {@link Files#copy(java.io.InputStream, Path, CopyOption...)}, the supported options are:
     * <ul>
     * <li>{@link StandardCopyOption#REPLACE_EXISTING}: replace {@code path} if it exists. The replacement is
     * atomic.</li>
     * <li>{@link StandardCopyOption#ATOMIC_MOVE}: accepted, but has no effect, as publishing is always atomic.</li>
     * </ul>
     *
     * @param path the file to write
     * @param definition the table definition
     * @param options how to publish the file
     * @throws FileAlreadyExistsException if {@code path} exists and {@link StandardCopyOption#REPLACE_EXISTING} is not
     *         specified
     * @throws UnsupportedOperationException if {@code options} contains an unsupported option, or, when not replacing,
     *         if the file system does not support hard links, which are used to publish without replacing
     * @throws IOException if the file cannot be written, or the file system cannot atomically publish {@code path}
     */
    public static void write(
            @NotNull final Path path,
            @NotNull final TableDefinition definition,
            @NotNull final CopyOption... options) throws IOException {
        boolean replaceExisting = false;
        for (final CopyOption option : options) {
            if (option == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true;
            } else if (option != StandardCopyOption.ATOMIC_MOVE) {
                throw new UnsupportedOperationException(Objects.requireNonNull(option) + " not supported");
            }
        }
        final byte[] bytes = serialize(definition);
        final Path target = path.toAbsolutePath();
        final Path temp = target.resolveSibling("." + target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            Files.write(temp, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            if (replaceExisting) {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
            } else {
                // Unlike a move, creating a link fails atomically if target already exists
                Files.createLink(target, temp);
                Files.delete(temp);
            }
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Reads a table definition from {@code path}, as written by {@link #write(Path, TableDefinition, CopyOption...)}.
     * Class names are resolved against the current thread's context class loader, or, if there is none, the class
     * loader that loaded this class.
     *
     * @param path the file to read
     * @return the table definition
     * @throws IOException if the file cannot be read, or is not a valid encoding (an
     *         {@link InvalidProtocolBufferException})
     * @throws IllegalArgumentException if the file does not describe a valid table definition, has unknown fields, or
     *         names a class that cannot be found
     */
    public static TableDefinition read(@NotNull final Path path) throws IOException {
        return deserialize(Files.readAllBytes(path));
    }

    static byte[] serialize(@NotNull final TableDefinition definition) {
        return PersistedTableDefinitionProto.newBuilder()
                .setTableDefinition(toProto(definition))
                .build()
                .toByteArray();
    }

    static TableDefinition deserialize(final byte @NotNull [] bytes) throws InvalidProtocolBufferException {
        return fromProto(PersistedTableDefinitionProto.parseFrom(bytes), defaultClassLoader());
    }

    static TableDefinitionProto toProto(@NotNull final TableDefinition definition) {
        final TableDefinitionProto.Builder builder = TableDefinitionProto.newBuilder();
        for (final ColumnDefinition<?> column : definition.getColumns()) {
            builder.addColumns(toProto(column));
        }
        return builder.build();
    }

    static ColumnDefinitionProto toProto(@NotNull final ColumnDefinition<?> column) {
        final ColumnDefinitionProto.Builder builder = ColumnDefinitionProto.newBuilder()
                .setName(column.getName())
                .setDataType(toProto(column.getDataType()))
                .setColumnType(toProto(column.getColumnType()));
        final Class<?> componentType = column.getComponentType();
        if (componentType != null) {
            builder.setComponentType(toProto(componentType));
        }
        return builder.build();
    }

    /**
     * Converts {@code proto} to a {@link TableDefinition}, resolving class names against the
     * {@link #defaultClassLoader() default class loader}.
     *
     * @throws IllegalArgumentException if {@code proto} does not describe a valid table definition, has unknown fields,
     *         or names a class that cannot be found
     */
    static TableDefinition fromProto(@NotNull final TableDefinitionProto proto) {
        return fromProto(proto, defaultClassLoader());
    }

    /**
     * Converts {@code proto} to a {@link TableDefinition}, resolving class names against {@code classLoader}.
     *
     * @throws IllegalArgumentException if {@code proto} does not describe a valid table definition, has unknown fields,
     *         or names a class that cannot be found
     */
    static TableDefinition fromProto(
            @NotNull final TableDefinitionProto proto,
            @NotNull final ClassLoader classLoader) {
        Objects.requireNonNull(classLoader);
        checkNoUnknownFields("Table definition", proto);
        final List<ColumnDefinition<?>> columns = new ArrayList<>(proto.getColumnsCount());
        for (final ColumnDefinitionProto column : proto.getColumnsList()) {
            columns.add(fromProto(column, classLoader));
        }
        // Rejects duplicate column names
        return TableDefinition.of(columns);
    }

    static ColumnDefinition<?> fromProto(@NotNull final ColumnDefinitionProto proto) {
        return fromProto(proto, defaultClassLoader());
    }

    static ColumnDefinition<?> fromProto(
            @NotNull final ColumnDefinitionProto proto,
            @NotNull final ClassLoader classLoader) {
        Objects.requireNonNull(classLoader);
        final String name = proto.getName();
        checkNoUnknownFields("Column '" + name + "'", proto);
        if (!proto.hasDataType()) {
            throw new IllegalArgumentException("Column '" + name + "' is missing its data type");
        }
        final Class<?> dataType = fromProto(name, proto.getDataType(), classLoader);
        final Class<?> componentType = proto.hasComponentType()
                ? fromProto(name, proto.getComponentType(), classLoader)
                : null;
        final ColumnDefinition.ColumnType columnType = fromProto(name, proto.getColumnType());
        // Validates that componentType is consistent with dataType
        return ColumnDefinition.fromGenericType(name, dataType, componentType, columnType);
    }

    /**
     * The class loader used to resolve class names when none is given: the current thread's context class loader, or,
     * if there is none, the class loader that loaded this class.
     */
    private static ClassLoader defaultClassLoader() {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        return contextClassLoader != null ? contextClassLoader : TableDefinitionProtos.class.getClassLoader();
    }

    private static TableDefinition fromProto(
            @NotNull final PersistedTableDefinitionProto proto,
            @NotNull final ClassLoader classLoader) {
        checkNoUnknownFields("Persisted table definition", proto);
        return switch (proto.getDefinitionCase()) {
            case TABLE_DEFINITION -> fromProto(proto.getTableDefinition(), classLoader);
            case DEFINITION_NOT_SET ->
                throw new IllegalArgumentException("Persisted table definition has no definition set");
        };
    }

    private static ColumnTypeProto toProto(@NotNull final ColumnDefinition.ColumnType columnType) {
        return switch (columnType) {
            case Normal -> ColumnTypeProto.COLUMN_TYPE_NORMAL;
            case Partitioning -> ColumnTypeProto.COLUMN_TYPE_PARTITIONING;
        };
    }

    private static ColumnDefinition.ColumnType fromProto(
            @NotNull final String columnName,
            @NotNull final ColumnTypeProto columnType) {
        return switch (columnType) {
            case COLUMN_TYPE_NORMAL -> ColumnDefinition.ColumnType.Normal;
            case COLUMN_TYPE_PARTITIONING -> ColumnDefinition.ColumnType.Partitioning;
            case COLUMN_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                    "Column '" + columnName + "' has unsupported column type " + columnType);
        };
    }

    private static JavaClassProto toProto(@NotNull final Class<?> clazz) {
        final JavaClassProto.Builder builder = JavaClassProto.newBuilder();
        if (clazz.isPrimitive()) {
            builder.setPrimitive(toPrimitiveProto(clazz));
        } else {
            builder.setClassName(clazz.getName());
        }
        return builder.build();
    }

    private static PrimitiveTypeProto toPrimitiveProto(@NotNull final Class<?> clazz) {
        if (clazz == boolean.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_BOOLEAN;
        }
        if (clazz == byte.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_BYTE;
        }
        if (clazz == char.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_CHAR;
        }
        if (clazz == short.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_SHORT;
        }
        if (clazz == int.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_INT;
        }
        if (clazz == long.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_LONG;
        }
        if (clazz == float.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_FLOAT;
        }
        if (clazz == double.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_DOUBLE;
        }
        if (clazz == void.class) {
            return PrimitiveTypeProto.PRIMITIVE_TYPE_VOID;
        }
        throw new IllegalArgumentException("Unsupported primitive class " + clazz);
    }

    private static Class<?> fromProto(
            @NotNull final String columnName,
            @NotNull final JavaClassProto proto,
            @NotNull final ClassLoader classLoader) {
        checkNoUnknownFields("Column '" + columnName + "' class", proto);
        return switch (proto.getKindCase()) {
            case PRIMITIVE -> fromPrimitiveProto(columnName, proto.getPrimitive());
            case CLASS_NAME -> forName(columnName, proto.getClassName(), classLoader);
            case KIND_NOT_SET -> throw new IllegalArgumentException("Column '" + columnName + "' has an unset class");
        };
    }

    private static Class<?> forName(
            @NotNull final String columnName,
            @NotNull final String className,
            @NotNull final ClassLoader classLoader) {
        try {
            return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' references class '" + className + "', which was not found", e);
        }
    }

    private static Class<?> fromPrimitiveProto(
            @NotNull final String columnName,
            @NotNull final PrimitiveTypeProto proto) {
        return switch (proto) {
            case PRIMITIVE_TYPE_BOOLEAN -> boolean.class;
            case PRIMITIVE_TYPE_BYTE -> byte.class;
            case PRIMITIVE_TYPE_CHAR -> char.class;
            case PRIMITIVE_TYPE_SHORT -> short.class;
            case PRIMITIVE_TYPE_INT -> int.class;
            case PRIMITIVE_TYPE_LONG -> long.class;
            case PRIMITIVE_TYPE_FLOAT -> float.class;
            case PRIMITIVE_TYPE_DOUBLE -> double.class;
            case PRIMITIVE_TYPE_VOID -> void.class;
            case PRIMITIVE_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                    "Column '" + columnName + "' has unsupported primitive type " + proto);
        };
    }

    /**
     * Rejects {@code proto} if it carries fields this version does not know about, rather than silently dropping
     * information that a newer writer intended to convey.
     */
    private static void checkNoUnknownFields(@NotNull final String description, @NotNull final Message proto) {
        final Set<Integer> unknownFieldNumbers = proto.getUnknownFields().asMap().keySet();
        if (!unknownFieldNumbers.isEmpty()) {
            throw new IllegalArgumentException(
                    description + " has unknown fields with field numbers " + unknownFieldNumbers);
        }
    }

    private TableDefinitionProtos() {}
}
