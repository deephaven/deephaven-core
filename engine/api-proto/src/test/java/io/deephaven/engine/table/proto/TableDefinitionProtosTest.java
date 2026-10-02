//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.proto.gen.ColumnDefinitionProto;
import io.deephaven.engine.table.proto.gen.ColumnTypeProto;
import io.deephaven.engine.table.proto.gen.JavaClassProto;
import io.deephaven.engine.table.proto.gen.PersistedTableDefinitionProto;
import io.deephaven.engine.table.proto.gen.PrimitiveTypeProto;
import io.deephaven.engine.table.proto.gen.TableDefinitionProto;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.ObjectVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TableDefinitionProtosTest {

    private static final TableDefinition EVERYTHING = TableDefinition.of(
            ColumnDefinition.ofString("Partition").withPartitioning(),
            ColumnDefinition.ofBoolean("Boolean"),
            ColumnDefinition.ofByte("Byte"),
            ColumnDefinition.ofChar("Char"),
            ColumnDefinition.ofShort("Short"),
            ColumnDefinition.ofInt("Int"),
            ColumnDefinition.ofLong("Long"),
            ColumnDefinition.ofFloat("Float"),
            ColumnDefinition.ofDouble("Double"),
            ColumnDefinition.fromGenericType("BoxedInt", Integer.class),
            ColumnDefinition.fromGenericType("PrimitiveBoolean", boolean.class),
            ColumnDefinition.fromGenericType("PrimitiveVoid", void.class),
            ColumnDefinition.fromGenericType("BoxedVoid", Void.class),
            ColumnDefinition.ofString("String"),
            ColumnDefinition.ofTime("Instant"),
            ColumnDefinition.ofLocalDate("LocalDate"),
            ColumnDefinition.ofLocalTime("LocalTime"),
            ColumnDefinition.ofDuration("Duration"),
            ColumnDefinition.fromGenericType("Custom", BigDecimal.class),
            ColumnDefinition.fromGenericType("IntArray", int[].class),
            ColumnDefinition.fromGenericType("StringArray", String[].class),
            ColumnDefinition.fromGenericType("NestedArray", int[][].class),
            ColumnDefinition.fromGenericType("ObjectArrayOfInstant", Object[].class, Instant.class),
            ColumnDefinition.ofVector("IntVector", IntVector.class),
            ColumnDefinition.ofVector("DoubleVector", DoubleVector.class),
            ColumnDefinition.ofVector("ObjectVector", ObjectVector.class),
            ColumnDefinition.ofVector("StringVector", ObjectVector.class, String.class),
            ColumnDefinition.ofLong("PartitionLong").withPartitioning());

    @Test
    void writeThenRead(@TempDir final Path dir) throws IOException {
        final Path path = dir.resolve("definition.bin");
        TableDefinitionProtos.write(path, TableDefinition.of(ColumnDefinition.ofInt("Old")));
        // Replaces the existing file, leaving nothing else behind
        TableDefinitionProtos.write(path, EVERYTHING);
        assertThat(TableDefinitionProtos.read(path)).isEqualTo(EVERYTHING);
        try (final var files = Files.list(dir)) {
            assertThat(files).containsExactly(path);
        }
    }

    @Test
    void readInvalidFile(@TempDir final Path dir) throws IOException {
        final Path path = dir.resolve("definition.bin");
        Files.write(path, new byte[] {(byte) 0xFF});
        assertThatThrownBy(() -> TableDefinitionProtos.read(path))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void roundTripEverything() {
        assertRoundTrip(EVERYTHING);
    }

    @Test
    void roundTripEmpty() {
        assertRoundTrip(TableDefinition.of());
        // The wrapper's table_definition field is present, though empty
        assertThat(TableDefinitionProtos.serialize(TableDefinition.of())).containsExactly(0x0A, 0x00);
    }

    @Test
    void persistedShape() throws InvalidProtocolBufferException {
        final TableDefinition definition = TableDefinition.of(ColumnDefinition.ofInt("X"));
        assertThat(PersistedTableDefinitionProto.parseFrom(TableDefinitionProtos.serialize(definition)))
                .isEqualTo(PersistedTableDefinitionProto.newBuilder()
                        .setTableDefinition(TableDefinitionProtos.toProto(definition))
                        .build());
    }

    @Test
    void noDefinition() {
        // A zero-byte file, for example one that was truncated, is not an empty table definition
        assertThatThrownBy(() -> TableDefinitionProtos.deserialize(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Persisted table definition has no definition set");
    }

    @Test
    void unknownPersistedField() {
        final byte[] bytes = PersistedTableDefinitionProto.newBuilder()
                .setTableDefinition(table(column("X")))
                .setUnknownFields(unknownField())
                .build()
                .toByteArray();
        assertThatThrownBy(() -> TableDefinitionProtos.deserialize(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Persisted table definition has unknown fields with field numbers [" + UNKNOWN + "]");
    }

    @Test
    void roundTripPreservesOrder() {
        final TableDefinition forward = TableDefinition.of(ColumnDefinition.ofInt("A"), ColumnDefinition.ofInt("B"));
        final TableDefinition reverse = TableDefinition.of(ColumnDefinition.ofInt("B"), ColumnDefinition.ofInt("A"));
        assertRoundTrip(forward);
        assertRoundTrip(reverse);
        assertThat(TableDefinitionProtos.serialize(forward)).isNotEqualTo(TableDefinitionProtos.serialize(reverse));
    }

    @Test
    void roundTripWithClassLoader() {
        final TableDefinitionProto proto = TableDefinitionProtos.toProto(EVERYTHING);
        assertThat(TableDefinitionProtos.fromProto(proto, getClass().getClassLoader())).isEqualTo(EVERYTHING);
    }

    @Test
    void roundTripWithoutContextClassLoader() {
        final Thread thread = Thread.currentThread();
        final ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(null);
        try {
            assertRoundTrip(EVERYTHING);
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    @Test
    void protoShape() {
        final TableDefinitionProto proto = TableDefinitionProtos.toProto(TableDefinition.of(
                ColumnDefinition.ofInt("I").withPartitioning(),
                ColumnDefinition.ofVector("V", ObjectVector.class, String.class)));
        assertThat(proto).isEqualTo(TableDefinitionProto.newBuilder()
                .addColumns(ColumnDefinitionProto.newBuilder()
                        .setName("I")
                        .setDataType(primitive(PrimitiveTypeProto.PRIMITIVE_TYPE_INT))
                        .setColumnType(ColumnTypeProto.COLUMN_TYPE_PARTITIONING))
                .addColumns(ColumnDefinitionProto.newBuilder()
                        .setName("V")
                        .setDataType(className("io.deephaven.vector.ObjectVector"))
                        .setComponentType(className("java.lang.String"))
                        .setColumnType(ColumnTypeProto.COLUMN_TYPE_NORMAL))
                .build());
    }

    @Test
    void columnRoundTrip() {
        for (final ColumnDefinition<?> column : EVERYTHING.getColumns()) {
            assertThat(TableDefinitionProtos.fromProto(TableDefinitionProtos.toProto(column))).isEqualTo(column);
        }
    }

    @Test
    void invalidBytes() {
        assertThatThrownBy(() -> TableDefinitionProtos.deserialize(new byte[] {(byte) 0xFF}))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void missingDataType() {
        assertInvalid(column("X").clearDataType(), "missing its data type");
    }

    @Test
    void unsetClass() {
        assertInvalid(column("X").setDataType(JavaClassProto.getDefaultInstance()), "unset class");
    }

    @Test
    void unspecifiedPrimitive() {
        assertInvalid(column("X").setDataType(primitive(PrimitiveTypeProto.PRIMITIVE_TYPE_UNSPECIFIED)),
                "unsupported primitive type");
    }

    @Test
    void unrecognizedPrimitive() {
        assertInvalid(column("X").setDataType(JavaClassProto.newBuilder().setPrimitiveValue(1000)),
                "unsupported primitive type");
    }

    @Test
    void unspecifiedColumnType() {
        assertInvalid(column("X").setColumnType(ColumnTypeProto.COLUMN_TYPE_UNSPECIFIED),
                "unsupported column type");
    }

    @Test
    void unrecognizedColumnType() {
        assertInvalid(column("X").setColumnTypeValue(1000), "unsupported column type");
    }

    @Test
    void unknownClass() {
        assertInvalid(column("X").setDataType(className("com.example.DoesNotExist")), "was not found");
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(
                table(column("X").setDataType(className("com.example.DoesNotExist"))),
                getClass().getClassLoader()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("was not found");
    }

    @Test
    void inconsistentComponentType() {
        assertInvalid(column("X")
                .setDataType(className(int[].class.getName()))
                .setComponentType(className("java.lang.String")),
                "Invalid component type");
    }

    @Test
    void duplicateColumnNames() {
        final TableDefinitionProto proto = TableDefinitionProto.newBuilder()
                .addColumns(column("X"))
                .addColumns(column("X"))
                .build();
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(proto))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownTableField() {
        final TableDefinitionProto proto = table(column("X")).toBuilder().setUnknownFields(unknownField()).build();
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(proto))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table definition has unknown fields with field numbers [" + UNKNOWN + "]");
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(proto, getClass().getClassLoader()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown fields");
    }

    @Test
    void unknownColumnField() {
        final ColumnDefinitionProto.Builder column = column("X").setUnknownFields(unknownField());
        assertInvalid(column, "Column 'X' has unknown fields with field numbers [" + UNKNOWN + "]");
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(column.build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown fields");
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(column.build(), getClass().getClassLoader()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown fields");
    }

    @Test
    void unknownDataTypeField() {
        assertInvalid(column("X").setDataType(
                primitive(PrimitiveTypeProto.PRIMITIVE_TYPE_INT).toBuilder().setUnknownFields(unknownField())),
                "Column 'X' class has unknown fields");
    }

    @Test
    void unknownComponentTypeField() {
        assertInvalid(column("X")
                .setDataType(className(int[].class.getName()))
                .setComponentType(primitive(PrimitiveTypeProto.PRIMITIVE_TYPE_INT).toBuilder()
                        .setUnknownFields(unknownField())),
                "Column 'X' class has unknown fields");
    }

    @Test
    void unknownFieldInBytes() {
        final byte[] valid = TableDefinitionProtos.serialize(TableDefinition.of(ColumnDefinition.ofInt("X")));
        // Field UNKNOWN, wire type varint, value 1: what a newer writer's extra field looks like on the wire
        final byte[] tag = {(byte) (((UNKNOWN << 3) & 0x7F) | 0x80), (byte) (UNKNOWN >>> 4), 1};
        final byte[] withUnknown = Arrays.copyOf(valid, valid.length + tag.length);
        System.arraycopy(tag, 0, withUnknown, valid.length, tag.length);
        assertThatThrownBy(() -> TableDefinitionProtos.deserialize(withUnknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Persisted table definition has unknown fields with field numbers [" + UNKNOWN + "]");
    }

    /**
     * A field number not defined by any message; fits in a two-byte varint tag.
     */
    private static final int UNKNOWN = 100;

    private static UnknownFieldSet unknownField() {
        return UnknownFieldSet.newBuilder()
                .addField(UNKNOWN, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                .build();
    }

    private static void assertRoundTrip(final TableDefinition definition) {
        assertThat(TableDefinitionProtos.fromProto(TableDefinitionProtos.toProto(definition))).isEqualTo(definition);
        try {
            assertThat(TableDefinitionProtos.deserialize(TableDefinitionProtos.serialize(definition)))
                    .isEqualTo(definition);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertInvalid(final ColumnDefinitionProto.Builder column, final String message) {
        assertThatThrownBy(() -> TableDefinitionProtos.fromProto(table(column)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static TableDefinitionProto table(final ColumnDefinitionProto.Builder... columns) {
        final TableDefinitionProto.Builder builder = TableDefinitionProto.newBuilder();
        for (final ColumnDefinitionProto.Builder column : List.of(columns)) {
            builder.addColumns(column);
        }
        return builder.build();
    }

    /**
     * A valid {@code int} column, for tests to break.
     */
    private static ColumnDefinitionProto.Builder column(final String name) {
        return ColumnDefinitionProto.newBuilder()
                .setName(name)
                .setDataType(primitive(PrimitiveTypeProto.PRIMITIVE_TYPE_INT))
                .setColumnType(ColumnTypeProto.COLUMN_TYPE_NORMAL);
    }

    private static JavaClassProto primitive(final PrimitiveTypeProto primitive) {
        return JavaClassProto.newBuilder().setPrimitive(primitive).build();
    }

    private static JavaClassProto className(final String className) {
        return JavaClassProto.newBuilder().setClassName(className).build();
    }
}
