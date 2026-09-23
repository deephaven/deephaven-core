//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.ColumnDefinition;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests {@link ParquetSchemaReader#convertSchema} directly, against hand-built schemas. Nothing here touches a file or
 * the engine, so these cover inference in isolation from the read path.
 */
public class TestParquetSchemaReader {

    private static final String PARQUET_COL = "uint64_col";

    private static MessageType uint64Schema(final String parquetColumnName, final boolean isRepeated) {
        return Types.buildMessage()
                .addFields(isRepeated
                        ? Types.repeated(INT64).as(intType(64, false)).named(parquetColumnName)
                        : optional(INT64).as(intType(64, false)).named(parquetColumnName))
                .named("schema");
    }

    private static ColumnDefinition<?> convertSoleColumn(
            final MessageType schema, final ParquetInstructions instructions) {
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> converted =
                ParquetSchemaReader.convertSchema(schema, Map.of(), instructions);
        assertEquals(1, converted.getFirst().size());
        return converted.getFirst().get(0);
    }

    private static ParquetInstructions withTarget(final ParquetInstructions.UnsignedLongTarget target) {
        return ParquetInstructions.builder().setUnsignedLongTarget(PARQUET_COL, target).build();
    }

    @Test
    public void unsignedLongDefaultsToBigInteger() {
        final ColumnDefinition<?> column =
                convertSoleColumn(uint64Schema(PARQUET_COL, false), ParquetInstructions.EMPTY);
        assertEquals(PARQUET_COL, column.getName());
        assertEquals(BigInteger.class, column.getDataType());
        assertNull(column.getComponentType());
    }

    @Test
    public void unsignedLongTargetBigInteger() {
        assertEquals(BigInteger.class,
                convertSoleColumn(uint64Schema(PARQUET_COL, false),
                        withTarget(ParquetInstructions.UnsignedLongTarget.BIG_INTEGER)).getDataType());
    }

    @Test
    public void unsignedLongTargetLong() {
        assertEquals(long.class,
                convertSoleColumn(uint64Schema(PARQUET_COL, false),
                        withTarget(ParquetInstructions.UnsignedLongTarget.LONG)).getDataType());
    }

    @Test
    public void unsignedLongTargetSignedLong() {
        assertEquals(long.class,
                convertSoleColumn(uint64Schema(PARQUET_COL, false),
                        withTarget(ParquetInstructions.UnsignedLongTarget.SIGNED_LONG)).getDataType());
    }

    /**
     * Per-column instructions are keyed by Deephaven column name, not parquet column name, so a target set on the
     * mapped name must still be found.
     */
    @Test
    public void unsignedLongTargetIsKeyedOnDeephavenColumnName() {
        final String deephavenName = "MyCol";
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping(PARQUET_COL, deephavenName)
                .setUnsignedLongTarget(deephavenName, ParquetInstructions.UnsignedLongTarget.LONG)
                .build();
        final ColumnDefinition<?> column = convertSoleColumn(uint64Schema(PARQUET_COL, false), instructions);
        assertEquals(deephavenName, column.getName());
        assertEquals(long.class, column.getDataType());
    }

    /**
     * A target keyed on the parquet name is not consulted, so the column falls back to the default.
     */
    @Test
    public void unsignedLongTargetKeyedOnParquetColumnNameIsNotApplied() {
        final String deephavenName = "MyCol";
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .addColumnNameMapping(PARQUET_COL, deephavenName)
                .setUnsignedLongTarget(PARQUET_COL, ParquetInstructions.UnsignedLongTarget.LONG)
                .build();
        assertEquals(BigInteger.class,
                convertSoleColumn(uint64Schema(PARQUET_COL, false), instructions).getDataType());
    }

    @Test
    public void repeatedUnsignedLongDefaultsToBigIntegerArray() {
        final ColumnDefinition<?> column =
                convertSoleColumn(uint64Schema(PARQUET_COL, true), ParquetInstructions.EMPTY);
        assertEquals(BigInteger[].class, column.getDataType());
        assertEquals(BigInteger.class, column.getComponentType());
    }

    /**
     * The target describes the elements; the outer array type follows from it.
     */
    @Test
    public void repeatedUnsignedLongTargetAppliesToElements() {
        final ColumnDefinition<?> column = convertSoleColumn(uint64Schema(PARQUET_COL, true),
                withTarget(ParquetInstructions.UnsignedLongTarget.LONG));
        assertEquals(long[].class, column.getDataType());
        assertEquals(long.class, column.getComponentType());
    }

    /**
     * The target is scoped to unsigned 64-bit columns, so it does not redirect a signed one.
     */
    @Test
    public void unsignedLongTargetDoesNotAffectSignedColumn() {
        final MessageType schema = Types.buildMessage()
                .addFields(optional(INT64).as(intType(64, true)).named(PARQUET_COL))
                .named("schema");
        assertEquals(long.class,
                convertSoleColumn(schema, withTarget(ParquetInstructions.UnsignedLongTarget.BIG_INTEGER))
                        .getDataType());
    }
}
