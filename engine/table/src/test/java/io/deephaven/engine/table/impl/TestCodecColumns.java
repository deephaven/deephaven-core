//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.util.codec.*;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Unit tests for ObjectCodec ColumnSource and AppendableColumn implementations.
 */
public class TestCodecColumns {

    // TODO: Figure out how to come up with a BigInteger of a specified width.
    // private static final ColumnDefinition<BigInteger> FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
    // static {
    // final ColumnDefinition<BigInteger> definition = new ColumnDefinition<>("FWBI", BigInteger.class);
    // definition.setObjectCodecClass(BigIntegerCodec.class.getName());
    // definition.setObjectCodecArguments(null);
    // definition.setObjectWidth(11);
    // FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = definition;
    // }

    private static final ColumnDefinition<byte[]> VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    private static final ColumnDefinition<ArrayTuple> VARIABLE_WIDTH_COLUMN_DEFINITION_2;
    private static final ColumnDefinition<byte[]> FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S;
    private static final ParquetInstructions expectedReadInstructions, writeInstructions;
    static {
        final ParquetInstructions.Builder readBuilder = new ParquetInstructions.Builder();
        final ParquetInstructions.Builder writeBuilder = new ParquetInstructions.Builder();
        VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION =
                ColumnDefinition.fromGenericType("VWBA", byte[].class, byte.class);
        writeBuilder.addColumnCodec("VWBA", SimpleByteArrayCodec.class.getName());
        readBuilder.addColumnCodec("VWBA", SimpleByteArrayCodec.class.getName());

        VARIABLE_WIDTH_COLUMN_DEFINITION_2 = ColumnDefinition.fromGenericType("VWCD", ArrayTuple.class);
        readBuilder.addColumnCodec("VWCD", ExternalizableCodec.class.getName(), ArrayTuple.class.getName());
        FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("FWBA", byte[].class, byte.class);
        writeBuilder.addColumnCodec("FWBA", SimpleByteArrayCodec.class.getName(), "9");
        readBuilder.addColumnCodec("FWBA", SimpleByteArrayCodec.class.getName(), "9");

        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("VWBI", BigInteger.class);
        writeBuilder.addColumnCodec("VWBI", SerializableCodec.class.getName());
        readBuilder.addColumnCodec("VWBI", SerializableCodec.class.getName());

        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S = ColumnDefinition.fromGenericType("VWBIS", BigInteger.class);
        expectedReadInstructions = readBuilder.build();
        writeInstructions = writeBuilder.build();
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_COLUMN_DEFINITION_2,
            FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private Table table;

    @Before
    public void setUp() {
        table = TableTools.newTable(TABLE_DEFINITION,
                TableTools.col("VWBA", new byte[] {0, 1, 2}, null, new byte[] {3, 4, 5, 6}, EMPTY_BYTE_ARRAY),
                TableTools.col("VWCD", null, new ArrayTuple(0, 2, 4, 6), new ArrayTuple(1, 3, 5, 7), null),
                TableTools.col("FWBA", new byte[] {7, 8, 9, 10, 11, 12, 13, 14, 15},
                        new byte[] {16, 17, 18, 19, 20, 21, 22, 23, 24}, new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0},
                        EMPTY_BYTE_ARRAY),
                TableTools.col("VWBI", BigInteger.valueOf(91), BigInteger.valueOf(111111111111111L), null, null),
                TableTools.col("VWBIS", BigInteger.valueOf(94), null, BigInteger.valueOf(111111111111112L), null));
    }

    @Test
    public void doColumnsTest() throws IOException {
        final File dir = Files.createTempDirectory(Paths.get(""), "CODEC_TEST").toFile();
        final File dest = new File(dir, "Test.parquet");
        try {
            ParquetTools.writeTable(table, dest.getPath(), writeInstructions);
            doColumnsTestHelper(dest);
        } finally {
            FileUtils.deleteRecursively(dir);
        }
    }

    @Test
    public void doLegacyColumnsTest() {
        // Make sure that we can read legacy data encoded with the old codec implementations.
        final String path =
                TestCodecColumns.class.getResource("/ReferenceParquetWithCodecColumns.parquet").getFile();
        doColumnsTestHelper(new File(path));
    }

    private void doColumnsTestHelper(final File dest) {
        final MutableObject<ParquetInstructions> instructionsOut = new MutableObject<>();
        final Table result =
                ParquetTools.readParquetSchemaAndTable(dest, ParquetInstructions.EMPTY, instructionsOut);
        TableTools.show(result);
        TestCase.assertEquals(TABLE_DEFINITION, result.getDefinition());
        final ParquetInstructions readInstructions = instructionsOut.getValue();
        TestCase.assertTrue(
                ParquetInstructions.sameColumnNamesAndCodecMappings(expectedReadInstructions, readInstructions));
        TstUtils.assertTableEquals(table, result);
    }

    @Test
    public void doCacheTest() {
        try {
            CodecCache.DEFAULT.getCodec("java.lang.String", "param");
            TestCase.fail("Expected exception");
        } catch (CodecCacheException e) {
            TestCase.assertEquals(e.getCause().getClass(), ClassCastException.class);
        }
    }
}
