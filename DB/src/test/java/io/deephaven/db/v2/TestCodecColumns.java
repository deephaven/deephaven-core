package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.util.codec.*;
import junit.framework.TestCase;
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
//    private static final ColumnDefinition<BigInteger> FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
//    static {
//        final ColumnDefinition<BigInteger> definition = new ColumnDefinition<>("FWBI", BigInteger.class);
//        definition.setObjectCodecClass(BigIntegerCodec.class.getName());
//        definition.setObjectCodecArguments(null);
//        definition.setObjectWidth(11);
//        FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = definition;
//    }

    private static final ColumnDefinition<byte[]> VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    static {
        VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION = ColumnDefinition.ofVariableWidthCodec("VWBA", byte[].class, byte.class, SimpleByteArrayCodec.class.getName());
    }

    private static final ColumnDefinition<ColumnDefinition> VARIABLE_WIDTH_COLUMN_DEFINITION_2;
    static {
        VARIABLE_WIDTH_COLUMN_DEFINITION_2 = ColumnDefinition.fromGenericType("VWCD", ColumnDefinition.class);
    }

    private static final ColumnDefinition<ColumnDefinition> VARIABLE_WIDTH_COLUMN_DEFINITION_2_EXPLICIT_CODEC;
    static {
        VARIABLE_WIDTH_COLUMN_DEFINITION_2_EXPLICIT_CODEC = ColumnDefinition.ofVariableWidthCodec("VWCD", ColumnDefinition.class, null, ExternalizableCodec.class.getName(), ColumnDefinition.class.getName());
    }

    private static final ColumnDefinition<byte[]> FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    static {
        FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION = ColumnDefinition.ofFixedWidthCodec("FWBA", byte[].class, byte.class, SimpleByteArrayCodec.class.getName(), "9", 9);
    }

    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
    static {
        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = ColumnDefinition.ofVariableWidthCodec("VWBI", BigInteger.class, BigIntegerCodec.class.getName());
    }

    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S;
    static {
        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S = ColumnDefinition.fromGenericType("VWBIS", BigInteger.class);
    }

    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S_EXPLICIT_CODEC;
    static {
        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S_EXPLICIT_CODEC = ColumnDefinition.ofVariableWidthCodec("VWBIS", BigInteger.class, SerializableCodec.class.getName());
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_COLUMN_DEFINITION_2,
            FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S);

    private static final TableDefinition EXPECTED_RESULT_DEFINITION = TableDefinition.of(
            VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_COLUMN_DEFINITION_2_EXPLICIT_CODEC,
            FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION_S_EXPLICIT_CODEC);

    private static final Table TABLE = TableTools.newTable(TABLE_DEFINITION,
            TableTools.col("VWBA", new byte[]{0,1,2}, null, new byte[]{3,4,5,6}),
            TableTools.col("VWCD", null, VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION, VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION),
            TableTools.col("FWBA", new byte[]{7,8,9,10,11,12,13,14,15}, new byte[]{16,17,18,19,20,21,22,23,24}, new byte[]{0,0,0,0,0,0,0,0,0}),
            TableTools.col("VWBI", BigInteger.valueOf(91), BigInteger.valueOf(111111111111111L), null),
            TableTools.col("VWBIS", BigInteger.valueOf(94), null, BigInteger.valueOf(111111111111112L))
    );

    @Test
    public void doColumnsTest() throws IOException {
        final File dir = Files.createTempDirectory(Paths.get(""), "CODEC_TEST").toFile();
        final File dest = new File(dir, "Test.parquet");
        try {
            ParquetTools.writeTable(TABLE, dest);
            final Table result = ParquetTools.readTable(dest);
            TableTools.show(result);
            TestCase.assertEquals(EXPECTED_RESULT_DEFINITION, result.getDefinition());
            TstUtils.assertTableEquals(TABLE, result);
        } finally {
            FileUtils.deleteRecursively(dir);
        }
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
