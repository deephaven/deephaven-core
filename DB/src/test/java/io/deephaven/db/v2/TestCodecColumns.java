package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.TableManagementTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.util.codec.BigIntegerCodec;
import io.deephaven.util.codec.ByteArrayCodec;
import io.deephaven.util.codec.CodecCache;
import io.deephaven.util.codec.CodecCacheException;
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
    private static final TableManagementTools.StorageFormat storageFormat = TableManagementTools.StorageFormat.Parquet;

    private static final ColumnDefinition<byte[]> VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    static {
        VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION = ColumnDefinition.ofVariableWidthCodec("VWBA", byte[].class, byte.class, ByteArrayCodec.class.getName());
    }

    private static final ColumnDefinition<byte[]> FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION;
    static {
        FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION = ColumnDefinition.ofFixedWidthCodec("FWBA", byte[].class, byte.class, ByteArrayCodec.class.getName(), "9,notnull", 9);
    }

    private static final ColumnDefinition<BigInteger> VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
    static {
        VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = ColumnDefinition.ofVariableWidthCodec("VWBI", BigInteger.class, BigIntegerCodec.class.getName());
    }

    // TODO: Figure out how to come up with a BigInteger of a specified width.
//    private static final ColumnDefinition<BigInteger> FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION;
//    static {
//        final ColumnDefinition<BigInteger> definition = new ColumnDefinition<>("FWBI", BigInteger.class);
//        definition.setObjectCodecClass(BigIntegerCodec.class.getName());
//        definition.setObjectCodecArguments(null);
//        definition.setObjectWidth(11);
//        FIXED_WIDTH_BIG_INTEGER_COLUMN_DEFINITION = definition;
//    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            VARIABLE_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            FIXED_WIDTH_BYTE_ARRAY_COLUMN_DEFINITION,
            VARIABLE_WIDTH_BIG_INTEGER_COLUMN_DEFINITION);

    private static final Table TABLE = TableTools.newTable(TABLE_DEFINITION,
            TableTools.col("VWBA", new byte[]{0,1,2}, null, new byte[]{3,4,5,6}),
            TableTools.col("FWBA", new byte[]{7,8,9,10,11,12,13,14,15}, new byte[]{16,17,18,19,20,21,22,23,24}, new byte[]{0,0,0,0,0,0,0,0,0}),
            TableTools.col("VWBI", BigInteger.valueOf(91), BigInteger.valueOf(111111111111111L), null)
    );

    @Test
    public void doColumnsTest() throws IOException {
        final File dir = Files.createTempDirectory(Paths.get(""), "CODEC_TEST").toFile();
        try {
            TableManagementTools.writeTable(TABLE, dir, storageFormat);
            final Table result = TableManagementTools.readTable(dir);
            TableTools.show(result);
            TestCase.assertEquals(TABLE_DEFINITION, result.getDefinition());
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
