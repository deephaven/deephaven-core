package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.util.codec.*;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Unit tests for ObjectCodec ColumnSource and AppendableColumn implementations.
 */
public class TestMapCodecColumns {
    private static final ColumnDefinition<Map<String, String>> STRING_MAP_COLUMN_DEFINITION;
    private static final ColumnDefinition<Map<String, Boolean>> BOOLEAN_MAP_COLUMN_DEFINITION;
    private static final ColumnDefinition<Map<String, Boolean>> INT_MAP_COLUMN_DEFINITION;
    private static final ColumnDefinition<Map<String, Boolean>> LONG_MAP_COLUMN_DEFINITION;
    private static final ColumnDefinition<Map<String, Boolean>> FLOAT_MAP_COLUMN_DEFINITION;
    private static final ColumnDefinition<Map<String, Boolean>> DOUBLE_MAP_COLUMN_DEFINITION;
    private static final ParquetInstructions writeInstructions;
    static {
        final ParquetInstructions.Builder builder = new ParquetInstructions.Builder();
        // noinspection unchecked
        STRING_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrStrMap", (Class)Map.class);
        builder.addColumnCodec("StrStrMap", StringStringMapCodec.class.getName());
        // noinspection unchecked
        BOOLEAN_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrBoolMap", (Class)Map.class);
        builder.addColumnCodec("StrBoolMap", StringBooleanMapCodec.class.getName());
        // noinspection unchecked
        INT_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrIntMap", (Class)Map.class);
        builder.addColumnCodec("StrIntMap", StringIntMapCodec.class.getName());
        // noinspection unchecked
        LONG_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrLongMap", (Class)Map.class);
        builder.addColumnCodec("StrLongMap", StringLongMapCodec.class.getName());
        // noinspection unchecked
        FLOAT_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrFloatMap", (Class)Map.class);
        builder.addColumnCodec("StrFloatMap", StringFloatMapCodec.class.getName());
        // noinspection unchecked
        DOUBLE_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrDoubleMap", (Class)Map.class);
        builder.addColumnCodec("StrDoubleMap", StringDoubleMapCodec.class.getName());
        writeInstructions = builder.build();
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            STRING_MAP_COLUMN_DEFINITION,
            BOOLEAN_MAP_COLUMN_DEFINITION,
            DOUBLE_MAP_COLUMN_DEFINITION,
            FLOAT_MAP_COLUMN_DEFINITION,
            INT_MAP_COLUMN_DEFINITION,
            LONG_MAP_COLUMN_DEFINITION);

    @SuppressWarnings("unchecked")
    private static final Table TABLE = TableTools.newTable(TABLE_DEFINITION,
            TableTools.col("StrStrMap", CollectionUtil.mapFromArray(String.class, String.class, "AK", "AV", "BK", "BV"), null, Collections.singletonMap("Key", "Value")),
            TableTools.col("StrBoolMap", CollectionUtil.mapFromArray(String.class, Boolean.class, "True", true, "False", false, "Null", null), null, Collections.singletonMap("Truthiness", true)),
            TableTools.col("StrDoubleMap", CollectionUtil.mapFromArray(String.class, Double.class, "One", 1.0, "Two", 2.0, "Null", null), null, Collections.singletonMap("Pi", Math.PI)),
            TableTools.col("StrFloatMap", CollectionUtil.mapFromArray(String.class, Float.class, "Ten", 10.0f, "Twenty", 20.0f, "Null", null), null, Collections.singletonMap("e", (float)Math.E)),
            TableTools.col("StrIntMap", CollectionUtil.mapFromArray(String.class, Integer.class, "Million", 1_000_000, "Billion", 1_000_000_000, "Null", null), null, Collections.singletonMap("Negative", -1)),
            TableTools.col("StrLongMap", CollectionUtil.mapFromArray(String.class, Long.class, "Trillion", 1_000_000_000_000L, "Billion", 1_000_000_000L, "Null", null), null, Collections.singletonMap("Negative", -1L))
    );

    @Test
    public void doColumnsTest() throws IOException {
        final File dir = Files.createTempDirectory(Paths.get(""), "CODEC_TEST").toFile();
        final File dest = new File(dir, "Table.parquet");
        try {
            ParquetTools.writeTable(TABLE, dest, TABLE.getDefinition(), writeInstructions);
            final Table result = ParquetTools.readTable(dest);
            TableTools.show(result);
            TestCase.assertEquals(TABLE_DEFINITION, result.getDefinition());
            TstUtils.assertTableEquals(TABLE, result);
        } finally {
            FileUtils.deleteRecursively(dir);
        }
    }
}
