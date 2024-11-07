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
import io.deephaven.util.codec.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
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
        STRING_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrStrMap", (Class) Map.class);
        builder.addColumnCodec("StrStrMap", StringStringMapCodec.class.getName());
        // noinspection unchecked
        BOOLEAN_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrBoolMap", (Class) Map.class);
        builder.addColumnCodec("StrBoolMap", StringBooleanMapCodec.class.getName());
        // noinspection unchecked
        INT_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrIntMap", (Class) Map.class);
        builder.addColumnCodec("StrIntMap", StringIntMapCodec.class.getName());
        // noinspection unchecked
        LONG_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrLongMap", (Class) Map.class);
        builder.addColumnCodec("StrLongMap", StringLongMapCodec.class.getName());
        // noinspection unchecked
        FLOAT_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrFloatMap", (Class) Map.class);
        builder.addColumnCodec("StrFloatMap", StringFloatMapCodec.class.getName());
        // noinspection unchecked
        DOUBLE_MAP_COLUMN_DEFINITION = ColumnDefinition.fromGenericType("StrDoubleMap", (Class) Map.class);
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

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private Table table;

    @Before
    public void setUp() {
        table = TableTools.newTable(TABLE_DEFINITION,
                TableTools.col("StrStrMap",
                        mapFromArray("AK", "AV", "BK", "BV"),
                        null, Collections.singletonMap("Key", "Value")),
                TableTools.col("StrBoolMap",
                        mapFromArray("True", true, "False", false, "Null",
                                null),
                        null, Collections.singletonMap("Truthiness", true)),
                TableTools.col("StrDoubleMap",
                        mapFromArray("One", 1.0, "Two", 2.0, "Null", null),
                        null,
                        Collections.singletonMap("Pi", Math.PI)),
                TableTools.col("StrFloatMap",
                        mapFromArray("Ten", 10.0f, "Twenty", 20.0f, "Null",
                                null),
                        null, Collections.singletonMap("e", (float) Math.E)),
                TableTools.col("StrIntMap",
                        mapFromArray("Million", 1_000_000, "Billion",
                                1_000_000_000, "Null", null),
                        null, Collections.singletonMap("Negative", -1)),
                TableTools.col("StrLongMap",
                        mapFromArray("Trillion", 1_000_000_000_000L,
                                "Billion", 1_000_000_000L, "Null", null),
                        null, Collections.singletonMap("Negative", -1L)));
    }

    @Test
    public void doColumnsTest() throws IOException {
        final File dir = Files.createTempDirectory(Paths.get(""), "CODEC_TEST").toFile();
        final String dest = new File(dir, "Table.parquet").getPath();
        try {
            ParquetTools.writeTable(table, dest, writeInstructions);
            doColumnsTestHelper(dest);
        } finally {
            FileUtils.deleteRecursively(dir);
        }
    }

    @Test
    public void doLegacyColumnsTest() {
        // Make sure that we can read legacy data encoded with the old codec implementations.
        final String dest =
                TestMapCodecColumns.class.getResource("/ReferenceParquetWithMapCodecData.parquet").getFile();
        doColumnsTestHelper(dest);
    }

    private void doColumnsTestHelper(final String dest) {
        final Table result = ParquetTools.readTable(dest);
        TableTools.show(result);
        Assert.assertEquals(TABLE_DEFINITION, result.getDefinition());
        TstUtils.assertTableEquals(table, result);
    }

    @SuppressWarnings({"unchecked"})
    public static <K, V> Map<K, V> mapFromArray(Object... data) {
        Map<K, V> map = new LinkedHashMap<K, V>();
        for (int nIndex = 0; nIndex < data.length; nIndex += 2) {
            map.put((K) data[nIndex], (V) data[nIndex + 1]);
        }
        return map;
    }
}
