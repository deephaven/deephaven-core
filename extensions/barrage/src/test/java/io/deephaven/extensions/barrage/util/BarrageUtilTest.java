//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnEncoding;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.*;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")

public class BarrageUtilTest extends RefreshingTableTestCase {

    /**
     * Builds the encoding sample row set over {@code table} (current values) and runs
     * {@link BarrageUtil#sampleColumnsForEncoding} against it, mirroring how {@code inferEncodings} drives sampling. A
     * table too small to sample yields a {@code null} sample and no detection, exactly as in production.
     */
    private static void sampleColumnsForEncoding(final Table table, final Map<String, ColumnEncoding> detected,
            final boolean detectRee, final boolean detectDict) {
        try (final WritableRowSet sample = BarrageUtil.buildEncodingSampleRowSet(table.getRowSet())) {
            if (sample != null) {
                BarrageUtil.sampleColumnsForEncoding(table, detected, false, sample, detectRee, detectDict);
            }
        }
    }

    public void testMergedTableKeyColumnsGetREE() {
        // Tests detectStructuralRunEndEncoding directly: merged key columns always get REE
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL", "MSFT"),
                intCol("Exchange", 1, 1, 2),
                doubleCol("Price", 100.0, 101.0, 200.0),
                longCol("Size", 100L, 200L, 150L))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Map<String, ColumnEncoding> encodings = new java.util.HashMap<>();
        BarrageUtil.detectStructuralRunEndEncoding(table, encodings);

        assertThat(encodings.get("Symbol")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(encodings.get("Exchange")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(encodings).doesNotContainKey("Price");
        assertThat(encodings).doesNotContainKey("Size");
    }

    public void testNonMergedTableNoAutoREE() {
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "MSFT"),
                intCol("Exchange", 1, 2))
                .withAttributes(Map.of(Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertFieldIsNotREE(schema, "Symbol");
        assertFieldIsNotREE(schema, "Exchange");
    }

    public void testMergedTableWithoutKeyColumnsAttributeNoAutoREE() {
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "MSFT"),
                intCol("Exchange", 1, 2))
                .withAttributes(Map.of(Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertFieldIsNotREE(schema, "Symbol");
        assertFieldIsNotREE(schema, "Exchange");
    }

    public void testSymbolTableColumnGetsStructuralDictionary() throws Exception {
        // detectStructuralDictionaryEncoding keys off whether a source is symbol-table backed, NOT off whether it is a
        // String. To make the negative meaningful both columns are Strings; they differ only in cardinality:
        // Sym -- 5 distinct values, stays under the dictionary-key cap -> Parquet dictionary-encodes it -> read back
        // symbol-table backed -> must be marked DICTIONARY_ENCODED_INT32.
        // Id -- 100k distinct values (Long.toString(k)), busts the cap below -> Parquet falls back to plain
        // encoding -> read back NOT symbol-table backed -> must be left alone.
        // A low maximumDictionaryKeys forces that fallback for Id while leaving low-cardinality Sym dictionary-encoded.
        final File dataDir = Files.createTempDirectory(BarrageUtilTest.class.getName()).toFile();
        try {
            final Table source = emptyTable(100_000).update("Sym = `S` + (k % 5)", "Id = Long.toString(k)");
            final File parquetFile = new File(dataDir, "table.parquet");
            final ParquetInstructions instructions = ParquetInstructions.builder()
                    .setMaximumDictionaryKeys(100)
                    .build();
            ParquetTools.writeTable(source, parquetFile.getPath(), instructions);
            final Table readBack = ParquetTools.readTable(parquetFile.getPath());

            // Sanity: the low-cardinality String column really is symbol-table backed (the branch
            // detectStructuralDictionaryEncoding keys off), while the high-cardinality String column is not.
            assertThat(SymbolTableSource.hasSymbolTable(readBack.getColumnSource("Sym"), readBack.getRowSet()))
                    .isTrue();
            assertThat(SymbolTableSource.hasSymbolTable(readBack.getColumnSource("Id"), readBack.getRowSet()))
                    .isFalse();

            final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
            BarrageUtil.detectStructuralDictionaryEncoding(readBack, readBack.getRowSet(), detected);

            assertThat(detected.get("Sym")).isEqualTo(ColumnEncoding.DICTIONARY_ENCODED_INT32);
            assertThat(detected).doesNotContainKey("Id");
        } finally {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDir);
        }
    }

    public void testClusteredSymbolColumnGetsBothDictionaryAndRee() throws Exception {
        // A clustered low-cardinality String column read back from Parquet is the canonical doubly-encoded candidate:
        // it is symbol-table backed (structural dictionary fires) AND laid out in long adjacent runs (REE sampling
        // fires). Sym2 = `U` + (((int)(k / 100)) % 3) over 10k rows is exactly that: 3 distinct values, each in runs
        // of 100 consecutive rows. inferEncodings must mark it as combined RunEndEncoded<Dictionary<...>>.
        final File dataDir = Files.createTempDirectory(BarrageUtilTest.class.getName()).toFile();
        try {
            final Table source = emptyTable(10_000).update("Sym2 = `U` + (((int) (k / 100)) % 3)");
            final File parquetFile = new File(dataDir, "table.parquet");
            ParquetTools.writeTable(source, parquetFile.getPath());
            final Table readBack = ParquetTools.readTable(parquetFile.getPath());

            // Precondition: the column really is symbol-table backed (the branch structural detection keys off).
            assertThat(SymbolTableSource.hasSymbolTable(readBack.getColumnSource("Sym2"), readBack.getRowSet()))
                    .isTrue();

            final ColumnEncoding reeAndDict =
                    ColumnEncoding.of(ColumnEncoding.RunEndWidth.INT32, ColumnEncoding.DictWidth.INT32);

            // Full inferEncodings order: structural dictionary detection marks the dictionary facet, then the sampling
            // pass augments it with the REE facet on the same map.
            final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
            try (final WritableRowSet sample = BarrageUtil.buildEncodingSampleRowSet(readBack.getRowSet())) {
                assertThat(sample).as("table should be large enough to sample").isNotNull();
                BarrageUtil.detectStructuralDictionaryEncoding(readBack, sample, detected);
                assertThat(detected.get("Sym2"))
                        .as("structural detection marks the dictionary facet first")
                        .isEqualTo(ColumnEncoding.DICTIONARY_ENCODED_INT32);
                BarrageUtil.sampleColumnsForEncoding(readBack, detected, false, sample, true, true);
            }

            assertThat(detected.get("Sym2"))
                    .as("a clustered symbol column should be doubly encoded as REE+Dict")
                    .isEqualTo(reeAndDict);
        } finally {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDir);
        }
    }

    public void testStructuralDictionaryComposesWithSampledReeForClusteredSymbolColumn() throws Exception {
        // Regression anchor for the structural-dictionary / REE-sampling interaction.
        //
        // A grouped (clustered) low-cardinality String column is the ideal candidate for the doubly-encoded
        // RunEndEncoded<Dictionary<...>> form: few distinct values (dictionary wins) AND long adjacent runs (REE
        // wins). When such a column is symbol-table backed (read back from Parquet), inferEncodings runs
        // detectStructuralDictionaryEncoding first, marking the dictionary facet; the subsequent sampling pass must
        // then AUGMENT that marking with the REE facet rather than short-circuiting on encodings.containsKey(name).
        final File dataDir = Files.createTempDirectory(BarrageUtilTest.class.getName()).toFile();
        try {
            // 4 distinct symbols in 4 contiguous runs of 25 -> low cardinality and long runs. Cast the quotient to
            // int so the query does integer (not floating-point) division and the symbols actually cluster.
            final Table source = emptyTable(100).update("Sym = `S` + (int) (ii / 25)", "Id = ii");
            final File parquetFile = new File(dataDir, "table.parquet");
            ParquetTools.writeTable(source, parquetFile.getPath());
            final Table readBack = ParquetTools.readTable(parquetFile.getPath());

            // Precondition: the column really is symbol-table backed (the branch structural detection keys off).
            assertThat(SymbolTableSource.hasSymbolTable(readBack.getColumnSource("Sym"), readBack.getRowSet()))
                    .isTrue();

            final ColumnEncoding reeAndDict =
                    ColumnEncoding.of(ColumnEncoding.RunEndWidth.INT32, ColumnEncoding.DictWidth.INT32);

            // Control: sampling alone (no prior structural detection) sees both facets and composes REE+Dict.
            final Map<String, ColumnEncoding> samplingOnly = new java.util.HashMap<>();
            sampleColumnsForEncoding(readBack, samplingOnly, true, true);
            assertThat(samplingOnly.get("Sym"))
                    .as("sampling on the clustered symbol column should compose REE+Dict")
                    .isEqualTo(reeAndDict);

            // Full inferEncodings order: structural dictionary detection first, then sampling on the same map. The
            // structural dictionary marking is augmented -- not overwritten -- with the sampled REE facet.
            final Map<String, ColumnEncoding> pipeline = new java.util.HashMap<>();
            try (final WritableRowSet sample = BarrageUtil.buildEncodingSampleRowSet(readBack.getRowSet())) {
                assertThat(sample).as("table should be large enough to sample").isNotNull();
                BarrageUtil.detectStructuralDictionaryEncoding(readBack, sample, pipeline);
                assertThat(pipeline.get("Sym"))
                        .as("structural detection marks the dictionary facet first")
                        .isEqualTo(ColumnEncoding.DICTIONARY_ENCODED_INT32);
                BarrageUtil.sampleColumnsForEncoding(readBack, pipeline, false, sample, true, true);
            }

            // Sampling augments the structural dictionary marking with the REE facet: doubly-encoded REE+Dict.
            assertThat(pipeline.get("Sym"))
                    .as("sampling should augment the structural dictionary marking with the REE facet")
                    .isEqualTo(reeAndDict);
        } finally {
            TrackedFileHandleFactory.getInstance().closeAll();
            FileUtils.deleteRecursively(dataDir);
        }
    }

    public void testStructuralReeComposesWithSampledDictForMergedKeyColumn() {
        // Mirror-image regression anchor to testStructuralDictionaryComposesWithSampledReeForClusteredSymbolColumn.
        //
        // A merged-partitioned-table key column is constant per region, so detectStructuralRunEndEncoding marks it
        // REE. But across regions it repeats a small set of String values, so it is also an ideal dictionary
        // candidate -- the doubly-encoded RunEndEncoded<Dictionary<...>> form. Structural REE runs first and writes
        // the column, so the later sampling pass must AUGMENT that marking with the dictionary facet rather than
        // short-circuiting on encodings.containsKey(name).
        //
        // 4 distinct symbols in 4 contiguous runs of 25: low cardinality (dictionary wins) and long runs (REE wins).
        final int N = 100;
        final String[] symbols = new String[N];
        final String[] pool = {"AAPL", "MSFT", "GOOG", "AMZN"};
        for (int i = 0; i < N; i++) {
            symbols[i] = pool[i / 25];
        }
        final Table base = newTable(stringCol("Symbol", symbols));

        final ColumnEncoding reeAndDict =
                ColumnEncoding.of(ColumnEncoding.RunEndWidth.INT32, ColumnEncoding.DictWidth.INT32);

        // Control: sampling alone (no prior structural detection) sees both facets and composes REE+Dict.
        final Map<String, ColumnEncoding> samplingOnly = new java.util.HashMap<>();
        sampleColumnsForEncoding(base, samplingOnly, true, true);
        assertThat(samplingOnly.get("Symbol"))
                .as("sampling on the clustered symbol column should compose REE+Dict")
                .isEqualTo(reeAndDict);

        // Mark Symbol as a merged-table key column so structural REE fires on it.
        final Table table = base.withAttributes(Map.of(
                Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                Table.KEY_COLUMNS_ATTRIBUTE, "Symbol"));

        // Full inferEncodings order: structural REE first, then structural dictionary, then sampling on the same map.
        final Map<String, ColumnEncoding> pipeline = new java.util.HashMap<>();
        BarrageUtil.detectStructuralRunEndEncoding(table, pipeline);
        assertThat(pipeline.get("Symbol"))
                .as("structural REE marks the run-end facet first")
                .isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        try (final WritableRowSet sample = BarrageUtil.buildEncodingSampleRowSet(table.getRowSet())) {
            assertThat(sample).as("table should be large enough to sample").isNotNull();
            // The in-memory column is not symbol-table backed, so the dictionary facet comes from sampling.
            BarrageUtil.detectStructuralDictionaryEncoding(table, sample, pipeline);
            BarrageUtil.sampleColumnsForEncoding(table, pipeline, false, sample, true, true);
        }

        // Sampling augments the structural REE marking with the dictionary facet: doubly-encoded REE+Dict.
        assertThat(pipeline.get("Symbol"))
                .as("sampling should augment the structural REE marking with the dictionary facet")
                .isEqualTo(reeAndDict);
    }

    public void testSamplingDoesNotAddDictionaryToSingleValueColumn() {
        // A SingleValueColumnSource is a single run of one distinct value: REE is the whole story and a dictionary
        // would be pure overhead. Structural REE marks it, and the augmenting sampling pass must NOT add a dictionary
        // facet even though the sampled cardinality (1 distinct value) is trivially below the threshold.
        final Table base = emptyTable(100).update("Y = `hello`");

        final Map<String, ColumnEncoding> pipeline = new java.util.HashMap<>();
        BarrageUtil.detectStructuralRunEndEncoding(base, pipeline);
        assertThat(pipeline.get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        try (final WritableRowSet sample = BarrageUtil.buildEncodingSampleRowSet(base.getRowSet())) {
            assertThat(sample).as("table should be large enough to sample").isNotNull();
            BarrageUtil.sampleColumnsForEncoding(base, pipeline, false, sample, true, true);
        }

        // Stays REE-only: the single-value guard suppresses the otherwise-eligible dictionary facet.
        assertThat(pipeline.get("Y"))
                .as("a single-value column must not gain a dictionary facet from sampling")
                .isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
    }

    public void testExplicitBarrageSchemaAttributeSuppressesAutoREE() {
        final Table base = newTable(
                stringCol("Symbol", "AAPL", "MSFT"),
                intCol("Exchange", 1, 2));
        final Schema plainSchema = BarrageUtil.schemaFromTable(base);

        final Table table = base.withAttributes(Map.of(
                Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange",
                Table.BARRAGE_SCHEMA_ATTRIBUTE, plainSchema));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertFieldIsNotREE(schema, "Symbol");
        assertFieldIsNotREE(schema, "Exchange");
    }

    public void testSingleValueColumnSourceGetREE() {
        // update() with a constant expression produces SingleValueColumnSource for each column
        final Table table = emptyTable(100).update("X = 42", "Y = `hello`", "Z = 1.5");

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        BarrageUtil.detectStructuralRunEndEncoding(table, detected);

        assertThat(detected.get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected.get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected.get("Z")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
    }

    public void testNullValueColumnSourceGetREE() {
        // NullValueColumnSource represents a column that is always null (e.g. outer-join missing side)
        // and is a trivial single-run case for REE.
        final ColumnSource<?> nullSource = NullValueColumnSource.getInstance(int.class, null);
        final Table table = newTable(5, Map.of("X", nullSource));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        BarrageUtil.detectStructuralRunEndEncoding(table, detected);

        assertThat(detected.get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
    }

    public void testREEFieldStructureInt32RunEndsForLargeBatch() {
        // Verify the flatbuf IPC path: Int32 run_ends regardless of batch size.
        // Uses explicit encodings to bypass the global REE_AUTO_DETECT_ENABLED flag.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL"))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol"));

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                .batchSize(Short.MAX_VALUE + 1)
                .build();

        final Map<String, ColumnEncoding> encodings = Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT32);
        final Schema schema = BarrageUtil.makeSchema(
                options, table.getDefinition(), table.getAttributes(), table.isFlat(), encodings);
        final com.google.protobuf.ByteString schemaBytes =
                BarrageUtil.schemaBytes(schema::getSchema);
        final org.apache.arrow.flatbuf.Schema flatSchema =
                SchemaHelper.flatbufSchema(schemaBytes.asReadOnlyByteBuffer());
        final Field symbolField = Field.convertField(flatSchema.fields(0));

        assertThat(symbolField.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
        final Field runEnds = symbolField.getChildren().get(0);
        assertThat(runEnds.getName()).isEqualTo("run_ends");
        assertThat(((ArrowType.Int) runEnds.getType()).getBitWidth()).isEqualTo(32);
        assertThat(((ArrowType.Int) runEnds.getType()).getIsSigned()).isTrue();
    }

    public void testREEFieldStructure() {
        // Uses explicit encodings to bypass the global REE_AUTO_DETECT_ENABLED flag.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL"),
                intCol("Exchange", 1, 1))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol"));

        final Map<String, ColumnEncoding> encodings = Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT32);
        final Schema schema = BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, table.getDefinition(), table.getAttributes(),
                table.isFlat(), encodings);
        final Field symbolField = schema.findField("Symbol");

        assertThat(symbolField.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
        assertThat(symbolField.getChildren()).hasSize(2);

        final Field runEnds = symbolField.getChildren().get(0);
        assertThat(runEnds.getName()).isEqualTo("run_ends");
        assertThat(runEnds.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.Int);
        assertThat(((ArrowType.Int) runEnds.getType()).getBitWidth()).isEqualTo(32);
        assertThat(((ArrowType.Int) runEnds.getType()).getIsSigned()).isTrue();

        final Field values = symbolField.getChildren().get(1);
        assertThat(values.getName()).isEqualTo("values");
        assertThat(values.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.Utf8);
    }

    public void testConvertArrowSchemaRoundtrip() {
        // Uses explicit encodings to bypass the global REE_AUTO_DETECT_ENABLED flag.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL", "MSFT"),
                intCol("Exchange", 1, 1, 2),
                doubleCol("Price", 100.0, 101.0, 200.0),
                longCol("Size", 100L, 200L, 150L))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Map<String, ColumnEncoding> encodings = Map.of(
                "Symbol", ColumnEncoding.RUN_END_ENCODED_INT32,
                "Exchange", ColumnEncoding.RUN_END_ENCODED_INT32);
        final Schema schema = BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, table.getDefinition(), table.getAttributes(),
                table.isFlat(), encodings);

        // Confirm the schema has REE on the key columns before converting back.
        assertFieldIsREE(schema, "Symbol");
        assertFieldIsREE(schema, "Exchange");

        final BarrageUtil.ConvertedArrowSchema converted = BarrageUtil.convertArrowSchema(schema);

        // Column names are preserved.
        assertThat(converted.tableDef.getColumnNames())
                .containsExactly("Symbol", "Exchange", "Price", "Size");

        // REE wrapper is transparent: column types survive the roundtrip unchanged.
        assertThat(converted.tableDef.getColumn("Symbol").getDataType()).isEqualTo(String.class);
        assertThat(converted.tableDef.getColumn("Exchange").getDataType()).isEqualTo(int.class);
        assertThat(converted.tableDef.getColumn("Price").getDataType()).isEqualTo(double.class);
        assertThat(converted.tableDef.getColumn("Size").getDataType()).isEqualTo(long.class);
    }

    public void testSamplingDetectsRepetitiveColumn() {
        // Calls sampleColumnsForEncoding directly (REE detection on, dictionary off) to validate sampling logic.
        final int N = 100;
        final String[] symbols = new String[N];
        Arrays.fill(symbols, "AAPL");
        final int[] prices = new int[N];
        Arrays.fill(prices, 42);
        final Table table = newTable(stringCol("Symbol", symbols), intCol("Price", prices));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, true, false);

        assertThat(detected.get("Symbol")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected.get("Price")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
    }

    public void testSamplingSkipsDistinctColumn() {
        // All-distinct values produce a run ratio of 1.0, above the threshold — no REE.
        final int N = 100;
        final int[] x = new int[N];
        for (int i = 0; i < N; i++) {
            x[i] = i;
        }
        final Table table = newTable(intCol("X", x));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, true, false);

        assertThat(detected).doesNotContainKey("X");
    }

    public void testSamplingMixedTable() {
        // Repetitive column (runs of 4) gets REE; all-distinct column does not.
        final int N = 100;
        final int[] y = new int[N];
        final int[] x = new int[N];
        for (int i = 0; i < N; i++) {
            y[i] = i / 4; // runs of 4
            x[i] = i;
        }
        final Table table = newTable(intCol("Y", y), intCol("X", x));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, true, false);

        assertThat(detected.get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected).doesNotContainKey("X");
    }

    public void testSamplingSkippedForSmallTable() {
        // Tables with fewer than REE_MIN_SAMPLE_SIZE rows return early without sampling.
        final int N = BarrageUtil.REE_MIN_SAMPLE_SIZE - 1;
        final int[] y = new int[N];
        for (int i = 0; i < N; i++) {
            y[i] = i / 4; // runs of 4
        }
        final Table table = newTable(intCol("Y", y));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, true, false);

        assertThat(detected).doesNotContainKey("Y");
    }

    public void testSamplingDetectsLowCardinalityStringColumn() {
        // A String column with few distinct values, but not clustered (values alternate every row so run-end
        // encoding would not help). Dictionary detection on, REE off.
        final int N = 100;
        final String[] symbols = new String[N];
        final String[] pool = {"AAPL", "MSFT", "GOOG", "AMZN"};
        for (int i = 0; i < N; i++) {
            symbols[i] = pool[i % pool.length];
        }
        final Table table = newTable(stringCol("Symbol", symbols));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, false, true);

        assertThat(detected.get("Symbol")).isEqualTo(ColumnEncoding.DICTIONARY_ENCODED_INT32);
    }

    public void testSamplingSkipsHighCardinalityStringColumn() {
        // All-distinct String values produce a cardinality ratio of 1.0, above the threshold — no dictionary.
        final int N = 100;
        final String[] symbols = new String[N];
        for (int i = 0; i < N; i++) {
            symbols[i] = "SYM" + i;
        }
        final Table table = newTable(stringCol("Symbol", symbols));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, false, true);

        assertThat(detected).doesNotContainKey("Symbol");
    }

    public void testSamplingDictOnlyForObjectColumns() {
        // Primitive columns are never dictionary-encoded even when highly repetitive; REE is the better fit.
        final int N = 100;
        final int[] x = new int[N];
        for (int i = 0; i < N; i++) {
            x[i] = i % 4;
        }
        final Table table = newTable(intCol("X", x));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, false, true);

        assertThat(detected).doesNotContainKey("X");
    }

    public void testSamplingComposesReeAndDictForClusteredStrings() {
        // A clustered low-cardinality String column shows an advantage for both facets, so sampling composes them
        // into a doubly-encoded RunEndEncoded<Dictionary<...>>.
        final int N = 100;
        final String[] symbols = new String[N];
        final String[] pool = {"AAPL", "MSFT", "GOOG", "AMZN"};
        for (int i = 0; i < N; i++) {
            symbols[i] = pool[i / 25]; // 4 long runs
        }
        final Table table = newTable(stringCol("Symbol", symbols));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        sampleColumnsForEncoding(table, detected, true, true);

        assertThat(detected.get("Symbol")).isEqualTo(
                ColumnEncoding.of(ColumnEncoding.RunEndWidth.INT32, ColumnEncoding.DictWidth.INT32));
    }

    public void testExplicitReeSchemaHonoredWhenGlobalDisabled() {
        // Even when REE_AUTO_DETECT_ENABLED is false, a user-supplied BARRAGE_SCHEMA_ATTRIBUTE with
        // REE columns must be passed through verbatim.
        //
        // Build the REE schema the exhaustive way per barrage-schema.md: extract the base schema,
        // reuse the original field's FieldType so deephaven:type metadata is preserved, then
        // construct the run_ends / values children and the REE parent field manually.
        final Table base = newTable(
                stringCol("Symbol", "AAPL", "MSFT"),
                intCol("Exchange", 1, 2));

        final Schema baseSchema = BarrageUtil.schemaFromTable(base);
        final java.util.List<Field> fields = new java.util.ArrayList<>(baseSchema.getFields());

        final Field originalSymbol = baseSchema.findField("Symbol");
        final Field runEndsField = new Field(
                "run_ends",
                new FieldType(false, new ArrowType.Int(32, true), null, null),
                Collections.emptyList());
        final Field valuesField = new Field(
                "values",
                originalSymbol.getFieldType(),
                originalSymbol.getChildren());
        final Field reeSymbolField = new Field(
                "Symbol",
                new FieldType(true, new ArrowType.RunEndEncoded(), null, null),
                List.of(runEndsField, valuesField));

        fields.set(fields.indexOf(originalSymbol), reeSymbolField);
        final Schema reedSchema = new Schema(fields);

        final Table table = base.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, reedSchema));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertFieldIsREE(schema, "Symbol");
        assertFieldIsNotREE(schema, "Exchange");
    }

    private static void assertFieldIsREE(final Schema schema, final String columnName) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        assertThat(field.getType().getTypeID())
                .as("field %s should be RunEndEncoded", columnName)
                .isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
    }

    public void testUserSuppliedInt16ReeSchemaPreservesWidth() {
        final Table base = newTable(intCol("X", 1, 2), stringCol("Y", "a", "b"));
        final Schema userSchema = buildReeSchema("X", Types.MinorType.SMALLINT, "Y", Types.MinorType.INT);
        final Table table = base.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, userSchema));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertRunEndsWidth(schema, "X", 16);
        assertRunEndsWidth(schema, "Y", 32);
    }

    public void testUserSuppliedInt64ReeSchemaPreservesWidth() {
        final Table base = newTable(intCol("X", 1, 2), stringCol("Y", "a", "b"));
        final Schema userSchema = buildReeSchema("X", Types.MinorType.BIGINT, "Y", Types.MinorType.INT);
        final Table table = base.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, userSchema));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertRunEndsWidth(schema, "X", 64);
        assertRunEndsWidth(schema, "Y", 32);
    }

    private static Schema buildReeSchema(
            final String col1, final Types.MinorType col1RunEndsType,
            final String col2, final Types.MinorType col2RunEndsType) {
        return new Schema(List.of(
                buildReeField(col1, col1RunEndsType, Types.MinorType.INT),
                buildReeField(col2, col2RunEndsType, Types.MinorType.VARCHAR)));
    }

    private static Field buildReeField(
            final String name, final Types.MinorType runEndsType, final Types.MinorType valuesType) {
        final Field runEndsField = new Field(
                "run_ends", new FieldType(false, runEndsType.getType(), null), Collections.emptyList());
        final Field valuesField = new Field(
                "values", new FieldType(true, valuesType.getType(), null), Collections.emptyList());
        return new Field(
                name, new FieldType(false, new ArrowType.RunEndEncoded(), null), List.of(runEndsField, valuesField));
    }

    private static void assertRunEndsWidth(final Schema schema, final String columnName, final int expectedBitWidth) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        assertThat(field.getType().getTypeID())
                .as("field %s should be RunEndEncoded", columnName)
                .isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
        final Field runEnds = field.getChildren().get(0);
        assertThat(((ArrowType.Int) runEnds.getType()).getBitWidth())
                .as("field %s run_ends bitWidth", columnName)
                .isEqualTo(expectedBitWidth);
    }

    private static void assertFieldIsNotREE(final Schema schema, final String columnName) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        assertThat(field.getType().getTypeID())
                .as("field %s should not be RunEndEncoded", columnName)
                .isNotEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
    }

    // -------------------------------------------------------------------------
    // Dictionary encoding schema tests
    // -------------------------------------------------------------------------

    public void testMakeSchemaWithDictionaryInt32() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT32));

        assertFieldIsDictionary(schema, "Symbol", 32);
        final DictionaryEncoding enc = schema.findField("Symbol").getDictionary();
        assertThat(enc.getId()).isEqualTo(0L);
    }

    public void testMakeSchemaWithDictionaryInt8() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT8));

        assertFieldIsDictionary(schema, "Symbol", 8);
        final DictionaryEncoding enc = schema.findField("Symbol").getDictionary();
        assertThat(enc.getId()).isEqualTo(0L);
    }

    public void testMakeSchemaWithDictionaryInt16() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT16));

        assertFieldIsDictionary(schema, "Symbol", 16);
        final DictionaryEncoding enc = schema.findField("Symbol").getDictionary();
        assertThat(enc.getId()).isEqualTo(0L);
    }

    public void testTwoDictionaryColumnsGetSequentialIds() {
        final TableDefinition tableDef = TableDefinition.of(
                ColumnDefinition.ofString("Symbol"),
                ColumnDefinition.ofString("Exchange"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of(
                        "Symbol", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT32,
                        "Exchange", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT32));

        assertFieldIsDictionary(schema, "Symbol", 32);
        assertFieldIsDictionary(schema, "Exchange", 32);
        final long symbolId = schema.findField("Symbol").getDictionary().getId();
        final long exchangeId = schema.findField("Exchange").getDictionary().getId();
        assertThat(new long[] {symbolId, exchangeId}).containsExactlyInAnyOrder(0L, 1L);
    }

    public void testEncodingsFromSchemaRoundTripsDictionary() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema dictSchema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", io.deephaven.extensions.barrage.ColumnEncoding.DICTIONARY_ENCODED_INT16));

        final Table base = newTable(stringCol("Symbol", "AAPL"));
        final Table annotated = base.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, dictSchema));
        final Schema result = BarrageUtil.schemaFromTable(annotated);

        assertFieldIsDictionary(result, "Symbol", 16);
    }

    private static void assertFieldIsDictionary(final Schema schema, final String columnName,
            final int expectedIndexBitWidth) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        final DictionaryEncoding enc = field.getDictionary();
        assertThat(enc).as("field %s should have DictionaryEncoding", columnName).isNotNull();
        assertThat(enc.getIndexType()).as("field %s index type", columnName).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) enc.getIndexType()).getBitWidth())
                .as("field %s index bit width", columnName)
                .isEqualTo(expectedIndexBitWidth);
    }

    // -------------------------------------------------------------------------
    // Combined REE + dictionary (doubly-encoded) schema tests
    // -------------------------------------------------------------------------

    public void testMakeSchemaComposesReeOverDictionary() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT32.withDictionary(ColumnEncoding.DictWidth.INT32)));

        // The parent is RunEndEncoded; its values child carries the DictionaryEncoding.
        assertFieldIsReeOverDictionary(schema, "Symbol", 32, 32);
    }

    public void testMakeSchemaComposesReeOverDictionaryNonDefaultWidths() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT16.withDictionary(ColumnEncoding.DictWidth.INT8)));

        assertFieldIsReeOverDictionary(schema, "Symbol", 16, 8);
    }

    public void testEncodingsFromSchemaRoundTripsReeDictionary() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema composed = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT32.withDictionary(ColumnEncoding.DictWidth.INT16)));

        final Table base = newTable(stringCol("Symbol", "AAPL"));
        final Table annotated = base.withAttributes(Map.of(Table.BARRAGE_SCHEMA_ATTRIBUTE, composed));
        final Schema result = BarrageUtil.schemaFromTable(annotated);

        // The explicit schema's combined encoding must survive the schema -> encodings -> schema round trip.
        assertFieldIsReeOverDictionary(result, "Symbol", 32, 16);
    }

    public void testMakeSchemaComposesReeOverDictionaryColumnsAsList() {
        final TableDefinition tableDef = TableDefinition.of(ColumnDefinition.ofString("Symbol"));
        final Schema schema = BarrageUtil.makeSchema(
                BarrageSubscriptionOptions.builder().columnsAsList(true).build(),
                tableDef,
                Collections.emptyMap(),
                false,
                Map.of("Symbol", ColumnEncoding.RUN_END_ENCODED_INT32.withDictionary(ColumnEncoding.DictWidth.INT32)));

        // columnsAsList wraps each column in an outer List; the encoding must apply to the inner field, giving
        // List<RunEndEncoded<Dictionary<...>>>.
        final Field listField = schema.findField("Symbol");
        assertThat(listField.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.List);
        assertThat(listField.getChildren()).hasSize(1);
        final Field inner = listField.getChildren().get(0);
        assertThat(inner.getType().getTypeID()).isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
        assertThat(inner.getDictionary()).as("REE parent under List must not carry a dictionary").isNull();
        final Field values = inner.getChildren().get(1);
        assertThat(values.getDictionary()).as("values child under List<REE<...>> must be dictionary encoded")
                .isNotNull();
    }

    private static void assertFieldIsReeOverDictionary(final Schema schema, final String columnName,
            final int expectedRunEndsBitWidth, final int expectedIndexBitWidth) {
        final Field parent = schema.findField(columnName);
        assertThat(parent).as("field %s", columnName).isNotNull();
        assertThat(parent.getType().getTypeID())
                .as("field %s should be RunEndEncoded", columnName)
                .isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
        assertThat(parent.getDictionary())
                .as("field %s REE parent must NOT carry a dictionary (dictionary belongs on the values child)",
                        columnName)
                .isNull();
        assertThat(parent.getChildren()).as("field %s children", columnName).hasSize(2);

        final Field runEnds = parent.getChildren().get(0);
        assertThat(runEnds.getType()).as("field %s run_ends type", columnName).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) runEnds.getType()).getBitWidth())
                .as("field %s run_ends bit width", columnName)
                .isEqualTo(expectedRunEndsBitWidth);

        final Field values = parent.getChildren().get(1);
        final DictionaryEncoding enc = values.getDictionary();
        assertThat(enc).as("field %s values child should be dictionary encoded", columnName).isNotNull();
        assertThat(((ArrowType.Int) enc.getIndexType()).getBitWidth())
                .as("field %s dictionary index bit width", columnName)
                .isEqualTo(expectedIndexBitWidth);
    }
}
