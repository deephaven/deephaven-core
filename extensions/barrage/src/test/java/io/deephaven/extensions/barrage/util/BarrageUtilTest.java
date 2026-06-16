//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnEncoding;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.*;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")

public class BarrageUtilTest extends RefreshingTableTestCase {

    public void testMergedTableKeyColumnsGetREE() {
        // newTable produces array-backed sources; only the key columns get REE via attribute detection
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL", "MSFT"),
                intCol("Exchange", 1, 1, 2),
                doubleCol("Price", 100.0, 101.0, 200.0),
                longCol("Size", 100L, 200L, 150L))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Schema schema = BarrageUtil.schemaFromTable(table);

        assertFieldIsREE(schema, "Symbol");
        assertFieldIsREE(schema, "Exchange");
        assertFieldIsNotREE(schema, "Price");
        assertFieldIsNotREE(schema, "Size");
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

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected.get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected.get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected.get("Z")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
    }

    public void testNullValueColumnSourceGetREE() {
        // NullValueColumnSource represents a column that is always null (e.g. outer-join missing side)
        // and is a trivial single-run case for REE.
        final ColumnSource<?> nullSource = NullValueColumnSource.getInstance(int.class, null);
        final Table table = newTable(5, Map.of("X", nullSource));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected.get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
    }

    public void testREEFieldStructureInt32RunEndsForLargeBatch() {
        // REE fields always use Int32 run_ends regardless of batch size; verify via makeTableSchemaPayload.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL"))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol"));

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                .batchSize(Short.MAX_VALUE + 1)
                .build();

        // schemaBytes handles IPC framing; makeTableSchemaPayload provides the payload.
        final com.google.protobuf.ByteString schemaBytes =
                BarrageUtil.schemaBytes(b -> BarrageUtil.makeTableSchemaPayload(b, options, table));
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
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL"),
                intCol("Exchange", 1, 1))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol"));

        final Schema schema = BarrageUtil.schemaFromTable(table);
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
        // Build a merged table so Symbol and Exchange get REE auto-detected, while Price and Size stay plain.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL", "MSFT"),
                intCol("Exchange", 1, 1, 2),
                doubleCol("Price", 100.0, 101.0, 200.0),
                longCol("Size", 100L, 200L, 150L))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Schema schema = BarrageUtil.schemaFromTable(table);

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
        // Array-backed columns (not SingleValueColumnSource) with all identical values should be
        // auto-detected via sampling.
        final int N = 100;
        final String[] symbols = new String[N];
        Arrays.fill(symbols, "AAPL");
        final int[] prices = new int[N];
        Arrays.fill(prices, 42);
        final Table table = newTable(stringCol("Symbol", symbols), intCol("Price", prices));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected.get("Symbol")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected.get("Price")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
    }

    public void testSamplingSkipsDistinctColumn() {
        // All-distinct values produce a run ratio of 1.0, which is above the threshold — no REE.
        final int N = 100;
        final int[] prices = new int[N];
        for (int i = 0; i < N; i++) {
            prices[i] = i;
        }
        final Table table = newTable(intCol("Price", prices));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected).doesNotContainKey("Price");
    }

    public void testSamplingMixedTable() {
        // Repetitive column gets REE; all-distinct column does not.
        final int N = 100;
        final String[] symbols = new String[N];
        Arrays.fill(symbols, "AAPL");
        final int[] prices = new int[N];
        for (int i = 0; i < N; i++) {
            prices[i] = i;
        }
        final Table table = newTable(stringCol("Symbol", symbols), intCol("Price", prices));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected.get("Symbol")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected).doesNotContainKey("Price");
    }

    public void testSamplingSkippedForSmallTable() {
        // Tables with fewer than REE_MIN_SAMPLE_SIZE (16) rows are not sampled.
        final String[] symbols = new String[15];
        Arrays.fill(symbols, "AAPL");
        final Table table = newTable(stringCol("Symbol", symbols));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

        assertThat(detected).doesNotContainKey("Symbol");
    }

    private static void assertFieldIsREE(final Schema schema, final String columnName) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        assertThat(field.getType().getTypeID())
                .as("field %s should be RunEndEncoded", columnName)
                .isEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
    }

    private static void assertFieldIsNotREE(final Schema schema, final String columnName) {
        final Field field = schema.findField(columnName);
        assertThat(field).as("field %s", columnName).isNotNull();
        assertThat(field.getType().getTypeID())
                .as("field %s should not be RunEndEncoded", columnName)
                .isNotEqualTo(ArrowType.ArrowTypeID.RunEndEncoded);
    }
}
