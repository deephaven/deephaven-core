//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnEncoding;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.*;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")

public class BarrageUtilTest extends RefreshingTableTestCase {

    public void testMergedTableKeyColumnsGetREE() {
        // Tests inferEncodings directly: merged key columns always get REE
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "AAPL", "MSFT"),
                intCol("Exchange", 1, 1, 2),
                doubleCol("Price", 100.0, 101.0, 200.0),
                longCol("Size", 100L, 200L, 150L))
                .withAttributes(Map.of(
                        Table.MERGED_TABLE_ATTRIBUTE, Boolean.TRUE,
                        Table.KEY_COLUMNS_ATTRIBUTE, "Symbol,Exchange"));

        final Map<String, ColumnEncoding> encodings = BarrageUtil.inferEncodings(table);

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

        assertThat(detected.get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected.get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
        assertThat(detected.get("Z")).isEqualTo(ColumnEncoding.RUN_END_ENCODED_INT32);
    }

    public void testNullValueColumnSourceGetREE() {
        // NullValueColumnSource represents a column that is always null (e.g. outer-join missing side)
        // and is a trivial single-run case for REE.
        final ColumnSource<?> nullSource = NullValueColumnSource.getInstance(int.class, null);
        final Table table = newTable(5, Map.of("X", nullSource));

        final Map<String, ColumnEncoding> detected = BarrageUtil.inferEncodings(table);

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
        // Calls sampleColumnsForREE directly to validate sampling logic regardless of the enable flag.
        final int N = 100;
        final String[] symbols = new String[N];
        Arrays.fill(symbols, "AAPL");
        final int[] prices = new int[N];
        Arrays.fill(prices, 42);
        final Table table = newTable(stringCol("Symbol", symbols), intCol("Price", prices));

        final Map<String, ColumnEncoding> detected = new java.util.HashMap<>();
        BarrageUtil.sampleColumnsForREE(table, detected, false);

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
        BarrageUtil.sampleColumnsForREE(table, detected, false);

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
        BarrageUtil.sampleColumnsForREE(table, detected, false);

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
        BarrageUtil.sampleColumnsForREE(table, detected, false);

        assertThat(detected).doesNotContainKey("Y");
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
}
