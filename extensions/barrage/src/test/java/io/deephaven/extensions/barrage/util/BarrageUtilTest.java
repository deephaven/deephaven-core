//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.extensions.barrage.ArrowSchemaControl;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnEncoding;
import io.deephaven.proto.flight.util.SchemaHelper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

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

        final ArrowSchemaControl detected = BarrageUtil.inferSchemaProperties(table, ArrowSchemaControl.DEFAULT);

        assertThat(detected.encodings().get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected.encodings().get("Y")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
        assertThat(detected.encodings().get("Z")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
    }

    public void testNullValueColumnSourceGetREE() {
        // NullValueColumnSource represents a column that is always null (e.g. outer-join missing side)
        // and is a trivial single-run case for REE.
        final ColumnSource<?> nullSource = NullValueColumnSource.getInstance(int.class, null);
        final Table table = newTable(5, Map.of("X", nullSource));

        final ArrowSchemaControl detected = BarrageUtil.inferSchemaProperties(table, ArrowSchemaControl.DEFAULT);

        assertThat(detected.encodings().get("X")).isEqualTo(ColumnEncoding.RUN_END_ENCODED);
    }

    public void testExplicitColumnEncodingOverrideForceREE() {
        // A plain array-backed table with no merged/key-column attributes and no constant sources
        // should produce no auto-REE columns.
        final Table table = newTable(
                stringCol("Symbol", "AAPL", "MSFT", "GOOG"),
                intCol("Price", 100, 200, 300));

        assertThat(BarrageUtil.inferSchemaProperties(table, ArrowSchemaControl.DEFAULT).encodings()).isEmpty();

        // Explicitly request REE for Symbol via the 2-arg schemaFromTable overload.
        final Schema schema = BarrageUtil.schemaFromTable(table,
                ArrowSchemaControl.builder()
                        .putEncoding("Symbol", ColumnEncoding.RUN_END_ENCODED)
                        .build());

        assertFieldIsREE(schema, "Symbol");
        assertFieldIsNotREE(schema, "Price");
    }

    public void testREEFieldStructureInt32RunEndsForLargeBatch() {
        // When batchSize > Short.MAX_VALUE, arrowReeFieldFor must select Int32 run_ends.
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
        // DEFAULT_BATCH_SIZE == Short.MAX_VALUE, so arrowReeFieldFor selects Int16 run_ends.
        assertThat(((ArrowType.Int) runEnds.getType()).getBitWidth()).isEqualTo(16);
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
