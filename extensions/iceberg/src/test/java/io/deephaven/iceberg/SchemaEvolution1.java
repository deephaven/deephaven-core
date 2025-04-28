//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.util.InferenceInstructions;
import io.deephaven.iceberg.util.LoadTableOptions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.InferenceResolver;
import io.deephaven.iceberg.util.SchemaProvider;
import io.deephaven.iceberg.util.TypeInference;
import io.deephaven.util.QueryConstants;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static io.deephaven.iceberg.util.ColumnInstructions.unmapped;
import static org.assertj.core.api.Assertions.assertThat;

@Tag("security-manager-allow")
public class SchemaEvolution1 {

    private static final String CATALOG_NAME = "schema-evolution";
    private static final String NAMESPACE = "schema-evolution";
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "table-1");

    private static final TableDefinition IDEF_0 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1"),
            ColumnDefinition.ofInt("Field2"));

    private static final TableDefinition IDEF_1 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1_B"),
            ColumnDefinition.ofInt("Field2_B"));

    private static final TableDefinition IDEF_2 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1_C"),
            ColumnDefinition.ofInt("Field2_C"));

    private static final TableDefinition IDEF_3 = TableDefinition.of(
            ColumnDefinition.ofInt("Field2_C"),
            ColumnDefinition.ofInt("Field1_C"));

    private static final TableDefinition IDEF_4 = TableDefinition.of(
            ColumnDefinition.ofInt("Field1_D"),
            ColumnDefinition.ofInt("Field2_D"),
            ColumnDefinition.ofInt("Field3_D"));

    private int fieldId1;
    private int fieldId2;
    private int fieldId3;

    private static IcebergTableAdapter loadTable(LoadTableOptions options) throws URISyntaxException {
        return DbResource.openCatalog(CATALOG_NAME).loadTable(options);
    }

    private static IcebergTableAdapter loadWithSchema(SchemaProvider schema) throws URISyntaxException {
        return loadTable(builder().resolver(InferenceResolver.builder().schema(schema).build()).build());
    }

    private static LoadTableOptions.Builder builder() {
        return LoadTableOptions.builder().id(TABLE_ID);
    }

    @BeforeEach
    void setUp() throws URISyntaxException {
        {
            final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
            final Schema initialSchema = tableAdapter.currentSchema();
            fieldId1 = initialSchema.findField(IDEF_4.getColumnNames().get(0)).fieldId();
            fieldId2 = initialSchema.findField(IDEF_4.getColumnNames().get(1)).fieldId();
            fieldId3 = initialSchema.findField(IDEF_4.getColumnNames().get(2)).fieldId();
        }
    }

    @Test
    void schemas() throws URISyntaxException {
        final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        // This is a meta test, making sure test setup is correct
        {
            final Map<Integer, Schema> schemas = tableAdapter.schemas();
            assertThat(schemas).hasSize(5);
            assertThat(schemas).extractingByKey(0).usingEquals(Schema::sameSchema).isEqualTo(schema_0());
            assertThat(schemas).extractingByKey(1).usingEquals(Schema::sameSchema).isEqualTo(schema_1());
            assertThat(schemas).extractingByKey(2).usingEquals(Schema::sameSchema).isEqualTo(schema_2());
            assertThat(schemas).extractingByKey(3).usingEquals(Schema::sameSchema).isEqualTo(schema_3());
            assertThat(schemas).extractingByKey(4).usingEquals(Schema::sameSchema).isEqualTo(schema_4());
        }
        assertThat(tableAdapter.currentSchema()).usingEquals(Schema::sameSchema).isEqualTo(schema_4());
    }

    @Test
    void snapshots() throws URISyntaxException {
        final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        // This is a meta test, making sure test setup is correct
        assertThat(tableAdapter.listSnapshots()).hasSize(6);
    }

    @Test
    void inference() throws TypeInference.Exception {
        // This is a meta test, making sure test setup is correct
        assertThat(Resolver.infer(ia(schema_0())).definition()).isEqualTo(IDEF_0);
        assertThat(Resolver.infer(ia(schema_1())).definition()).isEqualTo(IDEF_1);
        assertThat(Resolver.infer(ia(schema_2())).definition()).isEqualTo(IDEF_2);
        assertThat(Resolver.infer(ia(schema_3())).definition()).isEqualTo(IDEF_3);
        assertThat(Resolver.infer(ia(schema_4())).definition()).isEqualTo(IDEF_4);
    }

    @Test
    void schemaLatestAt() throws URISyntaxException {
        schemaYAt(IDEF_4, SchemaProvider.fromCurrent());
    }

    @Test
    void schema0At() throws URISyntaxException {
        schemaXAt(IDEF_0, SchemaProvider.fromSchemaId(0), false);
    }

    @Test
    void schema1At() throws URISyntaxException {
        schemaXAt(IDEF_1, SchemaProvider.fromSchemaId(1), false);
    }

    @Test
    void schema2At() throws URISyntaxException {
        schemaXAt(IDEF_2, SchemaProvider.fromSchemaId(2), false);
    }

    @Test
    void schema3At() throws URISyntaxException {
        schemaXAt(IDEF_3, SchemaProvider.fromSchemaId(3), true);
    }

    @Test
    void schema4At() throws URISyntaxException {
        schemaYAt(IDEF_4, SchemaProvider.fromSchemaId(4));
    }

    @Test
    void customDefinitions() throws URISyntaxException {
        // subset, just id1
        {
            final String col1 = "Foo";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col1));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col1, data(60, false)));
            final IcebergTableAdapter ta = loadTable(builder().resolver(Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col1, schemaField(fieldId1))
                    .build())
                    .build());
            TstUtils.assertTableEquals(expected, ta.table());
        }
        // subset, just id2
        {
            final String col2 = "Bar";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col2));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col2, data(60, true)));
            final IcebergTableAdapter ta = loadTable(builder().resolver(Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col2, schemaField(fieldId2))
                    .build())
                    .build());
            TstUtils.assertTableEquals(expected, ta.table());
        }
        // subset, just id3 (ideally, )
        {
            final String col3 = "Baz";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col3));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col3, nullData(60)));
            final IcebergTableAdapter ta = loadTable(builder().resolver(Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col3, schemaField(fieldId3))
                    .build())
                    .build());
            TstUtils.assertTableEquals(expected, ta.table());
        }
        // superset
        {
            final String col1 = "Foo";
            final String col2 = "Bar";
            final String col3 = "Baz";
            final String col4 = "Zap";
            final TableDefinition td = TableDefinition.of(
                    ColumnDefinition.ofInt(col1),
                    ColumnDefinition.ofInt(col2),
                    ColumnDefinition.ofInt(col3),
                    ColumnDefinition.ofInt(col4));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col1, data(60, false)),
                    TableTools.intCol(col2, data(60, true)),
                    TableTools.intCol(col3, nullData(60)),
                    TableTools.intCol(col4, nullData(60)));
            final IcebergTableAdapter ta = loadTable(builder().resolver(Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col1, schemaField(fieldId1))
                    .putColumnInstructions(col2, schemaField(fieldId2))
                    .putColumnInstructions(col3, schemaField(fieldId3))
                    .putColumnInstructions(col4, unmapped())
                    .build())
                    .build());
            TstUtils.assertTableEquals(expected, ta.table());
        }
    }

    static void schemaXAt(TableDefinition def, SchemaProvider schema, boolean swapped) throws URISyntaxException {
        final IcebergTableAdapter ta = loadWithSchema(schema);
        final List<Snapshot> snapshots = ta.listSnapshots();
        TstUtils.assertTableEquals(expected(def, 10, swapped), ta.table(si(snapshots, 0)));
        TstUtils.assertTableEquals(expected(def, 20, swapped), ta.table(si(snapshots, 1)));
        TstUtils.assertTableEquals(expected(def, 30, swapped), ta.table(si(snapshots, 2)));
        TstUtils.assertTableEquals(expected(def, 40, swapped), ta.table(si(snapshots, 3)));
        TstUtils.assertTableEquals(expected(def, 50, swapped), ta.table(si(snapshots, 4)));
        TstUtils.assertTableEquals(expected(def, 60, swapped), ta.table(si(snapshots, 5)));
        TstUtils.assertTableEquals(expected(def, 60, swapped), ta.table());
    }

    static void schemaYAt(TableDefinition def, SchemaProvider schema) throws URISyntaxException {
        final IcebergTableAdapter ta = loadWithSchema(schema);
        final List<Snapshot> snapshots = ta.listSnapshots();
        TstUtils.assertTableEquals(expected3(def, 10), ta.table(si(snapshots, 0)));
        TstUtils.assertTableEquals(expected3(def, 20), ta.table(si(snapshots, 1)));
        TstUtils.assertTableEquals(expected3(def, 30), ta.table(si(snapshots, 2)));
        TstUtils.assertTableEquals(expected3(def, 40), ta.table(si(snapshots, 3)));
        TstUtils.assertTableEquals(expected3(def, 50), ta.table(si(snapshots, 4)));
        TstUtils.assertTableEquals(expected3(def, 60), ta.table(si(snapshots, 5)));
        TstUtils.assertTableEquals(expected3(def, 60), ta.table());
    }

    private static IcebergReadInstructions si(List<Snapshot> snapshots, int index) {
        return IcebergReadInstructions.builder().snapshot(snapshots.get(index)).build();
    }

    private static Table expected(TableDefinition td, int size, boolean swapped) {
        return TableTools.newTable(td,
                TableTools.intCol(td.getColumnNames().get(0), data(size, swapped)),
                TableTools.intCol(td.getColumnNames().get(1), data(size, !swapped)));
    }

    private static Table expected3(TableDefinition td, int size) {
        return TableTools.newTable(td,
                TableTools.intCol(td.getColumnNames().get(0), data(size, false)),
                TableTools.intCol(td.getColumnNames().get(1), data(size, true)),
                TableTools.intCol(td.getColumnNames().get(2), nullData(size)));
    }

    private static int[] data(int size, boolean neg) {
        final int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = neg ? -i : i;
        }
        return data;
    }

    private static int[] nullData(int size) {
        final int[] data = new int[size];
        Arrays.fill(data, QueryConstants.NULL_INT);
        return data;
    }

    private Schema schema_0() {
        return new Schema(
                NestedField.of(fieldId1, true, "Field1", IntegerType.get()),
                NestedField.of(fieldId2, true, "Field2", IntegerType.get()));
    }

    private Schema schema_1() {
        return new Schema(
                NestedField.of(fieldId1, true, "Field1_B", IntegerType.get()),
                NestedField.of(fieldId2, true, "Field2_B", IntegerType.get()));
    }

    private Schema schema_2() {
        return new Schema(
                NestedField.of(fieldId1, true, "Field1_C", IntegerType.get()),
                NestedField.of(fieldId2, true, "Field2_C", IntegerType.get()));
    }

    private Schema schema_3() {
        return new Schema(
                NestedField.of(fieldId2, true, "Field2_C", IntegerType.get()),
                NestedField.of(fieldId1, true, "Field1_C", IntegerType.get()));
    }

    private Schema schema_4() {
        return new Schema(
                NestedField.of(fieldId1, true, "Field1_D", IntegerType.get()),
                NestedField.of(fieldId2, true, "Field2_D", IntegerType.get()),
                NestedField.of(fieldId3, true, "Field3_D", IntegerType.get()));
    }

    static InferenceInstructions ia(Schema schema) {
        return InferenceInstructions.builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                .failOnUnsupportedTypes(true)
                .build();
    }
}
