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
import io.deephaven.iceberg.util.InferenceResolver;
import io.deephaven.iceberg.util.LoadTableOptions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.SchemaProvider;
import io.deephaven.iceberg.util.TypeInference;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("security-manager-allow")
public class SchemaEvolutionNested {

    private static final String CATALOG_NAME = "schema-evolution";
    private static final String NAMESPACE = "schema-evolution";

    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "nested-rename");

    private static final TableDefinition IDEF_0 = TableDefinition.of(
            ColumnDefinition.ofInt("Foo_Field1"),
            ColumnDefinition.ofInt("Foo_Field2"),
            ColumnDefinition.ofInt("Bar_Field1"),
            ColumnDefinition.ofInt("Bar_Field2"));

    private static final TableDefinition IDEF_1 = TableDefinition.of(
            ColumnDefinition.ofInt("Foo_B_Field1"),
            ColumnDefinition.ofInt("Foo_B_Field2"),
            ColumnDefinition.ofInt("Bar_Field1_B"),
            ColumnDefinition.ofInt("Bar_Field2_B"));

    private static final TableDefinition IDEF_2 = TableDefinition.of(
            ColumnDefinition.ofInt("Foo_C_Field1_C"),
            ColumnDefinition.ofInt("Foo_C_Field2_C"),
            ColumnDefinition.ofInt("Bar_Field1_B"),
            ColumnDefinition.ofInt("Bar_Field2_B"));

    private static IcebergTableAdapter loadTable(LoadTableOptions options) throws URISyntaxException {
        return DbResource.openCatalog(CATALOG_NAME).loadTable(options);
    }

    private static IcebergTableAdapter loadWithSchema(SchemaProvider schema) throws URISyntaxException {
        return loadTable(builder().resolver(InferenceResolver.builder().schema(schema).build()).build());
    }

    private static LoadTableOptions.Builder builder() {
        return LoadTableOptions.builder().id(TABLE_ID);
    }

    private int fooId;
    private int fooField1Id;
    private int fooField2Id;

    private int barId;
    private int barField1Id;
    private int barField2Id;

    @BeforeEach
    void setUp() throws URISyntaxException {
        {
            final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
            final Schema initialSchema = tableAdapter.schema(0).orElseThrow();
            fooId = initialSchema.findField("Foo").fieldId();
            fooField1Id = initialSchema.findField("Foo.Field1").fieldId();
            fooField2Id = initialSchema.findField("Foo.Field2").fieldId();

            barId = initialSchema.findField("Bar").fieldId();
            barField1Id = initialSchema.findField("Bar.Field1").fieldId();
            barField2Id = initialSchema.findField("Bar.Field2").fieldId();
        }
    }

    @Test
    void schemas() throws URISyntaxException {
        final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        // This is a meta test, making sure test setup is correct
        {
            final Map<Integer, Schema> schemas = tableAdapter.schemas();
            assertThat(schemas).hasSize(3);
            assertThat(schemas).extractingByKey(0).usingEquals(Schema::sameSchema).isEqualTo(schema_0());
            assertThat(schemas).extractingByKey(1).usingEquals(Schema::sameSchema).isEqualTo(schema_1());
            assertThat(schemas).extractingByKey(2).usingEquals(Schema::sameSchema).isEqualTo(schema_2());
        }
        assertThat(tableAdapter.currentSchema()).usingEquals(Schema::sameSchema).isEqualTo(schema_2());
    }

    @Test
    void snapshots() throws URISyntaxException {
        final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        // This is a meta test, making sure test setup is correct
        assertThat(tableAdapter.listSnapshots()).hasSize(3);
    }

    @Test
    void inference() throws TypeInference.Exception {
        // This is a meta test, making sure test setup is correct
        assertThat(Resolver.infer(ia(schema_0())).definition()).isEqualTo(IDEF_0);
        assertThat(Resolver.infer(ia(schema_1())).definition()).isEqualTo(IDEF_1);
        assertThat(Resolver.infer(ia(schema_2())).definition()).isEqualTo(IDEF_2);
    }

    @Test
    void readLatest() throws URISyntaxException {
        final IcebergTableAdapter tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        assertThat(tableAdapter.definition()).isEqualTo(IDEF_2);
        TstUtils.assertTableEquals(expected(IDEF_2, 15), tableAdapter.table());
    }

    @Test
    void schemaLatestAt() throws URISyntaxException {
        schemaAt(IDEF_2, SchemaProvider.fromCurrent());
    }

    @Test
    void schema0At() throws URISyntaxException {
        schemaAt(IDEF_0, SchemaProvider.fromSchemaId(0));
    }

    @Test
    void schema1At() throws URISyntaxException {
        schemaAt(IDEF_1, SchemaProvider.fromSchemaId(1));
    }

    @Test
    void schema2At() throws URISyntaxException {
        schemaAt(IDEF_2, SchemaProvider.fromSchemaId(2));
    }

    private Schema schema_0() {
        return new Schema(List.of(
                NestedField.required(fooId, "Foo", StructType.of(
                        NestedField.optional(fooField1Id, "Field1", Types.IntegerType.get()),
                        NestedField.optional(fooField2Id, "Field2", Types.IntegerType.get()))),
                NestedField.optional(barId, "Bar", StructType.of(
                        NestedField.required(barField1Id, "Field1", Types.IntegerType.get()),
                        NestedField.required(barField2Id, "Field2", Types.IntegerType.get())))));
    }

    private Schema schema_1() {
        return new Schema(List.of(
                NestedField.required(fooId, "Foo_B", StructType.of(
                        NestedField.optional(fooField1Id, "Field1", Types.IntegerType.get()),
                        NestedField.optional(fooField2Id, "Field2", Types.IntegerType.get()))),
                NestedField.optional(barId, "Bar", StructType.of(
                        NestedField.required(barField1Id, "Field1_B", Types.IntegerType.get()),
                        NestedField.required(barField2Id, "Field2_B", Types.IntegerType.get())))));
    }

    private Schema schema_2() {
        return new Schema(List.of(
                NestedField.required(fooId, "Foo_C", StructType.of(
                        NestedField.optional(fooField1Id, "Field1_C", Types.IntegerType.get()),
                        NestedField.optional(fooField2Id, "Field2_C", Types.IntegerType.get()))),
                NestedField.optional(barId, "Bar", StructType.of(
                        NestedField.required(barField1Id, "Field1_B", Types.IntegerType.get()),
                        NestedField.required(barField2Id, "Field2_B", Types.IntegerType.get())))));
    }

    static void schemaAt(TableDefinition def, SchemaProvider schema) throws URISyntaxException {
        final IcebergTableAdapter ta = loadWithSchema(schema);
        final List<Snapshot> snapshots = ta.listSnapshots();
        TstUtils.assertTableEquals(expected(def, 5), ta.table(si(snapshots, 0)));
        TstUtils.assertTableEquals(expected(def, 10), ta.table(si(snapshots, 1)));
        TstUtils.assertTableEquals(expected(def, 15), ta.table(si(snapshots, 2)));
        TstUtils.assertTableEquals(expected(def, 15), ta.table());
    }

    private static IcebergReadInstructions si(List<Snapshot> snapshots, int index) {
        return IcebergReadInstructions.builder().snapshot(snapshots.get(index)).build();
    }

    static InferenceInstructions ia(Schema schema) {
        return InferenceInstructions.builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                .failOnUnsupportedTypes(true)
                .build();
    }

    private static Table expected(TableDefinition td, int size) {
        return TableTools.newTable(td,
                TableTools.intCol(td.getColumnNames().get(0), data(size, false)),
                TableTools.intCol(td.getColumnNames().get(1), data(size, true)),
                TableTools.intCol(td.getColumnNames().get(2), data(size, false)),
                TableTools.intCol(td.getColumnNames().get(3), data(size, true)));
    }

    private static int[] data(int size, boolean neg) {
        final int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = neg ? -i : i;
        }
        return data;
    }
}
