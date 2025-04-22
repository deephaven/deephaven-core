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
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.TypeInference;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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

    private IcebergTableAdapter tableAdapter;
    private int fooId;
    private int fooField1Id;
    private int fooField2Id;

    private int barId;
    private int barField1Id;
    private int barField2Id;

    @BeforeEach
    void setUp() throws URISyntaxException {
        tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        {
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
    void schemas() {
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
    void snapshots() {
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
    void readLatest() {
        assertThat(tableAdapter.definition()).isEqualTo(IDEF_2);
        TstUtils.assertTableEquals(expected(IDEF_2, 15), tableAdapter.table());
    }

    @Test
    void readLatestAs() throws TypeInference.Exception {
        read(expected(IDEF_0, 15), readLatestAs(0));
        read(expected(IDEF_1, 15), readLatestAs(1));
        read(expected(IDEF_2, 15), readLatestAs(2));
    }

    @Test
    void readSnapshot2As() throws TypeInference.Exception {
        read(expected(IDEF_0, 15), readSnapshotAs(2, 0));
        read(expected(IDEF_1, 15), readSnapshotAs(2, 1));
        read(expected(IDEF_2, 15), readSnapshotAs(2, 2));
    }

    @Test
    void readSnapshot1As() throws TypeInference.Exception {
        read(expected(IDEF_0, 10), readSnapshotAs(1, 0));
        read(expected(IDEF_1, 10), readSnapshotAs(1, 1));
        read(expected(IDEF_2, 10), readSnapshotAs(1, 2));
    }

    @Test
    void readSnapshot0As() throws TypeInference.Exception {
        read(expected(IDEF_0, 5), readSnapshotAs(0, 0));
        read(expected(IDEF_1, 5), readSnapshotAs(0, 1));
        read(expected(IDEF_2, 5), readSnapshotAs(0, 2));
    }

    private IcebergReadInstructions readLatestAs(int schemaVersion) throws TypeInference.Exception {
        return IcebergReadInstructions.builder()
                .resolver(Resolver.infer(ia(schema(schemaVersion))))
                .build();
    }

    private IcebergReadInstructions readSnapshotAs(int snapshotIx, int schemaVersion) throws TypeInference.Exception {
        return IcebergReadInstructions.builder()
                .snapshot(tableAdapter.listSnapshots().get(snapshotIx))
                .resolver(Resolver.infer(ia(schema(schemaVersion))))
                .build();
    }

    private Schema schema(int version) {
        switch (version) {
            case 0:
                return schema_0();
            case 1:
                return schema_1();
            case 2:
                return schema_2();
            default:
                throw new IllegalStateException();
        }
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

    private void read(Table expected, IcebergReadInstructions instructions) {
        assertThat(tableAdapter.definition(instructions)).isEqualTo(expected.getDefinition());
        TstUtils.assertTableEquals(expected, tableAdapter.table(instructions));
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
