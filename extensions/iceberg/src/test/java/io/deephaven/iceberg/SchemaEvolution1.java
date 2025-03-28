//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.sqlite.DbResource;
import io.deephaven.iceberg.util.ColumnInstructions;
import io.deephaven.iceberg.util.InferenceInstructions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.iceberg.util.FieldPath;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.util.QueryConstants;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
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

    private IcebergTableAdapter tableAdapter;
    private int fieldId1;
    private int fieldId2;
    private int fieldId3;

    @BeforeEach
    void setUp() throws URISyntaxException {
        tableAdapter = DbResource.openCatalog(CATALOG_NAME).loadTable(TABLE_ID);
        {
            final Schema initialSchema = tableAdapter.currentSchema();
            fieldId1 = initialSchema.findField(IDEF_4.getColumnNames().get(0)).fieldId();
            fieldId2 = initialSchema.findField(IDEF_4.getColumnNames().get(1)).fieldId();
            fieldId3 = initialSchema.findField(IDEF_4.getColumnNames().get(2)).fieldId();
        }
    }

    @Test
    void schemas() {
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
    void snapshots() {
        // This is a meta test, making sure test setup is correct
        assertThat(tableAdapter.listSnapshots()).hasSize(6);
    }

    @Test
    void inference() throws Inference.Exception {
        // This is a meta test, making sure test setup is correct
        assertThat(Resolver.infer(ia(schema_0())).definition()).isEqualTo(IDEF_0);
        assertThat(Resolver.infer(ia(schema_1())).definition()).isEqualTo(IDEF_1);
        assertThat(Resolver.infer(ia(schema_2())).definition()).isEqualTo(IDEF_2);
        assertThat(Resolver.infer(ia(schema_3())).definition()).isEqualTo(IDEF_3);
        assertThat(Resolver.infer(ia(schema_4())).definition()).isEqualTo(IDEF_4);
    }

    @Test
    void readLatest() {
        final Table expected = expected3(IDEF_4, 60);
        assertThat(tableAdapter.definition()).isEqualTo(IDEF_4);
        TstUtils.assertTableEquals(expected, tableAdapter.table());
    }

    @Test
    void readLatestAs() throws Inference.Exception {
        read(expected(IDEF_0, 60, false), readLatestAs(0));
        read(expected(IDEF_1, 60, false), readLatestAs(1));
        read(expected(IDEF_2, 60, false), readLatestAs(2));
        read(expected(IDEF_3, 60, true), readLatestAs(3));
        read(expected3(IDEF_4, 60), readLatestAs(4));
    }

    @Test
    void readSnapshot5As() throws Inference.Exception {
        read(expected(IDEF_0, 60, false), readSnapshotAs(5, 0));
        read(expected(IDEF_1, 60, false), readSnapshotAs(5, 1));
        read(expected(IDEF_2, 60, false), readSnapshotAs(5, 2));
        read(expected(IDEF_3, 60, true), readSnapshotAs(5, 3));
        read(expected3(IDEF_4, 60), readSnapshotAs(5, 4));
    }

    @Test
    void readSnapshot4As() throws Inference.Exception {
        read(expected(IDEF_0, 50, false), readSnapshotAs(4, 0));
        read(expected(IDEF_1, 50, false), readSnapshotAs(4, 1));
        read(expected(IDEF_2, 50, false), readSnapshotAs(4, 2));
        read(expected(IDEF_3, 50, true), readSnapshotAs(4, 3));
        read(expected3(IDEF_4, 50), readSnapshotAs(4, 4));

    }

    @Test
    void readSnapshot3As() throws Inference.Exception {
        read(expected(IDEF_0, 40, false), readSnapshotAs(3, 0));
        read(expected(IDEF_1, 40, false), readSnapshotAs(3, 1));
        read(expected(IDEF_2, 40, false), readSnapshotAs(3, 2));
        read(expected(IDEF_3, 40, true), readSnapshotAs(3, 3));
        read(expected3(IDEF_4, 40), readSnapshotAs(3, 4));
    }

    @Test
    void readSnapshot2As() throws Inference.Exception {
        read(expected(IDEF_0, 30, false), readSnapshotAs(2, 0));
        read(expected(IDEF_1, 30, false), readSnapshotAs(2, 1));
        read(expected(IDEF_2, 30, false), readSnapshotAs(2, 2));
        read(expected(IDEF_3, 30, true), readSnapshotAs(2, 3));
        read(expected3(IDEF_4, 30), readSnapshotAs(2, 4));
    }

    @Test
    void readSnapshot1As() throws Inference.Exception {
        read(expected(IDEF_0, 20, false), readSnapshotAs(1, 0));
        read(expected(IDEF_1, 20, false), readSnapshotAs(1, 1));
        read(expected(IDEF_2, 20, false), readSnapshotAs(1, 2));
        read(expected(IDEF_3, 20, true), readSnapshotAs(1, 3));
        read(expected3(IDEF_4, 20), readSnapshotAs(1, 4));
    }

    @Test
    void readSnapshot0As() throws Inference.Exception {
        read(expected(IDEF_0, 10, false), readSnapshotAs(0, 0));
        read(expected(IDEF_1, 10, false), readSnapshotAs(0, 1));
        read(expected(IDEF_2, 10, false), readSnapshotAs(0, 2));
        read(expected(IDEF_3, 10, true), readSnapshotAs(0, 3));
        read(expected3(IDEF_4, 10), readSnapshotAs(0, 4));
    }

    @Test
    void customDefinitions() {
        // subset, just id1
        {
            final String col1 = "Foo";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col1));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col1, data(60, false)));
            read(expected, Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col1, schemaField(fieldId1))
                    .build());
        }
        // subset, just id2
        {
            final String col2 = "Bar";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col2));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col2, data(60, true)));
            read(expected, Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col2, schemaField(fieldId2))
                    .build());
        }
        // subset, just id3 (ideally, )
        {
            final String col3 = "Baz";
            final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(col3));
            final Table expected = TableTools.newTable(
                    td,
                    TableTools.intCol(col3, nullData(60)));
            read(expected, Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .allowUnmappedColumns(true)
                    // .putColumnInstructions(col3, FieldPath.of(fieldId3))
                    .build());
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
            read(expected, Resolver.builder()
                    .schema(schema_4())
                    .definition(td)
                    .putColumnInstructions(col1, schemaField(fieldId1))
                    .putColumnInstructions(col2, schemaField(fieldId2))
                    .putColumnInstructions(col3, schemaField(fieldId3))
                    .allowUnmappedColumns(true)
                    .build());
        }
    }

    private void read(Table expected, Resolver di) {
        read(expected, IcebergReadInstructions.builder().resolver(di).build());
    }

    private void read(Table expected, IcebergReadInstructions instructions) {
        assertThat(tableAdapter.definition(instructions)).isEqualTo(expected.getDefinition());
        TstUtils.assertTableEquals(expected, tableAdapter.table(instructions));
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

    private IcebergReadInstructions readLatestAs(int schemaVersion) throws Inference.Exception {
        return IcebergReadInstructions.builder()
                .resolver(Resolver.infer(ia(schema(schemaVersion))))
                .build();
    }

    private IcebergReadInstructions readSnapshotAs(int snapshotIx, int schemaVersion) throws Inference.Exception {
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
            case 3:
                return schema_3();
            case 4:
                return schema_4();
            default:
                throw new IllegalStateException();
        }
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

    static InferenceInstructions i(Schema schema) {
        return InferenceInstructions.builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                // .nameMapping(NameMapping.empty())
                .build();
    }

    static InferenceInstructions ia(Schema schema) {
        return InferenceInstructions.builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                // .nameMapping(NameMapping.empty())
                .failOnUnsupportedTypes(true)
                .build();
    }
}
