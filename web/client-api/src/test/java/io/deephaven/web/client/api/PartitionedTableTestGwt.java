//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

public class PartitionedTableTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("source = empty_table(100).update(['MyKey=``+i%5', 'x=i'])")
            .script("partitioned_source = source.partition_by(by=['MyKey'])")
            .script("partitioned_result = partitioned_source.transform(func=lambda t: t.drop_columns('MyKey'))")
            .script("constituent_result = partitioned_result.get_constituent(['0'])");


    public void testPartitionedTable() {
        connect(tables)
                .then(partitionedTable("partitioned_source"))
                .then(partitionedTable -> {
                    delayTestFinish(1500);
                    Column[] keyColumns = partitionedTable.getKeyColumns();
                    assertEquals(1, keyColumns.length);
                    assertEquals("MyKey", keyColumns[0].getName());

                    Column[] columns = partitionedTable.getColumns();
                    assertEquals(2, columns.length);
                    assertEquals("MyKey", columns[0].getName());
                    assertEquals("x", columns[1].getName());

                    return partitionedTable.getKeyTable().then(keyTable -> {
                        System.out.println("KeyTable size: " + keyTable.getSize());
                        assertEquals(5d, keyTable.getSize());

                        return partitionedTable.getTable("2");
                    }).then(constituentTable -> {
                        assertEquals(20d, constituentTable.getSize());
                        partitionedTable.close();

                        return null;
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testTransformedPartitionedTable() {
        connect(tables)
                .then(partitionedTable("partitioned_result"))
                .then(partitionedTable -> {
                    delayTestFinish(1500);
                    Column[] keyColumns = partitionedTable.getKeyColumns();
                    assertEquals(1, keyColumns.length);
                    assertEquals("MyKey", keyColumns[0].getName());

                    Column[] columns = partitionedTable.getColumns();
                    assertEquals(1, columns.length);
                    assertEquals("x", columns[0].getName());

                    return partitionedTable.getKeyTable().then(keyTable -> {
                        assertEquals(5d, keyTable.getSize());

                        return partitionedTable.getTable("2");
                    }).then(constituentTable -> {
                        assertEquals(20d, constituentTable.getSize());
                        partitionedTable.close();

                        return null;
                    });
                })
                .then(this::finish).catch_(this::report);
    }
}
