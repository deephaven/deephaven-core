//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import io.deephaven.web.client.api.event.Event;

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

    private final TableSourceBuilder tickingTables = new TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("source = time_table('PT0.1s').update(['MyKey=``+i%5', 'x=i'])")
            .script("partitioned_source = source.partition_by(by=['MyKey'])")
            .script("partitioned_result = partitioned_source.transform(func=lambda t: t.drop_columns('MyKey'))");


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

                    return partitionedTable.getBaseTable().then(keyTable -> {
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

                    return partitionedTable.getBaseTable().then(keyTable -> {
                        assertEquals(5d, keyTable.getSize());

                        return partitionedTable.getTable("2");
                    }).then(constituentTable -> {
                        assertEquals(20d, constituentTable.getSize());
                        constituentTable.close();
                        partitionedTable.close();
                        return null;
                    });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testConstituentResult() {
        connect(tables)
                .then(table("constituent_result"))
                .then(table -> {
                    delayTestFinish(1500);

                    JsArray<Column> columns = table.getColumns();
                    assertEquals(1, columns.length);
                    assertEquals("x", columns.getAt(0).getName());

                    table.close();
                    return null;
                })
                .then(this::finish).catch_(this::report);
    }

    public void testTickingPartitionedTable() {
        connect(tickingTables)
                .then(partitionedTable("partitioned_source"))
                .then(partitionedTable -> {
                    delayTestFinish(20_000);
                    Column[] keyColumns = partitionedTable.getKeyColumns();
                    assertEquals(1, keyColumns.length);
                    assertEquals("MyKey", keyColumns[0].getName());

                    Column[] columns = partitionedTable.getColumns();
                    assertEquals(3, columns.length);
                    assertEquals("MyKey", columns[1].getName());
                    assertEquals("x", columns[2].getName());

                    return waitForEventWhere(partitionedTable, JsPartitionedTable.EVENT_KEYADDED,
                            (Event<JsArray<Object>> e) -> e.getDetail().getAt(0).equals("2"), 14004)
                            .then(event -> partitionedTable.getTable("2")).then(constituentTable -> {
                                assertEquals(3, constituentTable.getColumns().length);
                                assertTrue(constituentTable.getSize() >= 2);

                                constituentTable.close();
                                partitionedTable.close();

                                return null;
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testTickingTransformedPartitionedTable() {
        connect(tickingTables)
                .then(partitionedTable("partitioned_result"))
                .then(partitionedTable -> {
                    delayTestFinish(20_000);
                    Column[] keyColumns = partitionedTable.getKeyColumns();
                    assertEquals(1, keyColumns.length);
                    assertEquals("MyKey", keyColumns[0].getName());

                    Column[] columns = partitionedTable.getColumns();
                    assertEquals(2, columns.length);
                    assertEquals("Timestamp", columns[0].getName());
                    assertEquals("x", columns[1].getName());

                    return waitForEventWhere(partitionedTable, JsPartitionedTable.EVENT_KEYADDED,
                            (Event<JsArray<Object>> e) -> e.getDetail().getAt(0).equals("2"), 14005)
                            .then(event -> partitionedTable.getTable("2")).then(constituentTable -> {
                                assertEquals(2, constituentTable.getColumns().length);
                                assertTrue(constituentTable.getSize() >= 1);

                                constituentTable.close();
                                partitionedTable.close();

                                return null;
                            });
                })
                .then(this::finish).catch_(this::report);
    }
}
