//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.ide.IdeConnection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionedTableTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, merge")
            .script("from deephaven.table import PartitionedTable")
            .script("source = empty_table(100).update(['MyKey=``+i%5', 'x=i'])")
            .script("partitioned_source = source.partition_by(by=['MyKey'])")
            .script("partitioned_result = partitioned_source.transform(func=lambda t: t.drop_columns('MyKey'))")
            .script("constituent_result = partitioned_result.get_constituent(['0'])")
            .script("doubled_partitioned_result = (PartitionedTable.from_partitioned_table(merge([partitioned_result.table, partitioned_result.table]),"
                    +
                    "key_cols=partitioned_result.key_columns," +
                    "unique_keys=False," +
                    "constituent_column=partitioned_result.constituent_column," +
                    "constituent_table_columns=partitioned_result.constituent_table_columns," +
                    "constituent_changes_permitted=False))");

    private final TableSourceBuilder tickingTables = new TableSourceBuilder()
            .script("from deephaven import time_table")
            .script("source = time_table('PT0.1s').update(['MyKey=``+i%5', 'x=i'])")
            .script("partitioned_source = source.partition_by(by=['MyKey'])")
            .script("partitioned_result = partitioned_source.transform(func=lambda t: t.drop_columns('MyKey'))")
            // The `hot` key has two rows, one replaced every half second, and a new key appears every 1.5 seconds,
            // so both the key subscription and a constituent viewport keep updating
            .script("from datetime import datetime, timedelta")
            .script("growing_keys = time_table('PT0.25s', start_time=datetime.now() - timedelta(seconds=10))"
                    + ".update(['MyKey = i % 2 == 0 ? `hot` : ``+(int)(i/6)', 'Slot = i % 4', 'x = i'])"
                    + ".last_by(by=['MyKey', 'Slot']).partition_by(by=['MyKey'])")
            // The same shape keyed by a date, which the client holds as a wrapper object rather than a string
            .script("from deephaven.query_library import import_class\nimport_class('java.time.LocalDate')")
            .script("growing_date_keys = time_table('PT0.25s', start_time=datetime.now() - timedelta(seconds=10))"
                    + ".update(['MyKey = i % 2 == 0 ? LocalDate.ofEpochDay(0) : LocalDate.ofEpochDay((int)(i/6) + 1)',"
                    + " 'Slot = i % 4', 'x = i'])"
                    + ".last_by(by=['MyKey', 'Slot']).partition_by(by=['MyKey'])");

    private static Promise<Object> delay(int millis) {
        return new Promise<>((resolve, reject) -> DomGlobal.setTimeout(p -> resolve.onInvoke((Object) null), millis));
    }

    /**
     * When the server ends the key subscription's stream with an error, the partitioned table reports requestfailed.
     */
    public void testPartitionedTableReportsKeyStreamFailure() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tickingTables, options)
                .then(session -> {
                    delayTestFinish(20_047);
                    return Promise.resolve(session);
                })
                .then(partitionedTable("growing_keys"))
                .then(partitionedTable -> {
                    List<String> failures = new ArrayList<>();
                    partitionedTable.addEventListener(JsTable.EVENT_REQUEST_FAILED,
                            e -> failures.add(String.valueOf(e.getDetail())));
                    // The server closes the key stream with a failure that is not a transport error
                    transport.failStreams("DoExchange", 9, "key stream failed by test");
                    return waitFor(() -> !failures.isEmpty(), 100, 5048, partitionedTable)
                            .then(t -> {
                                assertEquals(List.of("key stream failed by test"), failures);
                                partitionedTable.close();
                                return Promise.resolve(partitionedTable);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    /**
     * Keys that arrive as wrapper objects, here dates, are recognized after a reconnect as well: no known key is
     * announced again, and a key created during the outage is.
     */
    public void testDateKeyedPartitionedTableResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tickingTables, options)
                .then(session -> {
                    delayTestFinish(25_072);
                    return Promise.resolve(session);
                })
                .then(partitionedTable("growing_date_keys"))
                .then(partitionedTable -> {
                    IdeConnection ide = lastIdeConnection;
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    List<String> keysAdded = new ArrayList<>();
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_KEYADDED,
                            (Event<JsArray<Object>> e) -> keysAdded.add(String.valueOf(e.getDetail().getAt(0))));
                    Set<String> keysAtCut = new HashSet<>();
                    return waitForEvent(partitionedTable, JsPartitionedTable.EVENT_KEYADDED, e -> {
                    }, 5073)
                            .then(ignore -> {
                                for (Object key : JsItr.iterate(partitionedTable.getKeys().values())) {
                                    keysAtCut.add(String.valueOf(key));
                                }
                                int keyEventsBeforeCut = keysAdded.size();
                                int sizeAtCut = partitionedTable.size();
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5074).onInvoke(ide)
                                        .then(reconnected -> Promise.race(
                                                waitForEventWhere(partitionedTable, JsPartitionedTable.EVENT_KEYADDED,
                                                        (Event<JsArray<Object>> e) -> !keysAtCut.contains(
                                                                String.valueOf(e.getDetail().getAt(0))),
                                                        8075),
                                                delay(8000).then(late -> Promise.reject(
                                                        "partitioned table keys did not resume after the transport was cut"))))
                                        .then(all -> {
                                            List<String> repeated = keysAdded.subList(keyEventsBeforeCut,
                                                    keysAdded.size()).stream().filter(keysAtCut::contains)
                                                    .collect(Collectors.toList());
                                            assertTrue("keyadded fired again for keys known before the cut: "
                                                    + repeated, repeated.isEmpty());
                                            assertEquals("size counts each key once", sizeAtCut + 1,
                                                    partitionedTable.size());
                                            return null;
                                        });
                            })
                            .then(ignore -> {
                                partitionedTable.close();
                                return Promise.resolve(partitionedTable);
                            });
                })
                .then(this::finish).catch_(this::report);
    }

    public void testPartitionedTableResumesAfterTransportCut() {
        CuttingTransportFactory transport = new CuttingTransportFactory();
        ConnectOptions options = new ConnectOptions();
        options.transportFactory = transport;
        connect(tickingTables, options)
                .then(session -> {
                    delayTestFinish(25_027);
                    return Promise.resolve(session);
                })
                .then(partitionedTable("growing_keys"))
                .then(partitionedTable -> {
                    IdeConnection ide = lastIdeConnection;
                    // An unhandled disconnect event is logged as an error, which the harness treats as a failure
                    ide.addEventListener(CoreClient.EVENT_DISCONNECT, e -> {
                    });
                    List<String> keysAdded = new ArrayList<>();
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_KEYADDED,
                            (Event<JsArray<Object>> e) -> keysAdded.add(String.valueOf(e.getDetail().getAt(0))));
                    List<String> lifecycle = new ArrayList<>();
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_DISCONNECT,
                            e -> lifecycle.add("disconnect"));
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_RECONNECT,
                            e -> lifecycle.add("reconnect"));
                    partitionedTable.addEventListener(JsPartitionedTable.EVENT_RECONNECTFAILED,
                            e -> lifecycle.add("reconnectfailed"));
                    Set<String> keysAtCut = new HashSet<>();
                    JsTable[] constituent = {null};
                    return waitForEvent(partitionedTable, JsPartitionedTable.EVENT_KEYADDED, e -> {
                    }, 5028)
                            .then(ignore -> partitionedTable.getTable("hot"))
                            .then(hot -> {
                                constituent[0] = hot;
                                hot.setViewport(0, 5, null);
                                return assertUpdateReceived(hot, 2, 5029);
                            })
                            .then(hot -> {
                                for (Object key : JsItr.iterate(partitionedTable.getKeys().values())) {
                                    keysAtCut.add(String.valueOf(key));
                                }
                                int keyEventsBeforeCut = keysAdded.size();
                                // Every open stream dies, the key subscription and the constituent's viewport
                                // included; the connection sees a transport error and reconnects within the
                                // same session
                                transport.cutAll();
                                return waitForEvent(ide, CoreClient.EVENT_RECONNECT, 5030).onInvoke(ide)
                                        .then(reconnected -> Promise.all(new Promise[] {
                                                Promise.race(
                                                        waitForEventWhere(partitionedTable,
                                                                JsPartitionedTable.EVENT_KEYADDED,
                                                                (Event<JsArray<Object>> e) -> !keysAtCut.contains(
                                                                        String.valueOf(e.getDetail().getAt(0))),
                                                                8031),
                                                        delay(8000).then(late -> Promise.reject(
                                                                "partitioned table keys did not resume after the transport was cut"))),
                                                Promise.race(
                                                        assertUpdateReceived(hot, 2, 8032),
                                                        delay(8000).then(late -> Promise.reject(
                                                                "constituent viewport did not resume after the transport was cut")))
                                        }))
                                        .then(all -> {
                                            List<String> repeated = keysAdded.subList(keyEventsBeforeCut,
                                                    keysAdded.size()).stream().filter(keysAtCut::contains)
                                                    .collect(Collectors.toList());
                                            assertTrue("keyadded fired again for keys known before the cut: "
                                                    + repeated, repeated.isEmpty());
                                            assertEquals(List.of("disconnect", "reconnect"), lifecycle);
                                            return Promise.resolve(hot);
                                        });
                            })
                            .then(ignore -> {
                                constituent[0].close();
                                partitionedTable.close();
                                return Promise.resolve(partitionedTable);
                            });
                })
                .then(this::finish).catch_(this::report);
    }


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

    public void testGetNonUnique() {
        connect(tables)
                .then(partitionedTable("doubled_partitioned_result"))
                .then(partitionedTable -> {
                    delayTestFinish(1500);
                    Column[] keyColumns = partitionedTable.getKeyColumns();
                    assertEquals(1, keyColumns.length);
                    assertEquals("MyKey", keyColumns[0].getName());

                    Column[] columns = partitionedTable.getColumns();
                    assertEquals(1, columns.length);
                    assertEquals("x", columns[0].getName());

                    return partitionedTable.getBaseTable().then(keyTable -> {
                        // the keys are not uniqued
                        assertEquals(10d, keyTable.getSize());

                        return partitionedTable.getTable("2");
                    }).then(constituentTable -> {
                        // and we have both tables here
                        assertEquals(40d, constituentTable.getSize());
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
                                assertTrue(constituentTable.getSize() >= 1);

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
