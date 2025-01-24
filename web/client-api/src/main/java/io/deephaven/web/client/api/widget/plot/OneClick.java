//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsMap;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.OneClickDescriptor;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsPartitionedTable;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;

@TsInterface
@TsName(namespace = "dh.plot")
public class OneClick {
    private final JsFigure jsFigure;
    private final OneClickDescriptor oneClick;
    private final JsSeries jsSeries;

    private final JsMap<String, Any> values = new JsMap<>();

    private JsPartitionedTable partitionedTable;

    private Object[] currentKeys;
    private RemoverFn keyAddedListener;
    private JsTable currentTable;

    public OneClick(JsFigure jsFigure, OneClickDescriptor oneClick, JsSeries jsSeries) {
        this.jsFigure = jsFigure;
        this.oneClick = oneClick;
        this.jsSeries = jsSeries;
    }

    public void setPartitionedTable(JsPartitionedTable partitionedTable) {
        if (keyAddedListener != null) {
            keyAddedListener.remove();
        }
        this.partitionedTable = partitionedTable;
        keyAddedListener = partitionedTable.addEventListener(JsPartitionedTable.EVENT_KEYADDED, e -> {
            if (currentKeys != null) {
                // Fetch the table will only do something if the keys have actually changed
                fetchTable();
            }
        });

        if (allRequiredValuesSet()) {
            fetchTable();
        }
    }

    @JsProperty
    public Column[] getColumns() {
        JsPropertyMap<Object>[] fakeColumns = new JsPropertyMap[oneClick.getColumnsList().length];
        for (int i = 0; i < fakeColumns.length; i++) {
            fakeColumns[i] = JsPropertyMap.of("name", oneClick.getColumnsList().getAt(i), "type",
                    oneClick.getColumnTypesList().getAt(i));
        }
        return Js.uncheckedCast(fakeColumns);
    }

    // @JsMethod
    // public Promise<JsArray<Any>> getValuesForColumn(String columnName) {
    // // selectDistinct and snapshot the column into an array
    // return null;
    // }

    @JsMethod
    public void setValueForColumn(String columnName, Any value) {
        boolean allWereSet = allRequiredValuesSet();
        // assign the value
        if (value != null) {
            values.set(columnName, value);
        } else {
            values.delete(columnName);
        }
        if (allRequiredValuesSet()) {
            fetchTable();
        } else if (allWereSet) {
            // if we now don't have all values set(since we removed one),
            // trigger subscription check
            if (currentTable != null) {
                currentTable.close();
                currentTable = null;
            }
            jsFigure.enqueueSubscriptionCheck();
        }
    }

    /**
     * Get the array of keys to fetch. Note that each key may be a String OR a String[]
     */
    private Object[] getCurrentKeys() {
        if (values.size == 0) {
            return null;
        }

        if (oneClick.getColumnsList().length == 1) {
            Object key = values.get(oneClick.getColumnsList().getAt(0));
            if (key != null) {
                return new Object[] {key};
            } else {
                return null;
            }
        }

        String[] key = new String[oneClick.getColumnsList().length];
        for (int i = 0; i < oneClick.getColumnsList().length; i++) {
            Any value = values.get(oneClick.getColumnsList().getAt(i));
            if (value != null) {
                key[i] = value.asString();
            }
        }

        if (allValuesSet()) {
            return new Object[] {key};
        }

        // Some of the values aren't set, need to iterate through all the table map keys and select the ones that match
        return JsArray.from(partitionedTable.getKeys()).filter((tableKey, index) -> {
            if (!(tableKey instanceof String[])) {
                return false;
            }

            String[] strKey = (String[]) tableKey;
            if (strKey.length != key.length) {
                return false;
            }
            for (int i = 0; i < strKey.length; ++i) {
                if (key[i] != null && !key[i].equals(strKey[i])) {
                    return false;
                }
            }

            return true;
        }).asArray(new String[0][0]);
    }

    private Promise<JsTable> doFetchTable(Object[] keys) {
        if (keys == null || keys.length == 0) {
            return partitionedTable.getMergedTable();
        } else if (keys.length == 1) {
            return partitionedTable.getTable(keys[0]);
        } else {
            Promise<JsTable>[] promises =
                    Arrays.stream(keys).map(key -> partitionedTable.getTable(key)).toArray(Promise[]::new);
            return Promise.all(promises)
                    .then(resolved -> {
                        JsTable[] tables =
                                Arrays.stream(resolved).filter(table -> table != null).toArray(JsTable[]::new);
                        if (tables.length > 1) {
                            return tables[0].getConnection().mergeTables(tables, partitionedTable);
                        } else if (tables.length == 1) {
                            return Promise.resolve(tables[0]);
                        } else {
                            // No keys matched, just hand back a null table
                            return Promise.resolve((JsTable) null);
                        }
                    });
        }
    }

    private void fetchTable() {
        Object keys[] = getCurrentKeys();
        if (allKeysMatch(keys, currentKeys) && currentTable != null) {
            return;
        }
        currentKeys = keys;

        doFetchTable(keys).then(table -> {
            if (currentKeys != keys) {
                // A newer request is running instead, throw away this result
                table.close();
            } else {
                if (currentTable != null) {
                    // Get rid of the current table
                    currentTable.close();
                    currentTable = null;
                }

                if (table == null) {
                    // No table, no need to change the figure subscription, just trigger a
                    // synthetic event indicating no items
                    jsFigure.fireEvent(JsFigure.EVENT_UPDATED, DataUpdateEvent.empty(jsSeries));
                } else {
                    // Subscribe to this key and wait for it...
                    currentTable = table;
                }
                jsFigure.enqueueSubscriptionCheck();
            }
            return null;
        });
    }

    private static boolean keyMatches(Object key1, Object key2) {
        if (key1 == null && key2 == null) {
            return true;
        } else if (key1 == null || key2 == null) {
            return false;
        }
        if (key1 instanceof String) {
            return key1.equals(key2);
        }
        assert key1 instanceof String[];
        if (key2 instanceof String[]) {
            return Arrays.equals((String[]) key1, (String[]) key2);
        }
        return false;// key2 isn't String[], so fail
    }

    private static boolean anyKeyMatches(Object[] keys, Object key) {
        if (keys == null || key == null) {
            return false;
        }

        for (int i = 0; i < keys.length; ++i) {
            if (keyMatches(keys[i], key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allKeysMatch(Object[] keys1, Object[] keys2) {
        if (keys1 == null && keys2 == null) {
            return true;
        } else if (keys1 == null || keys2 == null || keys1.length != keys2.length) {
            return false;
        }
        for (int i = 0; i < keys1.length; ++i) {
            if (!keyMatches(keys1[i], keys2[i])) {
                return false;
            }
        }
        return true;
    }

    public JsTable getTable() {
        return currentTable;
    }

    @JsMethod
    public Any getValueForColumn(String columName) {
        return values.get(columName);
    }

    public boolean allValuesSet() {
        return values.size == oneClick.getColumnsList().length;
    }

    public boolean allRequiredValuesSet() {
        return !isRequireAllFiltersToDisplay() || allValuesSet();
    }

    @JsProperty
    public boolean isRequireAllFiltersToDisplay() {
        return oneClick.getRequireAllFiltersToDisplay();
    }

    public OneClickDescriptor getDescriptor() {
        return this.oneClick;
    }
}
