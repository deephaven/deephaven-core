package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TableAttributesDefinition implements Serializable {
    public static final String INPUT_TABLE_ATTRIBUTE = "InputTable",
        TOTALS_TABLE_ATTRIBUTE = "TotalsTable",
        TABLE_DESCRIPTION_ATTRIBUTE = "TableDescription",
        COLUMN_DESCRIPTIONS_ATTRIBUTE = "ColumnDescriptions",
        HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE = "HierarchicalSourceTable",
        HIERARCHICAL_SOURCE_INFO_ATTRIBUTE = "HierarchicalSourceTableInfo",
        PLUGIN_NAME = "PluginName";

    // special cased attributes that have a complex type yet are always sent
    private RollupDefinition rollupDefinition;// rollup subtype of "HierarchicalSourceTableInfo"
    private String treeHierarchicalColumnName;// technically a part of
                                              // "HierarchicalSourceTableInfo", won't be copied
                                              // separately
    private String[][] columnDescriptions;// "ColumnDescriptions"

    // enumerate the plain keys/values
    // this includes description, plugin name, totals table config
    private String[] keys;
    private String[] values;

    // list the unhandled keys
    private String[] remainingKeys;

    private transient Map<String, String> map;

    public boolean isInputTable() {
        return Arrays.stream(remainingKeys).anyMatch(key -> key.equals(INPUT_TABLE_ATTRIBUTE));
    }

    public RollupDefinition getRollupDefinition() {
        return rollupDefinition;
    }

    public void setRollupDefinition(final RollupDefinition rollupDefinition) {
        this.rollupDefinition = rollupDefinition;
    }

    public String getTreeHierarchicalColumnName() {
        return treeHierarchicalColumnName;
    }

    public void setTreeHierarchicalColumnName(final String treeHierarchicalColumnName) {
        this.treeHierarchicalColumnName = treeHierarchicalColumnName;
    }

    public String[][] getColumnDescriptions() {
        return columnDescriptions;
    }

    public void setColumnDescriptions(final String[][] columnDescriptions) {
        this.columnDescriptions = columnDescriptions;
    }

    public String[] getKeys() {
        return keys;
    }

    public void setKeys(final String[] keys) {
        this.keys = keys;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(final String[] values) {
        this.values = values;
    }

    public String[] getRemainingKeys() {
        return remainingKeys;
    }

    public void setRemainingKeys(final String[] remainingKeys) {
        this.remainingKeys = remainingKeys;
    }

    // helpers for attributes that used to be on the table def
    public Map<String, String> getAsMap() {
        if (map == null) {
            map = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                map.put(keys[i], values[i]);
            }
        }
        return map;
    }

    public String getTotalsTableConfig() {
        return getAsMap().get(TOTALS_TABLE_ATTRIBUTE);
    }

    public String getDescription() {
        return getAsMap().get(TABLE_DESCRIPTION_ATTRIBUTE);
    }

    public String getPluginName() {
        return getAsMap().get(PLUGIN_NAME);
    }
}
