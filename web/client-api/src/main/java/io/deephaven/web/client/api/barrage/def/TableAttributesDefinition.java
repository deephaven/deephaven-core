package io.deephaven.web.client.api.barrage.def;

import io.deephaven.web.shared.data.RollupDefinition;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class TableAttributesDefinition {
    public static final String INPUT_TABLE_ATTRIBUTE = "InputTable",
            TOTALS_TABLE_ATTRIBUTE = "TotalsTable",
            TABLE_DESCRIPTION_ATTRIBUTE = "TableDescription",
            HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE = "HierarchicalSourceTable",
            HIERARCHICAL_SOURCE_INFO_ATTRIBUTE = "HierarchicalSourceTableInfo",
            LAYOUT_HINTS_ATTRIBUTE = "LayoutHints",
            PLUGIN_NAME = "PluginName";

    private static final String HIERARCHICAL_COLUMN_NAME =
            HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".hierarchicalColumnName";
    private static final String HIERARCHICAL_BY_COLUMN = HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".byColumns";
    private static final String HIERARCHICAL_LEAF_TYPE = HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".leafType";

    // special cased attributes that have a complex type yet are always sent
    private RollupDefinition rollupDefinition;// rollup subtype of "HierarchicalSourceTableInfo"

    private final Map<String, String> map;
    private final Set<String> remainingAttributeKeys;

    public TableAttributesDefinition(Map<String, String> keys, Set<String> remainingAttributes) {
        map = keys;
        this.remainingAttributeKeys = remainingAttributes;
        if (map.containsKey(HIERARCHICAL_COLUMN_NAME)) {
            // marker present for tree table metadata
            rollupDefinition = new RollupDefinition();
            rollupDefinition.setByColumns(map.get(HIERARCHICAL_BY_COLUMN).split(","));
            rollupDefinition.setLeafType(RollupDefinition.LeafType.valueOf(map.get(HIERARCHICAL_LEAF_TYPE)));
        }
    }

    public boolean isInputTable() {
        return remainingAttributeKeys.contains(INPUT_TABLE_ATTRIBUTE);
    }

    public RollupDefinition getRollupDefinition() {
        return rollupDefinition;
    }

    public void setRollupDefinition(final RollupDefinition rollupDefinition) {
        this.rollupDefinition = rollupDefinition;
    }

    public String getTreeHierarchicalColumnName() {
        return map.get(HIERARCHICAL_COLUMN_NAME);
    }

    public String[] getKeys() {
        return map.keySet().toArray(new String[0]);
    }

    public String getValue(String key) {
        return map.get(key);
    }

    public Set<String> getRemainingAttributeKeys() {
        return remainingAttributeKeys;
    }

    public String getTotalsTableConfig() {
        return map.get(TOTALS_TABLE_ATTRIBUTE);
    }

    public String getDescription() {
        return map.get(TABLE_DESCRIPTION_ATTRIBUTE);
    }

    public String getPluginName() {
        return map.get(PLUGIN_NAME);
    }

    public String getLayoutHints() {
        return map.get(LAYOUT_HINTS_ATTRIBUTE);
    }
}
