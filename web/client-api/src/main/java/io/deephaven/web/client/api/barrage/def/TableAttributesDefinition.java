/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.barrage.def;

import java.util.Map;
import java.util.Set;

public class TableAttributesDefinition {
    public static final String INPUT_TABLE_ATTRIBUTE = "InputTable",
            TOTALS_TABLE_ATTRIBUTE = "TotalsTable",
            TABLE_DESCRIPTION_ATTRIBUTE = "TableDescription",
            LAYOUT_HINTS_ATTRIBUTE = "LayoutHints",
            STREAM_TABLE_ATTRIBUTE = "StreamTable",
            PLUGIN_NAME = "PluginName";

    private final Map<String, String> map;
    private final Map<String, String> typeMap;
    private final Set<String> remainingAttributeKeys;

    public TableAttributesDefinition(
            Map<String, String> keys, Map<String, String> keyTypes, Set<String> remainingAttributes) {
        map = keys;
        typeMap = keyTypes;
        this.remainingAttributeKeys = remainingAttributes;
    }

    public boolean isInputTable() {
        return remainingAttributeKeys.contains(INPUT_TABLE_ATTRIBUTE);
    }

    public boolean isStreamTable() {
        return "true".equals(map.get(STREAM_TABLE_ATTRIBUTE));
    }

    public String[] getKeys() {
        return map.keySet().toArray(new String[0]);
    }

    public String getValue(String key) {
        return map.get(key);
    }

    public String getValueType(String key) {
        return typeMap.getOrDefault(key, "java.lang.String");
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
