package io.deephaven.web.client.api.tree;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Configuration object for running Table.treeTable to produce a hierarchical view of a given "flat"
 * table.
 */
@JsType(name = "TreeTableConfig", namespace = "dh")
public class JsTreeTableConfig {
    public String idColumn = null;
    public String parentColumn = null;
    public boolean promoteOrphansToRoot = false;

    public JsTreeTableConfig() {}

    @JsIgnore
    public JsTreeTableConfig(JsPropertyMap<Object> source) {
        this();
        if (source.has("idColumn")) {
            idColumn = source.getAny("idColumn").asString();
        }
        if (source.has("parentColumn")) {
            parentColumn = source.getAny("parentColumn").asString();
        }
        if (source.has("promoteOrphansToRoot")) {
            promoteOrphansToRoot = Js.isTruthy(source.getAny("promoteOrphansToRoot"));
        }
    }
}
