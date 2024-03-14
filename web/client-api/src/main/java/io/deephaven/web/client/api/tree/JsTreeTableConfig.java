//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.tree;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Configuration object for running Table.treeTable to produce a hierarchical view of a given "flat" table.
 *
 * Like TotalsTableConfig, `TreeTableConfig` supports an operation map indicating how to aggregate the data, as well as
 * an array of column names which will be the layers in the roll-up tree, grouped at each level. An additional optional
 * value can be provided describing the strategy the engine should use when grouping the rows.
 */
@JsType(name = "TreeTableConfig", namespace = "dh")
public class JsTreeTableConfig {
    /**
     * The column representing the unique ID for each item
     */
    public String idColumn = null;
    /**
     * The column representing the parent ID for each item
     */
    public String parentColumn = null;
    /**
     * Optional parameter indicating if items with an invalid parent ID should be promoted to root. Defaults to false.
     */
    public boolean promoteOrphansToRoot = false;

    public JsTreeTableConfig() {}

    @JsIgnore
    public JsTreeTableConfig(JsPropertyMap<Object> source) {
        this();
        if (source.has("idColumn")) {
            idColumn = source.getAsAny("idColumn").asString();
        }
        if (source.has("parentColumn")) {
            parentColumn = source.getAsAny("parentColumn").asString();
        }
        if (source.has("promoteOrphansToRoot")) {
            promoteOrphansToRoot = Js.isTruthy(source.getAsAny("promoteOrphansToRoot"));
        }
    }
}
