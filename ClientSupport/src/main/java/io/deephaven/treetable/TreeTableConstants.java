/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.engine.table.Table;

public class TreeTableConstants {
    // This is the object used to identify the root table in fetch operations
    public static final String ROOT_TABLE_KEY = "__ROOT_KEY__";
    public static final String RE_TREE_KEY = "__RE_TREED__";


    // This is the key into the sources map of the synthetic column that identifies which tables own what rows.
    public static final String TABLE_KEY_COLUMN = "__TABLE_KEY__";
    public static final String CHILD_PRESENCE_COLUMN = "__CHILD_PRESENCE__";

    public static boolean isTreeTable(Table t) {
        return t.hasAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE);
    }
}
