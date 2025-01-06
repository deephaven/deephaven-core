//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.tree;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public interface TreeViewportData extends TableData {
    @JsProperty
    double getTreeSize();

    @Override
    @TsTypeRef(TreeRow.class)
    default TableData.Row get(RowPositionUnion index) {
        return TableData.super.get(index);
    }

    @JsProperty
    @Override
    JsArray<TableData.@TsTypeRef(TreeRow.class) Row> getRows();

    /**
     * Row implementation that also provides additional read-only properties. represents visible rows in the table, but
     * with additional properties to reflect the tree structure.
     */
    @JsType
    interface TreeRow extends TableData.Row {
        /**
         * True if this node is currently expanded to show its children; false otherwise. Those children will be the
         * rows below this one with a greater depth than this one.
         *
         * @return boolean
         */
        @JsProperty(name = "isExpanded")
        boolean isExpanded();

        /**
         * True if this node has children and can be expanded; false otherwise. Note that this value may change when the
         * table updates, depending on the table's configuration.
         *
         * @return boolean
         */
        @JsProperty(name = "hasChildren")
        boolean hasChildren();

        /**
         * The number of levels above this node; zero for top level nodes. Generally used by the UI to indent the row
         * and its expand/collapse icon.
         *
         * @return int
         */
        @JsProperty(name = "depth")
        int depth();
    }
}
