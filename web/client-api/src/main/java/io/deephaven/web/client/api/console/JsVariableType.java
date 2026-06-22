//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

/**
 * A set of string constants that can be used to describe the different objects the JS API can export.
 */
@JsType(namespace = "dh", name = "VariableType")
@TsTypeDef(tsType = "string")
public class JsVariableType {
    /**
     * A {@link io.deephaven.web.client.api.JsTable}.
     */
    public static final String TABLE = "Table";

    /**
     * A {@link io.deephaven.web.client.api.tree.JsTreeTable}.
     */
    public static final String TREETABLE = "TreeTable";

    /**
     * A hierarchical table.
     */
    public static final String HIERARCHICALTABLE = "HierarchicalTable";

    /**
     * A table map.
     */
    public static final String TABLEMAP = "TableMap";

    /**
     * A {@link io.deephaven.web.client.api.JsPartitionedTable}.
     */
    public static final String PARTITIONEDTABLE = "PartitionedTable";

    /**
     * A {@link io.deephaven.web.client.api.widget.plot.JsFigure}.
     */
    public static final String FIGURE = "Figure";

    /**
     * An exported object that is not one of the explicitly listed Deephaven JS API widget types.
     */
    public static final String OTHERWIDGET = "OtherWidget";

    /**
     * A Python pandas DataFrame.
     */
    public static final String PANDAS = "pandas.DataFrame";

    /**
     * A treemap widget.
     *
     * <p>
     * Treemaps are represented in the JS API as a {@link io.deephaven.web.client.api.widget.plot.JsFigure} containing a
     * chart whose type is {@link io.deephaven.web.client.api.widget.plot.enums.JsChartType#TREEMAP}.
     */
    public static final String TREEMAP = "Treemap";
}
