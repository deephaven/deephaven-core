package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

/**
 * A set of string constants that can be used to describe the different objects the JS API can export.
 */
@JsType(namespace = "dh", name = "VariableType")
@TsTypeDef(tsType = "string")
public class JsVariableType {
    public static final String TABLE = "Table",
            TREETABLE = "TreeTable",
            HIERARCHICALTABLE = "HierarchicalTable",
            TABLEMAP = "TableMap",
            PARTITIONEDTABLE = "PartitionedTable",
            FIGURE = "Figure",
            OTHERWIDGET = "OtherWidget",
            PANDAS = "pandas.DataFrame",
            TREEMAP = "Treemap";
}
