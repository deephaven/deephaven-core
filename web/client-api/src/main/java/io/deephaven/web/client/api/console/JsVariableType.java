package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

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
