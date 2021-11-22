package io.deephaven.engine.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableMap;
import static io.deephaven.engine.table.Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE;
import static io.deephaven.engine.table.Table.NON_DISPLAY_TABLE;

public enum ExportedObjectType {
    Table,
    TreeTable,
    TableMap,
    Figure,
    OtherWidget,
    Pandas,
    NonDisplayable;

    public static ExportedObjectType fromObject(Object value) {
        if (IsWidget.isTable(value)) {
            Table table = IsWidget.getTable(value);
            if (!table.hasAttribute(NON_DISPLAY_TABLE)) {
                if (table.hasAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)) {
                    return ExportedObjectType.TreeTable;
                } else {
                    return ExportedObjectType.Table;
                }
            }
        } else if (IsWidget.isWidget(value)) {
            final LiveWidget widget = IsWidget.getWidget(value);
            if (widget instanceof FigureWidgetMarker) {
                return ExportedObjectType.Figure;
            } else if (widget instanceof PandasWidgetMarker) {
                return ExportedObjectType.Pandas;
            } else {
                return ExportedObjectType.OtherWidget;
            }
        } else if (value instanceof TableMap) {
            return ExportedObjectType.TableMap;
        }
        return NonDisplayable;
    }

    public boolean isTableType() {
        return this == Table || this == TreeTable;
    }

    public boolean isWidgetType() {
        return this == Figure || this == OtherWidget || this == Pandas;
    }

    public boolean isDisplayable() {
        return this != NonDisplayable && this != TableMap;
    }
}

