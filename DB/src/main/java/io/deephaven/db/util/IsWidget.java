package io.deephaven.db.util;

import io.deephaven.db.exceptions.ArgumentException;
import io.deephaven.db.exceptions.OperationException;
import io.deephaven.db.exceptions.StateException;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.LiveWidget;
import io.deephaven.db.tables.utils.LiveWidgetVisibilityProvider;
import org.jpy.PyObject;

/**
 * Get a widget from an object.
 */
public class IsWidget {
    private static final String GET_WIDGET_ATTRIBUTE = "getWidget";
    private static final String GET_WIDGET_VISIBILITY_ATTRIBUTE = "getValidGroups";
    private static final String GET_TABLE_ATTRIBUTE = "get_dh_table";

    public static boolean isWidget(Object value) {
        if (value instanceof LiveWidget) {
            return true;
        } else if ((value instanceof PyObject
            && ((PyObject) value).hasAttribute(GET_WIDGET_ATTRIBUTE))) {
            try (final PyObject widget = ((PyObject) value).callMethod(GET_WIDGET_ATTRIBUTE)) {
                return !widget.isNone();
            }
        }

        return false;
    }

    public static LiveWidget getWidget(Object value) {
        if (value instanceof LiveWidget) {
            return (LiveWidget) value;
        } else if (value instanceof PyObject) {
            return IsWidget.getWidget((PyObject) value);
        }

        throw new OperationException("Can not convert value=" + value + " to a LiveWidget.");
    }

    public static LiveWidget getWidget(PyObject pyObject) {
        boolean isWidget = pyObject.hasAttribute(GET_WIDGET_ATTRIBUTE);
        if (isWidget) {
            try (final PyObject widget = pyObject.callMethod(GET_WIDGET_ATTRIBUTE)) {
                if (!widget.isNone()) {
                    return (LiveWidget) widget.getObjectValue();
                }
            }
        }

        throw new OperationException("Can not convert pyOjbect=" + pyObject + " to a LiveWidget.");
    }

    public static boolean isLiveWidgetVisibilityProvider(Object value) {
        if (value instanceof LiveWidgetVisibilityProvider) {
            return true;
        } else if ((value instanceof PyObject
            && ((PyObject) value).hasAttribute(GET_WIDGET_VISIBILITY_ATTRIBUTE))) {
            return true;
        }

        return false;
    }

    public static String[] getLiveWidgetVisibility(final Object object) {
        if (object instanceof LiveWidgetVisibilityProvider) {
            return ((LiveWidgetVisibilityProvider) object).getValidGroups();
        } else if (object instanceof PyObject) {
            final PyObject pyObject = (PyObject) object;
            boolean isWidget = pyObject.hasAttribute(GET_WIDGET_VISIBILITY_ATTRIBUTE);
            if (isWidget) {
                try (final PyObject widget = pyObject.callMethod(GET_WIDGET_VISIBILITY_ATTRIBUTE)) {
                    if (!widget.isNone()) {
                        return widget.getObjectArrayValue(String.class);
                    } else {
                        return null;
                    }
                }
            } else {
                throw new StateException(
                    "PyObject " + object + " isLiveWidgetVisibilityProvider, but has no attribute "
                        + GET_WIDGET_VISIBILITY_ATTRIBUTE);
            }
        }

        throw new ArgumentException("Unknown LiveWidgetVisibilityProvider type " + object);
    }

    public static boolean isTable(Object value) {
        if (value instanceof Table) {
            return true;
        } else if ((value instanceof PyObject
            && ((PyObject) value).hasAttribute(GET_TABLE_ATTRIBUTE))) {
            try (final PyObject widget = ((PyObject) value).callMethod(GET_TABLE_ATTRIBUTE)) {
                return !widget.isNone();
            }
        }

        return false;
    }

    public static Table getTable(Object value) {
        if (value instanceof Table) {
            return (Table) value;
        } else if (value instanceof PyObject) {
            return getTable((PyObject) value);
        }
        throw new OperationException("Can not convert value=" + value + " to a Table.");
    }

    public static Table getTable(PyObject pyObject) {
        if (pyObject.hasAttribute(GET_TABLE_ATTRIBUTE)) {
            try (final PyObject widget = pyObject.callMethod(GET_TABLE_ATTRIBUTE)) {
                if (!widget.isNone()) {
                    return (Table) widget.getObjectValue();
                }
            }
        }

        throw new OperationException("Can not convert pyObject=" + pyObject + " to a Table.");
    }
}
