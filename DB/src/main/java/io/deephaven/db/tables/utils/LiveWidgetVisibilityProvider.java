/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

/**
 * <p>
 * LiveWidgets may implement this interface to restrict the users who can see a particular widget.
 * </p>
 *
 * <p>
 * This interface only enables the widget developer to limit the users who may open the widget; it
 * does not provide any control over what users can see after the widget is opened. The widget
 * itself is responsible for determining which data should be presented to the user and applying any
 * appropriate viewer permissions.
 * </p>
 *
 * <p>
 * If widgets do not implement this interface, they are visible to all users of the query.
 * </p>
 *
 * <p>
 * Unlike tables, limiting the visibility of one widget does not affect the visibility of other
 * widgets.
 * </p>
 */
public interface LiveWidgetVisibilityProvider {
    /**
     * Provide a list of groups which may view this widget. null indicates that there are no viewing
     * restrictions on this widget.
     *
     * @return the list of groups which may view this widget, null for no restrictions
     */
    String[] getValidGroups();
}
