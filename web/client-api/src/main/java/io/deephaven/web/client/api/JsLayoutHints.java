//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsObject;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@TsInterface
@JsType(namespace = "dh", name = "LayoutHints")
/**
 * Layout hint values that can be used by clients when rendering a table.
 */
public class JsLayoutHints {
    @TsInterface
    @JsType(namespace = "dh")
    /**
     * Describes a column group to use when rendering a table.
     */
    public static class ColumnGroup {
        /**
         * The group name.
         */
        @JsNullable
        public final String name;
        /**
         * The column names contained by this group.
         */
        @JsNullable
        public final String[] children;
        /**
         * The group color.
         */
        @JsNullable
        public final String color;

        @JsIgnore
        public ColumnGroup(String groupStr) {
            if (groupStr == null || groupStr.isEmpty()) {
                name = null;
                children = null;
                color = null;
                return;
            }

            final Map<String, String> options = Arrays.stream(groupStr.split("::"))
                    .map(option -> option.split(":"))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts.length == 2 ? parts[1] : ""));

            final String nameStr = options.get("name");
            if (nameStr != null && !nameStr.isEmpty()) {
                name = nameStr;
            } else {
                name = null;
            }

            final String childrenStr = options.get("children");
            if (childrenStr != null && !childrenStr.isEmpty()) {
                children = JsObject.freeze(childrenStr.split(","));
            } else {
                children = null;
            }

            final String colorStr = options.get("color");
            if (colorStr != null && !colorStr.isEmpty()) {
                color = colorStr;
            } else {
                color = null;
            }
        }
    }

    private boolean savedLayoutsAllowed = true;

    /**
     * The display mode for the search bar.
     */
    private String searchDisplayMode = SearchDisplayMode.SEARCH_DISPLAY_DEFAULT;
    private String[] frontColumns;
    private String[] backColumns;
    private String[] hiddenColumns;
    private String[] frozenColumns;

    private ColumnGroup[] columnGroups;

    @JsIgnore
    public JsLayoutHints parse(String hints) {
        if (hints == null || hints.isEmpty()) {
            return this;
        }

        final Map<String, String> options = Arrays.stream(hints.split(";"))
                .map(hint -> hint.split("="))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts.length == 2 ? parts[1] : ""));


        if (options.containsKey("noSavedLayouts")) {
            savedLayoutsAllowed = false;
        }

        final String frontStr = options.get("front");
        if (frontStr != null && !frontStr.isEmpty()) {
            frontColumns = JsObject.freeze(frontStr.split(","));
        }

        final String endStr = options.get("back");
        if (endStr != null && !endStr.isEmpty()) {
            backColumns = JsObject.freeze(endStr.split(","));
        }

        final String hideStr = options.get("hide");
        if (hideStr != null && !hideStr.isEmpty()) {
            hiddenColumns = JsObject.freeze(hideStr.split(","));
        }

        final String freezeStr = options.get("freeze");
        if (freezeStr != null && !freezeStr.isEmpty()) {
            frozenColumns = JsObject.freeze(freezeStr.split(","));
        }

        final String searchableStr = options.get("searchable");
        if (searchableStr != null && !searchableStr.isEmpty()) {
            searchDisplayMode = searchableStr;
        }

        final String groupsStr = options.get("columnGroups");
        if (groupsStr != null && !groupsStr.isEmpty()) {
            ColumnGroup[] groups =
                    Arrays.stream(groupsStr.split("\\|")).map(ColumnGroup::new).map(JsObject::freeze)
                            .toArray(ColumnGroup[]::new);

            columnGroups = JsObject.freeze(groups);
        }

        return this;
    }

    /**
     * Whether saved layouts are allowed.
     */
    @JsProperty
    public boolean getAreSavedLayoutsAllowed() {
        return savedLayoutsAllowed;
    }

    /**
     * The display mode for the search bar.
     */
    @JsNullable
    @JsProperty
    @TsTypeRef(SearchDisplayMode.class)
    public String getSearchDisplayMode() {
        return searchDisplayMode;
    }

    /**
     * Column names to display at the front of the table.
     */
    @JsProperty
    @JsNullable
    public String[] getFrontColumns() {
        return frontColumns;
    }

    /**
     * Column names to display at the back of the table.
     */
    @JsNullable
    @JsProperty
    public String[] getBackColumns() {
        return backColumns;
    }

    /**
     * Column names to hide.
     */
    @JsNullable
    @JsProperty
    public String[] getHiddenColumns() {
        return hiddenColumns;
    }

    /**
     * Column names to render as frozen.
     */
    @JsNullable
    @JsProperty
    public String[] getFrozenColumns() {
        return frozenColumns;
    }

    /**
     * Column groups to use when rendering the table.
     */
    @JsNullable
    @JsProperty
    public ColumnGroup[] getColumnGroups() {
        return columnGroups;
    }
}
