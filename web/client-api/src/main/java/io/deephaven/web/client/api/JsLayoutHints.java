/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import elemental2.core.JsObject;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsLayoutHints {
    private class ColumnGroup {
        @JsProperty
        public String name;
        @JsProperty
        public String[] children;
        @JsProperty
        public String color;

        public ColumnGroup(String groupStr) {
            if (groupStr == null || groupStr.isEmpty()) {
                return;
            }

            final Map<String, String> options = Arrays.stream(groupStr.split("::"))
                    .map(option -> option.split(":"))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts.length == 2 ? parts[1] : ""));

            final String nameStr = options.get("name");
            if (nameStr != null && !nameStr.isEmpty()) {
                name = nameStr;
            }

            final String childrenStr = options.get("children");
            if (childrenStr != null && !childrenStr.isEmpty()) {
                children = JsObject.freeze(childrenStr.split(","));
            }

            final String colorStr = options.get("color");
            if (colorStr != null && !colorStr.isEmpty()) {
                color = colorStr;
            }
        }
    }

    private boolean savedLayoutsAllowed = true;
    private String[] frontColumns;
    private String[] backColumns;
    private String[] hiddenColumns;
    private String[] frozenColumns;

    private ColumnGroup[] columnGroups;

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

        final String groupsStr = options.get("columnGroups");
        if (groupsStr != null && !groupsStr.isEmpty()) {
            ColumnGroup[] groups =
                    Arrays.stream(groupsStr.split("\\|")).map(ColumnGroup::new).map(JsObject::freeze)
                            .toArray(ColumnGroup[]::new);

            columnGroups = JsObject.freeze(groups);
        }

        return this;
    }

    @JsProperty
    public boolean getAreSavedLayoutsAllowed() {
        return savedLayoutsAllowed;
    }

    @JsProperty
    public String[] getFrontColumns() {
        return frontColumns;
    }

    @JsProperty
    public String[] getBackColumns() {
        return backColumns;
    }

    @JsProperty
    public String[] getHiddenColumns() {
        return hiddenColumns;
    }

    @JsProperty
    public String[] getFrozenColumns() {
        return frozenColumns;
    }

    @JsProperty
    public ColumnGroup[] getColumnGroups() {
        return columnGroups;
    }
}
