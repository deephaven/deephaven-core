/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsObject;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@TsInterface
@TsName(name = "LayoutHints", namespace = "dh")
public class JsLayoutHints {
    @TsInterface
    @TsName(namespace = "dh")
    public static class ColumnGroup {
        private String name;
        private String[] children;
        private String color;

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

        @JsProperty
        public String getName() {
            return name;
        }

        @JsProperty
        public String[] getChildren() {
            return children;
        }

        @JsProperty
        public String getColor() {
            return color;
        }

        @JsProperty
        public void setName(String name) {
            this.name = name;
        }

        @JsProperty
        public void setChildren(String[] children) {
            this.children = children;
        }

        @JsProperty
        public void setColor(String color) {
            this.color = color;
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
