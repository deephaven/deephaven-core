//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.web.shared.data.CustomColumnDescriptor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

@JsType(namespace = "dh")
public class CustomColumn {
    public static final String TYPE_FORMAT_COLOR = "FORMAT_COLOR",
            TYPE_FORMAT_NUMBER = "FORMAT_NUMBER",
            TYPE_FORMAT_DATE = "FORMAT_DATE",
            TYPE_NEW = "NEW";

    // Copied from ColumnFormattingValues
    protected static final String ROW_FORMAT_NAME = "__ROW";
    private static final String TABLE_STYLE_FORMAT_SUFFIX = "__TABLE_STYLE_FORMAT";
    private static final String TABLE_NUMBER_FORMAT_SUFFIX = "__TABLE_NUMBER_FORMAT";
    private static final String TABLE_DATE_FORMAT_SUFFIX = "__TABLE_DATE_FORMAT";

    /**
     * Get the suffix to append to the name for the provided type
     * 
     * @param type The type of format, see TYPE_*
     * @return The suffix to append to the name for the provided type
     */
    private static String getNameSuffix(String type) {
        if (type.equals(TYPE_FORMAT_COLOR)) {
            return TABLE_STYLE_FORMAT_SUFFIX;
        }
        if (type.equals(TYPE_FORMAT_NUMBER)) {
            return TABLE_NUMBER_FORMAT_SUFFIX;
        }
        if (type.equals(TYPE_FORMAT_DATE)) {
            return TABLE_DATE_FORMAT_SUFFIX;
        }
        if (type.equals(TYPE_NEW)) {
            return "";
        }

        throw new IllegalArgumentException("Unrecognized type: " + type);
    }

    private final String name;
    private final String type;
    private final String expression;

    @JsIgnore
    public CustomColumn(String name, String type, String expression) {
        this.name = name;
        this.type = type;
        this.expression = expression;
    }

    @JsIgnore
    public CustomColumn(CustomColumnDescriptor descriptor) {
        String descriptorExpression = descriptor.getExpression();
        String descriptorName = descriptor.getName();
        if (descriptorName.endsWith(TABLE_STYLE_FORMAT_SUFFIX)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_STYLE_FORMAT_SUFFIX.length());
            type = TYPE_FORMAT_COLOR;
        } else if (descriptorName.endsWith(TABLE_NUMBER_FORMAT_SUFFIX)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_NUMBER_FORMAT_SUFFIX.length());
            type = TYPE_FORMAT_NUMBER;
        } else if (descriptorName.endsWith(TABLE_DATE_FORMAT_SUFFIX)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_DATE_FORMAT_SUFFIX.length());
            type = TYPE_FORMAT_DATE;
        } else {
            name = descriptorName;
            type = TYPE_NEW;
        }
        // Substring from after the name and equals sign
        expression = descriptorExpression.substring(descriptorName.length() + 1);
    }

    @JsIgnore
    public CustomColumn(JsPropertyMap<Object> source) {
        if (!source.has("name") || !source.has("type") || !source.has("expression")) {
            throw new IllegalArgumentException("Unrecognized CustomColumn format: " + source);
        }

        name = source.getAsAny("name").asString();
        type = source.getAsAny("type").asString();
        expression = source.getAsAny("expression").asString();
    }

    /**
     * The name of the column to use.
     * 
     * @return String
     */
    @JsProperty
    public String getName() {
        return name;
    }

    /**
     * Type of custom column. One of
     *
     * <ul>
     * <li>FORMAT_COLOR</li>
     * <li>FORMAT_NUMBER</li>
     * <li>FORMAT_DATE</li>
     * <li>NEW</li>
     * </ul>
     *
     * @return String
     */
    @JsProperty
    public String getType() {
        return type;
    }

    /**
     * The expression to evaluate this custom column.
     * 
     * @return String
     */
    @JsProperty
    public String getExpression() {
        return expression;
    }

    @JsMethod
    public String valueOf() {
        return toString();
    }

    @JsMethod
    @Override
    public String toString() {
        return "" + name + getNameSuffix(type) + "=" + expression;
    }
}
