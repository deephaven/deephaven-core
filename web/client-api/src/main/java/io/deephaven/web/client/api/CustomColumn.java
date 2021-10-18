package io.deephaven.web.client.api;

import io.deephaven.web.shared.data.CustomColumnDescriptor;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

public class CustomColumn {
    @JsProperty(namespace = "dh.CustomColumn")
    public static final String TYPE_FORMAT_COLOR = "FORMAT_COLOR",
            TYPE_FORMAT_NUMBER = "FORMAT_NUMBER",
            TYPE_FORMAT_DATE = "FORMAT_DATE",
            TYPE_NEW = "NEW";

    // Copied from ColumnFormattingValues
    public static final String ROW_FORMAT_NAME = "__ROWFORMATTED";
    public static final String TABLE_FORMAT_NAME = "__WTABLE_FORMAT";
    public static final String TABLE_NUMERIC_FORMAT_NAME = "__WTABLE_NUM_FORMAT";
    public static final String TABLE_DATE_FORMAT_NAME = "__WTABLE_DATE_FORMAT";

    /**
     * Get the suffix to append to the name for the provided type
     * 
     * @param type The type of format, see TYPE_*
     * @return The suffix to append to the name for the provided type
     */
    private static String getNameSuffix(String type) {
        if (type.equals(TYPE_FORMAT_COLOR)) {
            return TABLE_FORMAT_NAME;
        }
        if (type.equals(TYPE_FORMAT_NUMBER)) {
            return TABLE_NUMERIC_FORMAT_NAME;
        }
        if (type.equals(TYPE_FORMAT_DATE)) {
            return TABLE_DATE_FORMAT_NAME;
        }
        if (type.equals(TYPE_NEW)) {
            return "";
        }

        throw new IllegalArgumentException("Unrecognized type: " + type);
    }

    private final String name;
    private final String type;
    private final String expression;

    public CustomColumn(String name, String type, String expression) {
        this.name = name;
        this.type = type;
        this.expression = expression;
    }

    public CustomColumn(CustomColumnDescriptor descriptor) {
        String descriptorExpression = descriptor.getExpression();
        String descriptorName = descriptor.getName();
        if (descriptorName.endsWith(TABLE_FORMAT_NAME)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_FORMAT_NAME.length());
            type = TYPE_FORMAT_COLOR;
        } else if (descriptorName.endsWith(TABLE_NUMERIC_FORMAT_NAME)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_NUMERIC_FORMAT_NAME.length());
            type = TYPE_FORMAT_NUMBER;
        } else if (descriptorName.endsWith(TABLE_DATE_FORMAT_NAME)) {
            name = descriptorName.substring(0, descriptorName.length() - TABLE_DATE_FORMAT_NAME.length());
            type = TYPE_FORMAT_DATE;
        } else {
            name = descriptorName;
            type = TYPE_NEW;
        }
        // Substring from after the name and equals sign
        expression = descriptorExpression.substring(descriptorName.length() + 1);
    }

    public CustomColumn(JsPropertyMap<Object> source) {
        if (!source.has("name") || !source.has("type") || !source.has("expression")) {
            throw new IllegalArgumentException("Unrecognized CustomColumn format: " + source);
        }

        name = source.getAny("name").asString();
        type = source.getAny("type").asString();
        expression = source.getAny("expression").asString();
    }

    @JsProperty
    public String getName() {
        return name;
    }

    @JsProperty
    public String getType() {
        return type;
    }

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
