package io.deephaven.web.client.fu;

import elemental2.core.JsArray;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * A place to collect utility methods for managing data in/out of javascript / handling column type information.
 */
public class JsData {
    /**
     * @return a new js array which handles primitive types sanely.
     */
    public static JsArray<Any> newArray(String type) {
        switch (type) {
            case "double":
                return Js.uncheckedCast(new double[0]);
            case "float":
                return Js.uncheckedCast(new float[0]);
            case "io.deephaven.time.DateTime":
            case "long":
                return Js.uncheckedCast(new long[0]);
            case "int":
                return Js.uncheckedCast(new int[0]);
            case "char":
                return Js.uncheckedCast(new char[0]);
            case "short":
                return Js.uncheckedCast(new short[0]);
            case "byte":
                return Js.uncheckedCast(new byte[0]);
            case "boolean":
                return Js.uncheckedCast(new boolean[0]);
        }
        return new JsArray<>();
    }

    /**
     * Gets a required property from a JsPropertyMap. Will throw if the value isn't set
     * 
     * @param source The property map to get the value from
     * @param propertyName The property to get value for
     * @return The value
     */
    public static Any getRequiredProperty(JsPropertyMap<Object> source, String propertyName) {
        if (!source.has(propertyName)) {
            throw new IllegalArgumentException("Property '" + propertyName + "' must be specified.");
        }
        Any value = source.getAny(propertyName);
        if (value == null) {
            throw new IllegalArgumentException("Property '" + propertyName + "' must not be null.");
        }
        return value;
    }

    public static Any getProperty(JsPropertyMap<Object> source, String propertyName) {
        return getProperty(source, propertyName, null);
    }

    public static Any getProperty(JsPropertyMap<Object> source, String propertyName, Any defaultValue) {
        if (source.has(propertyName)) {
            return source.getAny(propertyName);
        }

        return defaultValue;
    }

    public static String getRequiredStringProperty(JsPropertyMap<Object> source, String propertyName) {
        return getRequiredProperty(source, propertyName).asString();
    }

    public static String getStringProperty(JsPropertyMap<Object> source, String propertyName) {
        return getStringProperty(source, propertyName, null);
    }

    public static String getStringProperty(JsPropertyMap<Object> source, String propertyName, String defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? null : value.asString();
    }

    public static int getRequiredIntProperty(JsPropertyMap<Object> source, String propertyName) {
        return getRequiredProperty(source, propertyName).asInt();
    }

    public static int getIntProperty(JsPropertyMap<Object> source, String propertyName) {
        return getIntProperty(source, propertyName, 0);
    }

    public static int getIntProperty(JsPropertyMap<Object> source, String propertyName, int defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? 0 : value.asInt();
    }

    public static double getRequiredDoubleProperty(JsPropertyMap<Object> source, String propertyName) {
        return getRequiredProperty(source, propertyName).asDouble();
    }

    public static double getDoubleProperty(JsPropertyMap<Object> source, String propertyName) {
        return getDoubleProperty(source, propertyName, 0);
    }

    public static double getDoubleProperty(JsPropertyMap<Object> source, String propertyName, double defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? 0 : value.asDouble();
    }

    public static Double getNullableDoubleProperty(JsPropertyMap<Object> source, String propertyName) {
        return getNullableDoubleProperty(source, propertyName, null);
    }

    public static Double getNullableDoubleProperty(JsPropertyMap<Object> source, String propertyName,
            Double defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? null : value.asDouble();
    }

    public static boolean getRequiredBooleanProperty(JsPropertyMap<Object> source, String propertyName) {
        return getRequiredProperty(source, propertyName).asBoolean();
    }

    public static boolean getBooleanProperty(JsPropertyMap<Object> source, String propertyName) {
        return getBooleanProperty(source, propertyName, false);
    }

    public static boolean getBooleanProperty(JsPropertyMap<Object> source, String propertyName, boolean defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? false : value.asBoolean();
    }

    public static Boolean getNullableBooleanProperty(JsPropertyMap<Object> source, String propertyName) {
        return getNullableBooleanProperty(source, propertyName, null);
    }

    public static Boolean getNullableBooleanProperty(JsPropertyMap<Object> source, String propertyName,
            Boolean defaultValue) {
        Any value = getProperty(source, propertyName, Js.asAny(defaultValue));
        return value == null ? null : value.asBoolean();
    }
}
