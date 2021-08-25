package io.deephaven.web.client.api.input;

import elemental2.core.Function;
import elemental2.core.JsArray;
import elemental2.core.Reflect;
import io.deephaven.web.shared.data.ColumnValue;
import jsinterop.base.Js;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.deephaven.web.shared.data.ColumnValue.*;

/**
 * A tool for turning javascript objects (and all their "glory") into usable {@link ColumnValue}
 * objects.
 *
 * This is only usable within client code.
 */
public class ColumnValueDehydrater {

    public static String serialize(String type, Object value) {
        return serialize(type, value, false);
    }

    private static String serialize(String type, Object value, boolean arrayComponent) {
        // Should be symmetric with ColumnValueRehydrater.
        // for now, we will fall back to .toString support

        if (value == null || ("null".equals(value) && !"java.lang.String".equals(type))) {
            // we'll permit the string "null" if the type is a string, otherwise assume client
            // accidentally toString'd a null value
            return nullSentinel();
        }
        if (type.contains("[]")) {
            if (!JsArray.isArray(value)) {
                throw new UnsupportedOperationException(
                    "Expected array type " + type + " but got " + value);
            }
            final String componentType = type.replace("[]", "");
            StringBuilder result = new StringBuilder();
            Js.<JsArray<Object>>uncheckedCast(value).forEach((v, i, a) -> {
                if (result.length() > 0) {
                    result.append(ARRAY_DELIMITER);
                }
                if (v == null) {
                    result.append(nullSentinel());
                } else {
                    String subSer = serialize(componentType, v, true);
                    // handle our escaped characters:
                    for (char c : subSer.toCharArray()) {
                        switch (c) {
                            // \0 (null) -> \1\0
                            case NULL_SENTINEL:
                                result.append(ESCAPER).append(NULL_SENTINEL);
                                break;
                            // \1 (escaper) -> \1\2
                            case ESCAPER:
                                result.append(ESCAPER).append(ESCAPER_ESCAPED);
                                break;
                            // , (array delimiter)
                            case ARRAY_DELIMITER:
                                result.append(ESCAPER).append(ARRAY_DELIMITER);
                                break;
                            default:
                                result.append(c);
                        }
                    }
                }
                return result.toString();
            });
            return result.toString();
        }

        switch (Js.typeof(value)) {
            case "boolean": {
                switch (type) {
                    case "boolean":
                    case "java.lang.Boolean":
                        return Js.isTruthy(value) ? "1" : "0";
                }
                throw illegal("boolean", value);
            }
            case "number": {
                // only support number-y types.
                double val = (double) value;
                switch (type) {
                    case "java.lang.String":
                        return String.valueOf(value);
                    case "int":
                    case "java.lang.Integer":
                        // perhaps add (optional) validation that users are actually sending ints.
                        // like `web.typesafe=true` to turn on paranoia checks.
                        return val == Integer.MIN_VALUE ? nullSentinel()
                            : Integer.toString((int) val);
                    case "double":
                    case "java.lang.Double":
                    case "java.math.BigDecimal":
                    case "java.math.BigInteger":
                        return val == -Double.MAX_VALUE ? nullSentinel() : Double.toString(val);
                    case "long":
                    case "java.lang.Long":
                    case "io.deephaven.db.tables.utils.DBDateTime":
                        // TODO: check if Long.MIN_VALUE actually works as expected from js;
                        // in theory, the cast here will make the rounding, if any, equivalent
                        return val == (double) Long.MIN_VALUE ? nullSentinel()
                            : Long.toString((long) val);
                    case "byte":
                    case "java.lang.Byte":
                        return val == Byte.MIN_VALUE ? nullSentinel() : Byte.toString((byte) val);
                    case "boolean":
                    case "java.lang.Boolean":
                        checkBooleanValue(val);
                        assert val == (byte) val : "Malformed boolean value " + val;
                        return Byte.toString((byte) val);
                    case "char":
                    case "java.lang.Character":
                        return val == Character.MAX_VALUE - 1 ? nullSentinel()
                            : String.valueOf(value).substring(0, 1);
                    case "short":
                    case "java.lang.Short":
                        return val == Short.MIN_VALUE ? nullSentinel()
                            : Short.toString((short) val);
                    case "float":
                    case "java.lang.Float":
                        return val == Float.MIN_VALUE ? nullSentinel()
                            : Float.toString((float) val);
                    default:
                        throw unsupported(value);
                }
            } // done w/ number
            case "string": {

                String v = (String) value;

                switch (type) {
                    case "java.lang.String":
                        return arrayComponent ? v : // arrays are escaped in the forEach loop above
                            v.replaceAll(nullSentinel(), ESCAPER + nullSentinel());
                    case "io.deephaven.db.tables.utils.DBDateTime":
                        // TODO: check if datetime string to parse into a long timestamp.
                        // otherwise, we expect long ints for DBDateTime (for now)
                    case "int":
                    case "java.lang.Integer":
                    case "long":
                    case "java.lang.Long":
                    case "byte":
                    case "java.lang.Byte":
                    case "short":
                    case "java.lang.Short":
                    case "java.math.BigInteger":
                        // new BigInteger() should validate all of the above
                        assert !new BigInteger(v).toString().isEmpty(); // something we don't want
                                                                        // to pay for in prod
                        return v.split("[.]")[0]; // be forgiving to js...
                    case "double":
                    case "java.lang.Double":
                    case "float":
                    case "java.lang.Float":
                    case "java.math.BigDecimal":
                        // new BigDecimal() should validate all of the above
                        assert !new BigDecimal(v).toString().isEmpty(); // something we don't want
                                                                        // to pay for in prod
                        return v;
                    case "boolean":
                    case "java.lang.Boolean":
                        if ("false".equals(v.toLowerCase())) {
                            return "0";
                        }
                        if ("true".equals(v.toLowerCase())) {
                            return "1";
                        }
                        if ("null".equals(v.toLowerCase())) {
                            return "-1";
                        }
                        checkBooleanValue(Byte.parseByte(v));
                        return v;
                    case "char":
                    case "java.lang.Character":
                        return v.substring(0, 1);
                }
                assert !(arrayComponent && v.contains(escaper()))
                    : "\\1 is a control character that should not be sent in unhandled array types.";
                return v; // good luck
            }
            case "object": {
                if (JsArray.isArray(value)) {
                    // arrays should already be handled
                    throw unsupported(value);
                }
                // check for a function starting w/ name (enums)...
                Function name = findNameFunction(value);
                if (name != null) {
                    return String.valueOf(name.bind(value).call());
                }
                // fail
                throw unsupported(value);
            }
        }
        throw unsupported(value);
    }

    private static boolean checkBooleanValue(double val) {
        if (!(val == 1 || val == 0 || val == -1)) {
            throw illegal("boolean", val);
        }
        return true;
    }

    private static Function findNameFunction(Object value) {
        Function name = findFunction(value, "name");
        if (name == null) {
            name = findFunction(value, "n");
        }
        return name;
    }

    private static Function findFunction(Object value, String prefix) {
        // This probably isn't the correct way to "guess" enum values,
        // but we can probably come up with the correct heuristic for
        // PRETTY vs. OBF compilation obfuscation levels.
        // For now, if these lookups fail, the enum type in question
        // will throw an error (we'll fix this up when we add testing
        // and more rigorous support for "common types").
        Object proto = Reflect.get(value, "prototype");
        for (Object o : Reflect.ownKeys(proto)) {
            final String key = String.valueOf(o);
            if (key.startsWith(prefix)) {
                Object result = Reflect.get(proto, key);
                if ("function".equals(Js.typeof(result))) {
                    return Js.uncheckedCast(result);
                }
            }
        }
        return null;
    }

    private static UnsupportedOperationException unsupported(Object value) {
        return new UnsupportedOperationException(
            "Cannot handle " + Js.typeof(value) + " : " + value);
    }

    private static IllegalArgumentException illegal(String type, Object value) {
        return new IllegalArgumentException(
            "Cannot handle " + Js.typeof(value) + " value " + value + " for " + type + " column");
    }
}
