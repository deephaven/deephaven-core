//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.parse;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.gwt.i18n.client.TimeZone;
import elemental2.core.JsDate;
import io.deephaven.util.BooleanUtils;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import jsinterop.base.Js;
import org.apache.arrow.flatbuf.Binary;
import org.apache.arrow.flatbuf.Date;
import org.apache.arrow.flatbuf.DateUnit;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Time;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.flatbuf.Utf8;

import java.util.HashMap;
import java.util.Map;

/**
 * Given the expected type of a column, pick one of the enum entries and use that to read the data into arrow buffers.
 */
public enum JsDataHandler {
    STRING(Type.Utf8, "java.lang.String", "string") {

        @Override
        public int writeType(FlatBufferBuilder builder) {
            Utf8.startUtf8(builder);
            return Utf8.endUtf8(builder);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            // Scan until we find an element that needs conversion
            int firstNonString = 0;
            while (firstNonString < data.length) {
                if (!(data[firstNonString] instanceof String) && data[firstNonString] != null) {
                    break;
                }
                firstNonString++;
            }
            if (firstNonString == data.length) {
                // everything was already a string or null
                return data;
            }
            // Copy preceding elements, then convert the rest
            Object[] result = new Object[data.length];
            System.arraycopy(data, 0, result, 0, firstNonString);
            for (int i = firstNonString; i < data.length; i++) {
                if (data[i] == null || data[i] instanceof String) {
                    result[i] = data[i];
                } else {
                    result[i] = data[i].toString();
                }
            }
            return result;
        }
    },
    DATE_TIME(Type.Int, "java.time.Instant", "datetime", "java.time.ZonedDateTime") {
        // Ensures that the 'T' separator character is in the date time
        private String ensureSeparator(String s) {
            if (s.charAt(SEPARATOR_INDEX) == ' ') {
                StringBuilder stringBuilder = new StringBuilder(s);
                stringBuilder.setCharAt(SEPARATOR_INDEX, 'T');
                return stringBuilder.toString();
            }
            return s;
        }

        // Guess the pattern for the correct number of subsecond digits 'S'
        private String getSubsecondPattern(String s) {
            final int decimalIndex = s.indexOf('.');
            if (decimalIndex == -1) {
                // No subsecond digits
                return DEFAULT_DATE_TIME_PATTERN;
            }
            final int numDigits = s.length() - decimalIndex - 1;
            final StringBuilder stringBuilder = new StringBuilder(numDigits);
            for (int i = 0; i < numDigits; i++) {
                stringBuilder.append('S');
            }
            return DEFAULT_DATE_TIME_PATTERN + "." + stringBuilder;
        }

        private long parseDateString(String str, ParseContext context) {
            final String s = ensureSeparator(str);
            final int spaceIndex = s.indexOf(' ');
            final String dateTimeString;
            final String timeZoneString;
            if (spaceIndex == -1) {
                // Zulu is an exception to the space rule
                if (s.endsWith("Z")) {
                    dateTimeString = s.substring(0, s.length() - 1);
                    timeZoneString = "Z";
                } else {
                    dateTimeString = s;
                    timeZoneString = null;
                }
            } else {
                dateTimeString = s.substring(0, spaceIndex);
                timeZoneString = s.substring(spaceIndex + 1);
            }

            final TimeZone timeZone = timeZoneString == null
                    ? context.timeZone.unwrap()
                    : JsTimeZone.getTimeZone(timeZoneString).unwrap();
            boolean needsAdjustment = JsTimeZone.needsDstAdjustment(timeZoneString);

            try {
                // First try with the pattern we already have
                return JsDateTimeFormat.getFormat(context.dateTimePattern).parseWithTimezoneAsLong(dateTimeString,
                        timeZone, needsAdjustment);
            } catch (IllegalArgumentException e) {
                // We failed to parse with the existing context pattern, try and update the pattern from the string of
                // text and do it again
                context.dateTimePattern = getSubsecondPattern(dateTimeString);
                return JsDateTimeFormat.getFormat(context.dateTimePattern).parseWithTimezoneAsLong(dateTimeString,
                        timeZone, needsAdjustment);
            }
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 64, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            // Scan until we find a non-LongWrapper (and non-null) instance
            int firstNonLong = 0;
            while (firstNonLong < data.length) {
                if (!(data[firstNonLong] instanceof LongWrapper) && data[firstNonLong] != null) {
                    break;
                }
                firstNonLong++;
            }
            if (firstNonLong == data.length) {
                // everything was already a LongWrapper or null
                return data;
            }
            // Copy preceding elements, then convert the rest
            Object[] result = new Object[data.length];
            System.arraycopy(data, 0, result, 0, firstNonLong);
            for (int i = firstNonLong; i < data.length; i++) {
                if (data[i] == null || data[i] instanceof LongWrapper) {
                    result[i] = data[i];
                } else if (data[i] instanceof JsDate) {
                    result[i] = LongWrapper.of(1_000_000L * ((JsDate) data[i]).getDate());
                } else {
                    // fall back to assuming it is a string, figure out what it formats as
                    String str = data[i].toString().trim();
                    if (!str.isEmpty()) {
                        // take the format string and the timezone, and solve for a date
                        result[i] = LongWrapper.of(parseDateString(str, context));
                    } // consider this to be null, leave it empty
                }
            }

            return result;
        }
    },
    INTEGER(Type.Int, "int") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 32, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    SHORT(Type.Int, "short") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 16, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    LONG(Type.Int, "long") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 64, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            // Scan until we find a non-LongWrapper (and non-null) instance
            int firstNonLong = 0;
            while (firstNonLong < data.length) {
                if (!(data[firstNonLong] instanceof LongWrapper) && data[firstNonLong] != null) {
                    break;
                }
                firstNonLong++;
            }
            if (firstNonLong == data.length) {
                // everything was already a LongWrapper or null
                return data;
            }
            // Copy preceding elements, then convert the rest
            Object[] result = new Object[data.length];
            System.arraycopy(data, 0, result, 0, firstNonLong);
            for (int i = firstNonLong; i < data.length; i++) {
                if (data[i] == null || data[i] instanceof LongWrapper) {
                    result[i] = data[i];
                } else if (Js.typeof(data[i]).equals("string")) {
                    String str = ((String) data[i]).trim().replaceAll(",", "");
                    if (!str.isEmpty()) {
                        result[i] = LongWrapper.ofString(str);
                    } // otherwise leave it null
                } else {
                    // fall back to whatever we can get - this is null safe, since we already tested for null and empty
                    // string above
                    result[i] = LongWrapper.of((long) (double) doubleFromData(data[i]));
                }
            }
            return result;
        }
    },
    BYTE(Type.Int, "byte") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 8, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    CHAR(Type.Int, "char") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 16, false);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    FLOAT(Type.FloatingPoint, "float") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return FloatingPoint.createFloatingPoint(builder, Precision.SINGLE);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    DOUBLE(Type.FloatingPoint, "double") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return FloatingPoint.createFloatingPoint(builder, Precision.DOUBLE);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            return writeSimpleNumbers(data);
        }
    },
    BOOLEAN(Type.Bool, "boolean", "bool", "java.lang.Boolean") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Int.createInt(builder, 8, true);
        }

        @Override
        public Object[] process(Object[] data, ParseContext context) {
            // Scan until we find a non-Boolean (and non-null) instance
            int firstNonBoolean = 0;
            while (firstNonBoolean < data.length) {
                if (!(data[firstNonBoolean] instanceof Boolean) && data[firstNonBoolean] != null) {
                    break;
                }
                firstNonBoolean++;
            }
            if (firstNonBoolean == data.length) {
                // everything was already a Boolean or null
                return data;
            }
            // Copy preceding elements, then convert the rest
            Object[] result = new Object[data.length];
            System.arraycopy(data, 0, result, 0, firstNonBoolean);
            for (int i = firstNonBoolean; i < data.length; i++) {
                Object val = data[i];
                if (val == null || val instanceof Boolean) {
                    result[i] = val;
                } else {
                    String t = Js.typeof(val);
                    switch (t) {
                        case "boolean":
                            result[i] = Js.isTruthy(val);
                            break;
                        case "number":
                            result[i] = BooleanUtils.byteAsBoolean(Js.asByte(val));
                            break;
                        case "string":
                            String str = Js.asString(val);
                            switch (str.toLowerCase()) {
                                case "true":
                                    result[i] = true;
                                    break;
                                case "false":
                                    result[i] = false;
                                    break;
                                case "null":
                                    result[i] = null;
                                    break;
                                default:
                                    result[i] = BooleanUtils.byteAsBoolean(Byte.parseByte(str));
                                    break;
                            }
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported type to handle as a boolean value " + t);
                    }
                }
            }

            return result;
        }
    },
    BIG_DECIMAL(Type.Binary, "java.util.BigDecimal") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            Binary.startBinary(builder);
            return Binary.endBinary(builder);
        }
    },
    BIG_INTEGER(Type.Binary, "java.util.BigInteger") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            Binary.startBinary(builder);
            return Binary.endBinary(builder);
        }
    },
    LOCAL_DATE(Type.Date, "java.time.LocalDate", "localdate") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Date.createDate(builder, DateUnit.DAY);
        }
    },
    LOCAL_TIME(Type.Time, "java.time.LocalTime", "localtime") {
        @Override
        public int writeType(FlatBufferBuilder builder) {
            return Time.createTime(builder, TimeUnit.NANOSECOND, 64);
        }
    },
    // LIST(),
    ;

    public static JsDataHandler getHandler(String deephavenType) {
        return HandlersHolder.HANDLERS.computeIfAbsent(deephavenType, type -> {
            throw new IllegalStateException("No handler registered for type " + type);
        });
    }

    /**
     * Helper to write numeric types that JS can represent in a consistent way.
     *
     * @param data the data passed from the user
     */
    private static Object[] writeSimpleNumbers(Object[] data) {
        // Scan until we find a non-number (and non-null) instance
        int firstNonNumber = 0;
        while (firstNonNumber < data.length) {
            if (!Js.typeof(data[firstNonNumber]).equals("number") && data[firstNonNumber] != null) {
                break;
            }
            firstNonNumber++;
        }
        if (firstNonNumber == data.length) {
            // everything was already a number or null
            return data;
        }
        // Copy preceding elements, then convert the rest
        Object[] result = new Object[data.length];
        System.arraycopy(data, 0, result, 0, firstNonNumber);
        for (int i = firstNonNumber; i < data.length; i++) {
            Double doubleFromData = doubleFromData(data[i]);
            if (doubleFromData != null) {
                result[i] = doubleFromData;
            } // otherwise leave it null
        }
        return result;
    }

    /**
     * Helper to read some js value as a double, so it can be handled as some type narrower than a js number. Do not use
     * this to handle wider types, check each possible type and fallback to this.
     *
     * @param data the data to turn into a js number
     * @return null or a java double
     */
    private static Double doubleFromData(Object data) {
        if (data == null) {
            return null;
        }
        if (Js.typeof(data).equals("number")) {
            return Js.asDouble(data);
        }
        if (data instanceof LongWrapper) {
            // we aren't expecting a long here, so bring it down to double
            return ((LongWrapper) data).asNumber();
        }
        String asString = data.toString().trim().replaceAll(",", "");
        if (asString.isEmpty()) {
            return null;
        }
        // last ditch, parse as double, let that throw if the data doesn't make sense
        return Double.parseDouble(asString);
    }

    private static class HandlersHolder {
        private static final Map<String, JsDataHandler> HANDLERS = new HashMap<>();
    }

    private static final String DEFAULT_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

    private static final int SEPARATOR_INDEX = DEFAULT_DATE_TIME_PATTERN.indexOf('T');

    private final byte arrowTypeType;
    private final String deephavenType;

    JsDataHandler(byte arrowTypeType, String... typeNames) {
        this.arrowTypeType = arrowTypeType;
        assert typeNames.length > 0 : "Must have at least one name";
        this.deephavenType = typeNames[0];
        for (int i = 0; i < typeNames.length; i++) {
            JsDataHandler existing = HandlersHolder.HANDLERS.put(typeNames[i], this);
            assert existing == null : "Handler already registered for type " + typeNames[i] + ": " + name();
        }
    }

    public byte typeType() {
        return arrowTypeType;
    }

    public String deephavenType() {
        return deephavenType;
    }

    public abstract int writeType(FlatBufferBuilder builder);

    /**
     * Normalizes data of the given type to be wrapped in chunks and sent to the server, performing any required type
     * coercion from JS types to what will be expected on the server. Primitive values must not be boxed,but instead
     * should be passed as Double or wrapped as Any (at runtime, these will result in the same thing).
     *
     * @param data the data to parse and normalize
     * @param context added detail about how the user directs the values to be parsed
     * @return the new array, or the existing one if no values were changed
     */
    public Object[] process(Object[] data, ParseContext context) {
        return data;
    }

    public static class ParseContext {
        public JsTimeZone timeZone;
        public String dateTimePattern = DEFAULT_DATE_TIME_PATTERN;
    }
}
