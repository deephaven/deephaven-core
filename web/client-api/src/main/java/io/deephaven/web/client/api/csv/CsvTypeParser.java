package io.deephaven.web.client.api.csv;

import elemental2.core.ArrayBuffer;
import elemental2.core.Float64Array;
import elemental2.core.Int32Array;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.*;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.subscription.QueryConstants;
import io.deephaven.web.shared.data.LocalTime;
import io.deephaven.web.shared.fu.JsConsumer;
import jsinterop.base.Js;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.function.Function;

/**
 * Helper class for parsing CSV data into Columns of the correct type.
 */
public class CsvTypeParser {
    private static final Uint8Array EMPTY = new Uint8Array(0);
    enum ArrowType implements CsvColumn {
        STRING(Type.Utf8, "java.lang.String") {
            @Override
            public double writeType(Builder builder) {
                return Utf8.createUtf8(builder);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                int nullCount = 0;
                BitSet nulls = new BitSet(strings.length);
                Int32Array positions = ArrowType.makeBuffer(strings.length + 1, 4, Int32Array::new);
                //work out the total length we'll need for the payload, plus padding
                int payloadLength = Arrays.stream(strings).filter(Objects::nonNull).mapToInt(String::length).sum();
                Uint8Array payload = makeBuffer(payloadLength);

                int lastOffset = 0;
                for (int i = 0; i < strings.length; i++) {
                    String str = strings[i];
                    positions.setAt(i, (double) lastOffset);
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    payload.set(Js.<double[]>uncheckedCast(bytes), lastOffset);
                    lastOffset += bytes.length;
                }
                positions.setAt(strings.length, (double) lastOffset);

                // validity, positions, payload
                addBuffer.apply(makeValidityBuffer(nullCount, nulls));
                addBuffer.apply(new Uint8Array(positions.buffer));
                addBuffer.apply(payload);

                addNode.apply(new Node(strings.length, nullCount));
            }
        },
        DATE_TIME(Type.Int, DATE_TIME_TYPE) {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 64, true);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                // TODO #1041 this type is expected to work
                throw new IllegalArgumentException("Can't serialize DateTime for CSV");
            }
        },
        INTEGER(Type.Int, "int") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 32, true);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                int nullCount = 0;
                BitSet nulls = new BitSet(strings.length);
                Int32Array payload = ArrowType.makeBuffer(strings.length, Int32Array.BYTES_PER_ELEMENT, Int32Array::new);
                for (int i = 0; i < strings.length; i++) {
                    if (strings[i] == null || strings[i].trim().isEmpty()) {
                        payload.setAt(i, (double) QueryConstants.NULL_INT);
                        nullCount++;
                    } else {
                        payload.setAt(i, (double) Integer.parseInt(strings[i].trim().replaceAll(",", "")));
                        nulls.set(i);
                    }
                }

                // validity, then payload
                addBuffer.apply(makeValidityBuffer(nullCount, nulls));
                addBuffer.apply(new Uint8Array(payload.buffer));

                addNode.apply(new Node(strings.length, nullCount));
            }
        },
        SHORT(Type.Int, "short") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 16, true);
            }
        },
        LONG(Type.Int, "long") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 64, true);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                int nullCount = 0;
                BitSet nulls = new BitSet(strings.length);
                Float64Array payload = new Float64Array(strings.length);// using float because we can convert longs to doubles, though not cheaply
                for (int i = 0; i < strings.length; i++) {
                    long value;
                    if (strings[i] == null || strings[i].trim().isEmpty()) {
                        value = QueryConstants.NULL_LONG;
                        nullCount++;
                    } else {
                        value = Long.parseLong(strings[i].trim().replaceAll(",", ""));
                        nulls.set(i);
                    }
                    payload.setAt(i, Double.longBitsToDouble(value));
                }

                // validity, then payload
                addBuffer.apply(makeValidityBuffer(nullCount, nulls));
                addBuffer.apply(new Uint8Array(payload.buffer));

                addNode.apply(new Node(strings.length, nullCount));
            }
        },
        BYTE(Type.Int, "byte") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 8, true);
            }
        },
        CHAR(Type.Int, "char") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 16, false);
            }
        },
        FLOAT(Type.FloatingPoint, "float") {
            @Override
            public double writeType(Builder builder) {
                return FloatingPoint.createFloatingPoint(builder, Precision.SINGLE);
            }
        },
        DOUBLE(Type.FloatingPoint, "double") {
            @Override
            public double writeType(Builder builder) {
                return FloatingPoint.createFloatingPoint(builder, Precision.DOUBLE);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                int nullCount = 0;
                BitSet nulls = new BitSet(strings.length);
                Float64Array payload = new Float64Array(strings.length);//64 bits, already aligned
                for (int i = 0; i < strings.length; i++) {
                    if (strings[i] == null || strings[i].trim().isEmpty()) {
                        payload.setAt(i, QueryConstants.NULL_DOUBLE);
                        nullCount++;
                    } else {
                        payload.setAt(i, Double.parseDouble(strings[i].trim().replaceAll(",", "")));
                        nulls.set(i);
                    }
                }

                // validity, then payload
                addBuffer.apply(makeValidityBuffer(nullCount, nulls));
                addBuffer.apply(new Uint8Array(payload.buffer));

                addNode.apply(new Node(strings.length, nullCount));
            }
        },

        BOOLEAN(Type.Int, "boolean") {
            @Override
            public double writeType(Builder builder) {
                return Int.createInt(builder, 8, true);
            }
        },

        BIG_DECIMAL(Type.Binary, "java.util.BigDecimal") {
            @Override
            public double writeType(Builder builder) {
                return Binary.createBinary(builder);
            }
        },
        BIG_INTEGER(Type.Binary, "java.util.BigInteger") {
            @Override
            public double writeType(Builder builder) {
                return Binary.createBinary(builder);
            }
        },

        LOCAL_DATE(Type.FixedSizeBinary, "java.time.LocalDate") {
            @Override
            public double writeType(Builder builder) {
                return FixedSizeBinary.createFixedSizeBinary(builder, 6);
            }
        },
        LOCAL_TIME(Type.FixedSizeBinary, "java.time.LocalTime") {
            @Override
            public double writeType(Builder builder) {
                return FixedSizeBinary.createFixedSizeBinary(builder, 7);
            }

            @Override
            public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
                // TODO #1041 this type is expected to work
                throw new IllegalArgumentException("Can't serialize DateTime for CSV");
            }
        },

        LIST(Type.List) {
            @Override
            public double writeType(Builder builder) {
                return List.createList(builder);
            }
        };

        private static Uint8Array makeValidityBuffer(int nullCount, BitSet nulls) {
            if (nullCount != 0) {
                byte[] nullsAsByteArray = nulls.toByteArray();
                Uint8Array nullsAsTypedArray = makeBuffer(nullsAsByteArray.length);
                nullsAsTypedArray.set(Js.<double[]>uncheckedCast(nullsAsByteArray));
                return nullsAsTypedArray;
            } else {
                return EMPTY;
            }
        }
        private static <T> T makeBuffer(int elementCount, double bytesPerElement, Function<ArrayBuffer, T> constructor) {
            return constructor.apply(makeBuffer(elementCount * (int) bytesPerElement).buffer);
        }
        public static Uint8Array makeBuffer(int length) {
            int bytesExtended = length & 0x7;
            if (bytesExtended > 0) {
                length += 8 - bytesExtended;
            }
            return new Uint8Array(length);
        }

        private final int typeType;
        private final String deephavenType;

        ArrowType(int typeType, String deephavenType) {
            this.typeType = typeType;
            this.deephavenType = deephavenType;
        }
        ArrowType(int typeType) {
            this.typeType = typeType;
            this.deephavenType = "unknown";//TODO remove this
        }

        @Override
        public String deephavenType() {
            return deephavenType;
        }

        @Override
        public int typeType() {
            return typeType;
        }

        @Override
        public abstract double writeType(Builder builder);
        @Override
        public void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
            throw new IllegalArgumentException("Type " + this + " not yet supported for CSV upload");
        }
    }
    // CSV Column types
    public static final String INTEGER = "int";
    public static final String LONG = "long";
    public static final String DOUBLE = "double";
    public static final String BOOLEAN = "bool";
    public static final String DATE_TIME = "datetime";
    public static final String LOCAL_TIME = "localtime";
    public static final String LOCAL_DATE = "localdate";

    // DbDateTime and LocalTime are not visible to this code
    private static final String DATE_TIME_TYPE = "io.deephaven.db.tables.utils.DBDateTime";
    private static final String LOCAL_TIME_TYPE = "java.time.LocalTime";

    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    private static final int SEPARATOR_INDEX = 10;

    public static class Node {
        private final int length;
        private final int nullCount;

        public Node(int length, int nullCount) {
            this.length = length;
            this.nullCount = nullCount;
        }

        public int length() {
            return length;
        }
        public int nullCount() {
            return nullCount;
        }
    }

    public interface CsvColumn {
        String deephavenType();
        int typeType();
        double writeType(Builder builder);

        void writeColumn(String[] strings, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer);
    }

    public static CsvColumn getColumn(String columnType) {
        switch (columnType) {
            case "string":
            case "String":
            case "java.lang.String": {
                return ArrowType.STRING;
            }
            case DATE_TIME:
            case DATE_TIME_TYPE: {
                return ArrowType.DATE_TIME;
            }
            case LONG: {
                return ArrowType.LONG;
            }
            case INTEGER: {
                return ArrowType.INTEGER;
            }
            case "byte": {
                return ArrowType.BYTE;
            }
            case "char": {
                return ArrowType.CHAR;
            }
            case "short": {
                return ArrowType.SHORT;
            }
            case DOUBLE: {
                return ArrowType.DOUBLE;
            }
            case "float": {
                return ArrowType.FLOAT;
            }
            case "boolean":
            case BOOLEAN: {
                return ArrowType.BOOLEAN;
            }
            case LOCAL_DATE: {
                return ArrowType.LOCAL_DATE;
            }
            case LOCAL_TIME: {
                return ArrowType.LOCAL_TIME;
            }
            case "java.util.BigDecimal": {
                return ArrowType.BIG_DECIMAL;
            }
            case "java.util.BigInteger": {
                return ArrowType.BIG_INTEGER;
            }
            default: {
                throw new IllegalArgumentException("Unsupported type " + columnType);
            }
        }
    }

    @SuppressWarnings("unused")//TODO #1041
    private static long parseDateTime(String str, String userTimeZone) {
        final String s = ensureSeparator(str);
        final int spaceIndex = s.indexOf(' ');
        final String dateTimeString;
        final String timeZoneString;
        if (spaceIndex == - 1) {
            // Zulu is an exception to the space rule
            if (s.endsWith("Z")) {
                dateTimeString = s.substring(0, s.length() - 1);
                timeZoneString = "Z";
            } else {
                dateTimeString = s;
                timeZoneString = null;
            }
        } else {
            dateTimeString =  s.substring(0, spaceIndex);
            timeZoneString = s.substring(spaceIndex + 1);
        }
        final String pattern = getSubsecondPattern(dateTimeString);
        final String tzString = timeZoneString == null ? userTimeZone : timeZoneString;
        final com.google.gwt.i18n.client.TimeZone timeZone = JsTimeZone.getTimeZone(tzString).unwrap();
        return JsDateTimeFormat.getFormat(pattern).parseWithTimezoneAsLong(dateTimeString, timeZone, JsTimeZone.needsDstAdjustment(timeZoneString));
    }

    // Updates the pattern for the correct number of subsecond digits 'S'
    private static String getSubsecondPattern(String s) {
        final int decimalIndex = s.indexOf('.');
        if (decimalIndex == -1) {
            // No subsecond digits
            return DATE_TIME_PATTERN;
        }
        final int numDigits = s.length() - decimalIndex - 1;
        final StringBuilder stringBuilder = new StringBuilder(numDigits);
        for (int i = 0; i < numDigits; i++) {
            stringBuilder.append('S');
        }
        return DATE_TIME_PATTERN + "." + stringBuilder.toString();
    }

    // Ensures that the 'T' separator character is in the the date time
    private static String ensureSeparator(String s) {
        if (s.charAt(SEPARATOR_INDEX) == ' ') {
            return s.replaceFirst(" ", "T");
        }
        return s;
    }

    @SuppressWarnings("unused")//TODO #1041
    private static LocalTime parseLocalTime(String s) {
        long dayNanos = 0;
        long subsecondNanos = 0;

        final int tIndex = s.indexOf('T');
        if (tIndex != -1) {
            dayNanos = 86400000000000L * Integer.parseInt(s.substring(0, tIndex));
            s = s.substring(tIndex + 1);
        }

        final int decimalIndex = s.indexOf('.');
        if (decimalIndex != -1) {
            subsecondNanos = parseNanos(s.substring(decimalIndex+1));
            s = s.substring(0, decimalIndex);
        }

        final String[] tokens = s.split(":");
        if (tokens.length == 2) {   //hh:mm
            return new LocalTime(Byte.parseByte(tokens[0]), Byte.parseByte(tokens[1]), (byte) 0, (int) (dayNanos + subsecondNanos));
        } else if (tokens.length == 3) {   //hh:mm:ss
            return new LocalTime(Byte.parseByte(tokens[0]), Byte.parseByte(tokens[1]), Byte.parseByte(tokens[2]), (int) (dayNanos + subsecondNanos));
        }

        return null;
    }

    private static long parseNanos(final String input) {
        long result = 0;
        for (int i=0; i<9; i++) {
            result *= 10;
            final int digit;
            if (i >= input.length()) {
                digit = 0;
            } else {
                digit = Character.digit(input.charAt(i), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid character for nanoseconds conversion: " + input.charAt(i));
                }
            }
            result += digit;
        }
        return result;
    }
}
