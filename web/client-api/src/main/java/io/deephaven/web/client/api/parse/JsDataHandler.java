package io.deephaven.web.client.api.parse;

import elemental2.core.ArrayBuffer;
import elemental2.core.Float32Array;
import elemental2.core.Float64Array;
import elemental2.core.Int16Array;
import elemental2.core.Int32Array;
import elemental2.core.Int8Array;
import elemental2.core.JsDate;
import elemental2.core.TypedArray;
import elemental2.core.Uint16Array;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Binary;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.FixedSizeBinary;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.FloatingPoint;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Int;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Precision;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Type;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Utf8;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.web.client.api.subscription.QueryConstants.FALSE_BOOLEAN_AS_BYTE;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_BOOLEAN_AS_BYTE;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_BYTE;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_CHAR;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_DOUBLE;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_FLOAT;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_INT;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_LONG;
import static io.deephaven.web.client.api.subscription.QueryConstants.NULL_SHORT;
import static io.deephaven.web.client.api.subscription.QueryConstants.TRUE_BOOLEAN_AS_BYTE;

/**
 * Given the expected type of a column, pick one of the enum entries and use that to read the data into arrow buffers.
 */
public enum JsDataHandler {
    STRING(Type.Utf8, "java.lang.String", "string") {
        @Override
        public double writeType(Builder builder) {
            return Utf8.createUtf8(builder);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            int nullCount = 0;
            BitSet validity = new BitSet(data.length);
            Int32Array positions = makeBuffer(data.length + 1, 4, Int32Array::new);
            // work out the total length we'll need for the payload, plus padding
            int payloadLength =
                    Arrays.stream(data).filter(Objects::nonNull).map(Object::toString).mapToInt(String::length).sum();
            Uint8Array payload = makeBuffer(payloadLength);

            int lastOffset = 0;
            for (int i = 0; i < data.length; i++) {
                if (data[i] == null) {
                    nullCount++;
                    continue;
                }
                validity.set(i);
                String str = data[i].toString();
                positions.setAt(i, (double) lastOffset);
                byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                payload.set(Js.<double[]>uncheckedCast(bytes), lastOffset);
                lastOffset += bytes.length;
            }
            positions.setAt(data.length, (double) lastOffset);

            // validity, positions, payload
            addBuffer.apply(makeValidityBuffer(nullCount, validity));
            addBuffer.apply(new Uint8Array(positions.buffer));
            addBuffer.apply(payload);

            addNode.apply(new Node(data.length, nullCount));
        }
    },
    DATE_TIME(Type.Int, "io.deephaven.time.DateTime", "datetime") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 64, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            int nullCount = 0;
            BitSet validity = new BitSet(data.length);
            // using float because we can convert longs to
            // doubles, though not cheaply
            Float64Array payload = new Float64Array(data.length);
            for (int i = 0; i < data.length; i++) {

                final long dateValue;
                if (data[i] == null) {
                    dateValue = NULL_LONG;
                } else if (data[i] instanceof LongWrapper) {
                    dateValue = ((LongWrapper) data[i]).getWrapped();
                } else if (data[i] instanceof JsDate) {
                    dateValue = 1_000_000L * ((JsDate) data[i]).getDate();
                } else {
                    // fall back to assuming it is a string, figure out what it formats as
                    String str = data[i].toString().trim();
                    if (str.isEmpty()) {
                        dateValue = NULL_LONG;
                    } else {
                        // take the format string and the timezone, and solve for a date
                        dateValue = JsDateTimeFormat.parse(context.dateTimePattern, str, context.timeZone).getWrapped();
                    }
                }

                if (dateValue == NULL_LONG) {
                    nullCount++;
                } else {
                    validity.set(i);
                }
                payload.setAt(i, Double.longBitsToDouble(dateValue));
            }

            // validity, then payload
            addBuffer.apply(makeValidityBuffer(nullCount, validity));
            addBuffer.apply(new Uint8Array(payload.buffer));

            addNode.apply(new Node(data.length, nullCount));
        }
    },
    INTEGER(Type.Int, "int") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 32, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Int32Array.BYTES_PER_ELEMENT, NULL_INT, Int32Array::new);
        }
    },
    SHORT(Type.Int, "short") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 16, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Int16Array.BYTES_PER_ELEMENT, NULL_SHORT, Int16Array::new);
        }
    },
    LONG(Type.Int, "long") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 64, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            int nullCount = 0;
            BitSet validity = new BitSet(data.length);
            // using float because we can convert longs to
            // doubles, though not cheaply
            Float64Array payload = new Float64Array(data.length);
            for (int i = 0; i < data.length; i++) {
                final long value;
                if (data[i] == null) {
                    value = NULL_LONG;
                } else if (data[i] instanceof LongWrapper) {
                    value = ((LongWrapper) data[i]).getWrapped();
                } else if (Js.typeof(data[i]).equals("string")) {
                    String str = ((String) data[i]).trim().replaceAll(",", "");
                    if (str.length() == 0) {
                        value = NULL_LONG;
                    } else {
                        value = Long.parseLong(str);
                    }
                } else {
                    // fall back to whatever we can get - this is null safe, since we already tested for null above
                    value = (long) (double) JsDataHandler.doubleFromData(data[i]);
                }
                if (value == NULL_LONG) {
                    nullCount++;
                } else {
                    validity.set(i);
                }
                payload.setAt(i, Double.longBitsToDouble(value));
            }

            // validity, then payload
            addBuffer.apply(makeValidityBuffer(nullCount, validity));
            addBuffer.apply(new Uint8Array(payload.buffer));

            addNode.apply(new Node(data.length, nullCount));
        }
    },
    BYTE(Type.Int, "byte") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 8, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Int8Array.BYTES_PER_ELEMENT, NULL_BYTE, Int8Array::new);
        }
    },
    CHAR(Type.Int, "char") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 16, false);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Uint16Array.BYTES_PER_ELEMENT, NULL_CHAR, Uint16Array::new);
        }
    },
    FLOAT(Type.FloatingPoint, "float") {
        @Override
        public double writeType(Builder builder) {
            return FloatingPoint.createFloatingPoint(builder, Precision.SINGLE);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Float32Array.BYTES_PER_ELEMENT, NULL_FLOAT, Float32Array::new);
        }
    },
    DOUBLE(Type.FloatingPoint, "double") {
        @Override
        public double writeType(Builder builder) {
            return FloatingPoint.createFloatingPoint(builder, Precision.DOUBLE);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            writeSimpleNumbers(data, addNode, addBuffer, Float64Array.BYTES_PER_ELEMENT, NULL_DOUBLE,
                    Float64Array::new);
        }
    },
    BOOLEAN(Type.Int, "boolean", "bool") {
        @Override
        public double writeType(Builder builder) {
            return Int.createInt(builder, 8, true);
        }

        @Override
        public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode,
                JsConsumer<Uint8Array> addBuffer) {
            int nullCount = 0;
            BitSet validity = new BitSet(data.length);
            Int8Array payload = makeBuffer(data.length, Int8Array.BYTES_PER_ELEMENT, Int8Array::new);
            for (int i = 0; i < data.length; i++) {
                Object val = data[i];
                byte boolValue;
                if (val == null) {
                    boolValue = NULL_BOOLEAN_AS_BYTE;
                } else {
                    String typeof = Js.typeof(val);
                    switch (typeof) {
                        case "boolean":
                            boolValue = Js.isTruthy(val) ? TRUE_BOOLEAN_AS_BYTE : FALSE_BOOLEAN_AS_BYTE;
                            break;
                        case "number":
                            boolValue = Js.asByte(val);// checks the values to ensure it is a byte
                            break;
                        case "string":
                            String str = Js.asString(val);
                            switch (str.toLowerCase()) {
                                case "true":
                                    boolValue = TRUE_BOOLEAN_AS_BYTE;
                                    break;
                                case "false":
                                    boolValue = FALSE_BOOLEAN_AS_BYTE;
                                    break;
                                case "null":
                                    boolValue = NULL_BOOLEAN_AS_BYTE;
                                    break;
                                default:
                                    boolValue = Byte.parseByte(str);
                                    break;
                            }
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported type to handle as a boolean value " + typeof);
                    }
                }

                if (boolValue != FALSE_BOOLEAN_AS_BYTE && boolValue != TRUE_BOOLEAN_AS_BYTE
                        && boolValue != NULL_BOOLEAN_AS_BYTE) {
                    throw new IllegalArgumentException("Can't handle " + val + " as a boolean value");
                }

                // write the value, and mark non-null if necessary
                if (boolValue != NULL_BOOLEAN_AS_BYTE) {
                    validity.set(i);
                } else {
                    nullCount++;
                }
                payload.setAt(i, (double) boolValue);
            }

            // validity, then payload
            addBuffer.apply(makeValidityBuffer(nullCount, validity));
            addBuffer.apply(new Uint8Array(Js.<TypedArray>uncheckedCast(payload).buffer));

            addNode.apply(new Node(data.length, nullCount));
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
    LOCAL_DATE(Type.FixedSizeBinary, "java.time.LocalDate", "localdate") {
        @Override
        public double writeType(Builder builder) {
            return FixedSizeBinary.createFixedSizeBinary(builder, 6);
        }
    },
    LOCAL_TIME(Type.FixedSizeBinary, "java.time.LocalTime", "localtime") {
        @Override
        public double writeType(Builder builder) {
            return FixedSizeBinary.createFixedSizeBinary(builder, 7);
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
     * @param addNode a callback to append a node to the arrow payload
     * @param addBuffer a callback to append a buffer to the arrow payload
     * @param bytesPerElement the width of the arrow/deephaven type being created
     * @param nullValue the value to write in case of null data
     * @param bufferConstructor a constructor to produce a typedarray for the data being created
     */
    private static void writeSimpleNumbers(Object[] data, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer,
            double bytesPerElement, double nullValue, JsFunction<ArrayBuffer, ? extends TypedArray> bufferConstructor) {
        int nullCount = 0;
        BitSet validity = new BitSet(data.length);
        JsArrayLike<Double> payload = makeBuffer(data.length, bytesPerElement, bufferConstructor);
        for (int i = 0; i < data.length; i++) {
            Double doubleFromData = doubleFromData(data[i]);
            if (doubleFromData == null) {
                payload.setAt(i, nullValue);
                nullCount++;
            } else {
                payload.setAt(i, doubleFromData);
                validity.set(i);
            }
        }

        // validity, then payload
        addBuffer.apply(makeValidityBuffer(nullCount, validity));
        addBuffer.apply(new Uint8Array(Js.<TypedArray>uncheckedCast(payload).buffer));

        addNode.apply(new Node(data.length, nullCount));
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

    private static final Uint8Array EMPTY = new Uint8Array(0);

    private final int arrowTypeType;
    private final String deephavenType;

    JsDataHandler(int arrowTypeType, String... typeNames) {
        this.arrowTypeType = arrowTypeType;
        assert typeNames.length > 0 : "Must have at least one name";
        this.deephavenType = typeNames[0];
        for (int i = 0; i < typeNames.length; i++) {
            JsDataHandler existing = HandlersHolder.HANDLERS.put(typeNames[i], this);
            assert existing == null : "Handler already registered for type " + typeNames[i] + ": " + name();
        }
    }

    public int typeType() {
        return arrowTypeType;
    }

    public String deephavenType() {
        return deephavenType;
    }

    public abstract double writeType(Builder builder);

    public void write(Object[] data, ParseContext context, JsConsumer<Node> addNode, JsConsumer<Uint8Array> addBuffer) {
        throw new UnsupportedOperationException("Can't parse " + name());
    }

    public static class ParseContext {
        public JsTimeZone timeZone;
        public String dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss";
    }

    public static class Node {
        private final int length;
        private final int nullCount;

        public Node(int length, int nullCount) {
            this.length = length;
            this.nullCount = nullCount;
        }

        public int nullCount() {
            return nullCount;
        }

        public int length() {
            return length;
        }
    }

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

    private static <T> T makeBuffer(int elementCount, double bytesPerElement,
            JsFunction<ArrayBuffer, T> constructor) {
        return constructor.apply(makeBuffer(elementCount * (int) bytesPerElement).buffer);
    }

    private static Uint8Array makeBuffer(int length) {
        int bytesExtended = length & 0x7;
        if (bytesExtended > 0) {
            length += 8 - bytesExtended;
        }
        return new Uint8Array(length);
    }
}
