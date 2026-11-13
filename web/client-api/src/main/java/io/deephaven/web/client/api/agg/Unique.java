//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsLiteral;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Computes the single distinct value within each aggregation group. A group with more than one distinct value gives
 * {@link #nonUniqueSentinel}. A group whose values are all null gives null, whatever the sentinel.
 */
@JsType(namespace = "dh.agg")
public final class Unique extends ColumnAggregation {
    @TsLiteral
    public final String type = "Unique";

    /**
     * Whether {@code null} is treated as a value for the purpose of determining if the values in the aggregation group
     * are unique. When {@code true}, a group containing both null and a single non-null value is considered non-unique.
     * Defaults to {@code false}.
     */
    @JsNullable
    public Boolean includeNulls;

    /**
     * The value to output for a group with more than one distinct value. A group whose values are all null still gives
     * null. Its type must suit the aggregated columns, for example an {@code "int"} sentinel for an int column.
     * Defaults to {@code null}.
     */
    @JsNullable
    public NonUniqueSentinelUnion nonUniqueSentinel;

    /**
     * The proto form of {@code sentinel}, or of the {@code null} sentinel if it is not set.
     */
    static AggSpec.AggSpecNonUniqueSentinel toProto(NonUniqueSentinelUnion sentinel) {
        if (sentinel == null) {
            return AggSpec.AggSpecNonUniqueSentinel.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }
        return sentinel.toProto();
    }

    /**
     * A {@link #nonUniqueSentinel} value, whose {@code type} names the column type it suits.
     */
    @TsUnion(anonymous = false, namespace = "dh.agg")
    @JsType(isNative = true, namespace = JsPackage.GLOBAL, name = "?")
    public interface NonUniqueSentinelUnion {

        @TsUnionMember
        @JsOverlay
        default NullSentinel asNullSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default StringSentinel asStringSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default IntSentinel asIntSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default LongSentinel asLongSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default FloatSentinel asFloatSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default DoubleSentinel asDoubleSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default BooleanSentinel asBooleanSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default ByteSentinel asByteSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default ShortSentinel asShortSentinel() {
            return Js.uncheckedCast(this);
        }

        @TsUnionMember
        @JsOverlay
        default CharSentinel asCharSentinel() {
            return Js.uncheckedCast(this);
        }

        /**
         * Helper to read the type discriminant from any sentinel variant. Not visible from JS/TS as part of this union,
         * but allows access to the {@code type} field that each sentinel type has.
         */
        @JsProperty
        String getType();

        @JsOverlay
        default AggSpec.AggSpecNonUniqueSentinel toProto() {
            AggSpec.AggSpecNonUniqueSentinel.Builder builder = AggSpec.AggSpecNonUniqueSentinel.newBuilder();
            switch (getType()) {
                case "null":
                    builder.setNullValue(NullValue.NULL_VALUE);
                    break;
                case "string":
                    builder.setStringValue(asStringSentinel().value);
                    break;
                case "int":
                    builder.setIntValue(whole("int", asIntSentinel().value, Integer.MIN_VALUE, Integer.MAX_VALUE));
                    break;
                case "long":
                    builder.setLongValue(asLongSentinel().value.getWrapped());
                    break;
                case "float":
                    builder.setFloatValue((float) asFloatSentinel().value);
                    break;
                case "double":
                    builder.setDoubleValue(asDoubleSentinel().value);
                    break;
                case "boolean":
                    builder.setBoolValue(asBooleanSentinel().value);
                    break;
                case "byte":
                    builder.setByteValue(whole("byte", asByteSentinel().value, Byte.MIN_VALUE, Byte.MAX_VALUE));
                    break;
                case "short":
                    builder.setShortValue(whole("short", asShortSentinel().value, Short.MIN_VALUE, Short.MAX_VALUE));
                    break;
                case "char": {
                    String value = asCharSentinel().value;
                    if (value == null || value.length() != 1) {
                        throw new IllegalArgumentException(
                                "A char non-unique sentinel must be a single character, not " + value);
                    }
                    builder.setCharValue(value.charAt(0));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown non-unique sentinel type " + getType());
            }
            return builder.build();
        }

        /** {@code value} as an int, if it is a whole number within [{@code min}, {@code max}]. */
        @JsOverlay
        static int whole(String type, double value, int min, int max) {
            if (value != Math.floor(value) || value < min || value > max) {
                throw new IllegalArgumentException(
                        "A " + type + " non-unique sentinel must be a whole number in [" + min + ", " + max + "], not "
                                + value);
            }
            return (int) value;
        }
    }

    /** Outputs {@code null} for a group without a single unique value. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class NullSentinel {
        @TsLiteral
        public final String type = "null";
    }

    /** Outputs {@code value} for a group without a single unique value, for a String column. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class StringSentinel {
        @TsLiteral
        public final String type = "string";
        public String value;
    }

    /**
     * Outputs {@code value} for an int column. Must be a whole number from -2147483648 through 2147483647.
     */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class IntSentinel {
        @TsLiteral
        public final String type = "int";
        public double value;
    }

    /** Outputs {@code value} for a long column. Accepts any long value. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class LongSentinel {
        @TsLiteral
        public final String type = "long";
        public LongWrapper value;
    }

    /** Outputs {@code value} for a float column. The value is sent as a 32-bit float, losing any extra precision. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class FloatSentinel {
        @TsLiteral
        public final String type = "float";
        public double value;
    }

    /** Outputs {@code value} for a double column. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class DoubleSentinel {
        @TsLiteral
        public final String type = "double";
        public double value;
    }

    /** Outputs {@code value} for a boolean column. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class BooleanSentinel {
        @TsLiteral
        public final String type = "boolean";
        public boolean value;
    }

    /** Outputs {@code value} for a byte column. Must be a whole number from -128 through 127. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class ByteSentinel {
        @TsLiteral
        public final String type = "byte";
        public double value;
    }

    /** Outputs {@code value} for a short column. Must be a whole number from -32768 through 32767. */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class ShortSentinel {
        @TsLiteral
        public final String type = "short";
        public double value;
    }

    /**
     * Outputs {@code value} for a char column. Must be a string of exactly one UTF-16 code unit, so the client rejects
     * a character outside the Basic Multilingual Plane, such as most emoji.
     */
    @TsInterface
    @JsType(namespace = "dh.agg")
    public static final class CharSentinel {
        @TsLiteral
        public final String type = "char";
        public String value;
    }
}
