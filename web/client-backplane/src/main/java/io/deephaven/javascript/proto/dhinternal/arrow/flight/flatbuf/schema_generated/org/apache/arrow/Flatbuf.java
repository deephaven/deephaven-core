package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow;

import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Binary;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Bool;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Date;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Decimal;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Duration;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.FixedSizeBinary;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.FixedSizeList;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.FloatingPoint;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Int;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Interval;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.LargeBinary;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.LargeList;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.LargeUtf8;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.List;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Map;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Null;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Struct_;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Time;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Timestamp;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Union;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Utf8;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf",
        namespace = JsPackage.GLOBAL)
public class Flatbuf {
    @JsFunction
    public interface UnionListToTypeAccessorFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P1UnionType {
            @JsOverlay
            static Flatbuf.UnionListToTypeAccessorFn.P1UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default Binary asBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default Bool asBool() {
                return Js.cast(this);
            }

            @JsOverlay
            default Date asDate() {
                return Js.cast(this);
            }

            @JsOverlay
            default Decimal asDecimal() {
                return Js.cast(this);
            }

            @JsOverlay
            default Duration asDuration() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeBinary asFixedSizeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeList asFixedSizeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default FloatingPoint asFloatingPoint() {
                return Js.cast(this);
            }

            @JsOverlay
            default Int asInt() {
                return Js.cast(this);
            }

            @JsOverlay
            default Interval asInterval() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeBinary asLargeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeList asLargeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeUtf8 asLargeUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default List asList() {
                return Js.cast(this);
            }

            @JsOverlay
            default Map asMap() {
                return Js.cast(this);
            }

            @JsOverlay
            default Null asNull() {
                return Js.cast(this);
            }

            @JsOverlay
            default Struct_ asStruct_() {
                return Js.cast(this);
            }

            @JsOverlay
            default Time asTime() {
                return Js.cast(this);
            }

            @JsOverlay
            default Timestamp asTimestamp() {
                return Js.cast(this);
            }

            @JsOverlay
            default Union asUnion() {
                return Js.cast(this);
            }

            @JsOverlay
            default Utf8 asUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isBinary() {
                return (Object) this instanceof Binary;
            }

            @JsOverlay
            default boolean isBool() {
                return (Object) this instanceof Bool;
            }

            @JsOverlay
            default boolean isDate() {
                return (Object) this instanceof Date;
            }

            @JsOverlay
            default boolean isDecimal() {
                return (Object) this instanceof Decimal;
            }

            @JsOverlay
            default boolean isDuration() {
                return (Object) this instanceof Duration;
            }

            @JsOverlay
            default boolean isFixedSizeBinary() {
                return (Object) this instanceof FixedSizeBinary;
            }

            @JsOverlay
            default boolean isFixedSizeList() {
                return (Object) this instanceof FixedSizeList;
            }

            @JsOverlay
            default boolean isFloatingPoint() {
                return (Object) this instanceof FloatingPoint;
            }

            @JsOverlay
            default boolean isInt() {
                return (Object) this instanceof Int;
            }

            @JsOverlay
            default boolean isInterval() {
                return (Object) this instanceof Interval;
            }

            @JsOverlay
            default boolean isLargeBinary() {
                return (Object) this instanceof LargeBinary;
            }

            @JsOverlay
            default boolean isLargeList() {
                return (Object) this instanceof LargeList;
            }

            @JsOverlay
            default boolean isLargeUtf8() {
                return (Object) this instanceof LargeUtf8;
            }

            @JsOverlay
            default boolean isList() {
                return (Object) this instanceof List;
            }

            @JsOverlay
            default boolean isMap() {
                return (Object) this instanceof Map;
            }

            @JsOverlay
            default boolean isNull() {
                return (Object) this instanceof Null;
            }

            @JsOverlay
            default boolean isStruct_() {
                return (Object) this instanceof Struct_;
            }

            @JsOverlay
            default boolean isTime() {
                return (Object) this instanceof Time;
            }

            @JsOverlay
            default boolean isTimestamp() {
                return (Object) this instanceof Timestamp;
            }

            @JsOverlay
            default boolean isUnion() {
                return (Object) this instanceof Union;
            }

            @JsOverlay
            default boolean isUtf8() {
                return (Object) this instanceof Utf8;
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UnionType {
            @JsOverlay
            static Flatbuf.UnionListToTypeAccessorFn.UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default Binary asBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default Bool asBool() {
                return Js.cast(this);
            }

            @JsOverlay
            default Date asDate() {
                return Js.cast(this);
            }

            @JsOverlay
            default Decimal asDecimal() {
                return Js.cast(this);
            }

            @JsOverlay
            default Duration asDuration() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeBinary asFixedSizeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeList asFixedSizeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default FloatingPoint asFloatingPoint() {
                return Js.cast(this);
            }

            @JsOverlay
            default Int asInt() {
                return Js.cast(this);
            }

            @JsOverlay
            default Interval asInterval() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeBinary asLargeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeList asLargeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeUtf8 asLargeUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default List asList() {
                return Js.cast(this);
            }

            @JsOverlay
            default Map asMap() {
                return Js.cast(this);
            }

            @JsOverlay
            default Null asNull() {
                return Js.cast(this);
            }

            @JsOverlay
            default Struct_ asStruct_() {
                return Js.cast(this);
            }

            @JsOverlay
            default Time asTime() {
                return Js.cast(this);
            }

            @JsOverlay
            default Timestamp asTimestamp() {
                return Js.cast(this);
            }

            @JsOverlay
            default Union asUnion() {
                return Js.cast(this);
            }

            @JsOverlay
            default Utf8 asUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isBinary() {
                return (Object) this instanceof Binary;
            }

            @JsOverlay
            default boolean isBool() {
                return (Object) this instanceof Bool;
            }

            @JsOverlay
            default boolean isDate() {
                return (Object) this instanceof Date;
            }

            @JsOverlay
            default boolean isDecimal() {
                return (Object) this instanceof Decimal;
            }

            @JsOverlay
            default boolean isDuration() {
                return (Object) this instanceof Duration;
            }

            @JsOverlay
            default boolean isFixedSizeBinary() {
                return (Object) this instanceof FixedSizeBinary;
            }

            @JsOverlay
            default boolean isFixedSizeList() {
                return (Object) this instanceof FixedSizeList;
            }

            @JsOverlay
            default boolean isFloatingPoint() {
                return (Object) this instanceof FloatingPoint;
            }

            @JsOverlay
            default boolean isInt() {
                return (Object) this instanceof Int;
            }

            @JsOverlay
            default boolean isInterval() {
                return (Object) this instanceof Interval;
            }

            @JsOverlay
            default boolean isLargeBinary() {
                return (Object) this instanceof LargeBinary;
            }

            @JsOverlay
            default boolean isLargeList() {
                return (Object) this instanceof LargeList;
            }

            @JsOverlay
            default boolean isLargeUtf8() {
                return (Object) this instanceof LargeUtf8;
            }

            @JsOverlay
            default boolean isList() {
                return (Object) this instanceof List;
            }

            @JsOverlay
            default boolean isMap() {
                return (Object) this instanceof Map;
            }

            @JsOverlay
            default boolean isNull() {
                return (Object) this instanceof Null;
            }

            @JsOverlay
            default boolean isStruct_() {
                return (Object) this instanceof Struct_;
            }

            @JsOverlay
            default boolean isTime() {
                return (Object) this instanceof Time;
            }

            @JsOverlay
            default boolean isTimestamp() {
                return (Object) this instanceof Timestamp;
            }

            @JsOverlay
            default boolean isUnion() {
                return (Object) this instanceof Union;
            }

            @JsOverlay
            default boolean isUtf8() {
                return (Object) this instanceof Utf8;
            }
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Binary p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Bool p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Date p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Decimal p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Duration p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, FixedSizeBinary p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, FixedSizeList p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, FloatingPoint p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Int p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Interval p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, LargeBinary p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, LargeList p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, LargeUtf8 p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, List p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Map p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Null p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(
                double p0, Flatbuf.UnionListToTypeAccessorFn.P1UnionType p1);

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Struct_ p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Time p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Timestamp p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Union p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }

        @JsOverlay
        default Flatbuf.UnionListToTypeAccessorFn.UnionType onInvoke(double p0, Utf8 p1) {
            return onInvoke(p0, Js.<Flatbuf.UnionListToTypeAccessorFn.P1UnionType>uncheckedCast(p1));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionListToTypeUnionType {
        @JsOverlay
        static Flatbuf.UnionListToTypeUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Binary asBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default Bool asBool() {
            return Js.cast(this);
        }

        @JsOverlay
        default Date asDate() {
            return Js.cast(this);
        }

        @JsOverlay
        default Decimal asDecimal() {
            return Js.cast(this);
        }

        @JsOverlay
        default Duration asDuration() {
            return Js.cast(this);
        }

        @JsOverlay
        default FixedSizeBinary asFixedSizeBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default FixedSizeList asFixedSizeList() {
            return Js.cast(this);
        }

        @JsOverlay
        default FloatingPoint asFloatingPoint() {
            return Js.cast(this);
        }

        @JsOverlay
        default Int asInt() {
            return Js.cast(this);
        }

        @JsOverlay
        default Interval asInterval() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeBinary asLargeBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeList asLargeList() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeUtf8 asLargeUtf8() {
            return Js.cast(this);
        }

        @JsOverlay
        default List asList() {
            return Js.cast(this);
        }

        @JsOverlay
        default Map asMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default Null asNull() {
            return Js.cast(this);
        }

        @JsOverlay
        default Struct_ asStruct_() {
            return Js.cast(this);
        }

        @JsOverlay
        default Time asTime() {
            return Js.cast(this);
        }

        @JsOverlay
        default Timestamp asTimestamp() {
            return Js.cast(this);
        }

        @JsOverlay
        default Union asUnion() {
            return Js.cast(this);
        }

        @JsOverlay
        default Utf8 asUtf8() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBinary() {
            return (Object) this instanceof Binary;
        }

        @JsOverlay
        default boolean isBool() {
            return (Object) this instanceof Bool;
        }

        @JsOverlay
        default boolean isDate() {
            return (Object) this instanceof Date;
        }

        @JsOverlay
        default boolean isDecimal() {
            return (Object) this instanceof Decimal;
        }

        @JsOverlay
        default boolean isDuration() {
            return (Object) this instanceof Duration;
        }

        @JsOverlay
        default boolean isFixedSizeBinary() {
            return (Object) this instanceof FixedSizeBinary;
        }

        @JsOverlay
        default boolean isFixedSizeList() {
            return (Object) this instanceof FixedSizeList;
        }

        @JsOverlay
        default boolean isFloatingPoint() {
            return (Object) this instanceof FloatingPoint;
        }

        @JsOverlay
        default boolean isInt() {
            return (Object) this instanceof Int;
        }

        @JsOverlay
        default boolean isInterval() {
            return (Object) this instanceof Interval;
        }

        @JsOverlay
        default boolean isLargeBinary() {
            return (Object) this instanceof LargeBinary;
        }

        @JsOverlay
        default boolean isLargeList() {
            return (Object) this instanceof LargeList;
        }

        @JsOverlay
        default boolean isLargeUtf8() {
            return (Object) this instanceof LargeUtf8;
        }

        @JsOverlay
        default boolean isList() {
            return (Object) this instanceof List;
        }

        @JsOverlay
        default boolean isMap() {
            return (Object) this instanceof Map;
        }

        @JsOverlay
        default boolean isNull() {
            return (Object) this instanceof Null;
        }

        @JsOverlay
        default boolean isStruct_() {
            return (Object) this instanceof Struct_;
        }

        @JsOverlay
        default boolean isTime() {
            return (Object) this instanceof Time;
        }

        @JsOverlay
        default boolean isTimestamp() {
            return (Object) this instanceof Timestamp;
        }

        @JsOverlay
        default boolean isUnion() {
            return (Object) this instanceof Union;
        }

        @JsOverlay
        default boolean isUtf8() {
            return (Object) this instanceof Utf8;
        }
    }

    @JsFunction
    public interface UnionToTypeAccessorFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0UnionType {
            @JsOverlay
            static Flatbuf.UnionToTypeAccessorFn.P0UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default Binary asBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default Bool asBool() {
                return Js.cast(this);
            }

            @JsOverlay
            default Date asDate() {
                return Js.cast(this);
            }

            @JsOverlay
            default Decimal asDecimal() {
                return Js.cast(this);
            }

            @JsOverlay
            default Duration asDuration() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeBinary asFixedSizeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeList asFixedSizeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default FloatingPoint asFloatingPoint() {
                return Js.cast(this);
            }

            @JsOverlay
            default Int asInt() {
                return Js.cast(this);
            }

            @JsOverlay
            default Interval asInterval() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeBinary asLargeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeList asLargeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeUtf8 asLargeUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default List asList() {
                return Js.cast(this);
            }

            @JsOverlay
            default Map asMap() {
                return Js.cast(this);
            }

            @JsOverlay
            default Null asNull() {
                return Js.cast(this);
            }

            @JsOverlay
            default Struct_ asStruct_() {
                return Js.cast(this);
            }

            @JsOverlay
            default Time asTime() {
                return Js.cast(this);
            }

            @JsOverlay
            default Timestamp asTimestamp() {
                return Js.cast(this);
            }

            @JsOverlay
            default Union asUnion() {
                return Js.cast(this);
            }

            @JsOverlay
            default Utf8 asUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isBinary() {
                return (Object) this instanceof Binary;
            }

            @JsOverlay
            default boolean isBool() {
                return (Object) this instanceof Bool;
            }

            @JsOverlay
            default boolean isDate() {
                return (Object) this instanceof Date;
            }

            @JsOverlay
            default boolean isDecimal() {
                return (Object) this instanceof Decimal;
            }

            @JsOverlay
            default boolean isDuration() {
                return (Object) this instanceof Duration;
            }

            @JsOverlay
            default boolean isFixedSizeBinary() {
                return (Object) this instanceof FixedSizeBinary;
            }

            @JsOverlay
            default boolean isFixedSizeList() {
                return (Object) this instanceof FixedSizeList;
            }

            @JsOverlay
            default boolean isFloatingPoint() {
                return (Object) this instanceof FloatingPoint;
            }

            @JsOverlay
            default boolean isInt() {
                return (Object) this instanceof Int;
            }

            @JsOverlay
            default boolean isInterval() {
                return (Object) this instanceof Interval;
            }

            @JsOverlay
            default boolean isLargeBinary() {
                return (Object) this instanceof LargeBinary;
            }

            @JsOverlay
            default boolean isLargeList() {
                return (Object) this instanceof LargeList;
            }

            @JsOverlay
            default boolean isLargeUtf8() {
                return (Object) this instanceof LargeUtf8;
            }

            @JsOverlay
            default boolean isList() {
                return (Object) this instanceof List;
            }

            @JsOverlay
            default boolean isMap() {
                return (Object) this instanceof Map;
            }

            @JsOverlay
            default boolean isNull() {
                return (Object) this instanceof Null;
            }

            @JsOverlay
            default boolean isStruct_() {
                return (Object) this instanceof Struct_;
            }

            @JsOverlay
            default boolean isTime() {
                return (Object) this instanceof Time;
            }

            @JsOverlay
            default boolean isTimestamp() {
                return (Object) this instanceof Timestamp;
            }

            @JsOverlay
            default boolean isUnion() {
                return (Object) this instanceof Union;
            }

            @JsOverlay
            default boolean isUtf8() {
                return (Object) this instanceof Utf8;
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface UnionType {
            @JsOverlay
            static Flatbuf.UnionToTypeAccessorFn.UnionType of(Object o) {
                return Js.cast(o);
            }

            @JsOverlay
            default Binary asBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default Bool asBool() {
                return Js.cast(this);
            }

            @JsOverlay
            default Date asDate() {
                return Js.cast(this);
            }

            @JsOverlay
            default Decimal asDecimal() {
                return Js.cast(this);
            }

            @JsOverlay
            default Duration asDuration() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeBinary asFixedSizeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default FixedSizeList asFixedSizeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default FloatingPoint asFloatingPoint() {
                return Js.cast(this);
            }

            @JsOverlay
            default Int asInt() {
                return Js.cast(this);
            }

            @JsOverlay
            default Interval asInterval() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeBinary asLargeBinary() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeList asLargeList() {
                return Js.cast(this);
            }

            @JsOverlay
            default LargeUtf8 asLargeUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default List asList() {
                return Js.cast(this);
            }

            @JsOverlay
            default Map asMap() {
                return Js.cast(this);
            }

            @JsOverlay
            default Null asNull() {
                return Js.cast(this);
            }

            @JsOverlay
            default Struct_ asStruct_() {
                return Js.cast(this);
            }

            @JsOverlay
            default Time asTime() {
                return Js.cast(this);
            }

            @JsOverlay
            default Timestamp asTimestamp() {
                return Js.cast(this);
            }

            @JsOverlay
            default Union asUnion() {
                return Js.cast(this);
            }

            @JsOverlay
            default Utf8 asUtf8() {
                return Js.cast(this);
            }

            @JsOverlay
            default boolean isBinary() {
                return (Object) this instanceof Binary;
            }

            @JsOverlay
            default boolean isBool() {
                return (Object) this instanceof Bool;
            }

            @JsOverlay
            default boolean isDate() {
                return (Object) this instanceof Date;
            }

            @JsOverlay
            default boolean isDecimal() {
                return (Object) this instanceof Decimal;
            }

            @JsOverlay
            default boolean isDuration() {
                return (Object) this instanceof Duration;
            }

            @JsOverlay
            default boolean isFixedSizeBinary() {
                return (Object) this instanceof FixedSizeBinary;
            }

            @JsOverlay
            default boolean isFixedSizeList() {
                return (Object) this instanceof FixedSizeList;
            }

            @JsOverlay
            default boolean isFloatingPoint() {
                return (Object) this instanceof FloatingPoint;
            }

            @JsOverlay
            default boolean isInt() {
                return (Object) this instanceof Int;
            }

            @JsOverlay
            default boolean isInterval() {
                return (Object) this instanceof Interval;
            }

            @JsOverlay
            default boolean isLargeBinary() {
                return (Object) this instanceof LargeBinary;
            }

            @JsOverlay
            default boolean isLargeList() {
                return (Object) this instanceof LargeList;
            }

            @JsOverlay
            default boolean isLargeUtf8() {
                return (Object) this instanceof LargeUtf8;
            }

            @JsOverlay
            default boolean isList() {
                return (Object) this instanceof List;
            }

            @JsOverlay
            default boolean isMap() {
                return (Object) this instanceof Map;
            }

            @JsOverlay
            default boolean isNull() {
                return (Object) this instanceof Null;
            }

            @JsOverlay
            default boolean isStruct_() {
                return (Object) this instanceof Struct_;
            }

            @JsOverlay
            default boolean isTime() {
                return (Object) this instanceof Time;
            }

            @JsOverlay
            default boolean isTimestamp() {
                return (Object) this instanceof Timestamp;
            }

            @JsOverlay
            default boolean isUnion() {
                return (Object) this instanceof Union;
            }

            @JsOverlay
            default boolean isUtf8() {
                return (Object) this instanceof Utf8;
            }
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Binary p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Bool p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Date p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Decimal p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Duration p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(FixedSizeBinary p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(FixedSizeList p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(FloatingPoint p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Int p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Interval p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(LargeBinary p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(LargeList p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(LargeUtf8 p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(List p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Map p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Null p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Flatbuf.UnionToTypeAccessorFn.P0UnionType p0);

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Struct_ p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Time p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Timestamp p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Union p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }

        @JsOverlay
        default Flatbuf.UnionToTypeAccessorFn.UnionType onInvoke(Utf8 p0) {
            return onInvoke(Js.<Flatbuf.UnionToTypeAccessorFn.P0UnionType>uncheckedCast(p0));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionToTypeUnionType {
        @JsOverlay
        static Flatbuf.UnionToTypeUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Binary asBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default Bool asBool() {
            return Js.cast(this);
        }

        @JsOverlay
        default Date asDate() {
            return Js.cast(this);
        }

        @JsOverlay
        default Decimal asDecimal() {
            return Js.cast(this);
        }

        @JsOverlay
        default Duration asDuration() {
            return Js.cast(this);
        }

        @JsOverlay
        default FixedSizeBinary asFixedSizeBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default FixedSizeList asFixedSizeList() {
            return Js.cast(this);
        }

        @JsOverlay
        default FloatingPoint asFloatingPoint() {
            return Js.cast(this);
        }

        @JsOverlay
        default Int asInt() {
            return Js.cast(this);
        }

        @JsOverlay
        default Interval asInterval() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeBinary asLargeBinary() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeList asLargeList() {
            return Js.cast(this);
        }

        @JsOverlay
        default LargeUtf8 asLargeUtf8() {
            return Js.cast(this);
        }

        @JsOverlay
        default List asList() {
            return Js.cast(this);
        }

        @JsOverlay
        default Map asMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default Null asNull() {
            return Js.cast(this);
        }

        @JsOverlay
        default Struct_ asStruct_() {
            return Js.cast(this);
        }

        @JsOverlay
        default Time asTime() {
            return Js.cast(this);
        }

        @JsOverlay
        default Timestamp asTimestamp() {
            return Js.cast(this);
        }

        @JsOverlay
        default Union asUnion() {
            return Js.cast(this);
        }

        @JsOverlay
        default Utf8 asUtf8() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBinary() {
            return (Object) this instanceof Binary;
        }

        @JsOverlay
        default boolean isBool() {
            return (Object) this instanceof Bool;
        }

        @JsOverlay
        default boolean isDate() {
            return (Object) this instanceof Date;
        }

        @JsOverlay
        default boolean isDecimal() {
            return (Object) this instanceof Decimal;
        }

        @JsOverlay
        default boolean isDuration() {
            return (Object) this instanceof Duration;
        }

        @JsOverlay
        default boolean isFixedSizeBinary() {
            return (Object) this instanceof FixedSizeBinary;
        }

        @JsOverlay
        default boolean isFixedSizeList() {
            return (Object) this instanceof FixedSizeList;
        }

        @JsOverlay
        default boolean isFloatingPoint() {
            return (Object) this instanceof FloatingPoint;
        }

        @JsOverlay
        default boolean isInt() {
            return (Object) this instanceof Int;
        }

        @JsOverlay
        default boolean isInterval() {
            return (Object) this instanceof Interval;
        }

        @JsOverlay
        default boolean isLargeBinary() {
            return (Object) this instanceof LargeBinary;
        }

        @JsOverlay
        default boolean isLargeList() {
            return (Object) this instanceof LargeList;
        }

        @JsOverlay
        default boolean isLargeUtf8() {
            return (Object) this instanceof LargeUtf8;
        }

        @JsOverlay
        default boolean isList() {
            return (Object) this instanceof List;
        }

        @JsOverlay
        default boolean isMap() {
            return (Object) this instanceof Map;
        }

        @JsOverlay
        default boolean isNull() {
            return (Object) this instanceof Null;
        }

        @JsOverlay
        default boolean isStruct_() {
            return (Object) this instanceof Struct_;
        }

        @JsOverlay
        default boolean isTime() {
            return (Object) this instanceof Time;
        }

        @JsOverlay
        default boolean isTimestamp() {
            return (Object) this instanceof Timestamp;
        }

        @JsOverlay
        default boolean isUnion() {
            return (Object) this instanceof Union;
        }

        @JsOverlay
        default boolean isUtf8() {
            return (Object) this instanceof Utf8;
        }
    }

    public static native Flatbuf.UnionListToTypeUnionType unionListToType(
            int type, Flatbuf.UnionListToTypeAccessorFn accessor, double index);

    public static native Flatbuf.UnionToTypeUnionType unionToType(
            int type, Flatbuf.UnionToTypeAccessorFn accessor);
}
