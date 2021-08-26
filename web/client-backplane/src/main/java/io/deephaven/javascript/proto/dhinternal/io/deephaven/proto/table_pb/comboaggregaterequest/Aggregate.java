package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest.Aggregate",
        namespace = JsPackage.GLOBAL)
public class Aggregate {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Aggregate.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        double getPercentile();

        @JsProperty
        double getType();

        @JsProperty
        boolean isAvgMedian();

        @JsProperty
        void setAvgMedian(boolean avgMedian);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setPercentile(double percentile);

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Aggregate.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnName();

        @JsProperty
        JsArray<String> getMatchPairsList();

        @JsProperty
        double getPercentile();

        @JsProperty
        double getType();

        @JsProperty
        boolean isAvgMedian();

        @JsProperty
        void setAvgMedian(boolean avgMedian);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setMatchPairsList(JsArray<String> matchPairsList);

        @JsOverlay
        default void setMatchPairsList(String[] matchPairsList) {
            setMatchPairsList(Js.<JsArray<String>>uncheckedCast(matchPairsList));
        }

        @JsProperty
        void setPercentile(double percentile);

        @JsProperty
        void setType(double type);
    }

    public static native Aggregate deserializeBinary(Uint8Array bytes);

    public static native Aggregate deserializeBinaryFromReader(Aggregate message, Object reader);

    public static native void serializeBinaryToWriter(Aggregate message, Object writer);

    public static native Aggregate.ToObjectReturnType toObject(
            boolean includeInstance, Aggregate msg);

    public native String addMatchPairs(String value, double index);

    public native String addMatchPairs(String value);

    public native void clearMatchPairsList();

    public native boolean getAvgMedian();

    public native String getColumnName();

    public native JsArray<String> getMatchPairsList();

    public native double getPercentile();

    public native double getType();

    public native Uint8Array serializeBinary();

    public native void setAvgMedian(boolean value);

    public native void setColumnName(String value);

    public native void setMatchPairsList(JsArray<String> value);

    @JsOverlay
    public final void setMatchPairsList(String[] value) {
        setMatchPairsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setPercentile(double value);

    public native void setType(double value);

    public native Aggregate.ToObjectReturnType0 toObject();

    public native Aggregate.ToObjectReturnType0 toObject(boolean includeInstance);
}
