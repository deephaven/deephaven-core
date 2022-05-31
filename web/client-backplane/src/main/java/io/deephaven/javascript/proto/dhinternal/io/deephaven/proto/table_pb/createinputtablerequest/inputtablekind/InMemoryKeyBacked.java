package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.inputtablekind;

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
        name = "dhinternal.io.deephaven.proto.table_pb.CreateInputTableRequest.InputTableKind.InMemoryKeyBacked",
        namespace = JsPackage.GLOBAL)
public class InMemoryKeyBacked {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static InMemoryKeyBacked.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeyColumnsList();

        @JsProperty
        void setKeyColumnsList(JsArray<String> keyColumnsList);

        @JsOverlay
        default void setKeyColumnsList(String[] keyColumnsList) {
            setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static InMemoryKeyBacked.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getKeyColumnsList();

        @JsProperty
        void setKeyColumnsList(JsArray<String> keyColumnsList);

        @JsOverlay
        default void setKeyColumnsList(String[] keyColumnsList) {
            setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(keyColumnsList));
        }
    }

    public static native InMemoryKeyBacked deserializeBinary(Uint8Array bytes);

    public static native InMemoryKeyBacked deserializeBinaryFromReader(
            InMemoryKeyBacked message, Object reader);

    public static native void serializeBinaryToWriter(InMemoryKeyBacked message, Object writer);

    public static native InMemoryKeyBacked.ToObjectReturnType toObject(
            boolean includeInstance, InMemoryKeyBacked msg);

    public native String addKeyColumns(String value, double index);

    public native String addKeyColumns(String value);

    public native void clearKeyColumnsList();

    public native JsArray<String> getKeyColumnsList();

    public native Uint8Array serializeBinary();

    public native void setKeyColumnsList(JsArray<String> value);

    @JsOverlay
    public final void setKeyColumnsList(String[] value) {
        setKeyColumnsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native InMemoryKeyBacked.ToObjectReturnType0 toObject();

    public native InMemoryKeyBacked.ToObjectReturnType0 toObject(boolean includeInstance);
}
