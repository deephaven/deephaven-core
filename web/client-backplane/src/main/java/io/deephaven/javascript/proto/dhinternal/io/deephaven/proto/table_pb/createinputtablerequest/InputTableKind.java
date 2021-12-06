package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.inputtablekind.InMemoryAppendOnly;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.inputtablekind.InMemoryKeyBacked;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest.inputtablekind.KindCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.CreateInputTableRequest.InputTableKind",
        namespace = JsPackage.GLOBAL)
public class InputTableKind {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InMemoryKeyBackedFieldType {
            @JsOverlay
            static InputTableKind.ToObjectReturnType.InMemoryKeyBackedFieldType create() {
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

        @JsOverlay
        static InputTableKind.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getInMemoryAppendOnly();

        @JsProperty
        InputTableKind.ToObjectReturnType.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

        @JsProperty
        void setInMemoryAppendOnly(Object inMemoryAppendOnly);

        @JsProperty
        void setInMemoryKeyBacked(
                InputTableKind.ToObjectReturnType.InMemoryKeyBackedFieldType inMemoryKeyBacked);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InMemoryKeyBackedFieldType {
            @JsOverlay
            static InputTableKind.ToObjectReturnType0.InMemoryKeyBackedFieldType create() {
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

        @JsOverlay
        static InputTableKind.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getInMemoryAppendOnly();

        @JsProperty
        InputTableKind.ToObjectReturnType0.InMemoryKeyBackedFieldType getInMemoryKeyBacked();

        @JsProperty
        void setInMemoryAppendOnly(Object inMemoryAppendOnly);

        @JsProperty
        void setInMemoryKeyBacked(
                InputTableKind.ToObjectReturnType0.InMemoryKeyBackedFieldType inMemoryKeyBacked);
    }

    public static native InputTableKind deserializeBinary(Uint8Array bytes);

    public static native InputTableKind deserializeBinaryFromReader(
            InputTableKind message, Object reader);

    public static native void serializeBinaryToWriter(InputTableKind message, Object writer);

    public static native InputTableKind.ToObjectReturnType toObject(
            boolean includeInstance, InputTableKind msg);

    public native void clearInMemoryAppendOnly();

    public native void clearInMemoryKeyBacked();

    public native InMemoryAppendOnly getInMemoryAppendOnly();

    public native InMemoryKeyBacked getInMemoryKeyBacked();

    public native KindCase getKindCase();

    public native boolean hasInMemoryAppendOnly();

    public native boolean hasInMemoryKeyBacked();

    public native Uint8Array serializeBinary();

    public native void setInMemoryAppendOnly();

    public native void setInMemoryAppendOnly(InMemoryAppendOnly value);

    public native void setInMemoryKeyBacked();

    public native void setInMemoryKeyBacked(InMemoryKeyBacked value);

    public native InputTableKind.ToObjectReturnType0 toObject();

    public native InputTableKind.ToObjectReturnType0 toObject(boolean includeInstance);
}
