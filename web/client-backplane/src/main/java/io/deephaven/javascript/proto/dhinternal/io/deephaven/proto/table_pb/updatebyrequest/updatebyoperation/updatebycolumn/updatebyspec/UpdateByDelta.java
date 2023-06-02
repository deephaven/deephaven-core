package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UpdateByDeltaOptions;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByDelta",
        namespace = JsPackage.GLOBAL)
public class UpdateByDelta {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsOverlay
            static UpdateByDelta.ToObjectReturnType.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getNullBehavior();

            @JsProperty
            void setNullBehavior(double nullBehavior);
        }

        @JsOverlay
        static UpdateByDelta.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByDelta.ToObjectReturnType.OptionsFieldType getOptions();

        @JsProperty
        void setOptions(UpdateByDelta.ToObjectReturnType.OptionsFieldType options);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionsFieldType {
            @JsOverlay
            static UpdateByDelta.ToObjectReturnType0.OptionsFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getNullBehavior();

            @JsProperty
            void setNullBehavior(double nullBehavior);
        }

        @JsOverlay
        static UpdateByDelta.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByDelta.ToObjectReturnType0.OptionsFieldType getOptions();

        @JsProperty
        void setOptions(UpdateByDelta.ToObjectReturnType0.OptionsFieldType options);
    }

    public static native UpdateByDelta deserializeBinary(Uint8Array bytes);

    public static native UpdateByDelta deserializeBinaryFromReader(
            UpdateByDelta message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByDelta message, Object writer);

    public static native UpdateByDelta.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByDelta msg);

    public native void clearOptions();

    public native UpdateByDeltaOptions getOptions();

    public native boolean hasOptions();

    public native Uint8Array serializeBinary();

    public native void setOptions();

    public native void setOptions(UpdateByDeltaOptions value);

    public native UpdateByDelta.ToObjectReturnType0 toObject();

    public native UpdateByDelta.ToObjectReturnType0 toObject(boolean includeInstance);
}
