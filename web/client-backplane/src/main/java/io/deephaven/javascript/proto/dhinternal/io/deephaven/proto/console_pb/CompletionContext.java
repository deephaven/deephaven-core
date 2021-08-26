package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.CompletionContext",
        namespace = JsPackage.GLOBAL)
public class CompletionContext {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static CompletionContext.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getTriggerCharacter();

        @JsProperty
        double getTriggerKind();

        @JsProperty
        void setTriggerCharacter(String triggerCharacter);

        @JsProperty
        void setTriggerKind(double triggerKind);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static CompletionContext.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getTriggerCharacter();

        @JsProperty
        double getTriggerKind();

        @JsProperty
        void setTriggerCharacter(String triggerCharacter);

        @JsProperty
        void setTriggerKind(double triggerKind);
    }

    public static native CompletionContext deserializeBinary(Uint8Array bytes);

    public static native CompletionContext deserializeBinaryFromReader(
            CompletionContext message, Object reader);

    public static native void serializeBinaryToWriter(CompletionContext message, Object writer);

    public static native CompletionContext.ToObjectReturnType toObject(
            boolean includeInstance, CompletionContext msg);

    public native String getTriggerCharacter();

    public native double getTriggerKind();

    public native Uint8Array serializeBinary();

    public native void setTriggerCharacter(String value);

    public native void setTriggerKind(double value);

    public native CompletionContext.ToObjectReturnType0 toObject();

    public native CompletionContext.ToObjectReturnType0 toObject(boolean includeInstance);
}
