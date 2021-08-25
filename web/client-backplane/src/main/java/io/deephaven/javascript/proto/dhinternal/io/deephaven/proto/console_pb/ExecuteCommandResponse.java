package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
    name = "dhinternal.io.deephaven.proto.console_pb.ExecuteCommandResponse",
    namespace = JsPackage.GLOBAL)
public class ExecuteCommandResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CreatedListFieldType {
            @JsOverlay
            static ExecuteCommandResponse.ToObjectReturnType.CreatedListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getName();

            @JsProperty
            String getType();

            @JsProperty
            void setName(String name);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ExecuteCommandResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ExecuteCommandResponse.ToObjectReturnType.CreatedListFieldType> getCreatedList();

        @JsProperty
        String getErrorMessage();

        @JsProperty
        JsArray<Object> getRemovedList();

        @JsProperty
        JsArray<Object> getUpdatedList();

        @JsOverlay
        default void setCreatedList(
            ExecuteCommandResponse.ToObjectReturnType.CreatedListFieldType[] createdList) {
            setCreatedList(
                Js.<JsArray<ExecuteCommandResponse.ToObjectReturnType.CreatedListFieldType>>uncheckedCast(
                    createdList));
        }

        @JsProperty
        void setCreatedList(
            JsArray<ExecuteCommandResponse.ToObjectReturnType.CreatedListFieldType> createdList);

        @JsProperty
        void setErrorMessage(String errorMessage);

        @JsProperty
        void setRemovedList(JsArray<Object> removedList);

        @JsOverlay
        default void setRemovedList(Object[] removedList) {
            setRemovedList(Js.<JsArray<Object>>uncheckedCast(removedList));
        }

        @JsProperty
        void setUpdatedList(JsArray<Object> updatedList);

        @JsOverlay
        default void setUpdatedList(Object[] updatedList) {
            setUpdatedList(Js.<JsArray<Object>>uncheckedCast(updatedList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface CreatedListFieldType {
            @JsOverlay
            static ExecuteCommandResponse.ToObjectReturnType0.CreatedListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getName();

            @JsProperty
            String getType();

            @JsProperty
            void setName(String name);

            @JsProperty
            void setType(String type);
        }

        @JsOverlay
        static ExecuteCommandResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ExecuteCommandResponse.ToObjectReturnType0.CreatedListFieldType> getCreatedList();

        @JsProperty
        String getErrorMessage();

        @JsProperty
        JsArray<Object> getRemovedList();

        @JsProperty
        JsArray<Object> getUpdatedList();

        @JsOverlay
        default void setCreatedList(
            ExecuteCommandResponse.ToObjectReturnType0.CreatedListFieldType[] createdList) {
            setCreatedList(
                Js.<JsArray<ExecuteCommandResponse.ToObjectReturnType0.CreatedListFieldType>>uncheckedCast(
                    createdList));
        }

        @JsProperty
        void setCreatedList(
            JsArray<ExecuteCommandResponse.ToObjectReturnType0.CreatedListFieldType> createdList);

        @JsProperty
        void setErrorMessage(String errorMessage);

        @JsProperty
        void setRemovedList(JsArray<Object> removedList);

        @JsOverlay
        default void setRemovedList(Object[] removedList) {
            setRemovedList(Js.<JsArray<Object>>uncheckedCast(removedList));
        }

        @JsProperty
        void setUpdatedList(JsArray<Object> updatedList);

        @JsOverlay
        default void setUpdatedList(Object[] updatedList) {
            setUpdatedList(Js.<JsArray<Object>>uncheckedCast(updatedList));
        }
    }

    public static native ExecuteCommandResponse deserializeBinary(Uint8Array bytes);

    public static native ExecuteCommandResponse deserializeBinaryFromReader(
        ExecuteCommandResponse message, Object reader);

    public static native void serializeBinaryToWriter(ExecuteCommandResponse message,
        Object writer);

    public static native ExecuteCommandResponse.ToObjectReturnType toObject(
        boolean includeInstance, ExecuteCommandResponse msg);

    public native VariableDefinition addCreated();

    public native VariableDefinition addCreated(VariableDefinition value, double index);

    public native VariableDefinition addCreated(VariableDefinition value);

    public native VariableDefinition addRemoved();

    public native VariableDefinition addRemoved(VariableDefinition value, double index);

    public native VariableDefinition addRemoved(VariableDefinition value);

    public native VariableDefinition addUpdated();

    public native VariableDefinition addUpdated(VariableDefinition value, double index);

    public native VariableDefinition addUpdated(VariableDefinition value);

    public native void clearCreatedList();

    public native void clearRemovedList();

    public native void clearUpdatedList();

    public native JsArray<VariableDefinition> getCreatedList();

    public native String getErrorMessage();

    public native JsArray<VariableDefinition> getRemovedList();

    public native JsArray<VariableDefinition> getUpdatedList();

    public native Uint8Array serializeBinary();

    public native void setCreatedList(JsArray<VariableDefinition> value);

    @JsOverlay
    public final void setCreatedList(VariableDefinition[] value) {
        setCreatedList(Js.<JsArray<VariableDefinition>>uncheckedCast(value));
    }

    public native void setErrorMessage(String value);

    public native void setRemovedList(JsArray<VariableDefinition> value);

    @JsOverlay
    public final void setRemovedList(VariableDefinition[] value) {
        setRemovedList(Js.<JsArray<VariableDefinition>>uncheckedCast(value));
    }

    public native void setUpdatedList(JsArray<VariableDefinition> value);

    @JsOverlay
    public final void setUpdatedList(VariableDefinition[] value) {
        setUpdatedList(Js.<JsArray<VariableDefinition>>uncheckedCast(value));
    }

    public native ExecuteCommandResponse.ToObjectReturnType0 toObject();

    public native ExecuteCommandResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
