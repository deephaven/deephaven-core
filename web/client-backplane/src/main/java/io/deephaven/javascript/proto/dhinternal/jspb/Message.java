package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsConstructorFn;

@JsType(isNative = true, name = "dhinternal.jspb.Message", namespace = JsPackage.GLOBAL)
public class Message {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetFieldUnionType {
        @JsOverlay
        static Message.GetFieldUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default FieldValueArray asFieldValueArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface InitializeMessageIdUnionType {
        @JsOverlay
        static Message.InitializeMessageIdUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetFieldValueUnionType {
        @JsOverlay
        static Message.SetFieldValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default FieldValueArray asFieldValueArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetOneofFieldValueUnionType {
        @JsOverlay
        static Message.SetOneofFieldValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default FieldValueArray asFieldValueArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetWrapperFieldValueUnionType<T> {
        @JsOverlay
        static Message.SetWrapperFieldValueUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Map<Object, Object> asMap() {
            return Js.cast(this);
        }

        @JsOverlay
        default T asT() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isMap() {
            return (Object) this instanceof Map;
        }
    }

    @JsFunction
    public interface ToMapMapKeyGetterFn {
        String onInvoke(Object p0);
    }

    @JsFunction
    public interface ToMapToObjectFn {
        Object onInvoke(boolean p0, Message p1);
    }

    @JsFunction
    public interface ToObjectListToObjectFn<T> {
        Object onInvoke(boolean p0, T p1);
    }

    public static native void addToRepeatedField(
            Message msg, double fieldNumber, Object value, double index);

    public static native void addToRepeatedField(Message msg, double fieldNumber, Object value);

    @JsOverlay
    public static final <T> T addToRepeatedWrapperField(
            Message msg, double fieldNumber, T value, Class<? extends T> ctor, double index) {
        return addToRepeatedWrapperField(msg, fieldNumber, value, Js.asConstructorFn(ctor), index);
    }

    @JsOverlay
    public static final <T> T addToRepeatedWrapperField(
            Message msg, double fieldNumber, T value, Class<? extends T> ctor) {
        return addToRepeatedWrapperField(msg, fieldNumber, value, Js.asConstructorFn(ctor));
    }

    public static native <T> T addToRepeatedWrapperField(
            Message msg, double fieldNumber, T value, JsConstructorFn<? extends T> ctor, double index);

    public static native <T> T addToRepeatedWrapperField(
            Message msg, double fieldNumber, T value, JsConstructorFn<? extends T> ctor);

    public static native String bytesAsB64(Uint8Array bytes);

    public static native Uint8Array bytesAsU8(String str);

    public static native JsArray<String> bytesListAsB64(JsArray<Uint8Array> bytesList);

    @JsOverlay
    public static final JsArray<String> bytesListAsB64(Uint8Array[] bytesList) {
        return bytesListAsB64(Js.<JsArray<Uint8Array>>uncheckedCast(bytesList));
    }

    public static native JsArray<Uint8Array> bytesListAsU8(JsArray<String> strList);

    @JsOverlay
    public static final JsArray<Uint8Array> bytesListAsU8(String[] strList) {
        return bytesListAsU8(Js.<JsArray<String>>uncheckedCast(strList));
    }

    public static native <T> T clone(T msg);

    public static native <T> T cloneMessage(T msg);

    public static native boolean compareExtensions(Object extension1, Object extension2);

    public static native boolean compareFields(Object field1, Object field2);

    public static native double computeOneofCase(Message msg, JsArray<Double> oneof);

    @JsOverlay
    public static final double computeOneofCase(Message msg, double[] oneof) {
        return computeOneofCase(msg, Js.<JsArray<Double>>uncheckedCast(oneof));
    }

    public static native void copyInto(Message fromMessage, Message toMessage);

    public static native Message deserializeBinary(Uint8Array bytes);

    public static native Message deserializeBinaryFromReader(Message message, BinaryReader reader);

    public static native <T> T difference(T m1, T m2);

    public static native boolean equals(Message m1, Message m2);

    public static native Message.GetFieldUnionType getField(Message msg, double fieldNumber);

    public static native <T> T getFieldWithDefault(Message msg, double fieldNumber, T defaultValue);

    public static native Map<Object, Object> getMapField(
            Message msg, double fieldNumber, boolean noLazyCreate, Object valueCtor);

    public static native double getOptionalFloatingPointField(Message msg, double fieldNumber);

    public static native JsArray<Double> getRepeatedFloatingPointField(
            Message msg, double fieldNumber);

    @JsOverlay
    public static final <T> JsArray<T> getRepeatedWrapperField(
            Message msg, Class<? extends T> ctor, double fieldNumber) {
        return getRepeatedWrapperField(msg, Js.asConstructorFn(ctor), fieldNumber);
    }

    public static native <T> JsArray<T> getRepeatedWrapperField(
            Message msg, JsConstructorFn<? extends T> ctor, double fieldNumber);

    @JsOverlay
    public static final <T> T getWrapperField(
            Message msg, Class<? extends T> ctor, double fieldNumber, double required) {
        return getWrapperField(msg, Js.asConstructorFn(ctor), fieldNumber, required);
    }

    @JsOverlay
    public static final <T> T getWrapperField(
            Message msg, Class<? extends T> ctor, double fieldNumber) {
        return getWrapperField(msg, Js.asConstructorFn(ctor), fieldNumber);
    }

    public static native <T> T getWrapperField(
            Message msg, JsConstructorFn<? extends T> ctor, double fieldNumber, double required);

    public static native <T> T getWrapperField(
            Message msg, JsConstructorFn<? extends T> ctor, double fieldNumber);

    public static native void initialize(
            Message msg,
            JsArray<Object> data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields,
            JsArray<JsArray<Double>> oneofFields);

    public static native void initialize(
            Message msg,
            JsArray<Object> data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields);

    public static native void initialize(
            Message msg,
            JsArray<Object> data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot);

    @JsOverlay
    public static final void initialize(
            Message msg,
            JsArray<Object> data,
            String messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields,
            JsArray<JsArray<Double>> oneofFields) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot,
                repeatedFields,
                oneofFields);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            JsArray<Object> data,
            String messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot,
                repeatedFields);
    }

    @JsOverlay
    public static final void initialize(
            Message msg, JsArray<Object> data, String messageId, double suggestedPivot) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            JsArray<Object> data,
            double messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields,
            JsArray<JsArray<Double>> oneofFields) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot,
                repeatedFields,
                oneofFields);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            JsArray<Object> data,
            double messageId,
            double suggestedPivot,
            JsArray<Double> repeatedFields) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot,
                repeatedFields);
    }

    @JsOverlay
    public static final void initialize(
            Message msg, JsArray<Object> data, double messageId, double suggestedPivot) {
        initialize(
                msg,
                data,
                Js.<Message.InitializeMessageIdUnionType>uncheckedCast(messageId),
                suggestedPivot);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot,
            double[] repeatedFields,
            double[][] oneofFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields),
                Js.<JsArray<JsArray<Double>>>uncheckedCast(oneofFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot,
            double[] repeatedFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            Message.InitializeMessageIdUnionType messageId,
            double suggestedPivot) {
        initialize(msg, Js.<JsArray<Object>>uncheckedCast(data), messageId, suggestedPivot);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            String messageId,
            double suggestedPivot,
            double[] repeatedFields,
            double[][] oneofFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields),
                Js.<JsArray<JsArray<Double>>>uncheckedCast(oneofFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            String messageId,
            double suggestedPivot,
            double[] repeatedFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg, Object[] data, String messageId, double suggestedPivot) {
        initialize(msg, Js.<JsArray<Object>>uncheckedCast(data), messageId, suggestedPivot);
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            double messageId,
            double suggestedPivot,
            double[] repeatedFields,
            double[][] oneofFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields),
                Js.<JsArray<JsArray<Double>>>uncheckedCast(oneofFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg,
            Object[] data,
            double messageId,
            double suggestedPivot,
            double[] repeatedFields) {
        initialize(
                msg,
                Js.<JsArray<Object>>uncheckedCast(data),
                messageId,
                suggestedPivot,
                Js.<JsArray<Double>>uncheckedCast(repeatedFields));
    }

    @JsOverlay
    public static final void initialize(
            Message msg, Object[] data, double messageId, double suggestedPivot) {
        initialize(msg, Js.<JsArray<Object>>uncheckedCast(data), messageId, suggestedPivot);
    }

    public static native void registerMessageType(double id, Object constructor);

    public static native void serializeBinaryToWriter(Message message, BinaryWriter writer);

    @JsOverlay
    public static final void setField(Message msg, double fieldNumber, FieldValueArray value) {
        setField(msg, fieldNumber, Js.<Message.SetFieldValueUnionType>uncheckedCast(value));
    }

    public static native void setField(
            Message msg, double fieldNumber, Message.SetFieldValueUnionType value);

    @JsOverlay
    public static final void setField(Message msg, double fieldNumber, String value) {
        setField(msg, fieldNumber, Js.<Message.SetFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setField(Message msg, double fieldNumber, Uint8Array value) {
        setField(msg, fieldNumber, Js.<Message.SetFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setField(Message msg, double fieldNumber, boolean value) {
        setField(msg, fieldNumber, Js.<Message.SetFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setField(Message msg, double fieldNumber, double value) {
        setField(msg, fieldNumber, Js.<Message.SetFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, JsArray<Double> oneof, FieldValueArray value) {
        setOneofField(
                msg, fieldNumber, oneof, Js.<Message.SetOneofFieldValueUnionType>uncheckedCast(value));
    }

    public static native void setOneofField(
            Message msg,
            double fieldNumber,
            JsArray<Double> oneof,
            Message.SetOneofFieldValueUnionType value);

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, JsArray<Double> oneof, String value) {
        setOneofField(
                msg, fieldNumber, oneof, Js.<Message.SetOneofFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, JsArray<Double> oneof, Uint8Array value) {
        setOneofField(
                msg, fieldNumber, oneof, Js.<Message.SetOneofFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, JsArray<Double> oneof, boolean value) {
        setOneofField(
                msg, fieldNumber, oneof, Js.<Message.SetOneofFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, JsArray<Double> oneof, double value) {
        setOneofField(
                msg, fieldNumber, oneof, Js.<Message.SetOneofFieldValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, FieldValueArray value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, Message.SetOneofFieldValueUnionType value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, String value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, Uint8Array value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, boolean value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    @JsOverlay
    public static final void setOneofField(
            Message msg, double fieldNumber, double[] oneof, double value) {
        setOneofField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    public static native void setOneofWrapperField(
            Message msg, double fieldNumber, JsArray<Double> oneof, Object value);

    @JsOverlay
    public static final void setOneofWrapperField(
            Message msg, double fieldNumber, double[] oneof, Object value) {
        setOneofWrapperField(msg, fieldNumber, Js.<JsArray<Double>>uncheckedCast(oneof), value);
    }

    public static native <T> void setRepeatedWrapperField(
            Message msg, double fieldNumber, JsArray<T> value);

    @JsOverlay
    public static final <T> void setRepeatedWrapperField(Message msg, double fieldNumber, T[] value) {
        setRepeatedWrapperField(msg, fieldNumber, Js.<JsArray<T>>uncheckedCast(value));
    }

    public static native void setRepeatedWrapperField(Message msg, double fieldNumber);

    @JsOverlay
    public static final <T> void setWrapperField(
            Message msg, double fieldNumber, Map<Object, Object> value) {
        setWrapperField(
                msg, fieldNumber, Js.<Message.SetWrapperFieldValueUnionType<T>>uncheckedCast(value));
    }

    public static native <T> void setWrapperField(
            Message msg, double fieldNumber, Message.SetWrapperFieldValueUnionType<T> value);

    @JsOverlay
    public static final <T> void setWrapperField(Message msg, double fieldNumber, T value) {
        setWrapperField(
                msg, fieldNumber, Js.<Message.SetWrapperFieldValueUnionType<T>>uncheckedCast(value));
    }

    public static native void setWrapperField(Message msg, double fieldNumber);

    public static native void toMap(
            JsArray<Object> field,
            Message.ToMapMapKeyGetterFn mapKeyGetterFn,
            Message.ToMapToObjectFn toObjectFn,
            boolean includeInstance);

    public static native void toMap(
            JsArray<Object> field,
            Message.ToMapMapKeyGetterFn mapKeyGetterFn,
            Message.ToMapToObjectFn toObjectFn);

    public static native void toMap(
            JsArray<Object> field, Message.ToMapMapKeyGetterFn mapKeyGetterFn);

    @JsOverlay
    public static final void toMap(
            Object[] field,
            Message.ToMapMapKeyGetterFn mapKeyGetterFn,
            Message.ToMapToObjectFn toObjectFn,
            boolean includeInstance) {
        toMap(Js.<JsArray<Object>>uncheckedCast(field), mapKeyGetterFn, toObjectFn, includeInstance);
    }

    @JsOverlay
    public static final void toMap(
            Object[] field,
            Message.ToMapMapKeyGetterFn mapKeyGetterFn,
            Message.ToMapToObjectFn toObjectFn) {
        toMap(Js.<JsArray<Object>>uncheckedCast(field), mapKeyGetterFn, toObjectFn);
    }

    @JsOverlay
    public static final void toMap(Object[] field, Message.ToMapMapKeyGetterFn mapKeyGetterFn) {
        toMap(Js.<JsArray<Object>>uncheckedCast(field), mapKeyGetterFn);
    }

    public static native Object toObject(boolean includeInstance, Message msg);

    public static native void toObjectExtension(
            Message msg, Object obj, Object extensions, Object getExtensionFn, boolean includeInstance);

    public static native void toObjectExtension(
            Message msg, Object obj, Object extensions, Object getExtensionFn);

    public static native <T> JsArray<Object> toObjectList(
            JsArray<T> field,
            Message.ToObjectListToObjectFn<? super T> toObjectFn,
            boolean includeInstance);

    public static native <T> JsArray<Object> toObjectList(
            JsArray<T> field, Message.ToObjectListToObjectFn<? super T> toObjectFn);

    @JsOverlay
    public static final <T> JsArray<Object> toObjectList(
            T[] field, Message.ToObjectListToObjectFn<? super T> toObjectFn, boolean includeInstance) {
        return toObjectList(Js.<JsArray<T>>uncheckedCast(field), toObjectFn, includeInstance);
    }

    @JsOverlay
    public static final <T> JsArray<Object> toObjectList(
            T[] field, Message.ToObjectListToObjectFn<? super T> toObjectFn) {
        return toObjectList(Js.<JsArray<T>>uncheckedCast(field), toObjectFn);
    }

    public native Message cloneMessage();

    @JsMethod(name = "clone")
    public native Message clone_();

    public native <T> T getExtension(ExtensionFieldInfo<T> fieldInfo);

    public native String getJsPbMessageId();

    public native void readBinaryExtension(
            Message proto, BinaryReader reader, Object extensions, Object setExtensionFn);

    public native Uint8Array serializeBinary();

    public native void serializeBinaryExtensions(
            Message proto, BinaryWriter writer, Object extensions, Object getExtensionFn);

    public native <T> void setExtension(ExtensionFieldInfo<T> fieldInfo, T value);

    public native JsArray<Object> toArray();

    public native Object toObject();

    public native Object toObject(boolean includeInstance);

    @JsMethod(name = "toString")
    public native String toString_();
}
