package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.flightdescriptor.DescriptorTypeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.FlightDescriptor",
    namespace = JsPackage.GLOBAL)
public class FlightDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetCmdUnionType {
        @JsOverlay
        static FlightDescriptor.GetCmdUnionType of(Object o) {
            return Js.cast(o);
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
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetCmdValueUnionType {
        @JsOverlay
        static FlightDescriptor.SetCmdValueUnionType of(Object o) {
            return Js.cast(o);
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
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetCmdUnionType {
            @JsOverlay
            static FlightDescriptor.ToObjectReturnType.GetCmdUnionType of(Object o) {
                return Js.cast(o);
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
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static FlightDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FlightDescriptor.ToObjectReturnType.GetCmdUnionType getCmd();

        @JsProperty
        JsArray<String> getPathList();

        @JsProperty
        double getType();

        @JsProperty
        void setCmd(FlightDescriptor.ToObjectReturnType.GetCmdUnionType cmd);

        @JsOverlay
        default void setCmd(String cmd) {
            setCmd(Js.<FlightDescriptor.ToObjectReturnType.GetCmdUnionType>uncheckedCast(cmd));
        }

        @JsOverlay
        default void setCmd(Uint8Array cmd) {
            setCmd(Js.<FlightDescriptor.ToObjectReturnType.GetCmdUnionType>uncheckedCast(cmd));
        }

        @JsProperty
        void setPathList(JsArray<String> pathList);

        @JsOverlay
        default void setPathList(String[] pathList) {
            setPathList(Js.<JsArray<String>>uncheckedCast(pathList));
        }

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetCmdUnionType {
            @JsOverlay
            static FlightDescriptor.ToObjectReturnType0.GetCmdUnionType of(Object o) {
                return Js.cast(o);
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
            default boolean isString() {
                return (Object) this instanceof String;
            }

            @JsOverlay
            default boolean isUint8Array() {
                return (Object) this instanceof Uint8Array;
            }
        }

        @JsOverlay
        static FlightDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FlightDescriptor.ToObjectReturnType0.GetCmdUnionType getCmd();

        @JsProperty
        JsArray<String> getPathList();

        @JsProperty
        double getType();

        @JsProperty
        void setCmd(FlightDescriptor.ToObjectReturnType0.GetCmdUnionType cmd);

        @JsOverlay
        default void setCmd(String cmd) {
            setCmd(Js.<FlightDescriptor.ToObjectReturnType0.GetCmdUnionType>uncheckedCast(cmd));
        }

        @JsOverlay
        default void setCmd(Uint8Array cmd) {
            setCmd(Js.<FlightDescriptor.ToObjectReturnType0.GetCmdUnionType>uncheckedCast(cmd));
        }

        @JsProperty
        void setPathList(JsArray<String> pathList);

        @JsOverlay
        default void setPathList(String[] pathList) {
            setPathList(Js.<JsArray<String>>uncheckedCast(pathList));
        }

        @JsProperty
        void setType(double type);
    }

    public static DescriptorTypeMap DescriptorType;

    public static native FlightDescriptor deserializeBinary(Uint8Array bytes);

    public static native FlightDescriptor deserializeBinaryFromReader(
        FlightDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(FlightDescriptor message, Object writer);

    public static native FlightDescriptor.ToObjectReturnType toObject(
        boolean includeInstance, FlightDescriptor msg);

    public native String addPath(String value, double index);

    public native String addPath(String value);

    public native void clearPathList();

    public native FlightDescriptor.GetCmdUnionType getCmd();

    public native String getCmd_asB64();

    public native Uint8Array getCmd_asU8();

    public native JsArray<String> getPathList();

    public native double getType();

    public native Uint8Array serializeBinary();

    public native void setCmd(FlightDescriptor.SetCmdValueUnionType value);

    @JsOverlay
    public final void setCmd(String value) {
        setCmd(Js.<FlightDescriptor.SetCmdValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void setCmd(Uint8Array value) {
        setCmd(Js.<FlightDescriptor.SetCmdValueUnionType>uncheckedCast(value));
    }

    public native void setPathList(JsArray<String> value);

    @JsOverlay
    public final void setPathList(String[] value) {
        setPathList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setType(double value);

    public native FlightDescriptor.ToObjectReturnType0 toObject();

    public native FlightDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
