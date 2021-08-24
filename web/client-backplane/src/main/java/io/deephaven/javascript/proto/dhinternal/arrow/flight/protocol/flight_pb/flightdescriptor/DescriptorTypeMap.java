package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.flightdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.FlightDescriptor.DescriptorTypeMap",
    namespace = JsPackage.GLOBAL)
public interface DescriptorTypeMap {
    @JsOverlay
    static DescriptorTypeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "CMD")
    double getCMD();

    @JsProperty(name = "PATH")
    double getPATH();

    @JsProperty(name = "UNKNOWN")
    double getUNKNOWN();

    @JsProperty(name = "CMD")
    void setCMD(double CMD);

    @JsProperty(name = "PATH")
    void setPATH(double PATH);

    @JsProperty(name = "UNKNOWN")
    void setUNKNOWN(double UNKNOWN);
}
