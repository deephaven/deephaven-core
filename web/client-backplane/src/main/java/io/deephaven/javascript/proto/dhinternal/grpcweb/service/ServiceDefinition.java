package io.deephaven.javascript.proto.dhinternal.grpcweb.service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.service.ServiceDefinition",
        namespace = JsPackage.GLOBAL)
public interface ServiceDefinition {
    @JsOverlay
    static ServiceDefinition create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getServiceName();

    @JsProperty
    void setServiceName(String serviceName);
}
