package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.BrowserFlight_pb_service.RequestStream",
    namespace = JsPackage.GLOBAL)
public interface RequestStream<T> {
    @JsFunction
    public interface OnHandlerFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static RequestStream.OnHandlerFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getDetails();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setDetails(String details);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(RequestStream.OnHandlerFn.P0Type p0);
    }

    void cancel();

    void end();

    RequestStream on(String type, RequestStream.OnHandlerFn handler);

    RequestStream write(T message);
}
