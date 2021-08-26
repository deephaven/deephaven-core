package io.deephaven.web.client.api;

import elemental2.core.Function;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.web.shared.fu.JsConsumer;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Java wrapper to deal with the distinct ResponseStream types that are emitted. Provides strongly typed methods for
 * cleaner Java consumption, that can be used to represent any of the structural types that are used for grpc methods.
 *
 * @param <T> payload that is emitted from the stream
 */
@JsType(isNative = true, name = "Object", namespace = JsPackage.GLOBAL)
public class ResponseStreamWrapper<T> {
    @JsType(isNative = true)
    public interface Status {
        @JsProperty
        double getCode();

        @JsProperty
        String getDetails();

        @JsProperty
        BrowserHeaders getMetadata();

        @JsOverlay
        default boolean isOk() {
            return getCode() == Code.OK;
        }
    }

    @JsOverlay
    public static <T> ResponseStreamWrapper<T> of(Object someJsObject) {
        // this cast is always legal, since ResponseStreamWrapper is declared to be Object
        return Js.cast(someJsObject);
    }

    public native void cancel();

    public native ResponseStreamWrapper<T> on(String type, Function function);

    @JsOverlay
    public final ResponseStreamWrapper<T> onStatus(JsConsumer<Status> handler) {
        return on("status", Js.cast(handler));
    }

    @JsOverlay
    public final ResponseStreamWrapper<T> onData(JsConsumer<T> handler) {
        return on("data", Js.cast(handler));
    }

    @JsOverlay
    public final ResponseStreamWrapper<T> onEnd(JsConsumer<Status> handler) {
        return on("end", Js.cast(handler));
    }
}
