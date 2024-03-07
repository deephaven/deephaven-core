//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import elemental2.core.Function;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.web.shared.fu.JsConsumer;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

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
        @JsOverlay
        static Status of(int code, String details, BrowserHeaders metadata) {
            return (Status) JsPropertyMap.of(
                    "code", (double) code,
                    "details", details,
                    "metadata", metadata);
        }

        @JsProperty
        int getCode();

        @JsProperty
        String getDetails();

        @JsProperty
        BrowserHeaders getMetadata();

        @JsOverlay
        default boolean isOk() {
            return getCode() == Code.OK;
        }

        @JsOverlay
        default boolean isTransportError() {
            return getCode() == Code.Internal || getCode() == Code.Unknown || getCode() == Code.Unavailable;
        }
    }
    @JsType(isNative = true)
    public interface ServiceError {
        @JsProperty
        int getCode();

        @JsProperty
        String getMessage();

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

    @JsOverlay
    public final ResponseStreamWrapper<T> onHeaders(JsConsumer<Object> handler) {
        try {
            return on("headers", Js.cast(handler));
        } catch (Exception ignore) {
            // most implementations don't offer this, we can ignore this error
            return this;
        }
    }
}
