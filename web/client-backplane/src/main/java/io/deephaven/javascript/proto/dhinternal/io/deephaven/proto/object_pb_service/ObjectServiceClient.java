package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.FetchObjectRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.FetchObjectResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.object_pb_service.ObjectServiceClient",
        namespace = JsPackage.GLOBAL)
public class ObjectServiceClient {
    @JsFunction
    public interface FetchObjectCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ObjectServiceClient.FetchObjectCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(ObjectServiceClient.FetchObjectCallbackFn.P0Type p0, FetchObjectResponse p1);
    }

    @JsFunction
    public interface FetchObjectMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ObjectServiceClient.FetchObjectMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                ObjectServiceClient.FetchObjectMetadata_or_callbackFn.P0Type p0, FetchObjectResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchObjectMetadata_or_callbackUnionType {
        @JsOverlay
        static ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ObjectServiceClient.FetchObjectMetadata_or_callbackFn asFetchObjectMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchObjectMetadata_or_callbackFn() {
            return (Object) this instanceof ObjectServiceClient.FetchObjectMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public ObjectServiceClient(String serviceHost, Object options) {}

    public ObjectServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse fetchObject(
            FetchObjectRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            ObjectServiceClient.FetchObjectCallbackFn callback) {
        return fetchObject(
                requestMessage,
                Js.<ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchObject(
            FetchObjectRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchObject(
                requestMessage,
                Js.<ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchObject(
            FetchObjectRequest requestMessage,
            ObjectServiceClient.FetchObjectMetadata_or_callbackFn metadata_or_callback,
            ObjectServiceClient.FetchObjectCallbackFn callback) {
        return fetchObject(
                requestMessage,
                Js.<ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchObject(
            FetchObjectRequest requestMessage,
            ObjectServiceClient.FetchObjectMetadata_or_callbackFn metadata_or_callback) {
        return fetchObject(
                requestMessage,
                Js.<ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse fetchObject(
            FetchObjectRequest requestMessage,
            ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType metadata_or_callback,
            ObjectServiceClient.FetchObjectCallbackFn callback);

    public native UnaryResponse fetchObject(
            FetchObjectRequest requestMessage,
            ObjectServiceClient.FetchObjectMetadata_or_callbackUnionType metadata_or_callback);
}
