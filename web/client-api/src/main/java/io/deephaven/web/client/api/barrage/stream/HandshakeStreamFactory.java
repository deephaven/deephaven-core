//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import elemental2.core.Function;
import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service.BrowserFlightService;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service.ResponseStream;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeRequest;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeResponse;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb_service.BidirectionalStream;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb_service.FlightService;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.grpcweb.client.ClientRpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Client;
import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.InvokeRpcOptions;
import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.Request;
import io.deephaven.web.client.api.WorkerConnection;
import jsinterop.base.Js;

import java.util.HashMap;
import java.util.Map;

/**
 * Improbable-eng's grpc-web implementation doesn't pass headers to api callers, only trailers (which sometimes includes
 * headers) are included when calls fail, but never when successful. The current Flight auth v2 setup requires reading
 * headers from responses, but strictly speaking doesn't require reading them from all calls - we can make extra
 * FlightService/Handshake calls as long as we can read the response headers with them.
 * <p>
 * </p>
 * This class includes a custom implementation of the Handshake method that is able to notify callers about headers that
 * are received.
 */
public class HandshakeStreamFactory {

    private static final String DATA_EVENT_LISTENER_NAME = "data";
    private static final String END_EVENT_LISTENER_NAME = "end";
    private static final String STATUS_EVENT_LISTENER_NAME = "status";
    private static final String HEADERS_EVENT_LISTENER_NAME = "headers";

    public static BiDiStream<HandshakeRequest, HandshakeResponse> create(WorkerConnection connection) {
        return connection.<HandshakeRequest, HandshakeResponse>streamFactory().create(
                metadata -> {
                    Map<String, JsArray<Function>> listeners = listenerMap();
                    ClientRpcOptions options = ClientRpcOptions.create();
                    options.setHost(connection.flightServiceClient().serviceHost);
                    options.setTransport(null);// ts doesn't expose these two, stick with defaults for now
                    options.setDebug(false);
                    Client<HandshakeRequest, HandshakeResponse> client = Grpc.client(FlightService.Handshake,
                            (io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.ClientRpcOptions) options);
                    client.onEnd((status, statusMessage, trailers) -> {
                        listeners.get(STATUS_EVENT_LISTENER_NAME).forEach((item, index, arr) -> item.call(null,
                                ResponseStreamWrapper.Status.of(status, statusMessage, metadata)));
                        listeners.get(END_EVENT_LISTENER_NAME).forEach((item, index, arr) -> item.call(null,
                                ResponseStreamWrapper.Status.of(status, statusMessage, metadata)));
                        listeners.clear();
                    });
                    client.onMessage(message -> {
                        listeners.get(DATA_EVENT_LISTENER_NAME).forEach((item, index, arr) -> item.call(null, message));
                    });
                    client.onHeaders(headers -> {
                        listeners.get(HEADERS_EVENT_LISTENER_NAME)
                                .forEach((item, index, arr) -> item.call(null, headers));
                    });
                    client.start(metadata);

                    return new BidirectionalStream<HandshakeRequest, HandshakeResponse>() {
                        @Override
                        public void cancel() {
                            listeners.clear();
                            client.close();
                        }

                        @Override
                        public void end() {
                            client.finishSend();
                        }

                        @Override
                        public BidirectionalStream<HandshakeRequest, HandshakeResponse> on(String type,
                                Function handler) {
                            listeners.get(type).push(handler);
                            return this;
                        }

                        @Override
                        public BidirectionalStream<HandshakeRequest, HandshakeResponse> write(
                                HandshakeRequest message) {
                            client.send(message);
                            return this;
                        }
                    };
                },
                (first, metadata) -> {
                    Map<String, JsArray<Function>> listeners = listenerMap();
                    io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.InvokeRpcOptions<HandshakeRequest, HandshakeResponse> props =
                            Js.cast(InvokeRpcOptions.create());
                    props.setRequest(first);
                    props.setHost(connection.browserFlightServiceClient().serviceHost);
                    props.setMetadata(metadata);
                    props.setTransport(null);// ts doesnt expose these two, stick with defaults for now
                    props.setDebug(false);
                    props.setOnMessage(responseMessage -> {
                        listeners.get(DATA_EVENT_LISTENER_NAME)
                                .forEach((item, index, arr) -> item.call(null, responseMessage));
                    });
                    props.setOnEnd((status, statusMessage, trailers) -> {
                        listeners.get(STATUS_EVENT_LISTENER_NAME).forEach(
                                (item, index, arr) -> item.call(null,
                                        ResponseStreamWrapper.Status.of(status, statusMessage, metadata)));
                        listeners.get(END_EVENT_LISTENER_NAME).forEach(
                                (item, index, arr) -> item.call(null,
                                        ResponseStreamWrapper.Status.of(status, statusMessage, metadata)));
                        listeners.clear();
                    });
                    props.setOnHeaders(headers -> {
                        listeners.get(HEADERS_EVENT_LISTENER_NAME)
                                .forEach((item, index, arr) -> item.call(null, headers));
                    });
                    Request client = Grpc.invoke.onInvoke(BrowserFlightService.OpenHandshake, props);

                    return new ResponseStream<HandshakeResponse>() {
                        @Override
                        public void cancel() {
                            listeners.clear();
                            client.getClose().onInvoke();
                        }

                        @Override
                        public ResponseStream<HandshakeResponse> on(String type, Function handler) {
                            listeners.get(type).push(handler);
                            return this;
                        }
                    };
                },
                (next, headers, callback) -> connection.browserFlightServiceClient().nextHandshake(next, headers,
                        callback::apply),
                new HandshakeRequest());
    }

    private static Map<String, JsArray<Function>> listenerMap() {
        Map<String, JsArray<Function>> listeners = new HashMap<>();
        listeners.put(DATA_EVENT_LISTENER_NAME, new JsArray<>());
        listeners.put(END_EVENT_LISTENER_NAME, new JsArray<>());
        listeners.put(STATUS_EVENT_LISTENER_NAME, new JsArray<>());
        listeners.put(HEADERS_EVENT_LISTENER_NAME, new JsArray<>());
        return listeners;
    }
}
