/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.grpc;

import elemental2.core.ArrayBuffer;
import elemental2.core.DataView;
import elemental2.core.Int8Array;
import elemental2.core.JsError;
import elemental2.core.Uint8Array;
import elemental2.dom.CloseEvent;
import elemental2.dom.Event;
import elemental2.dom.EventListener;
import elemental2.dom.MessageEvent;
import elemental2.dom.URL;
import elemental2.dom.WebSocket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import io.deephaven.web.client.api.JsLazy;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom replacement for grpc-websockets transport that handles multiple grpc streams in a single websocket. All else
 * equal, this transport should be preferred to the default grpc-websockets transport, and in turn the fetch based
 * transport is usually superior to this.
 */
public class MultiplexedWebsocketTransport implements Transport {

    public static final String MULTIPLEX_PROTOCOL = "grpc-websockets-multiplex";
    public static final String SOCKET_PER_STREAM_PROTOCOL = "grpc-websockets";

    private static Uint8Array encodeASCII(String str) {
        Uint8Array encoded = new Uint8Array(str.length());
        for (int i = 0; i < str.length(); i++) {
            char charCode = str.charAt(i);
            // TODO validate
            encoded.setAt(i, (double) charCode);
        }
        return encoded;
    }

    private interface QueuedEntry {
        void send(WebSocket webSocket, int streamId);

        void sendFallback(Transport transport);
    }

    public static class HeaderFrame implements QueuedEntry {
        private final String path;
        private final BrowserHeaders metadata;

        public HeaderFrame(String path, BrowserHeaders metadata) {
            this.path = path;
            this.metadata = metadata;
        }

        @Override
        public void send(WebSocket webSocket, int streamId) {
            final Uint8Array headerBytes;
            final StringBuilder str = new StringBuilder();
            metadata.append("grpc-websockets-path", path);
            metadata.forEach((key, value) -> {
                str.append(key).append(": ").append(value.join(", ")).append("\r\n");
            });
            headerBytes = encodeASCII(str.toString());
            Int8Array payload = new Int8Array(headerBytes.byteLength + 4);
            new DataView(payload.buffer).setInt32(0, streamId);
            payload.set(headerBytes, 4);
            webSocket.send(payload);
        }

        @Override
        public void sendFallback(Transport transport) {
            transport.start(metadata);
        }
    }

    private static class GrpcMessageFrame implements QueuedEntry {
        private final Uint8Array msgBytes;

        public GrpcMessageFrame(Uint8Array msgBytes) {
            this.msgBytes = msgBytes;
        }

        @Override
        public void send(WebSocket webSocket, int streamId) {
            Int8Array payload = new Int8Array(msgBytes.byteLength + 5);
            new DataView(payload.buffer).setInt32(0, streamId);
            payload.setAt(4, 0d);
            payload.set(msgBytes, 5);
            webSocket.send(payload);
        }

        @Override
        public void sendFallback(Transport transport) {
            transport.sendMessage(msgBytes);
        }
    }

    private static class WebsocketFinishSignal implements QueuedEntry {
        @Override
        public void send(WebSocket webSocket, int streamId) {
            Uint8Array data = new Uint8Array(new double[] {0, 0, 0, 0, 1});
            streamId = streamId ^ (1 << 31);
            new DataView(data.buffer).setInt32(0, streamId);
            webSocket.send(data);
        }

        @Override
        public void sendFallback(Transport transport) {
            transport.finishSend();
        }
    }

    private static int nextStreamId = 0;
    private static final Map<String, WebSocket> activeSockets = new HashMap<>();

    private final WebSocket webSocket;
    private final int streamId = nextStreamId++;
    private final List<QueuedEntry> sendQueue = new ArrayList<>();
    private final TransportOptions options;
    private final String path;

    private final JsLazy<Transport> alternativeTransport;

    private JsRunnable cleanup = JsRunnable.doNothing();

    public MultiplexedWebsocketTransport(TransportOptions options, JsRunnable avoidMultiplexCallback) {
        this.options = options;
        String url = options.getUrl();
        URL urlWrapper = new URL(url);
        if (urlWrapper.protocol.equals("http:")) {
            urlWrapper.protocol = "ws:";
        } else {
            urlWrapper.protocol = "wss:";
        }
        // preserve the path to send as metadata, but still talk to the server with that path
        path = urlWrapper.pathname.substring(1);
        String actualUrl = urlWrapper.toString();
        urlWrapper.pathname = "/";
        String key = urlWrapper.toString();

        // note that we connect to the actual url so the server can inform us via subprotocols that it isn't supported,
        // but the global map removes the path as the key for each websocket
        webSocket = activeSockets.computeIfAbsent(key, ignore -> {
            WebSocket ws = new WebSocket(actualUrl, new String[] {MULTIPLEX_PROTOCOL, SOCKET_PER_STREAM_PROTOCOL});
            ws.binaryType = "arraybuffer";
            return ws;
        });

        // prepare a fallback
        alternativeTransport = new JsLazy<>(() -> {
            avoidMultiplexCallback.run();
            return Grpc.WebsocketTransport.onInvoke().onInvoke(options);
        });
    }

    @Override
    public void start(BrowserHeaders metadata) {
        if (alternativeTransport.isAvailable()) {
            alternativeTransport.get().start(metadata);
            return;
        }

        if (webSocket.readyState == WebSocket.CONNECTING) {
            // if the socket isn't open already, wait until the socket is
            // open, then flush the queue, otherwise everything will be
            // fine to send right away on the already open socket.
            addWebsocketEventListener("open", this::onOpen);
        }
        sendOrEnqueue(new HeaderFrame(path, metadata));

        addWebsocketEventListener("close", this::onClose);
        addWebsocketEventListener("error", this::onError);
        addWebsocketEventListener("message", this::onMessage);
    }

    private void addWebsocketEventListener(String eventName, EventListener listener) {
        webSocket.addEventListener(eventName, listener);
        cleanup = cleanup.andThen(() -> webSocket.removeEventListener(eventName, listener));
    }

    private void onOpen(Event event) {
        Object protocol = Js.asPropertyMap(webSocket).get("protocol");
        if (protocol.equals(SOCKET_PER_STREAM_PROTOCOL)) {
            // delegate to plain websocket impl, try to dissuade future users of this server
            Transport transport = alternativeTransport.get();

            // close our own websocket
            webSocket.close();

            // flush the queued items, which are now the new transport's problems - we'll forward all future work there
            // as well automatically
            for (int i = 0; i < sendQueue.size(); i++) {
                sendQueue.get(i).sendFallback(transport);
            }
            sendQueue.clear();
            return;
        } else if (!protocol.equals(MULTIPLEX_PROTOCOL)) {
            // give up, no way to handle this
            // TODO throw so the user can see this
            return;
        }
        for (int i = 0; i < sendQueue.size(); i++) {
            sendQueue.get(i).send(webSocket, streamId);
        }
        sendQueue.clear();
    }

    @Override
    public void sendMessage(Uint8Array msgBytes) {
        if (alternativeTransport.isAvailable()) {
            alternativeTransport.get().sendMessage(msgBytes);
            return;
        }

        sendOrEnqueue(new GrpcMessageFrame(msgBytes));
    }

    @Override
    public void finishSend() {
        if (alternativeTransport.isAvailable()) {
            alternativeTransport.get().finishSend();
            return;
        }

        sendOrEnqueue(new WebsocketFinishSignal());
    }

    @Override
    public void cancel() {
        if (alternativeTransport.isAvailable()) {
            alternativeTransport.get().cancel();
            return;
        }
        removeHandlers();
    }

    private void removeHandlers() {
        cleanup.run();
        cleanup = JsRunnable.doNothing();
    }

    private void onClose(Event event) {
        if (alternativeTransport.isAvailable()) {
            // must be downgrading to fallback
            return;
        }
        // each grpc transport will handle this as an error
        options.getOnEnd().onInvoke(new JsError("Unexpectedly closed " + Js.<CloseEvent>uncheckedCast(event).reason));
        removeHandlers();
    }

    private void onError(Event event) {

    }

    private void onMessage(Event event) {
        MessageEvent<ArrayBuffer> messageEvent = Js.uncheckedCast(event);
        // read the message, make sure it is for us, if so strip the stream id and fwd it
        int streamId = new DataView(messageEvent.data, 0, 4).getInt32(0);
        boolean closed;
        if (streamId < 0) {
            streamId = streamId ^ (1 << 31);
            closed = true;
        } else {
            closed = false;
        }
        if (streamId == this.streamId) {
            options.getOnChunk().onInvoke(new Uint8Array(messageEvent.data, 4), false);
            if (closed) {
                options.getOnEnd().onInvoke(null);
                removeHandlers();
            }
        }
    }

    private void sendOrEnqueue(QueuedEntry e) {
        if (webSocket.readyState == WebSocket.CONNECTING) {
            sendQueue.add(e);
        } else {
            e.send(webSocket, streamId);
        }
    }
}
