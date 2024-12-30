//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.web.client.api.JsLazy;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom replacement for grpc-websockets transport that handles multiple grpc streams in a single websocket. All else
 * equal, this transport should be preferred to the default grpc-websockets transport, and in turn the fetch based
 * transport is usually superior to this.
 */
public class MultiplexedWebsocketTransport implements GrpcTransport {

    public static final String MULTIPLEX_PROTOCOL = "grpc-websockets-multiplex";
    public static final String SOCKET_PER_STREAM_PROTOCOL = "grpc-websockets";

    public static class Factory implements GrpcTransportFactory {
        @Override
        public GrpcTransport create(GrpcTransportOptions options) {
            return new MultiplexedWebsocketTransport(options);
        }

        @Override
        public boolean getSupportsClientStreaming() {
            return true;
        }
    }

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
        private final JsPropertyMap<HeaderValueUnion> metadata;

        public HeaderFrame(String path, JsPropertyMap<HeaderValueUnion> metadata) {
            this.path = path;
            this.metadata = metadata;
        }

        @Override
        public void send(WebSocket webSocket, int streamId) {
            final Uint8Array headerBytes;
            final StringBuilder str = new StringBuilder();
            metadata.set("grpc-websockets-path", HeaderValueUnion.of(path));
            metadata.forEach((key) -> {
                HeaderValueUnion value = metadata.get(key);
                if (value.isArray()) {
                    str.append(key).append(": ").append(value.asArray().join(", ")).append("\r\n");
                } else {
                    str.append(key).append(": ").append(value.asString()).append("\r\n");
                }
            });
            headerBytes = encodeASCII(str.toString());
            Int8Array payload = new Int8Array(headerBytes.byteLength + 4);
            new DataView(payload.buffer).setInt32(0, streamId);
            payload.set(headerBytes, 4);
            webSocket.send(payload);
        }

        @Override
        public void sendFallback(Transport transport) {
            transport.start(new BrowserHeaders(metadata));
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

    static class ActiveTransport {
        private static final Map<String, ActiveTransport> activeSockets = new HashMap<>();
        private final WebSocket webSocket;
        private boolean closing;
        private int activeCount = 0;

        /**
         * Gets a websocket transport for the grpc url.
         *
         * @param grpcUrl The URL to access - the full path will be used (with ws/s instead of http/s) to connect to the
         *        service, but only the protocol+host+port will be used to share the instance.
         * @return a websocket instance to use to connect, newly created if necessary
         */
        public static ActiveTransport get(String grpcUrl) {
            URL urlWrapper = new URL(grpcUrl);
            if (urlWrapper.protocol.equals("http:")) {
                urlWrapper.protocol = "ws:";
            } else {
                urlWrapper.protocol = "wss:";
            }
            String actualUrl = urlWrapper.toString();
            urlWrapper.pathname = "/";
            String key = urlWrapper.toString();
            return activeSockets.computeIfAbsent(key, url -> new ActiveTransport(key, actualUrl));
        }

        /**
         * @param key URL to use as the key entry to reuse the transport
         * @param actualUrl the url to connect to
         */
        private ActiveTransport(String key, String actualUrl) {
            this.webSocket = new WebSocket(actualUrl, new String[] {MULTIPLEX_PROTOCOL, SOCKET_PER_STREAM_PROTOCOL});

            webSocket.binaryType = "arraybuffer";

            webSocket.addEventListener("message", event -> {
                MessageEvent<ArrayBuffer> messageEvent = Js.uncheckedCast(event);
                // read the message, check if it was a GO_AWAY
                int streamId = new DataView(messageEvent.data, 0, 4).getInt32(0);
                if (streamId == Integer.MAX_VALUE) {
                    // Server sent equiv of H2 GO_AWAY, time to wrap up.
                    // ACK the message
                    new WebsocketFinishSignal().send(webSocket, Integer.MAX_VALUE);

                    // We can attempt to create new transport instances, but cannot use this one any longer for new
                    // streams (and any new one is likely to fail unless some new server is ready for us)
                    activeSockets.remove(key);

                    // Mark that this transport should be closed when existing streams finish
                    this.closing = true;
                    if (activeCount == 0) {
                        webSocket.close();
                    }
                }
            });
            webSocket.addEventListener("close", event -> {
                // socket is closed, make room for another to be created
                activeSockets.remove(key);
            });
        }

        private void retain() {
            activeCount++;
        }

        /**
         * May be called once per transport
         */
        private void release() {
            activeCount--;
            if (activeCount == 0 && closing) {
                webSocket.close();
            }
        }
    }

    private ActiveTransport transport;
    private final int streamId = nextStreamId++;
    private final List<QueuedEntry> sendQueue = new ArrayList<>();
    private final GrpcTransportOptions options;
    private final String path;

    private final JsLazy<Transport> alternativeTransport;

    private JsRunnable cleanup = JsRunnable.doNothing();

    public MultiplexedWebsocketTransport(GrpcTransportOptions options) {
        this.options = options;
        String url = options.url.toString();
        URL urlWrapper = new URL(url);
        // preserve the path to send as metadata, but still talk to the server with that path
        path = urlWrapper.pathname.substring(1);

        // note that we connect to the actual url so the server can inform us via subprotocols that it isn't supported,
        // but the global map removes the path as the key for each websocket
        transport = ActiveTransport.get(url);

        // prepare a fallback
        alternativeTransport = new JsLazy<>(() -> Grpc.WebsocketTransport.onInvoke().onInvoke(options.originalOptions));
    }

    @Override
    public void start(JsPropertyMap<HeaderValueUnion> metadata) {
        if (alternativeTransport.isAvailable()) {
            alternativeTransport.get().start(new BrowserHeaders(metadata));
            return;
        }
        this.transport.retain();

        if (transport.webSocket.readyState == WebSocket.CONNECTING) {
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
        transport.webSocket.addEventListener(eventName, listener);
        cleanup = cleanup.andThen(() -> transport.webSocket.removeEventListener(eventName, listener));
    }

    private void onOpen(Event event) {
        Object protocol = Js.asPropertyMap(transport.webSocket).get("protocol");
        if (protocol.equals(SOCKET_PER_STREAM_PROTOCOL)) {
            // delegate to plain websocket impl, try to dissuade future users of this server
            Transport transport = alternativeTransport.get();

            // close our own websocket
            this.transport.webSocket.close();

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
            sendQueue.get(i).send(transport.webSocket, streamId);
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

        if (transport != null) {
            // release our reference to the transport, last one out will close the socket (if needed)
            transport.release();
            transport = null;
        }
    }

    private void onClose(Event event) {
        if (alternativeTransport.isAvailable()) {
            // must be downgrading to fallback
            return;
        }
        // each grpc transport will handle this as an error
        options.onEnd.onEnd(new JsError("Unexpectedly closed " + Js.<CloseEvent>uncheckedCast(event).reason));
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
            options.onChunk.onChunk(new Uint8Array(messageEvent.data, 4));
            if (closed) {
                options.onEnd.onEnd(null);
                removeHandlers();
            }
        }
    }

    private void sendOrEnqueue(QueuedEntry e) {
        if (transport.webSocket.readyState == WebSocket.CONNECTING) {
            sendQueue.add(e);
        } else {
            e.send(transport.webSocket, streamId);
        }
    }
}
