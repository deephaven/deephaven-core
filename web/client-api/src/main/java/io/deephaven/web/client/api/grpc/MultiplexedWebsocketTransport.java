/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.grpc;

import elemental2.core.ArrayBuffer;
import elemental2.core.DataView;
import elemental2.core.Int32Array;
import elemental2.core.Int8Array;
import elemental2.core.Uint8Array;
import elemental2.dom.Event;
import elemental2.dom.MessageEvent;
import elemental2.dom.URL;
import elemental2.dom.WebSocket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiplexedWebsocketTransport implements Transport {

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
    }
    public static class HeaderFrame implements QueuedEntry {
        private final Uint8Array headerBytes;

        public HeaderFrame(String path, BrowserHeaders metadata) {
            StringBuilder str = new StringBuilder();
            metadata.append("grpc-websockets-path", path);
            metadata.forEach((key, value) -> {
                str.append(key).append(": ").append(value.join(", ")).append("\r\n");
            });
            headerBytes = encodeASCII(str.toString());
        }

        @Override
        public void send(WebSocket webSocket, int streamId) {
            Int8Array payload = new Int8Array(headerBytes.byteLength + 4);
            new DataView(payload.buffer).setInt32(0, streamId);
            payload.set(headerBytes, 4);
            webSocket.send(payload);
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

    }
    private static class WebsocketFinishSignal implements QueuedEntry {

        @Override
        public void send(WebSocket webSocket, int streamId) {
            Uint8Array data = new Uint8Array(new double[] {0, 0, 0, 0, 1});
            streamId = streamId ^ (1 << 31);
            new DataView(data.buffer).setInt32(0, streamId);
            webSocket.send(data);
        }
    }

    private static int nextStreamId = 0;
    private static final Map<String, WebSocket> activeSockets = new HashMap<>();

    private final WebSocket webSocket;
    private final int streamId = nextStreamId++;
    private final List<QueuedEntry> sendQueue = new ArrayList<>();
    private final TransportOptions options;
    private final String path;

    public MultiplexedWebsocketTransport(TransportOptions options) {
        this.options = options;
        String url = options.getUrl();
        URL urlWrapper = new URL(url);
        if (urlWrapper.protocol.equals("http:")) {
            urlWrapper.protocol = "ws:";
        } else {
            urlWrapper.protocol = "wss:";
        }
        path = urlWrapper.pathname.substring(1);
        urlWrapper.pathname = "/grpc-websocket";
        url = urlWrapper.toString();

        webSocket = activeSockets.computeIfAbsent(url, u -> {
            WebSocket ws = new WebSocket(u, "grpc-websockets-multiplex");
            ws.binaryType = "arraybuffer";
            return ws;
        });
    }

    @Override
    public void sendMessage(Uint8Array msgBytes) {
        sendOrEnqueue(new GrpcMessageFrame(msgBytes));
    }

    @Override
    public void finishSend() {
        sendOrEnqueue(new WebsocketFinishSignal());
    }

    @Override
    public void start(BrowserHeaders metadata) {
        if (webSocket.readyState == WebSocket.CONNECTING) {
            // if the socket isn't open already, wait until the socket is
            // open, then flush the queue, otherwise everything will be
            // fine to send right away on the already open socket.
            webSocket.addEventListener("open", this::onOpen);
        }
        sendOrEnqueue(new HeaderFrame(path, metadata));

        webSocket.addEventListener("close", this::onClose);
        webSocket.addEventListener("error", this::onError);
        webSocket.addEventListener("message", this::onMessage);
    }


    private void onClose(Event event) {
        options.getOnEnd().onInvoke(null);
    }

    private void onError(Event event) {

    }

    private void onMessage(Event event) {
        MessageEvent<ArrayBuffer> messageEvent = (MessageEvent<ArrayBuffer>) event;
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

    private void onOpen(Event event) {
        for (int i = 0; i < sendQueue.size(); i++) {
            QueuedEntry queuedEntry = sendQueue.get(i);
            queuedEntry.send(webSocket, streamId);
        }
        sendQueue.clear();
    }

    @Override
    public void cancel() {
        // TODO remove handlers, and close if we're the last one out
    }
}
