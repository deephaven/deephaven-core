//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import elemental2.core.Function;
import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.function.IntSupplier;
import java.util.function.Supplier;

public abstract class BiDiStream<Req, Resp> {
    public interface BiDiStreamFactory {
        /**
         * should return a BidirectionalStream of some flavor
         */
        Object openBiDiStream(BrowserHeaders headers);
    }
    public interface OpenStreamFactory<Req> {
        /**
         * Should return a ResponseStream of some flavor
         */
        Object openStream(Req firstPayload, BrowserHeaders headers);
    }
    public interface NextStreamMessageFactory<Req> {
        /**
         * Should return a unary stream, handle the callback
         */
        void nextStreamMessage(Req nextPayload, BrowserHeaders headers, JsBiConsumer<Object, Object> callback);
    }
    public static class Factory<ReqT, RespT> {
        private final boolean supportsClientStreaming;
        private final Supplier<BrowserHeaders> headers;
        private final IntSupplier nextIntTicket;

        public Factory(boolean supportsClientStreaming, Supplier<BrowserHeaders> headers, IntSupplier nextIntTicket) {
            this.supportsClientStreaming = supportsClientStreaming;
            this.headers = headers;
            this.nextIntTicket = nextIntTicket;
        }

        public BiDiStream<ReqT, RespT> create(
                BiDiStreamFactory bidirectionalStream,
                OpenStreamFactory<ReqT> openEmulatedStream,
                NextStreamMessageFactory<ReqT> nextEmulatedStream,
                ReqT emptyReq) {
            if (supportsClientStreaming) {
                return bidi(bidirectionalStream.openBiDiStream(headers.get()));
            } else {
                return new EmulatedBiDiStream<>(
                        openEmulatedStream,
                        nextEmulatedStream,
                        emptyReq,
                        nextIntTicket.getAsInt(),
                        headers);
            }
        }
    }

    public static <Req, Resp> BiDiStream<Req, Resp> of(
            BiDiStreamFactory bidirectionalStream,
            OpenStreamFactory<Req> openEmulatedStream,
            NextStreamMessageFactory<Req> nextEmulatedStream,
            Req emptyReq,
            Supplier<BrowserHeaders> headers,
            IntSupplier nextIntTicket,
            boolean useWebsocket) {
        if (useWebsocket) {
            return bidi(bidirectionalStream.openBiDiStream(headers.get()));
        } else {
            return new EmulatedBiDiStream<>(
                    openEmulatedStream,
                    nextEmulatedStream,
                    emptyReq,
                    nextIntTicket.getAsInt(),
                    headers);
        }
    }

    public static <Req, Resp> BiDiStream<Req, Resp> bidi(Object bidirectionalStream) {
        return new WebsocketBiDiStream<>(Js.cast(bidirectionalStream));
    }

    public abstract void send(Req payload);

    public abstract void cancel();

    public abstract void end();

    public abstract void onData(JsConsumer<Resp> handler);

    public abstract void onStatus(JsConsumer<ResponseStreamWrapper.Status> handler);

    public abstract void onEnd(JsConsumer<ResponseStreamWrapper.Status> handler);

    public abstract void onHeaders(JsConsumer<Object> handler);

    static class WebsocketBiDiStream<T, U> extends BiDiStream<T, U> {
        @JsType(isNative = true, name = "Object", namespace = JsPackage.GLOBAL)
        private static class BidirectionalStreamWrapper<ReqT, ResT> {
            native void cancel();

            native void end();

            native BidirectionalStreamWrapper<ReqT, ResT> on(String type, Function handler);

            native BidirectionalStreamWrapper<ReqT, ResT> write(ReqT message);
        }

        private final BidirectionalStreamWrapper<T, U> wrapped;

        WebsocketBiDiStream(BidirectionalStreamWrapper<T, U> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void send(T payload) {
            wrapped.write(payload);
        }

        @Override
        public void cancel() {
            wrapped.cancel();
        }

        @Override
        public void end() {
            wrapped.end();
        }

        @Override
        public void onData(JsConsumer<U> handler) {
            wrapped.on("data", Js.cast(handler));
        }

        @Override
        public void onStatus(JsConsumer<ResponseStreamWrapper.Status> handler) {
            wrapped.on("status", Js.cast(handler));
        }

        @Override
        public void onEnd(JsConsumer<ResponseStreamWrapper.Status> handler) {
            wrapped.on("end", Js.cast(handler));
        }

        @Override
        public void onHeaders(JsConsumer<Object> handler) {
            try {
                wrapped.on("headers", Js.cast(handler));
            } catch (Exception ignored) {
                // most implementations don't offer this, we can ignore this error
            }
        }
    }

    static class EmulatedBiDiStream<T, U> extends BiDiStream<T, U> {
        private final JsFunction<T, ResponseStreamWrapper<U>> responseStreamFactory;
        private final JsArray<JsConsumer<ResponseStreamWrapper<U>>> pending = new JsArray<>();
        private final T emptyReq;
        private final int intTicket;

        private ResponseStreamWrapper<U> responseStream;
        private final NextStreamMessageFactory<T> nextWrapper;
        private final Supplier<BrowserHeaders> headers;

        private int nextSeq = 0;

        EmulatedBiDiStream(OpenStreamFactory<T> responseStreamFactory, NextStreamMessageFactory<T> nextWrapper,
                T emptyReq,
                int intTicket, Supplier<BrowserHeaders> headers) {
            this.responseStreamFactory =
                    firstReq -> ResponseStreamWrapper.of(responseStreamFactory.openStream(firstReq, makeHeaders()));
            this.nextWrapper = nextWrapper;
            this.emptyReq = emptyReq;
            this.intTicket = intTicket;
            this.headers = headers;
        }

        @Override
        public void send(T payload) {
            if (responseStream == null) {
                responseStream = responseStreamFactory.apply(payload);
                pending.forEach((p0, p1) -> {
                    p0.apply(responseStream);
                    return null;
                });
                pending.length = 0;
            } else {
                // TODO #730 handle failure of this call
                nextWrapper.nextStreamMessage(payload, makeHeaders(), (failure, success) -> {
                });
            }
        }

        private BrowserHeaders makeHeaders() {
            BrowserHeaders nextHeaders = new BrowserHeaders(headers.get());
            nextHeaders.set("x-deephaven-stream-sequence", "" + nextSeq++);
            nextHeaders.set("x-deephaven-stream-ticket", "" + intTicket);
            return nextHeaders;
        }

        @Override
        public void cancel() {
            // no need to hang up
            if (responseStream != null) {
                responseStream.cancel();
            }
        }

        @Override
        public void end() {
            if (responseStream == null) {
                return;
            }

            BrowserHeaders nextHeaders = makeHeaders();
            nextHeaders.set("x-deephaven-stream-halfclose", "1");
            // TODO #730 handle failure of this call
            nextWrapper.nextStreamMessage(emptyReq, nextHeaders, (failure, success) -> {
            });
        }

        private void waitForStream(JsConsumer<ResponseStreamWrapper<U>> action) {
            if (responseStream != null) {
                action.apply(responseStream);
            } else {
                pending.push(action);
            }
        }

        @Override
        public void onData(JsConsumer<U> handler) {
            waitForStream(s -> s.onData(handler));
        }

        @Override
        public void onStatus(JsConsumer<ResponseStreamWrapper.Status> handler) {
            waitForStream(s -> s.onStatus(handler));
        }

        @Override
        public void onEnd(JsConsumer<ResponseStreamWrapper.Status> handler) {
            waitForStream(s -> s.onEnd(handler));
        }

        @Override
        public void onHeaders(JsConsumer<Object> handler) {
            waitForStream(s -> s.onHeaders(handler));
        }
    }
}
