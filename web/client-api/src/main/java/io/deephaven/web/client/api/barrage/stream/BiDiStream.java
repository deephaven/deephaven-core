//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import io.deephaven.web.client.api.Callbacks;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;

import static io.deephaven.web.client.api.barrage.stream.ClientBrowserStreamInterceptor.HALFCLOSE_KEY;

/**
 * Single interface to interact with real bidi streams, or emulate them using Open/Next methods.
 *
 * @param <Req> the request message type
 * @param <Resp> the response message type
 */
public abstract class BiDiStream<Req, Resp> {
    /**
     * Represents a fully bidi capable stream.
     * 
     * @param <Req> the request message type
     * @param <Resp> the response message type
     */
    public interface BiDiStreamFactory<Req, Resp> {
        StreamObserver<Req> openBiDiStream(StreamObserver<Resp> observer);
    }

    /**
     * When bidi streams are not supported, this factory represents a server-streaming call, for server messages to be
     * delivered to the client.
     * 
     * @param <Req> the request message type
     * @param <Resp> the response message type
     */
    public interface OpenStreamFactory<Req, Resp> {
        void openStream(Req firstPayload, StreamObserver<Resp> observer);
    }

    /**
     * When bidi streams are not supported, this factory represents the ability to send streaming client messages to the
     * server, and have them handled as if they were part of the original stream.
     * 
     * @param <Req> the request message type
     * @param <NextT> empty ack message type
     */
    public interface NextStreamMessageFactory<Req, NextT> {
        void nextStreamMessage(Req nextPayload, StreamObserver<NextT> callback);
    }

    public static class Factory<ReqT, RespT> {
        private final boolean supportsClientStreaming;
        private final IntSupplier nextIntTicket;

        public Factory(boolean supportsClientStreaming, IntSupplier nextIntTicket) {
            this.supportsClientStreaming = supportsClientStreaming;
            this.nextIntTicket = nextIntTicket;
        }

        static class RealBidiStream<ReqT, RespT> extends BiDiStream<ReqT, RespT> {
            private final ResponseStreamWrapper<RespT> wrapper;
            private StreamObserver<ReqT> observer;

            RealBidiStream(BiDiStreamFactory<ReqT, RespT> factory) {
                wrapper = ResponseStreamWrapper.of(o -> {
                    observer = factory.openBiDiStream(o);
                });
            }

            @Override
            public void send(ReqT payload) {
                observer.onNext(payload);
            }

            @Override
            public void cancel() {
                wrapper.cancel();
            }

            @Override
            public void end() {
                observer.onCompleted();
            }

            @Override
            public void onData(Consumer<RespT> handler) {
                wrapper.onData(handler);
            }

            @Override
            public void onStatus(Consumer<Status> handler) {
                wrapper.onStatus(handler);
            }

            @Override
            public void onEnd(Consumer<Status> handler) {
                wrapper.onEnd(handler);
            }

            @Override
            public void onHeaders(Consumer<Object> handler) {
                wrapper.onHeaders(handler);
            }
        }

        public <NoopT> BiDiStream<ReqT, RespT> create(
                BiDiStreamFactory<ReqT, RespT> bidirectionalStream,
                OpenStreamFactory<ReqT, RespT> openEmulatedStream,
                NextStreamMessageFactory<ReqT, NoopT> nextEmulatedStream) {
            if (supportsClientStreaming) {
                return new RealBidiStream<>(bidirectionalStream);
            } else {
                return new EmulatedBiDiStream<>(
                        openEmulatedStream,
                        nextEmulatedStream,
                        nextIntTicket.getAsInt());
            }
        }
    }

    public abstract void send(Req payload);

    public abstract void cancel();

    public abstract void end();

    public abstract void onData(Consumer<Resp> handler);

    public abstract void onStatus(Consumer<Status> handler);

    public abstract void onEnd(Consumer<Status> handler);

    public abstract void onHeaders(Consumer<Object> handler);

    static class EmulatedBiDiStream<T, U> extends BiDiStream<T, U> {
        private final Function<T, ResponseStreamWrapper<U>> responseStreamFactory;
        private final List<Consumer<ResponseStreamWrapper<U>>> pending = new ArrayList<>();
        private final int intTicket;

        private ResponseStreamWrapper<U> responseStream;
        private final NextStreamMessageFactory<T, ?> nextWrapper;

        private int nextSeq = 0;

        EmulatedBiDiStream(OpenStreamFactory<T, U> responseStreamFactory, NextStreamMessageFactory<T, ?> nextWrapper,
                int intTicket) {
            this.responseStreamFactory =
                    firstReq -> ResponseStreamWrapper.of(o -> responseStreamFactory.openStream(firstReq, o));
            this.nextWrapper = nextWrapper;
            this.intTicket = intTicket;
        }

        @Override
        public void send(T payload) {
            if (responseStream == null) {
                try {
                    responseStream = getCtx().call(() -> responseStreamFactory.apply(payload));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                pending.forEach((p0) -> {
                    p0.accept(responseStream);
                });
                pending.clear();
            } else {
                // TODO #730 handle failure of this call
                getCtx().run(() -> nextWrapper.nextStreamMessage(payload, Callbacks.ignore()));
            }
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

            // TODO #730 handle failure of this call
            getCtx().withValue(HALFCLOSE_KEY, true).run(() -> {
                nextWrapper.nextStreamMessage(null, Callbacks.ignore());
            });
        }

        private Context getCtx() {
            return Context.current().withValues(
                    ClientBrowserStreamInterceptor.SEQUENCE_KEY, nextSeq++,
                    ClientBrowserStreamInterceptor.TICKET_KEY, intTicket);
        }

        private void waitForStream(Consumer<ResponseStreamWrapper<U>> action) {
            if (responseStream != null) {
                action.accept(responseStream);
            } else {
                pending.add(action);
            }
        }

        @Override
        public void onData(Consumer<U> handler) {
            waitForStream(s -> s.onData(handler));
        }

        @Override
        public void onStatus(Consumer<Status> handler) {
            waitForStream(s -> s.onStatus(handler));
        }

        @Override
        public void onEnd(Consumer<Status> handler) {
            waitForStream(s -> s.onEnd(handler));
        }

        @Override
        public void onHeaders(Consumer<Object> handler) {
            waitForStream(s -> s.onHeaders(handler));
        }
    }
}
