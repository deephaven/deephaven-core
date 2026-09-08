//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Client wiring corresponding to BrowserStreamInterceptor, this allows BiDiStream to simulate a single bidi stream
 * through Open/Next calls with appropriate metadata.
 */
public class ClientBrowserStreamInterceptor implements ClientInterceptor {
    public static final String TICKET_HEADER_NAME = "x-deephaven-stream-ticket";
    public static final String SEQUENCE_HEADER_NAME = "x-deephaven-stream-sequence";
    public static final String HALF_CLOSE_HEADER_NAME = "x-deephaven-stream-halfclose";

    /**
     * Export ticket int value.
     */
    private static final Metadata.Key<String> RPC_TICKET =
            Metadata.Key.of(TICKET_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /**
     * Payload sequence in the stream, starting with zero.
     */
    private static final Metadata.Key<String> SEQ_HEADER =
            Metadata.Key.of(SEQUENCE_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /**
     * True if the stream should be half-closed now
     */
    private static final Metadata.Key<String> HALF_CLOSE_HEADER =
            Metadata.Key.of(HALF_CLOSE_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);


    public static final Context.Key<Integer> TICKET_KEY = Context.key("emulated-stream-id");
    public static final Context.Key<Integer> SEQUENCE_KEY = Context.key("emulated-stream-sequence");
    public static final Context.Key<Boolean> HALFCLOSE_KEY = Context.key("emulated-halfclose");

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        // Ticket must always be set, or this isn't an emulated call
        Integer ticket = TICKET_KEY.get();
        if (ticket != null) {
            Integer sequence = SEQUENCE_KEY.get();
            assert sequence != null : "Sequence must be set if ticket is set";
            boolean halfClose = HALFCLOSE_KEY.get() == Boolean.TRUE;
            return new EmulatedBiDiCall<>(next.newCall(method, callOptions), ticket, sequence, halfClose);
        }
        return next.newCall(method, callOptions);
    }

    private final class EmulatedBiDiCall<ReqT, RespT>
            extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
        private final int ticket;
        private final int seq;
        private final boolean halfClose;

        private EmulatedBiDiCall(ClientCall<ReqT, RespT> delegate, int ticket, int seq, boolean halfClose) {
            super(delegate);
            this.ticket = ticket;
            this.seq = seq;
            this.halfClose = halfClose;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            Metadata copy = new Metadata();
            copy.merge(headers);
            // Append required headers
            copy.put(RPC_TICKET, Integer.toString(ticket));
            copy.put(SEQ_HEADER, Integer.toString(seq));
            if (halfClose) {
                copy.put(HALF_CLOSE_HEADER, "true");
            }
            super.start(responseListener, copy);
        }

    }
}
