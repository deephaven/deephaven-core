//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.stream;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
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

    /** Export ticket int value. */
    private static final Metadata.Key<String> RPC_TICKET =
            Metadata.Key.of(TICKET_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /** Payload sequence in the stream, starting with zero. */
    private static final Metadata.Key<String> SEQ_HEADER =
            Metadata.Key.of(SEQUENCE_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /** True if the stream should be half-closed now */
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
            if (halfClose) {
                // If halfClose is set, we want to send the request, but ignore the response, and then close the stream.
                ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
                Metadata headers = new Metadata();
                headers.put(RPC_TICKET, ticket.toString());
                headers.put(SEQ_HEADER, sequence.toString());
                headers.put(HALF_CLOSE_HEADER, "true");
                call.start(new ClientCall.Listener<RespT>() {}, headers);
                return call;
            }
            // This is a normal call, but we need to add the appropriate metadata to make sure the server treats it as
            // part of the stream.
            ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
            Metadata headers = new Metadata();
            headers.put(RPC_TICKET, ticket.toString());
            headers.put(SEQ_HEADER, sequence.toString());
            call.start(new ClientCall.Listener<RespT>() {}, headers);
            return call;
        }
        return next.newCall(method, callOptions);
    }
}
