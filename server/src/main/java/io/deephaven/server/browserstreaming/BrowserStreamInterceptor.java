package io.deephaven.server.browserstreaming;

import com.google.rpc.Code;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Interceptor to notice x-deephaven-stream headers in a request and provide them to later parts of BrowserStream
 * tooling so that unary and server-streaming calls can be combined into an emulated bidirectional stream.
 */
public class BrowserStreamInterceptor implements ServerInterceptor {
    private static final String TICKET_HEADER_NAME = "x-deephaven-stream-ticket";
    private static final String SEQUENCE_HEADER_NAME = "x-deephaven-stream-sequence";

    /** Export ticket int value. */
    private static final Metadata.Key<String> RPC_TICKET =
            Metadata.Key.of(TICKET_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /** Payload sequence in the stream, starting with zero. */
    private static final Metadata.Key<String> SEQ_HEADER =
            Metadata.Key.of(SEQUENCE_HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);
    /**
     * Present to indicate that this is a half-close operation. If this is the first payload, ticket and sequence are
     * not required, and the payload will be considered. Otherwise, payload and sequence are required. The payload will
     * be ignored to enable a client to open a stream, never send a message, and then close it.
     */
    private static final Metadata.Key<String> HALF_CLOSE_HEADER =
            Metadata.Key.of("x-deephaven-stream-halfclose", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String ticketInt = headers.get(RPC_TICKET);
        String sequenceString = headers.get(SEQ_HEADER);
        boolean hasTicket = ticketInt != null;
        boolean hasSeqString = sequenceString != null;
        if (hasTicket != hasSeqString) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Either both " + TICKET_HEADER_NAME + " and "
                    + SEQUENCE_HEADER_NAME + " must be provided, or neither");
        }

        boolean hasHalfClose = headers.containsKey(HALF_CLOSE_HEADER);
        if (ticketInt != null) {
            // ticket was provided, sequence is assumed to be provided as well, otherwise that is an error
            final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(Integer.parseInt(ticketInt));
            StreamData data = new StreamData(rpcTicket, Integer.parseInt(sequenceString), hasHalfClose);
            Context ctx = Context.current().withValue(StreamData.STREAM_DATA_KEY, data);
            return Contexts.interceptCall(ctx, call, headers, next);
        } else if (hasHalfClose) {
            // This is an open call with halfclose set, and no ticket, and without a ticket, the sequence
            // is irrelevant.
            StreamData data = new StreamData(null, 0, hasHalfClose);
            Context ctx = Context.current().withValue(StreamData.STREAM_DATA_KEY, data);
            return Contexts.interceptCall(ctx, call, headers, next);
        }

        // No headers were set, ignore the call and handle it normally.
        return next.startCall(call, headers);
    }
}
