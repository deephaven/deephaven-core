package io.deephaven.grpc_api.browserstreaming;

import com.google.rpc.Code;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.*;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Interceptor to notice x-deephaven-stream headers in a request and provide them to
 * later parts of BrowserStream tooling so that unary and server-streaming calls can
 * be combined into an emulated bidirectional stream.
 */
@Singleton
public class BrowserStreamInterceptor implements ServerInterceptor {
    // Browsers evidently cannot interact with binary h2 headers, so all of these are ascii
    /** Export ticket int value. */
    private static final Metadata.Key<String> RPC_TICKET = Metadata.Key.of("x-deephaven-stream-ticket", Metadata.ASCII_STRING_MARSHALLER);
    /** Payload sequence in the stream, starting with zero. */
    private static final Metadata.Key<String> SEQ_HEADER = Metadata.Key.of("x-deephaven-stream-sequence", Metadata.ASCII_STRING_MARSHALLER);
    /**
     * Present to indicate that this is a half-close operation. If this is the first payload,
     * ticket and sequence are not required, and the payload will be considered. Otherwise,
     * payload and sequence are required. The payload will be ignored to enable a client to
     * open a stream, never send a message, and then close it.
     */
    private static final Metadata.Key<String> HALF_CLOSE_HEADER = Metadata.Key.of("x-deephaven-stream-halfclose", Metadata.ASCII_STRING_MARSHALLER);

    @Inject
    public BrowserStreamInterceptor() {
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String ticketInt = headers.get(RPC_TICKET);
        boolean hasHalfClose = headers.containsKey(HALF_CLOSE_HEADER);
        if (ticketInt != null) {
            // ticket was provided, sequence is assumed to be provided as well, otherwise that is an error
            final Ticket rpcTicket = ExportTicketHelper.exportIdToTicket(Integer.parseInt(ticketInt));
            String sequenceString = headers.get(SEQ_HEADER);
            if (sequenceString == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Cannot set x-deephaven-stream-ticket without also setting x-deephaven-stream-sequence");
            }
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
