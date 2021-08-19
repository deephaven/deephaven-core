package io.deephaven.grpc_api.browserstreaming;

import com.google.protobuf.ByteString;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Objects;

@Singleton
public class BrowserStreamInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> RPC_TICKET = Metadata.Key.of("x-deephaven-stream-ticket", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SEQ_HEADER = Metadata.Key.of("x-deephaven-stream-sequence", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> HALF_CLOSE_HEADER = Metadata.Key.of("x-deephaven-stream-halfclose", Metadata.ASCII_STRING_MARSHALLER);

    public static final Context.Key<StreamData> STREAM_DATA_KEY = Context.key("stream-data");

    @Inject
    public BrowserStreamInterceptor() {
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String ticketInt = headers.get(RPC_TICKET);
        boolean hasHalfClose = headers.containsKey(HALF_CLOSE_HEADER);
        if (ticketInt != null) {
            final Ticket rpcTicket = ExportTicketHelper.exportIdToTicket(Integer.parseInt(ticketInt));
            // this is an emulated stream, could be open or next
            String sequenceString = headers.get(SEQ_HEADER);
            if (sequenceString != null) {
                // this is a next call or a close call
                assert isOpenOrNext(call);
                StreamData data = new StreamData(rpcTicket, Integer.parseInt(sequenceString), hasHalfClose);
                Context ctx = Context.current().withValue(STREAM_DATA_KEY, data);
                return Contexts.interceptCall(ctx, call, headers, next);
            }

            // this is an open call, forward it with the ticket
            StreamData data = new StreamData(rpcTicket, 0, hasHalfClose);
            Context ctx = Context.current().withValue(STREAM_DATA_KEY, data);
            return Contexts.interceptCall(ctx, call, headers, next);
        } else if (hasHalfClose) {
            assert Objects.requireNonNull(call.getMethodDescriptor().getBareMethodName(), "method descriptor bareMethodName").startsWith("Open");
            // this is an open call with halfclose set, no ticket
            // seq is irrelevant
            StreamData data = new StreamData(null, 0, hasHalfClose);
            Context ctx = Context.current().withValue(STREAM_DATA_KEY, data);
            return Contexts.interceptCall(ctx, call, headers, next);
        }
        return next.startCall(call, headers);
    }

    private boolean isOpenOrNext(ServerCall<?, ?> call) {
        String bareMethodName = Objects.requireNonNull(call.getMethodDescriptor().getBareMethodName(), "method descriptor bareMethodName is missing: " + call.getMethodDescriptor().getFullMethodName());
        return bareMethodName.startsWith("Open") || bareMethodName.startsWith("Next");
    }
}
