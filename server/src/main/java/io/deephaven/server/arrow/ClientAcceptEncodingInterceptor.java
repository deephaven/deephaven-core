//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.grpc.compression.CompressionCodecs;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Captures the message encodings a client advertises in its {@code grpc-accept-encoding} header, so that Barrage
 * handlers can pick a response encoding once they know which table is being sent.
 * <p>
 * The value is only visible on the gRPC thread that handles the call; handlers must read {@link #acceptedEncodings()}
 * at entry and carry it to any work they schedule elsewhere.
 */
@Singleton
public class ClientAcceptEncodingInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> ACCEPT_ENCODING_HEADER =
            Metadata.Key.of("grpc-accept-encoding", Metadata.ASCII_STRING_MARSHALLER);

    private static final Context.Key<Set<String>> ACCEPTED_ENCODINGS_KEY =
            Context.key("deephaven-accepted-encodings");

    @Inject
    public ClientAcceptEncodingInterceptor() {}

    /**
     * @return the encodings the current call's client advertised, or an empty set if it advertised none or this is not
     *         a gRPC thread handling a call
     */
    public static Set<String> acceptedEncodings() {
        final Set<String> accepted = ACCEPTED_ENCODINGS_KEY.get();
        return accepted == null ? Set.of() : accepted;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> call,
            final Metadata headers,
            final ServerCallHandler<ReqT, RespT> next) {
        final Iterable<String> values = headers.getAll(ACCEPT_ENCODING_HEADER);
        if (values == null) {
            return next.startCall(call, headers);
        }
        final Set<String> accepted = new LinkedHashSet<>();
        for (final String value : values) {
            accepted.addAll(CompressionCodecs.parseAcceptEncoding(value));
        }
        final Context context = Context.current().withValue(ACCEPTED_ENCODINGS_KEY, Set.copyOf(accepted));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
