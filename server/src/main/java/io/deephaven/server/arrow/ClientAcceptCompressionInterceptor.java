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
import io.grpc.internal.GrpcUtil;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Captures the compression types a client accepts, as listed in its {@code grpc-accept-encoding} header, so that
 * Barrage handlers can pick a response compression once they know which table is being sent.
 * <p>
 * The value lives in the call's gRPC {@link Context}, which gRPC attaches only on the thread that handles the call. A
 * handler that picks the compression on another thread captures {@link Context#current()} at entry and calls
 * {@link #acceptedCompression()} within it, for example with {@link Context#run(Runnable)}.
 */
@Singleton
public class ClientAcceptCompressionInterceptor implements ServerInterceptor {

    private static final Context.Key<Set<String>> ACCEPTED_COMPRESSION_KEY =
            Context.key("deephaven-accepted-compression");

    @Inject
    public ClientAcceptCompressionInterceptor() {}

    /**
     * @return the compression types the current call's client accepts, or an empty set if it listed none or no call's
     *         {@link Context} is attached
     */
    public static Set<String> acceptedCompression() {
        final Set<String> accepted = ACCEPTED_COMPRESSION_KEY.get();
        return accepted == null ? Set.of() : accepted;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> call,
            final Metadata headers,
            final ServerCallHandler<ReqT, RespT> next) {
        final Iterable<byte[]> values = headers.getAll(GrpcUtil.MESSAGE_ACCEPT_ENCODING_KEY);
        if (values == null) {
            return next.startCall(call, headers);
        }
        final Set<String> accepted = new LinkedHashSet<>();
        for (final byte[] value : values) {
            accepted.addAll(CompressionCodecs.parseAcceptEncoding(new String(value, StandardCharsets.US_ASCII)));
        }
        final Context context = Context.current().withValue(ACCEPTED_COMPRESSION_KEY, Set.copyOf(accepted));
        return Contexts.interceptCall(context, call, headers, next);
    }
}
