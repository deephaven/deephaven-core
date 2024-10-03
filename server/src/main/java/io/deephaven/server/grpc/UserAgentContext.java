//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.Optional;

public final class UserAgentContext {

    private static final String USER_AGENT_HEADER = "user-agent";

    private static final Metadata.Key<String> USER_AGENT_HEADER_KEY =
            Metadata.Key.of(USER_AGENT_HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private static final Context.Key<String> USER_AGENT_CONTEXT_KEY =
            Context.key(UserAgentContext.class.getSimpleName());

    /**
     * A server interceptor that adds the metadata header {@value USER_AGENT_HEADER} into the call's context, making it
     * retrievable via {@link #get(Context)}.
     *
     * @return the server interceptor
     */
    public static ServerInterceptor interceptor() {
        return Interceptor.USER_AGENT_INTERCEPTOR;
    }

    /**
     * Gets the user-agent from the current {@code context}. Used in combination with {@link #interceptor()}.
     *
     * @param context the context
     * @return the user-agent
     */
    public static Optional<String> get(Context context) {
        return Optional.ofNullable(USER_AGENT_CONTEXT_KEY.get(context));
    }

    private enum Interceptor implements ServerInterceptor {
        USER_AGENT_INTERCEPTOR;

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            final String userAgent = headers.get(USER_AGENT_HEADER_KEY);
            if (userAgent == null) {
                return next.startCall(call, headers);
            }
            final Context newContext = Context.current().withValue(USER_AGENT_CONTEXT_KEY, userAgent);
            return Contexts.interceptCall(newContext, call, headers, next);
        }
    }
}
