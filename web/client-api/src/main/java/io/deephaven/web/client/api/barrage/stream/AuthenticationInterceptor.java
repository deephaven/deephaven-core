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
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public class AuthenticationInterceptor implements ClientInterceptor {
    public static final Metadata.Key<String> AUTHORIZATION_HEADER =
            Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Boolean> SESSION_CREATED = Context.keyWithDefault("session-created", false);

    public enum State {
        /**
         * Haven't tried to authenticate, or failed last call due to auth reasons, so new auth attempt is required to
         * continue. "lastHeaderValue" is null, and "pending" is empty. "headerToSet" may be non-null, and will be
         * consumed on the next call.
         */
        UNAUTHENTICATED,
        /**
         * The provided "headerToSet" creds have been consumed, and a call is ongoing. Other calls made while this call
         * is pending are queued until it is complete. "lastHeaderValue" is null.
         */
        PENDING,

        /**
         * At least one call to the server has succeeded, and an auth token returned. "headerToSet" is now null, and
         * "lastHeaderValue" is non-null. "pending" is empty.
         */
        AUTHENTICATED
    }

    private String headerToSet;
    private String lastHeaderValue;

    private final List<Runnable> pending = new ArrayList<>();

    private State state = State.UNAUTHENTICATED;

    public void login(String authType, String authToken) {
        if (authToken == null) {
            this.headerToSet = authType;
        } else {
            this.headerToSet = (authType + " " + authToken).trim();
        }
        lastHeaderValue = null;
    }

    public void deauth() {
        state = State.UNAUTHENTICATED;
        lastHeaderValue = null;

        // If there were any pending calls, let them complete (and fail) now.
        flushPending();
    }


    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        return new BearerCall<>(next.newCall(method, callOptions));
    }

    /**
     * Handles the response info from the server, returning a context to signal if a session was created as a result or
     * not.
     */
    private Context handleMetadata(@Nullable Status status, Metadata metadata) {
        String authHeader = metadata.get(AUTHORIZATION_HEADER);
        if (authHeader == null && status == null) {
            // No useful response, ignore - probably looking at initial headers.
            return Context.current();
        }
        if (state == State.PENDING) {
            boolean created = false;
            if (authHeader != null) {
                // Server sent us a bearer token to use in future calls, mark as authenticated
                this.state = State.AUTHENTICATED;
                lastHeaderValue = authHeader;
                headerToSet = null;
                created = true;
            } else {
                // With no auth response, we must have a status of some kind. Since we're still pending though, it means
                // auth failed in some way. We'll let later calls continue in case we have new creds, or the failure was
                // just a transport issue.
                // noinspection ConstantValue
                assert status != null;
                this.state = State.UNAUTHENTICATED;
                lastHeaderValue = null;
                if (status.getCode() == Status.Code.UNAUTHENTICATED) {
                    // Auth failed, clear out the header so we don't try it again
                    headerToSet = null;
                }
            }

            // Continue with pending calls
            flushPending();

            if (created) {
                return Context.current().withValue(SESSION_CREATED, Boolean.TRUE);
            }
        } else {
            assert pending.isEmpty();
            if (status != null && status.getCode().equals(Status.Code.UNAUTHENTICATED)) {
                this.state = State.UNAUTHENTICATED;
                lastHeaderValue = null;
            } else if (authHeader != null) {
                this.state = State.AUTHENTICATED;
                lastHeaderValue = authHeader;
            }
        }
        return Context.current();
    }

    private void flushPending() {
        List<Runnable> copy = List.copyOf(pending);
        pending.clear();
        for (Runnable runnable : copy) {
            runnable.run();
        }
    }

    private final class BearerCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
        public BearerCall(ClientCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            if (state == State.PENDING) {
                // queue this call until we have a result
                pending.add(() -> start(new BearerListener<>(responseListener), headers));
                return;
            }

            if (state == State.UNAUTHENTICATED) {
                assert lastHeaderValue == null;
                if (headerToSet != null) {
                    headers.put(AUTHORIZATION_HEADER, headerToSet);
                    state = State.PENDING;
                }
            } else if (state == State.AUTHENTICATED) {
                assert headerToSet == null;
                assert lastHeaderValue != null;
                headers.put(AUTHORIZATION_HEADER, lastHeaderValue);
            }

            super.start(new BearerListener<>(responseListener), headers);
        }
    }
    private final class BearerListener<RespT>
            extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {
        public BearerListener(ClientCall.Listener<RespT> delegate) {
            super(delegate);
        }

        @Override
        public void onHeaders(Metadata headers) {
            handleMetadata(null, headers).run(() -> super.onHeaders(headers));
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            handleMetadata(status, trailers).run(() -> super.onClose(status, trailers));
        }
    }
}
