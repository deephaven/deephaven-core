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
import java.util.logging.Logger;

public class AuthenticationInterceptor implements ClientInterceptor {
    private static final Logger log = Logger.getLogger(AuthenticationInterceptor.class.getName());
    public static final Metadata.Key<String> AUTHORIZATION_HEADER =
            Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Boolean> SESSION_CREATED = Context.keyWithDefault("session-created", false);
    /**
     * If set, this must go out on the next request, and later requests must wait for this to return so we have an auth
     * token (or an auth failure).
     */
    private String headerToSet;

    private String lastHeaderValue;

    private final List<Runnable> pending = new ArrayList<>();

    private State state = State.UNAUTHENTICATED;

    public void deauth() {
        assert state != State.PENDING || pending.isEmpty();
        state = State.UNAUTHENTICATED;
        flushPending();
    }

    public enum State {
        UNAUTHENTICATED, PENDING, AUTHENTICATED;
    }

    public State getState() {
        return state;
    }

    private void setState(State state) {
        log.info("state set from " + this.state + " to " + state);
        if (this.state == state) {
            return;
        }
        if (state == State.UNAUTHENTICATED) {
            fireAuthFailed();
        }
        this.state = state;
    }

    private void fireAuthFailed() {
        // maybe we don't need this?
    }

    public void login(String authType, String authToken) {
        this.headerToSet = (authType + " " + authToken).trim();
        lastHeaderValue = null;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        return new BearerCall<>(next.newCall(method, callOptions));
    }

    private Context handleMetadata(@Nullable Status status, Metadata metadata) {
        log.info("Handling metadata with status " + status + " and headers " + metadata);
        String authHeader = metadata.get(AUTHORIZATION_HEADER);
        if (authHeader == null && status == null) {
            return Context.current();
        }
        if (state == State.PENDING) {
            boolean created = false;
            if (status != null && status.getCode().equals(Status.Code.UNAUTHENTICATED)) {
                setState(State.UNAUTHENTICATED);
                lastHeaderValue = null;
            } else if (authHeader != null) {
                setState(State.AUTHENTICATED);
                lastHeaderValue = authHeader;
                created = true;
            }
            // either way, continue with pending calls
            //TODO probably only in the two above cases
            flushPending();

            if (created) {
                return Context.current().withValue(SESSION_CREATED, Boolean.TRUE);
            }
        } else {
            assert pending.isEmpty();
            if (status != null && status.getCode().equals(Status.Code.UNAUTHENTICATED)) {
                setState(State.UNAUTHENTICATED);
                lastHeaderValue = null;
            } else if (authHeader != null) {
                setState(State.AUTHENTICATED);
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
            log.info("BearerCall.start state=" + state + ", headers= " + headers);
            if (state == State.PENDING) {
                // queue this call until we have a result
                pending.add(() -> start(new BearerListener<>(responseListener), headers));
                return;
            }

            if (state == State.UNAUTHENTICATED) {
                assert headerToSet != null;
                assert lastHeaderValue == null;
                headers.put(AUTHORIZATION_HEADER, headerToSet);
                state = State.PENDING;
                headerToSet = null;
            } else if (state == State.AUTHENTICATED) {
                // assert pending.isEmpty();
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
            handleMetadata(null, headers).run(() -> {
                super.onHeaders(headers);
            });
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            handleMetadata(status, trailers).run(() -> {
                super.onClose(status, trailers);
            });
        }
    }

}
