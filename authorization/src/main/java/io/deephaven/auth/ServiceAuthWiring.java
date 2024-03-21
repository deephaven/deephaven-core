//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.function.ThrowingRunnable;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * This class is a marker interface for the generated service auth wiring classes.
 */
public interface ServiceAuthWiring<ServiceImplBase> {
    static void operationNotAllowed() {
        operationNotAllowed("Operation not allowed");
    }

    static void operationNotAllowed(String reason) {
        throw Exceptions.statusRuntimeException(Code.PERMISSION_DENIED, reason);
    }

    @FunctionalInterface
    interface CallStartedCallback {
        void callStarted(AuthContext authContext);
    }

    @FunctionalInterface
    interface MessageReceivedCallback<T> {
        void messageReceived(AuthContext authContext, T message);
    }

    /**
     * Intercepts all public methods on the service implementation base, and wraps them with auth checks.
     *
     * @param delegate the real service implementation
     * @return a new service definition with wrapped auth checks
     */
    ServerServiceDefinition intercept(ServiceImplBase delegate);

    class AuthorizingServerCallHandler<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
        private static final Logger log = LoggerFactory.getLogger(AuthorizingServerCallHandler.class);

        private final ServerCallHandler<ReqT, RespT> delegate;
        private final CallStartedCallback callStartedCallback;
        private final MessageReceivedCallback<ReqT> messageReceivedCallback;

        private final boolean mustHaveRequest;
        private final String fullMethodName;

        public AuthorizingServerCallHandler(
                final ServerCallHandler<ReqT, RespT> delegate,
                final ServerMethodDefinition<ReqT, RespT> method,
                final CallStartedCallback callStartedCallback,
                final MessageReceivedCallback<ReqT> messageReceivedCallback) {
            this.delegate = delegate;
            this.callStartedCallback = callStartedCallback;
            this.messageReceivedCallback = messageReceivedCallback;
            mustHaveRequest = method.getMethodDescriptor().getType().clientSendsOneMessage();
            fullMethodName = method.getMethodDescriptor().getFullMethodName();
            // validate that we have handlers for the methods we will try to invoke
            if (!mustHaveRequest) {
                Assert.neqNull(callStartedCallback, "callStartedCallback");
            }
            Assert.neqNull(messageReceivedCallback, "messageReceivedCallback");
        }

        @Override
        public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
            final AuthContext authContext = ExecutionContext.getContext().getAuthContext();

            if (!mustHaveRequest && !validateAuth(call, () -> callStartedCallback.callStarted(authContext))) {
                return new ServerCall.Listener<>() {};
            }

            final ServerCall.Listener<ReqT> delegateCall = delegate.startCall(call, headers);
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegateCall) {
                @Override
                public void onMessage(ReqT message) {
                    if (!validateAuth(call, () -> messageReceivedCallback.messageReceived(authContext, message))) {
                        return;
                    }

                    // if we're still here, then it is safe to continue with the request
                    delegate().onMessage(message);
                }
            };
        }

        private boolean validateAuth(
                final ServerCall<ReqT, RespT> call,
                ThrowingRunnable<ReflectiveOperationException> authRunner) {
            try {
                authRunner.run();
            } catch (ReflectiveOperationException | RuntimeException originalErr) {
                Throwable err = originalErr;
                if (!(err instanceof StatusRuntimeException) && err.getCause() != null) {
                    err = err.getCause();
                }
                if (err instanceof StatusRuntimeException) {
                    final StatusRuntimeException sre = (StatusRuntimeException) err;
                    Status.Code status = sre.getStatus().getCode();
                    switch (status) {
                        case UNAUTHENTICATED:
                            log.info().append(fullMethodName).append(": request unauthenticated").endl();
                            break;
                        case PERMISSION_DENIED:
                            log.info().append(fullMethodName).append(": request unauthorized").endl();
                            break;
                        case RESOURCE_EXHAUSTED:
                            log.info().append(fullMethodName).append(": request throttled").endl();
                            break;
                        default:
                            log.error().append(fullMethodName).append(": authorization failed: ").append(err).endl();
                    }

                    quietlyCloseCall(call, sre.getStatus(), sre.getTrailers());
                    return false;
                }

                // this is an unexpected error from the auth wiring invocation, be loud about it
                log.error().append("Failed to invoke auth method: ").append(originalErr).endl();
                quietlyCloseCall(call, Status.UNAUTHENTICATED, new Metadata());
                return false;
            }

            return true;
        }
    }

    /**
     * Close the call, but don't throw any exceptions.
     */
    private static <ReqT, RespT> void quietlyCloseCall(
            final ServerCall<ReqT, RespT> call, final Status status, final Metadata trailers) {
        try {
            call.close(status, trailers);
        } catch (IllegalStateException ignored) {
            // could be thrown if the call was already closed; it's ok to ignore these race conditions
        }
    }

    /**
     * Wraps a given service method implementation with an auth check.
     * <p>
     *
     * @apiNote Unary or Server-Streaming methods will not callback on call started as every request is required to have
     *          a request message.
     *
     * @param service the service implementation
     * @param methodName the method name
     * @param callStartedCallback the callback to invoke when the call starts
     * @param messageReceivedCallback the callback to invoke when a message is received
     * @return the wrapped server method definition
     */
    static <ReqT, RespT> ServerMethodDefinition<ReqT, RespT> intercept(
            final ServerServiceDefinition service,
            final String methodName,
            final CallStartedCallback callStartedCallback,
            final MessageReceivedCallback<ReqT> messageReceivedCallback) {
        final String fullMethodName = service.getServiceDescriptor().getName() + "/" + methodName;
        // noinspection unchecked
        final ServerMethodDefinition<ReqT, RespT> method =
                (ServerMethodDefinition<ReqT, RespT>) service.getMethod(fullMethodName);
        if (method == null) {
            throw new IllegalStateException("No method found for name: " + fullMethodName);
        }
        final ServerCallHandler<ReqT, RespT> delegate = method.getServerCallHandler();
        final ServerCallHandler<ReqT, RespT> authServerCallHandler = new AuthorizingServerCallHandler<>(
                delegate, method, callStartedCallback, messageReceivedCallback);
        return method.withServerCallHandler(authServerCallHandler);
    }
}
