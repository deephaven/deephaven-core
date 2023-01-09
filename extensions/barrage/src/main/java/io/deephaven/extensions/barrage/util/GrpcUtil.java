/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.util;

import io.deephaven.io.logger.Logger;
import com.google.rpc.Code;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.SafeCloseable;
import io.deephaven.internal.log.LoggerFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;

public class GrpcUtil {
    private static final Logger log = LoggerFactory.getLogger(GrpcUtil.class);

    /**
     * Utility to avoid errors escaping to the stream, to make sure the server log and client both see the message if
     * there is an error, and if the error was not meant to propagate to a gRPC client, obfuscates it.
     *
     * @param log the current class's logger
     * @param response the responseStream used to send messages to the client
     * @param lambda the code to safely execute
     * @param <T> some IOException type, so that we can handle IO errors as well as runtime exceptions.
     */
    public static <T extends IOException> void rpcWrapper(final Logger log, final StreamObserver<?> response,
            final FunctionalInterfaces.ThrowingRunnable<T> lambda) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            lambda.run();
        } catch (final StatusRuntimeException err) {
            if (err.getStatus().equals(Status.UNAUTHENTICATED)) {
                log.info().append("ignoring unauthenticated request").endl();
            } else {
                log.error().append(err).endl();
            }
            safelyError(response, err);
        } catch (final RuntimeException | IOException err) {
            safelyError(response, securelyWrapError(log, err));
        }
    }

    /**
     * Utility to avoid errors escaping to the stream, to make sure the server log and client both see the message if
     * there is an error, and if the error was not meant to propagate to a gRPC client, obfuscates it.
     *
     * @param log the current class's logger
     * @param response the responseStream used to send messages to the client
     * @param lambda the code to safely execute
     * @param <T> the type of the value to be returned
     * @return the result of the lambda
     */
    public static <T> T rpcWrapper(final Logger log, final StreamObserver<?> response, final Callable<T> lambda) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return lambda.call();
        } catch (final StatusRuntimeException err) {
            if (err.getStatus().equals(Status.UNAUTHENTICATED)) {
                log.info().append("ignoring unauthenticated request").endl();
            } else {
                log.error().append(err).endl();
            }
            safelyError(response, err);
        } catch (final InterruptedException err) {
            Thread.currentThread().interrupt();
            safelyError(response, securelyWrapError(log, err, Code.UNAVAILABLE));
        } catch (final Throwable err) {
            safelyError(response, securelyWrapError(log, err));
        }
        return null;
    }

    public static StatusRuntimeException securelyWrapError(final Logger log, final Throwable err) {
        return securelyWrapError(log, err, Code.INVALID_ARGUMENT);
    }

    public static StatusRuntimeException securelyWrapError(final Logger log, final Throwable err,
            final Code statusCode) {
        if (err instanceof StatusRuntimeException) {
            return (StatusRuntimeException) err;
        }

        final UUID errorId = UUID.randomUUID();
        log.error().append("Internal Error '").append(errorId.toString()).append("' ").append(err).endl();
        return statusRuntimeException(statusCode, "Details Logged w/ID '" + errorId + "'");
    }

    public static StatusRuntimeException statusRuntimeException(final Code statusCode, final String details) {
        return Exceptions.statusRuntimeException(statusCode, details);
    }

    /**
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing externally supplied helper.
     *
     * @param observer the stream that will be used in the runnable
     * @param runner the runnable to execute safely
     */
    private static void safelyExecuteLocked(final StreamObserver<?> observer,
            final FunctionalInterfaces.ThrowingRunnable<Exception> runner) {
        try {
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (observer) {
                runner.run();
            }
        } catch (final Exception err) {
            log.debug().append("Unanticipated gRPC Error: ").append(err).endl();
        }
    }

    /**
     * Sends one message to the stream, ignoring any errors that may happen during that call.
     *
     * @param observer the stream to complete
     * @param message the message to send on this stream
     * @param <T> the type of message that the stream handles
     */
    public static <T> void safelyOnNext(StreamObserver<T> observer, T message) {
        safelyExecuteLocked(observer, () -> observer.onNext(message));
    }

    /**
     * Sends one message and then completes the stream, ignoring any errors that may happen during these calls. Useful
     * for unary responses.
     *
     * @param observer the stream to complete
     * @param message the last message to send on this stream before completing
     * @param <T> the type of message that the stream handles
     */
    public static <T> void safelyComplete(StreamObserver<T> observer, T message) {
        safelyExecuteLocked(observer, () -> {
            observer.onNext(message);
            observer.onCompleted();
        });
    }

    /**
     * Completes the stream, ignoring any errors that may happen during this call.
     *
     * @param observer the stream to complete
     */
    public static void safelyComplete(StreamObserver<?> observer) {
        safelyExecuteLocked(observer, observer::onCompleted);
    }

    /**
     * Writes an error to the observer in a try/catch block to minimize damage caused by failing observer call.
     * <p>
     * </p>
     * This will always synchronize on the observer to ensure thread safety when interacting with the grpc response
     * stream.
     */
    public static void safelyError(final StreamObserver<?> observer, final Code statusCode, final String msg) {
        safelyError(observer, statusRuntimeException(statusCode, msg));
    }

    /**
     * Writes an error to the observer in a try/catch block to minimize damage caused by failing observer call.
     * <p>
     * </p>
     * This will always synchronize on the observer to ensure thread safety when interacting with the grpc response
     * stream.
     */
    public static void safelyError(final StreamObserver<?> observer, StatusRuntimeException exception) {
        safelyExecuteLocked(observer, () -> observer.onError(exception));
    }
}
