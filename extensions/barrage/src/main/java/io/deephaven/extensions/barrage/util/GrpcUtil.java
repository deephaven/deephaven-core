//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.io.logger.Logger;
import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.function.ThrowingRunnable;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public class GrpcUtil {
    private static final Logger log = LoggerFactory.getLogger(GrpcUtil.class);

    /**
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing externally supplied helper.
     *
     * @param observer the stream that will be used in the runnable
     * @param runner the runnable to execute safely
     */
    private static void safelyExecuteLocked(final StreamObserver<?> observer,
            final ThrowingRunnable<Exception> runner) {
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
    public static <T> void safelyOnNextAndComplete(StreamObserver<T> observer, T message) {
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
     * This will always synchronize on the observer to ensure thread safety when interacting with the grpc response
     * stream.
     */
    public static void safelyError(
            @NotNull final StreamObserver<?> observer,
            final Code statusCode,
            @NotNull final String msg) {
        safelyError(observer, Exceptions.statusRuntimeException(statusCode, msg));
    }

    /**
     * Writes an error to the observer in a try/catch block to minimize damage caused by failing observer call.
     * <p>
     * This will always synchronize on the observer to ensure thread safety when interacting with the grpc response
     * stream.
     */
    public static void safelyError(
            @NotNull final StreamObserver<?> observer,
            @NotNull final StatusRuntimeException exception) {
        safelyExecuteLocked(observer, () -> observer.onError(exception));
    }

    /**
     * Cancels the observer in a try/catch block to minimize damage caused by failing observer call.
     * <p>
     * This will always synchronize on the observer to ensure thread safety when interacting with the grpc response
     * stream.
     * <p>
     * It is recommended that at least one of {@code message} or {@code cause} to be non-{@code null}, to provide useful
     * debug information. Both arguments being null may log warnings and result in suboptimal performance. Also note
     * that the provided information will not be sent to the server.
     *
     * @param observer the stream that will be used in the runnable
     * @param message if not {@code null}, will appear as the description of the CANCELLED status
     * @param cause if not {@code null}, will appear as the cause of the CANCELLED status
     */
    public static void safelyCancel(
            @NotNull final ClientCallStreamObserver<?> observer,
            @Nullable final String message,
            @Nullable final Throwable cause) {
        safelyExecuteLocked(observer, () -> observer.cancel(message, cause));
    }
}
