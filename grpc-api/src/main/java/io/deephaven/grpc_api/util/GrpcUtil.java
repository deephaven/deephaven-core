package io.deephaven.grpc_api.util;

import io.deephaven.io.logger.Logger;
import com.google.rpc.Code;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.SafeCloseable;
import io.deephaven.internal.log.LoggerFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class GrpcUtil {
    private static Logger log = LoggerFactory.getLogger(GrpcUtil.class);

    public static <T extends IOException> void rpcWrapper(final Logger log,
        final StreamObserver<?> response, final FunctionalInterfaces.ThrowingRunnable<T> lambda) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            lambda.run();
        } catch (final StatusRuntimeException err) {
            if (err.getStatus().equals(Status.UNAUTHENTICATED)) {
                log.debug().append("ignoring unauthenticated request: ").append(err).endl();
            } else {
                log.error().append(err).endl();
            }
            response.onError(err);
        } catch (final RuntimeException | IOException err) {
            response.onError(securelyWrapError(log, err));
        }
    }

    public static <T> T rpcWrapper(final Logger log, final StreamObserver<?> response,
        final Callable<T> lambda) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            return lambda.call();
        } catch (final StatusRuntimeException err) {
            log.error().append(err).endl();
            response.onError(err);
        } catch (final InterruptedException err) {
            Thread.currentThread().interrupt();
            response.onError(securelyWrapError(log, err, Code.UNAVAILABLE));
        } catch (final Throwable err) {
            response.onError(securelyWrapError(log, err));
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
        log.error().append("Internal Error '").append(errorId.toString()).append("' ").append(err)
            .endl();
        return statusRuntimeException(statusCode, "Details Logged w/ID '" + errorId + "'");
    }

    public static StatusRuntimeException statusRuntimeException(final Code statusCode,
        final String details) {
        return Exceptions.statusRuntimeException(statusCode, details);
    }

    /**
     * This helper allows one to propagate the onError/onComplete calls through to the delegate,
     * while applying the provided mapping function to the original input objects. The mapper may
     * return null to skip sending a message to the delegated stream observer.
     *
     * @param delegate the stream observer to ultimately receive this message
     * @param mapper the function that maps from input objects to the objects the stream observer
     *        expects
     * @param <T> input type
     * @param <V> output type
     * @return a new stream observer that maps from T to V before delivering to
     *         {@code delegate::onNext}
     */
    public static <T, V> StreamObserver<T> mapOnNext(final StreamObserver<V> delegate,
        final Function<T, V> mapper) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(final T value) {
                final V mapped = mapper.apply(value);
                if (mapped != null) {
                    delegate.onNext(mapped);
                }
            }

            @Override
            public void onError(final Throwable t) {
                delegate.onError(t);
            }

            @Override
            public void onCompleted() {
                delegate.onCompleted();
            }
        };
    }

    /**
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing
     * externally supplied helper.
     *
     * @param runner the runnable to execute safely
     */
    public static void safelyExecute(
        final FunctionalInterfaces.ThrowingRunnable<Exception> runner) {
        try {
            runner.run();
        } catch (final Exception err) {
            log.debug().append("Unanticipated gRPC Error: ").append(err).endl();
        }
    }

    /**
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing
     * externally supplied helper.
     *
     * @param runner the runnable to execute safely
     */
    public static void safelyExecuteLocked(final Object lockedObject,
        final FunctionalInterfaces.ThrowingRunnable<Exception> runner) {
        try {
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (lockedObject) {
                runner.run();
            }
        } catch (final Exception err) {
            log.debug().append("Unanticipated gRPC Error: ").append(err).endl();
        }
    }

    /**
     * Writes an error to the observer in a try/catch block to minimize damage caused by failing
     * observer call.
     */
    public static <T> void safelyError(final StreamObserver<T> observer, final Code statusCode,
        final String msg) {
        safelyExecute(() -> observer.onError(GrpcUtil.statusRuntimeException(statusCode, msg)));
    }
}
