package io.deephaven.grpc_api.util;

import io.deephaven.io.logger.Logger;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.SafeCloseable;
import io.deephaven.internal.log.LoggerFactory;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class GrpcUtil {
    private static Logger log = LoggerFactory.getLogger(GrpcUtil.class);

    public static ByteString longToByteString(long value) {
        // Note: Little-Endian
        final byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return ByteString.copyFrom(result);
    }

    public static long byteStringToLong(final ByteString value) {
        // Note: Little-Endian
        long result = 0;
        for (int i = 7; i >= 0; i--) {
            final byte bval = value.byteAt(i);
            result = (result << 8) + (bval & 0xffL);
        }
        return result;
    }

    public static void rpcWrapper(final Logger log, final StreamObserver<?> response, final Runnable lambda) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            lambda.run();
        } catch (final StatusRuntimeException err) {
            log.error().append(err).endl();
            response.onError(err);
        } catch (final RuntimeException | Error err) {
            response.onError(securelyWrapError(log, err));
        }
    }

    public static <T> T rpcWrapper(final Logger log, final StreamObserver<?> response, final Callable<T> lambda) {
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
        return securelyWrapError(log, err, Code.INTERNAL);
    }

    public static StatusRuntimeException securelyWrapError(final Logger log, final Throwable err, final Code statusCode) {
        final UUID errorId = UUID.randomUUID();
        log.error().append("Internal Error '").append(errorId.toString()).append("' ").append(err).endl();
        return statusRuntimeException(statusCode, "Details Logged w/ID '" + errorId + "'");
    }

    public static StatusRuntimeException statusRuntimeException(final Code statusCode, final String details) {
        return StatusProto.toStatusRuntimeException(Status.newBuilder()
                .setCode(statusCode.getNumber())
                .setMessage(details)
                .build());
    }

    public static <T, V> StreamObserver<T> wrapOnNext(final StreamObserver<V> delegate, final Function<T, V> wrap) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(final T value) {
                delegate.onNext(wrap.apply(value));
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
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing externally supplied helper.
     *
     * @param runner the runnable to execute safely
     */
    public static void safelyExecute(final FunctionalInterfaces.ThrowingRunnable<Exception> runner) {
        try {
            runner.run();
        } catch (final Exception | Error err) {
            log.error().append("Internal Error: ").append(err).endl();
        }
    }

    /**
     * Wraps the provided runner in a try/catch block to minimize damage caused by a failing externally supplied helper.
     *
     * @param runner the runnable to execute safely
     */
    public static void safelyExecuteLocked(final Object lockedObject, final FunctionalInterfaces.ThrowingRunnable<Exception> runner) {
        try {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (lockedObject) {
                runner.run();
            }
        } catch (final Exception | Error err) {
            log.error().append("Internal Error: ").append(err).endl();
        }
    }
}
