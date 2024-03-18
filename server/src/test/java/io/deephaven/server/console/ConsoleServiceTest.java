//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsoleServiceTest extends DeephavenApiServerSingleAuthenticatedBase {

    private static class Observer implements ClientResponseObserver<LogSubscriptionRequest, LogSubscriptionData> {
        private final CountDownLatch latch;
        private final CountDownLatch done;
        private ClientCallStreamObserver<?> stream;
        private volatile Throwable error;

        public Observer(int expected) {
            latch = new CountDownLatch(expected);
            done = new CountDownLatch(1);
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<LogSubscriptionRequest> requestStream) {
            this.stream = requestStream;
        }

        @Override
        public void onNext(LogSubscriptionData value) {
            if (latch.getCount() == 0) {
                throw new IllegalStateException("Expected latch count exceeded");
            }
            latch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done.countDown();
        }

        @Override
        public void onCompleted() {
            done.countDown();
        }
    }

    @Test
    public void subscribeToLogsHistory() throws InterruptedException {
        final LogBufferRecord record1 = record(Instant.now(), LogLevel.STDOUT, "hello");
        final LogBufferRecord record2 = record(Instant.now(), LogLevel.STDOUT, "world");
        logBuffer().record(record1);
        logBuffer().record(record2);
        final LogSubscriptionRequest request = LogSubscriptionRequest.getDefaultInstance();
        final Observer observer = new Observer(2);
        channel().console().subscribeToLogs(request, observer);
        assertThat(observer.latch.await(3, TimeUnit.SECONDS)).isTrue();
        observer.stream.cancel("done", null);
        assertThat(observer.done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        assertThat(observer.error).hasMessage("CANCELLED: done");
    }

    @Test
    public void subscribeToLogsInline() throws InterruptedException {
        final LogSubscriptionRequest request = LogSubscriptionRequest.getDefaultInstance();
        final Observer observer = new Observer(2);
        channel().console().subscribeToLogs(request, observer);
        final LogBufferRecord record1 = record(Instant.now(), LogLevel.STDOUT, "hello");
        final LogBufferRecord record2 = record(Instant.now(), LogLevel.STDOUT, "world");
        logBuffer().record(record1);
        logBuffer().record(record2);
        assertThat(observer.latch.await(3, TimeUnit.SECONDS)).isTrue();
        observer.stream.cancel("done", null);
        assertThat(observer.done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(observer.error).isInstanceOf(StatusRuntimeException.class);
        assertThat(observer.error).hasMessage("CANCELLED: done");
    }

    private static LogBufferRecord record(Instant timestamp, LogLevel level, String message) {
        final LogBufferRecord record = new LogBufferRecord();
        record.setTimestampMicros(timestamp.toEpochMilli() * 1000);
        record.setLevel(level);
        record.setData(ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)));
        return record;
    }
}
