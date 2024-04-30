//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportNotificationRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.TerminationNotificationRequest;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.runner.RpcServerStateInterceptor.RpcServerState;
import io.grpc.ClientInterceptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionServiceCloseTest extends DeephavenApiServerSingleAuthenticatedBase {

    @Test
    public void listFields() throws InterruptedException, TimeoutException {
        new ListFieldsObserver().doTest(StatusRuntimeException.class, "CANCELLED: subscription cancelled");
    }

    @Test
    public void exportNotifications() throws InterruptedException, TimeoutException {
        new ExportNotificationsObserver().doTestToCompleted();
    }

    @Test
    public void exportedTableUpdates() throws InterruptedException, TimeoutException {
        new ExportedTableUpdatesObserver().doTestToCompleted();
    }

    @Test
    public void messageStream() throws InterruptedException, TimeoutException {
        new MessageStreamObserver().doTest(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    @Test
    public void subscribeToLogs() throws InterruptedException, TimeoutException {
        new SubscribeToLogsObserver().doTest(StatusRuntimeException.class, "CANCELLED: Session closed");
    }

    @Test
    public void autoCompleteStream() throws InterruptedException, TimeoutException {
        new AutoCompleteStreamObserver().doTestToCompleted();
    }

    @Test
    public void terminationNotification() throws InterruptedException, TimeoutException {
        new TerminationObserver().doTest(StatusRuntimeException.class, "UNAUTHENTICATED: Session has ended");
    }

    final class ListFieldsObserver extends CloseSessionObserverBase<ListFieldsRequest, FieldsChangeUpdate> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .application()
                    .withInterceptors(clientInterceptor)
                    .listFields(ListFieldsRequest.getDefaultInstance(), this);
        }
    }

    final class ExportNotificationsObserver
            extends CloseSessionObserverBase<ExportNotificationRequest, ExportNotification> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .session()
                    .withInterceptors(clientInterceptor)
                    .exportNotifications(ExportNotificationRequest.getDefaultInstance(), this);
        }
    }

    final class ExportedTableUpdatesObserver
            extends CloseSessionObserverBase<ExportedTableUpdatesRequest, ExportedTableUpdateMessage> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .table()
                    .withInterceptors(clientInterceptor)
                    .exportedTableUpdates(ExportedTableUpdatesRequest.getDefaultInstance(), this);
        }
    }

    final class MessageStreamObserver extends CloseSessionObserverBase<StreamRequest, StreamResponse> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .object()
                    .withInterceptors(clientInterceptor)
                    .messageStream(this);
        }
    }

    final class SubscribeToLogsObserver extends CloseSessionObserverBase<LogSubscriptionRequest, LogSubscriptionData> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .console()
                    .withInterceptors(clientInterceptor)
                    .subscribeToLogs(LogSubscriptionRequest.getDefaultInstance(), this);
        }
    }

    final class AutoCompleteStreamObserver extends CloseSessionObserverBase<AutoCompleteRequest, AutoCompleteResponse> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .console()
                    .withInterceptors(clientInterceptor)
                    .autoCompleteStream(this);
        }
    }

    final class TerminationObserver
            extends CloseSessionObserverBase<TerminationNotificationRequest, TerminationNotificationResponse> {

        @Override
        void sendImpl(ClientInterceptor clientInterceptor) {
            channel()
                    .session()
                    .withInterceptors(clientInterceptor)
                    .terminationNotification(TerminationNotificationRequest.getDefaultInstance(), this);
        }
    }

    abstract class CloseSessionObserverBase<ReqT, RespT> implements ClientResponseObserver<ReqT, RespT> {
        ClientCallStreamObserver<ReqT> observer;
        Throwable t;
        boolean onCompleted;
        final CountDownLatch onDone = new CountDownLatch(1);

        public void doTest(Class<? extends Throwable> exceptionType, String message)
                throws InterruptedException, TimeoutException {
            sendImplCloseSessionAndWait();
            assertError(exceptionType, message);
        }

        public void doTestToCompleted() throws InterruptedException, TimeoutException {
            sendImplCloseSessionAndWait();
            assertCompleted();
        }

        private void sendImplCloseSessionAndWait() throws InterruptedException, TimeoutException {
            final RpcServerState serverState = serverStateInterceptor().newRpcServerState();
            sendImpl(serverState.clientInterceptor());
            serverState.awaitServerInvokeFinished(Duration.ofSeconds(3));
            closeSession();
            awaitOnDone(Duration.ofSeconds(3));
        }

        abstract void sendImpl(ClientInterceptor clientInterceptor) throws InterruptedException, TimeoutException;

        @Override
        public final void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
            this.observer = requestStream;
        }

        @Override
        public final void onNext(RespT value) {
            // ignore
        }

        @Override
        public final void onError(Throwable t) {
            this.t = t;
            onDone.countDown();
        }

        @Override
        public final void onCompleted() {
            onCompleted = true;
            onDone.countDown();
        }

        final void awaitOnDone(Duration duration) throws InterruptedException, TimeoutException {
            if (!onDone.await(duration.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException();
            }
        }

        final void assertCompleted() {
            assertThat(onCompleted).isTrue();
            assertThat(t).isNull();
        }

        final void assertError(Class<? extends Throwable> exceptionType, String message) {
            assertThat(onCompleted).isFalse();
            assertThat(t).isNotNull();
            assertThat(t).isInstanceOf(exceptionType);
            assertThat(t).hasMessage(message);
        }
    }
}
