package io.deephaven.server.appmode;

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class ApplicationExampleTest extends DeephavenApiServerSingleAuthenticatedBase {

    @AutoService(ApplicationState.Factory.class)
    public static class ExampleApplication implements ApplicationState.Factory {

        @Override
        public ApplicationState create(Listener listener) {
            final ApplicationState state =
                    new ApplicationState(listener, ExampleApplication.class.getName(), "Example Application");
            state.setField("example_field", TableTools.emptyTable(42));
            return state;
        }
    }

    @Test
    public void exampleField() throws InterruptedException, TimeoutException {
        final ListFieldsRequest request = ListFieldsRequest.newBuilder().build();
        final SingleValueListener listener = new SingleValueListener();
        channel().application().listFields(request, listener);
        if (!listener.latch.await(5, TimeUnit.SECONDS)) {
            throw new TimeoutException("Request didn't complete in 5 seconds");
        }
        assertThat(listener.error).isExactlyInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) listener.error).getStatus().getCode()).isEqualTo(Code.CANCELLED);
        assertThat(listener.value).isNotNull();
        final Ticket ticket = Ticket.newBuilder()
                .setTicket(ByteString.copyFromUtf8(
                        "a/io.deephaven.server.appmode.ApplicationExampleTest$ExampleApplication/f/example_field"))
                .build();
        final FieldInfo fieldInfo = FieldInfo.newBuilder()
                .setTypedTicket(TypedTicket.newBuilder().setType("Table").setTicket(ticket).build())
                .setApplicationId(ExampleApplication.class.getName())
                .setApplicationName("Example Application")
                .setFieldName("example_field")
                .build();
        final FieldsChangeUpdate expected = FieldsChangeUpdate.newBuilder().addCreated(fieldInfo).build();
        assertThat(listener.value).isEqualTo(expected);
    }

    private static class SingleValueListener implements ClientResponseObserver<ListFieldsRequest, FieldsChangeUpdate> {
        ClientCallStreamObserver<?> r;
        FieldsChangeUpdate value;
        Throwable error;

        CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void beforeStart(ClientCallStreamObserver<ListFieldsRequest> requestStream) {
            r = requestStream;
        }

        @Override
        public void onNext(FieldsChangeUpdate value) {
            this.value = value;
            r.cancel("Done", null);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            latch.countDown();
        }
    }
}
