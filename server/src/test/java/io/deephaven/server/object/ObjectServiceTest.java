//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.Exporter.Reference;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.ConnectRequest;
import io.deephaven.proto.backplane.grpc.ServerData;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.StreamResponse.MessageCase;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.stub.StreamObserver;
import org.assertj.core.api.Condition;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectServiceTest extends DeephavenApiServerSingleAuthenticatedBase {

    public static final String MY_OBJECT_TYPE_NAME = MyObject.class.getName();
    public static final String MY_REF_OBJECT_TYPE_NAME = MyRefObject.class.getName();
    private static final String MY_OBJECT_SOME_STRING = "some string";
    private static final int MY_OBJECT_SOME_INT = 42;
    private static final MyRefObject REF = new MyRefObject();
    private static final MyUnregisteredObject UNREG = new MyUnregisteredObject();

    public static MyObject createMyObject() {
        return new MyObject(MY_OBJECT_SOME_STRING, MY_OBJECT_SOME_INT,
                TableTools.emptyTable(MY_OBJECT_SOME_INT).view("I=i"), REF, UNREG);
    }

    @Test
    public void myObject() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        ExportObject<MyObject> export = authenticatedSessionState()
                .<MyObject>newExport(1)
                .submit(ObjectServiceTest::createMyObject);
        fetchMyObject(export.getExportId(), MY_OBJECT_SOME_STRING, MY_OBJECT_SOME_INT);
    }

    @Test
    public void myUnregisteredObject() throws InterruptedException, TimeoutException {
        ExportObject<MyUnregisteredObject> export = authenticatedSessionState()
                .<MyUnregisteredObject>newExport(1)
                .submit(MyUnregisteredObject::new);
        final CompletableFuture<Object> cf = new CompletableFuture<>();
        final StreamObserver<StreamRequest> observer = channel().object().messageStream(new StreamObserver<>() {
            @Override
            public void onNext(StreamResponse value) {
                cf.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                cf.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!cf.isDone()) {
                    cf.complete(new Object());
                }
            }
        });
        final StreamRequest connectRequest = StreamRequest.newBuilder()
                .setConnect(ConnectRequest.newBuilder()
                        .setSourceId(TypedTicket.newBuilder().setTicket(export.getExportId())))
                .build();
        observer.onNext(connectRequest);

        try {
            cf.get(5, TimeUnit.SECONDS);
            failBecauseExceptionWasNotThrown(ExecutionException.class);
        } catch (ExecutionException e) {
            // expected
        }
    }

    private void fetchMyObject(Ticket ticket, String expectedSomeString, int expectedSomeInt)
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        final StreamRequest connectRequest = StreamRequest.newBuilder()
                .setConnect(ConnectRequest.newBuilder()
                        .setSourceId(TypedTicket.newBuilder()
                                .setType(MY_OBJECT_TYPE_NAME)
                                .setTicket(ticket)))
                .build();
        final CompletableFuture<StreamResponse> cf = new CompletableFuture<>();
        final StreamObserver<StreamRequest> observer = channel().object().messageStream(new StreamObserver<>() {
            @Override
            public void onNext(StreamResponse value) {
                cf.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                cf.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!cf.isDone()) {
                    cf.completeExceptionally(new RuntimeException("Expected future to complete"));
                }
            }
        });
        observer.onNext(connectRequest);
        observer.onCompleted();
        final StreamResponse rr = cf.get(5, TimeUnit.SECONDS);
        final ServerData response = rr.getData();

        assertThat(rr.getMessageCase()).isEqualTo(MessageCase.DATA);
        assertThat(response.getExportedReferencesCount()).isEqualTo(5);
        assertThat(response.getExportedReferences(0).getType()).isEqualTo("Table");
        assertThat(response.getExportedReferences(1).getType()).isEqualTo(MY_REF_OBJECT_TYPE_NAME);
        assertThat(response.getExportedReferences(2).getType()).isEmpty();
        assertThat(response.getExportedReferences(3).getType()).isEqualTo("Table");
        assertThat(response.getExportedReferences(4).getType()).isEqualTo(MY_REF_OBJECT_TYPE_NAME);

        final DataInputStream dis = new DataInputStream(response.getPayload().newInput());

        // the original, out of order
        readRef(dis, 2);
        readRef(dis, 1);
        readRef(dis, 0);

        // the extras
        readRef(dis, 3); // our extra ref to table, now an additional export
        readRef(dis, 4); // our new extra ref

        readString(dis, expectedSomeString);
        readInt(dis, expectedSomeInt);
    }

    private void readRef(DataInputStream in, int expectedIndex) throws IOException {
        assertThat((int) in.readByte()).isEqualTo(expectedIndex);
    }

    private void readString(DataInput in, String expected) throws IOException {
        assertThat(in.readUTF()).isEqualTo(expected);
    }

    private void readInt(DataInput in, int expected) throws IOException {
        assertThat(in.readInt()).isEqualTo(expected);
    }

    public static class MyObject {
        private final String someString;
        private final int someInt;
        private final Table someTable;
        private final MyRefObject someObj;
        private final MyUnregisteredObject someUnknown;

        public MyObject(String someString, int someInt, Table someTable, MyRefObject someObj,
                MyUnregisteredObject someUnknown) {
            this.someString = Objects.requireNonNull(someString);
            this.someInt = someInt;
            this.someTable = Objects.requireNonNull(someTable);
            this.someObj = Objects.requireNonNull(someObj);
            this.someUnknown = Objects.requireNonNull(someUnknown);
        }
    }

    public static class MyRefObject {

    }

    public static class MyUnregisteredObject {
    }

    @AutoService(ObjectType.class)
    public static class MyObjectType extends ObjectTypeClassBase.FetchOnly<MyObject> {
        public MyObjectType() {
            super(MY_OBJECT_TYPE_NAME, MyObject.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, MyObject object, OutputStream out) throws IOException {
            final Reference tableRef = exporter.reference(object.someTable);
            final Reference objRef = exporter.reference(object.someObj);
            final Reference unknownRef = exporter.reference(object.someUnknown);

            final Reference extraTableRef = exporter.reference(object.someTable);
            final Reference extraNewObjRef = exporter.reference(object.someObj);

            assertThat(tableRef.type()).isEmpty();
            assertThat(objRef.type()).isEmpty();
            assertThat(unknownRef.type()).isEmpty();
            assertThat(extraTableRef.type()).isEmpty();
            assertThat(extraNewObjRef.type()).isEmpty();

            assertThat(tableRef.index()).isNotEqualTo(extraTableRef.index());
            assertThat(objRef.index()).isNotEqualTo(extraNewObjRef.index());

            final DataOutputStream doas = new DataOutputStream(out);

            // let's write them out of order
            writeRef(doas, unknownRef);
            writeRef(doas, objRef);
            writeRef(doas, tableRef);

            // and then write the extras
            writeRef(doas, extraTableRef);
            writeRef(doas, extraNewObjRef);

            doas.writeUTF(object.someString);
            doas.writeInt(object.someInt);
        }

        private static void writeRef(DataOutput out, Reference reference) throws IOException {
            out.writeByte(reference.index());
        }
    }

    @AutoService(ObjectType.class)
    public static class MyRefObjectType extends ObjectTypeClassBase.FetchOnly<MyRefObject> {
        public MyRefObjectType() {
            super(MY_REF_OBJECT_TYPE_NAME, MyRefObject.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, MyRefObject object, OutputStream out) throws IOException {
            // no-op
        }
    }

    private static Condition<byte[]> bytesEquals(byte[] expected) {
        return new Condition<>(b -> Arrays.equals(b, expected), "array bytes are equals");
    }
}
