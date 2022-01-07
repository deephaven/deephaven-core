package io.deephaven.server.object;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.plugin.PluginBase;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import io.deephaven.plugin.type.ObjectTypeCallback;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectServiceTest extends DeephavenApiServerSingleAuthenticatedBase {

    private static final String MY_OBJECT_SOME_STRING = "some string";
    private static final int MY_OBJECT_SOME_INT = 42;

    public static MyObject createMyObject() {
        return new MyObject(MY_OBJECT_SOME_STRING, MY_OBJECT_SOME_INT,
                TableTools.emptyTable(MY_OBJECT_SOME_INT).view("I=i"));
    }

    @Test
    public void myObject() throws IOException {
        ExportObject<MyObject> export = authenticatedSessionState()
                .<MyObject>newExport(1)
                .submit(ObjectServiceTest::createMyObject);
        fetchMyObject(export.getExportId(), MY_OBJECT_SOME_STRING, MY_OBJECT_SOME_INT);
    }

    @Test
    public void myUnregisteredObject() {
        ExportObject<MyUnregisteredObject> export = authenticatedSessionState()
                .<MyUnregisteredObject>newExport(1)
                .submit(MyUnregisteredObject::new);
        final FetchObjectRequest request = FetchObjectRequest.newBuilder().setSourceId(export.getExportId()).build();
        try {
            // noinspection ResultOfMethodCallIgnored
            channel().objectBlocking().fetchObject(request);
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            // expected
        }
    }

    private void fetchMyObject(Ticket ticket, String expectedSomeString, int expectedSomeInt) throws IOException {
        final FetchObjectRequest request = FetchObjectRequest.newBuilder().setSourceId(ticket).build();
        final FetchObjectResponse response = channel().objectBlocking().fetchObject(request);

        assertThat(response.getType()).isEqualTo(MyObjectPlugin.MY_OBJECT_TYPE_NAME);
        assertThat(response.getExportIdCount()).isEqualTo(1);

        final DataInputStream dis = new DataInputStream(response.getData().newInput());
        final String someString = dis.readUTF();
        final int someInt = dis.readInt();
        final byte ticketLen = dis.readByte();
        final byte[] someTableTicket = dis.readNBytes(ticketLen);

        assertThat(someString).isEqualTo(expectedSomeString);
        assertThat(someInt).isEqualTo(expectedSomeInt);
        assertThat(someTableTicket).containsExactly(response.getExportId(0).toByteArray());
    }

    public static class MyObjectPlugin extends PluginBase {

        public static final String MY_OBJECT_TYPE_NAME = MyObject.class.getName();

        @Override
        public void registerInto(ObjectTypeCallback callback) {
            callback.registerObjectType(new MyObjectType(MY_OBJECT_TYPE_NAME));
        }
    }

    public static class MyObject {
        private final String someString;
        private final int someInt;
        private final Table someTable;

        public MyObject(String someString, int someInt, Table someTable) {
            this.someString = someString;
            this.someInt = someInt;
            this.someTable = someTable;
        }
    }

    public static class MyUnregisteredObject {
    }

    private static class MyObjectType extends ObjectTypeClassBase<MyObject> {
        public MyObjectType(String name) {
            super(name, MyObject.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, MyObject object, OutputStream out) throws IOException {
            final Reference tableReference = exporter.newServerSideReference(object.someTable);
            final DataOutputStream doas = new DataOutputStream(out);
            doas.writeUTF(object.someString);
            doas.writeInt(object.someInt);

            final Ticket id = tableReference.id();
            final byte[] idBytes = id.toByteArray();
            doas.writeByte((byte) idBytes.length);
            doas.write(idBytes);
        }
    }
}
