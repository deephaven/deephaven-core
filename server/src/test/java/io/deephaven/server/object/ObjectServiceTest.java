package io.deephaven.server.object;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.type.ObjectType.Exporter.Reference;
import io.deephaven.plugin.type.ObjectTypeClassBase;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.runner.DeephavenApiServerSingleAuthenticatedBase;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class ObjectServiceTest extends DeephavenApiServerSingleAuthenticatedBase {

    private static final String MY_OBJECT_SOME_STRING = "some string";
    private static final int MY_OBJECT_SOME_INT = 42;
    private static final MyRefObject REF = new MyRefObject();
    private static final MyUnregisteredObject UNREG = new MyUnregisteredObject();

    public static MyObject createMyObject() {
        return new MyObject(MY_OBJECT_SOME_STRING, MY_OBJECT_SOME_INT,
                TableTools.emptyTable(MY_OBJECT_SOME_INT).view("I=i"), REF, UNREG);
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

        assertThat(response.getType()).isEqualTo(MyObjectRegistration.MY_OBJECT_TYPE_NAME);
        assertThat(response.getExportIdCount()).isEqualTo(4);
        assertThat(response.getExportId(0).getType()).isEqualTo("Table");
        assertThat(response.getExportId(1).getType()).isEqualTo(MyObjectRegistration.MY_REF_OBJECT_TYPE_NAME);
        assertThat(response.getExportId(2).hasType()).isFalse();
        assertThat(response.getExportId(3).getType()).isEqualTo(MyObjectRegistration.MY_REF_OBJECT_TYPE_NAME);

        final DataInputStream dis = new DataInputStream(response.getData().newInput());

        // the original, out of order
        readRef(dis, 2);
        readRef(dis, 1);
        readRef(dis, 0);

        // the extras
        readRef(dis, 0); // our extra ref to table
        readRef(dis, 3); // our new extra ref

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

    @AutoService(Registration.class)
    public static class MyObjectRegistration implements Registration {

        public static final String MY_OBJECT_TYPE_NAME = MyObject.class.getName();

        public static final String MY_REF_OBJECT_TYPE_NAME = MyRefObject.class.getName();

        @Override
        public void registerInto(Callback callback) {
            callback.register(new MyObjectType());
            callback.register(new MyRefObjectType());
        }
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

    private static class MyObjectType extends ObjectTypeClassBase<MyObject> {
        public MyObjectType() {
            super(MyObjectRegistration.MY_OBJECT_TYPE_NAME, MyObject.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, MyObject object, OutputStream out) throws IOException {
            final Reference tableRef = exporter.reference(object.someTable, false).orElseThrow();
            final Reference objRef = exporter.reference(object.someObj, false).orElseThrow();
            final Reference unknownRef = exporter.reference(object.someUnknown, true).orElseThrow();

            final Reference extraTableRef = exporter.reference(object.someTable, false).orElseThrow();
            final Reference extraNewObjRef = exporter.newReference(object.someObj, false).orElseThrow();

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

    private static class MyRefObjectType extends ObjectTypeClassBase<MyRefObject> {
        public MyRefObjectType() {
            super(MyObjectRegistration.MY_REF_OBJECT_TYPE_NAME, MyRefObject.class);
        }

        @Override
        public void writeToImpl(Exporter exporter, MyRefObject object, OutputStream out) throws IOException {
            // no-op
        }
    }
}
