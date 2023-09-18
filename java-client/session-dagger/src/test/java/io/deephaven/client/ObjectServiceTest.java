package io.deephaven.client;

import com.google.auto.service.AutoService;
import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.client.impl.CustomObject;
import io.deephaven.client.impl.FetchedObject;
import io.deephaven.client.impl.HasTypedTicket;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TicketId;
import io.deephaven.client.impl.TypedTicket;
import io.deephaven.client.impl.UnknownObject;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.TableTools;
import io.deephaven.figure.FigureWidgetTypePlugin;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import org.junit.Test;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectServiceTest extends DeephavenSessionTestBase {

    @AutoService(ObjectType.class)
    public static class MyObjectsObjectType extends ObjectTypeBase.FetchOnly {

        public static final String NAME = MyObjects.class.getSimpleName();

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public boolean isType(Object object) {
            return object instanceof MyObjects;
        }

        @Override
        public void writeCompatibleObjectTo(Exporter exporter, Object object, OutputStream out) {
            final MyObjects myObjects = (MyObjects) object;
            exporter.reference(myObjects.table);
            exporter.reference(myObjects.customObject);
            exporter.reference(myObjects.unknownObject);
        }
    }

    @AutoService(ObjectType.class)
    public static class EchoObjectType extends ObjectTypeBase {
        public static final String NAME = EchoObjectType.class.getSimpleName();
        public static final Object INSTANCE = new Object();

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public boolean isType(Object object) {
            return object == INSTANCE;
        }

        @Override
        public MessageStream compatibleClientConnection(Object object, MessageStream connection)
                throws ObjectCommunicationException {
            // The contract right now means we need to return a message right away.
            connection.onData(ByteBuffer.allocate(0));
            return connection;
        }
    }

    private static MyObjects myObjects() {
        return new MyObjects(TableTools.emptyTable(1).view("I=ii"), simpleXYTable());
    }

    private static Figure simpleXYTable() {
        final Table t = TableTools.emptyTable(10).view("X=ii", "Y=ii");
        return FigureFactory.figure().plot("Test", t, "X", "Y").show();
    }

    static class MyObjects {
        private final Table table;
        private final Object customObject;
        private final Object unknownObject;

        public MyObjects(Table table, Object customObject) {
            this.table = Objects.requireNonNull(table);
            this.customObject = Objects.requireNonNull(customObject);
            this.unknownObject = new Object();
        }
    }

    @Test
    public void myObjectsTest() throws ExecutionException, InterruptedException, TimeoutException,
            InvalidProtocolBufferException, TableHandleException {
        final ScriptSession scriptSession = testComponent().scriptSessionProvider().get();
        scriptSession.setVariable("my_objects", myObjects());
        final TicketId ticket = new TicketId("s/my_objects".getBytes(StandardCharsets.UTF_8));
        final TableHandle handle1;
        final TableHandle handle2;
        try (final FetchedObject myObjects = session
                .fetchObject(MyObjectsObjectType.NAME, ticket)
                .get(5, TimeUnit.SECONDS)) {

            assertThat(myObjects.size()).isZero();
            assertThat(myObjects.exports()).hasSize(3);
            assertThat(myObjects.exports().get(0)).isInstanceOf(TableObject.class);
            assertThat(myObjects.exports().get(1)).isInstanceOf(CustomObject.class);
            assertThat(myObjects.exports().get(2)).isInstanceOf(UnknownObject.class);

            final TableObject tableObject = (TableObject) myObjects.exports().get(0);
            final CustomObject customObject = (CustomObject) myObjects.exports().get(1);
            final UnknownObject unknownObject = (UnknownObject) myObjects.exports().get(2);

            handle1 = tableObject.executeTable();
            try (final FetchedObject figureObject = customObject.fetch().get(5, TimeUnit.SECONDS)) {
                handle2 = handleFromFigure(figureObject);
            }
        }
        handle2.close();
        handle1.close();
    }

    @Test
    public void messageStreamTest() throws InterruptedException {
        final ScriptSession scriptSession = testComponent().scriptSessionProvider().get();
        scriptSession.setVariable("my_echo", EchoObjectType.INSTANCE);
        scriptSession.setVariable("my_objects", myObjects());
        final TypedTicket echo = new TypedTicket(EchoObjectType.NAME, "s/my_echo".getBytes(StandardCharsets.UTF_8));
        final TypedTicket myObjects =
                new TypedTicket(MyObjectsObjectType.NAME, "s/my_objects".getBytes(StandardCharsets.UTF_8));
        final int times = 10;
        final BlockingQueue<Data> queue = new ArrayBlockingQueue<>(times);
        final CountDownLatch onClose = new CountDownLatch(1);
        // This is a simple protocol for testing purposes only. Essentially, the server will echo anything back to the
        // client. We are testing in such a way that the i'th response is expected to contain i bytes and i (my objects)
        // refs.
        final MessageStream<ServerObject> fromServer = new MessageStream<>() {
            @Override
            public void onData(ByteBuffer payload, List<? extends ServerObject> references) {
                queue.add(new Data(payload, references));
            }

            @Override
            public void onClose() {
                onClose.countDown();
            }
        };
        final MessageStream<HasTypedTicket> toServer = session.messageStream(echo, fromServer);

        // Ensure we get a message back right away.
        check(queue.poll(5, TimeUnit.SECONDS), 0);

        // We'll send all of our messages first
        for (int i = 1; i < times; ++i) {
            final List<? extends HasTypedTicket> refs =
                    Stream.generate(() -> myObjects).limit(i).collect(Collectors.toList());
            toServer.onData(ByteBuffer.allocate(i), refs);
        }

        // Then check we got the correct messages back
        for (int i = 1; i < times; ++i) {
            check(queue.poll(5, TimeUnit.SECONDS), i);
        }

        toServer.onClose();
        assertThat(onClose.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private static class Data {
        private final ByteBuffer payload;
        private final List<? extends ServerObject> refs;

        public Data(ByteBuffer payload, List<? extends ServerObject> refs) {
            this.payload = payload;
            this.refs = refs;
        }
    }

    private static void check(Data data, int index) {
        assertThat(data).isNotNull();
        assertThat(data.payload.remaining()).isEqualTo(index);
        assertThat(data.refs.size()).isEqualTo(index);
        for (ServerObject ref : data.refs) {
            assertThat(ref).isInstanceOf(CustomObject.class);
            assertThat(((CustomObject) ref).type()).isEqualTo(MyObjectsObjectType.NAME);
            ref.close();
        }
    }

    private static TableHandle handleFromFigure(FetchedObject figureObject)
            throws InvalidProtocolBufferException, TableHandleException, InterruptedException {
        assertThat(figureObject.type()).isEqualTo(FigureWidgetTypePlugin.NAME);
        assertThat(figureObject.exports()).hasSize(1);
        assertThat(figureObject.exports().get(0)).isInstanceOf(TableObject.class);
        // Note: don't really care about the contents of FigureDescriptor - just making sure it parses successfully
        FigureDescriptor.parseFrom(figureObject.toByteArray());
        final TableObject tableObject = (TableObject) figureObject.exports().get(0);
        return tableObject.executeTable();
    }
}
