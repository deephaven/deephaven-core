package io.deephaven.client;

import com.google.auto.service.AutoService;
import com.google.protobuf.InvalidProtocolBufferException;
import io.deephaven.client.impl.ClientData;
import io.deephaven.client.impl.CustomObject;
import io.deephaven.client.impl.HasTypedTicket;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TypedTicket;
import io.deephaven.client.impl.UnknownObject;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.TableTools;
import io.deephaven.figure.FigureWidgetTypePlugin;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plugin.EchoObjectType;
import io.deephaven.plugin.type.Exporter;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import org.junit.Test;

import java.io.OutputStream;
import java.nio.ByteBuffer;
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

    private static MyObjects myObjects() {
        return new MyObjects(TableTools.emptyTable(1).view("I=ii"), simpleXYTable());
    }

    private static Figure simpleXYTable() {
        final Table t = TableTools.emptyTable(10).view("X=ii", "Y=ii");
        return FigureFactory.figure().plot("Test", t, "X", "Y").show();
    }

    static class MyObjects {
        private final Table table;
        private final Figure customObject;
        private final Object unknownObject;

        public MyObjects(Table table, Figure customObject) {
            this.table = Objects.requireNonNull(table);
            this.customObject = Objects.requireNonNull(customObject);
            this.unknownObject = new Object();
        }
    }

    @Test
    public void fetchable() throws ExecutionException, InterruptedException, TimeoutException,
            InvalidProtocolBufferException, TableHandleException {
        getScriptSession().setVariable("my_objects", myObjects());
        final TypedTicket tt = new TypedTicket(MyObjectsObjectType.NAME, new ScopeId("my_objects"));
        try (
                final Fetchable fetchable = session.fetchable(tt).get(5, TimeUnit.SECONDS);
                final ServerData dataAndExports = fetchable.fetch().get(5, TimeUnit.SECONDS)) {
            checkMyObject(dataAndExports);
        }
    }

    @Test
    public void fetch() throws ExecutionException, InterruptedException, TimeoutException,
            InvalidProtocolBufferException, TableHandleException {
        getScriptSession().setVariable("my_objects", myObjects());
        final TypedTicket tt = new TypedTicket(MyObjectsObjectType.NAME, new ScopeId("my_objects"));
        try (final ServerData dataAndExports = session.fetch(tt).get(5, TimeUnit.SECONDS)) {
            checkMyObject(dataAndExports);
        }
    }

    @Test
    public void bidirectional() throws InterruptedException, ExecutionException, TimeoutException {
        final ScriptSession scriptSession = getScriptSession();
        scriptSession.setVariable("my_echo", EchoObjectType.INSTANCE);
        scriptSession.setVariable("my_objects", myObjects());
        final TypedTicket echo = new TypedTicket(EchoObjectType.NAME, new ScopeId("my_echo"));
        final TypedTicket myObjects = new TypedTicket(MyObjectsObjectType.NAME, new ScopeId("my_objects"));
        try (
                final Bidirectional echoRef = session.bidirectional(echo).get(5, TimeUnit.SECONDS);
                final ServerObject myObjectsRef = session.export(myObjects).get(5, TimeUnit.SECONDS)) {
            final EchoHandler echoHandler = new EchoHandler();
            final MessageStream<ClientData> toServer = echoRef.connect(echoHandler);
            checkEcho(echoHandler, toServer, myObjectsRef, 10);
        }
    }

    @Test
    public void messageStream() throws InterruptedException {
        final ScriptSession scriptSession = getScriptSession();
        scriptSession.setVariable("my_echo", EchoObjectType.INSTANCE);
        scriptSession.setVariable("my_objects", myObjects());
        final TypedTicket echo = new TypedTicket(EchoObjectType.NAME, new ScopeId("my_echo"));
        final TypedTicket myObjects = new TypedTicket(MyObjectsObjectType.NAME, new ScopeId("my_objects"));
        final EchoHandler echoHandler = new EchoHandler();
        final MessageStream<ClientData> toServer = session.connect(echo, echoHandler);
        checkEcho(echoHandler, toServer, myObjects, 10);
    }

    private static void checkEcho(EchoHandler handler, MessageStream<ClientData> toServer, HasTypedTicket tt,
            int times) throws InterruptedException {
        // Ensure we get a message back right away.
        check(handler.queue.poll(5, TimeUnit.SECONDS), 0);

        // We'll send all of our messages first
        for (int i = 1; i < times; ++i) {
            final List<? extends HasTypedTicket> refs =
                    Stream.generate(() -> tt).limit(i).collect(Collectors.toList());
            toServer.onData(new ClientData(ByteBuffer.allocate(i), refs));
        }

        // Then check we got the correct messages back
        for (int i = 1; i < times; ++i) {
            check(handler.queue.poll(5, TimeUnit.SECONDS), i);
        }

        toServer.onClose();
        assertThat(handler.onClose.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private static class EchoHandler implements MessageStream<ServerData> {
        final BlockingQueue<ServerData> queue = new ArrayBlockingQueue<>(32);
        final CountDownLatch onClose = new CountDownLatch(1);

        @Override
        public void onData(ServerData dataAndExports) {
            queue.add(dataAndExports);
        }

        @Override
        public void onClose() {
            onClose.countDown();
        }
    }

    private static void checkMyObject(ServerData myObjects) throws TableHandleException, InterruptedException,
            InvalidProtocolBufferException, ExecutionException, TimeoutException {
        final TableHandle handle2;
        final TableHandle handle1;
        assertThat(myObjects.data().remaining()).isZero();
        assertThat(myObjects.exports()).hasSize(3);
        assertThat(myObjects.exports().get(0)).isInstanceOf(TableObject.class);
        assertThat(myObjects.exports().get(1)).isInstanceOf(CustomObject.class);
        assertThat(myObjects.exports().get(2)).isInstanceOf(UnknownObject.class);

        final TableObject tableObject = (TableObject) myObjects.exports().get(0);
        final CustomObject customObject = (CustomObject) myObjects.exports().get(1);
        final UnknownObject unknownObject = (UnknownObject) myObjects.exports().get(2);

        assertThat(customObject.type()).isEqualTo(FigureWidgetTypePlugin.NAME);
        handle1 = tableObject.executeTable();
        try (final ServerData figureObject = customObject.fetch().get(5, TimeUnit.SECONDS)) {
            handle2 = handleFromFigure(figureObject);
        }
        handle2.close();
        handle1.close();
    }

    private static void check(ServerData data, int index) {
        assertThat(data).isNotNull();
        try (final ServerData _close = data) {
            assertThat(data.data().remaining()).isEqualTo(index);
            assertThat(data.exports().size()).isEqualTo(index);
            for (ServerObject ref : data.exports()) {
                assertThat(ref).isInstanceOf(CustomObject.class);
                assertThat(((CustomObject) ref).type()).isEqualTo(MyObjectsObjectType.NAME);
            }
        }
    }

    private static TableHandle handleFromFigure(ServerData figureData)
            throws InvalidProtocolBufferException, TableHandleException, InterruptedException {
        assertThat(figureData.exports()).hasSize(1);
        assertThat(figureData.exports().get(0)).isInstanceOf(TableObject.class);
        // Note: don't really care about the contents of FigureDescriptor - just making sure it parses successfully
        FigureDescriptor.parseFrom(figureData.data());
        final TableObject tableObject = (TableObject) figureData.exports().get(0);
        return tableObject.executeTable();
    }
}
