//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.ByteString;
import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.runner.DeephavenApiServerTestBase.TestComponent.Builder;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.isNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

@RunWith(JUnit4.class)
public class MyTest extends DeephavenApiServerTestBase {

    private static final Map<String, String> DEEPHAVEN_STRING = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "java.lang.String",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> DEEPHAVEN_BYTES = Map.of(
            "deephaven:isSortable", "false",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "byte[]",
            "deephaven:componentType", "byte",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> DEEPHAVEN_INT = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "int",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Field CATALOG_NAME_FIELD =
            new Field("catalog_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field DB_SCHEMA_NAME =
            new Field("db_schema_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field TABLE_NAME =
            new Field("table_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field TABLE_TYPE =
            new Field("table_type", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);
    private static final Field TABLE_SCHEMA =
            new Field("table_schema", new FieldType(true, ArrowType.List.INSTANCE, null, DEEPHAVEN_BYTES),
                    List.of(Field.nullable("", MinorType.TINYINT.getType())));
    private static final Map<String, String> FLAT_ATTRIBUTES = Map.of(
            "deephaven:attribute_type.IsFlat", "java.lang.Boolean",
            "deephaven:attribute.IsFlat", "true");

    @Module(includes = {
            TestModule.class,
            FlightSqlModule.class,
    })
    public interface MyModule {

    }

    @Singleton
    @Component(modules = MyModule.class)
    public interface MyComponent extends TestComponent {

        @Component.Builder
        interface Builder extends TestComponent.Builder {

            @BindsInstance
            Builder withServerConfig(ServerConfig serverConfig);

            @BindsInstance
            Builder withOut(@Named("out") PrintStream out);

            @BindsInstance
            Builder withErr(@Named("err") PrintStream err);

            @BindsInstance
            Builder withAuthorizationProvider(AuthorizationProvider authorizationProvider);

            MyComponent build();
        }
    }

    BufferAllocator bufferAllocator;
    ScheduledExecutorService sessionScheduler;
    FlightClient flightClient;
    FlightSqlClient flightSqlClient;

    @Override
    protected Builder testComponentBuilder() {
        return DaggerMyTest_MyComponent.builder();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        sessionScheduler = Executors.newScheduledThreadPool(2);
        bufferAllocator = new RootAllocator();
        // Note: this pattern of FlightClient owning the ManagedChannel does not mesh well with the idea that some
        // other entity may be managing the authentication lifecycle. We'd prefer to pass in the stubs or "intercepted"
        // channel directly, but that's not supported. So, we need to create the specific middleware interfaces so
        // flight can do its own shims.
        flightClient = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(bufferAllocator, channel,
                new ArrayList<>());
        // Note: this is not extensible, at least not with Auth v2 / JDBC.
        flightClient.authenticate(new ClientAuthHandler() {
            private byte[] callToken = new byte[0];

            @Override
            public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
                WrappedAuthenticationRequest request = WrappedAuthenticationRequest.newBuilder()
                        .setType("Anonymous")
                        .setPayload(ByteString.EMPTY)
                        .build();
                outgoing.send(request.toByteArray());
                callToken = incoming.next();
            }

            @Override
            public byte[] getCallToken() {
                return callToken;
            }
        });
        flightSqlClient = new FlightSqlClient(flightClient);
    }

    @Override
    public void tearDown() throws Exception {
        // this also closes flightClient
        flightSqlClient.close();
        bufferAllocator.close();
        sessionScheduler.shutdown();
        super.tearDown();
    }

    @Test
    public void listActions() {
        assertThat(flightClient.listActions())
                .usingElementComparator(Comparator.comparing(ActionType::getType))
                .containsExactlyInAnyOrder(
                        FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
                        FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
    }

    @Test
    public void getCatalogs() throws Exception {
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME_FIELD);
        {
            final SchemaResult schemaResult = flightSqlClient.getCatalogsSchema();
            assertThat(schemaResult.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo catalogs = flightSqlClient.getCatalogs();
            assertThat(catalogs.getSchema()).isEqualTo(expectedSchema);
            // todo
            try (final FlightStream stream = flightSqlClient.getStream(ticket(catalogs))) {
                System.out.println(getResults(stream));
            }
        }
    }

    @Test
    public void getSchemas() {
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME);
        {
            final SchemaResult schemasSchema = flightSqlClient.getSchemasSchema();
            assertThat(schemasSchema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            // We don't have any catalogs we list right now.
            final FlightInfo schemas = flightSqlClient.getSchemas(null, null);
            assertThat(schemas.getSchema()).isEqualTo(expectedSchema);
            // todo
        }
    }

    @Test
    public void getTables() {
        // Without schema field
        {
            final Schema expectedSchema = flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE);
            {
                final SchemaResult schema = flightSqlClient.getTablesSchema(false);
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo tables = flightSqlClient.getTables(null, null, null, null, false);
                assertThat(tables.getSchema()).isEqualTo(expectedSchema);
                // todo
            }
        }
        // With schema field
        {
            final Schema expectedSchema =
                    flatTableSchema(CATALOG_NAME_FIELD, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA);

            {
                final SchemaResult schema = flightSqlClient.getTablesSchema(true);
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo tables = flightSqlClient.getTables(null, null, null, null, true);
                assertThat(tables.getSchema()).isEqualTo(expectedSchema);
                // todo
            }
        }
    }

    @Test
    public void getTableTypes() {
        final Schema expectedSchema = flatTableSchema(TABLE_TYPE);
        {
            final SchemaResult schema = flightSqlClient.getTableTypesSchema();
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo tableTypes = flightSqlClient.getTableTypes();
            assertThat(tableTypes.getSchema()).isEqualTo(expectedSchema);
            // todo
        }
    }

    @Test
    public void select1() {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        {
            final SchemaResult schema = flightSqlClient.getExecuteSchema("SELECT 1 as Foo");
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.execute("SELECT 1 as Foo");
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            // todo
        }
    }

    @Test
    public void select1Prepared() {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final PreparedStatement preparedStatement = flightSqlClient.prepare("SELECT 1 as Foo")) {
            {
                final SchemaResult schema = preparedStatement.fetchSchema();
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo info = preparedStatement.execute();
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                // todo
            }
        }
    }

    @Test
    public void insert1() {
        try {
            flightSqlClient.executeUpdate("INSERT INTO fake(name) VALUES('Smith')");
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            // FAILED_PRECONDITION gets mapped to INVALID_ARGUMENT here.
            assertThat(e.status().code()).isEqualTo(FlightStatusCode.INVALID_ARGUMENT);
            assertThat(e).hasMessageContaining("FlightSQL descriptors cannot be published to");
        }
    }

//    @Disabled("need to fix server, should error out before")
    @Test
    public void insert1Prepared() {

        try (final PreparedStatement prepared = flightSqlClient.prepare("INSERT INTO fake(name) VALUES('Smith')")) {

            final SchemaResult schema = prepared.fetchSchema();
            // TODO: note the lack of a useful error from perspective of client.
            // INVALID_ARGUMENT: Export in state DEPENDENCY_FAILED
            //
            // final SessionState.ExportObject<Flight.FlightInfo> export =
            // ticketRouter.flightInfoFor(session, request, "request");
            //
            // if (session != null) {
            // session.nonExport()
            // .queryPerformanceRecorder(queryPerformanceRecorder)
            // .require(export)
            // .onError(responseObserver)
            // .submit(() -> {
            // responseObserver.onNext(export.get());
            // responseObserver.onCompleted();
            // });
            // return;
            // }
        }

    }

    @Test
    public void getSqlInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getSqlInfoSchema(), "arrow.flight.protocol.sql.CommandGetSqlInfo");
        commandUnimplemented(() -> flightSqlClient.getSqlInfo(), "arrow.flight.protocol.sql.CommandGetSqlInfo");
    }

    @Test
    public void getXdbcTypeInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getXdbcTypeInfoSchema(),
                "arrow.flight.protocol.sql.CommandGetXdbcTypeInfo");
        commandUnimplemented(() -> flightSqlClient.getXdbcTypeInfo(),
                "arrow.flight.protocol.sql.CommandGetXdbcTypeInfo");

    }

    @Test
    public void getCrossReference() {
        getSchemaUnimplemented(() -> flightSqlClient.getCrossReferenceSchema(),
                "arrow.flight.protocol.sql.CommandGetCrossReference");
        // Need actual refs
        // commandUnimplemented(() -> flightSqlClient.getCrossReference(),
        // "arrow.flight.protocol.sql.CommandGetCrossReference");
    }

    @Test
    public void getPrimaryKeys() {
        getSchemaUnimplemented(() -> flightSqlClient.getPrimaryKeysSchema(),
                "arrow.flight.protocol.sql.CommandGetPrimaryKeys");
        // Need actual refs
        // commandUnimplemented(() -> flightSqlClient.getPrimaryKeys(),
        // "arrow.flight.protocol.sql.CommandGetPrimaryKeys");
    }

    @Test
    public void getExportedKeys() {
        getSchemaUnimplemented(() -> flightSqlClient.getExportedKeysSchema(),
                "arrow.flight.protocol.sql.CommandGetExportedKeys");
        // Need actual refs
        // commandUnimplemented(() -> flightSqlClient.getExportedKeys(),
        // "arrow.flight.protocol.sql.CommandGetExportedKeys");
    }

    @Test
    public void getImportedKeys() {
        getSchemaUnimplemented(() -> flightSqlClient.getImportedKeysSchema(),
                "arrow.flight.protocol.sql.CommandGetImportedKeys");
        // Need actual refs
        // commandUnimplemented(() -> flightSqlClient.getImportedKeys(),
        // "arrow.flight.protocol.sql.CommandGetImportedKeys");
    }

    private void getSchemaUnimplemented(Runnable r, String command) {
        // right now our server impl routes all getSchema through their respective commands
        commandUnimplemented(r, command);
    }

    private void commandUnimplemented(Runnable r, String command) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            assertThat(e.status().code()).isEqualTo(FlightStatusCode.UNIMPLEMENTED);
            assertThat(e).hasMessageContaining(String.format("FlightSQL command '%s' is unimplemented", command));
        }
    }

    private static Ticket ticket(FlightInfo info) {
        assertThat(info.getEndpoints()).hasSize(1);
        return info.getEndpoints().get(0).getTicket();
    }

    private static Schema flatTableSchema(Field... fields) {
        return new Schema(List.of(fields), FLAT_ATTRIBUTES);
    }

    public static List<List<String>> getResults(FlightStream stream) {
        final List<List<String>> results = new ArrayList<>();
        while (stream.next()) {
            try (final VectorSchemaRoot root = stream.getRoot()) {
                final long rowCount = root.getRowCount();
                for (int i = 0; i < rowCount; ++i) {
                    results.add(new ArrayList<>());
                }

                root.getSchema()
                        .getFields()
                        .forEach(
                                field -> {
                                    try (final FieldVector fieldVector = root.getVector(field.getName())) {
                                        if (fieldVector instanceof VarCharVector) {
                                            final VarCharVector varcharVector = (VarCharVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Text data = varcharVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : data.toString());
                                            }
                                        } else if (fieldVector instanceof IntVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof VarBinaryVector) {
                                            final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final byte[] data = varbinaryVector.getObject(rowIndex);
                                                final String output;
                                                try {
                                                    output =
                                                            isNull(data)
                                                                    ? null
                                                                    : MessageSerializer.deserializeSchema(
                                                                            new ReadChannel(
                                                                                    Channels.newChannel(
                                                                                            new ByteArrayInputStream(
                                                                                                    data))))
                                                                            .toJson();
                                                } catch (final IOException e) {
                                                    throw new RuntimeException("Failed to deserialize schema", e);
                                                }
                                                results.get(rowIndex).add(output);
                                            }
                                        } else if (fieldVector instanceof DenseUnionVector) {
                                            final DenseUnionVector denseUnionVector = (DenseUnionVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = denseUnionVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof ListVector) {
                                            for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                                if (!fieldVector.isNull(i)) {
                                                    List<Text> elements =
                                                            (List<Text>) ((ListVector) fieldVector).getObject(i);
                                                    List<String> values = new ArrayList<>();

                                                    for (Text element : elements) {
                                                        values.add(element.toString());
                                                    }
                                                    results.get(i).add(values.toString());
                                                }
                                            }

                                        } else if (fieldVector instanceof UInt4Vector) {
                                            final UInt4Vector uInt4Vector = (UInt4Vector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = uInt4Vector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof UInt1Vector) {
                                            final UInt1Vector uInt1Vector = (UInt1Vector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = uInt1Vector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof BitVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof TimeStampNanoTZVector) {
                                            TimeStampNanoTZVector timeStampNanoTZVector =
                                                    (TimeStampNanoTZVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Long data = timeStampNanoTZVector.getObject(rowIndex);
                                                Instant instant = Instant.ofEpochSecond(0, data);
                                                results.get(rowIndex).add(isNull(instant) ? null : instant.toString());
                                            }
                                        } else if (fieldVector instanceof Float8Vector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof Float4Vector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof DecimalVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else {
                                            System.out.println("Unsupported vector type: " + fieldVector.getClass());
                                        }
                                    }
                                });
            }
        }
        return results;
    }

}
