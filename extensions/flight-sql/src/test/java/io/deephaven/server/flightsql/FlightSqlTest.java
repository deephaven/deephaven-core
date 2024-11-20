//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.runner.DeephavenApiServerTestBase.TestComponent.Builder;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.ProtocolExposer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlClient.Savepoint;
import org.apache.arrow.flight.sql.FlightSqlClient.SubstraitPlan;
import org.apache.arrow.flight.sql.FlightSqlClient.Transaction;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedSubstraitPlanRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.deephaven.server.flightsql.FlightSqlTicketHelper.TICKET_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

// using JUnit4 so we can inherit properly from DeephavenApiServerTestBase
@RunWith(JUnit4.class)
public class FlightSqlTest extends DeephavenApiServerTestBase {

    private static final Map<String, String> DEEPHAVEN_STRING = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "java.lang.String",
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

    private static final Map<String, String> DEEPHAVEN_BYTE = Map.of(
            "deephaven:isSortable", "true",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "byte",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> DEEPHAVEN_SCHEMA = Map.of(
            "deephaven:isSortable", "false",
            "deephaven:isRowStyle", "false",
            "deephaven:isPartitioning", "false",
            "deephaven:type", "org.apache.arrow.vector.types.pojo.Schema",
            "deephaven:isNumberFormat", "false",
            "deephaven:isStyle", "false",
            "deephaven:isDateFormat", "false");

    private static final Map<String, String> FLAT_ATTRIBUTES = Map.of(
            "deephaven:attribute_type.IsFlat", "java.lang.Boolean",
            "deephaven:attribute.IsFlat", "true");

    private static final Field CATALOG_NAME =
            new Field("catalog_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field PK_CATALOG_NAME =
            new Field("pk_catalog_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field FK_CATALOG_NAME =
            new Field("fk_catalog_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field DB_SCHEMA_NAME =
            new Field("db_schema_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field PK_DB_SCHEMA_NAME =
            new Field("pk_db_schema_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field FK_DB_SCHEMA_NAME =
            new Field("fk_db_schema_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field TABLE_NAME =
            new Field("table_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field COLUMN_NAME =
            new Field("column_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field KEY_NAME =
            new Field("key_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field PK_TABLE_NAME =
            new Field("pk_table_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field FK_TABLE_NAME =
            new Field("fk_table_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field TABLE_TYPE =
            new Field("table_type", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field PK_COLUMN_NAME =
            new Field("pk_column_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field FK_COLUMN_NAME =
            new Field("fk_column_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field KEY_SEQUENCE =
            new Field("key_sequence", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null);

    private static final Field PK_KEY_NAME =
            new Field("pk_key_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field FK_KEY_NAME =
            new Field("fk_key_name", new FieldType(true, Utf8.INSTANCE, null, DEEPHAVEN_STRING), null);

    private static final Field UPDATE_RULE =
            new Field("update_rule", new FieldType(true, MinorType.TINYINT.getType(), null, DEEPHAVEN_BYTE), null);

    private static final Field DELETE_RULE =
            new Field("delete_rule", new FieldType(true, MinorType.TINYINT.getType(), null, DEEPHAVEN_BYTE), null);

    private static final Field TABLE_SCHEMA =
            new Field("table_schema", new FieldType(true, MinorType.VARBINARY.getType(), null, DEEPHAVEN_SCHEMA), null);

    private static final TableRef FOO_TABLE_REF = TableRef.of(null, null, "foo_table");
    public static final TableRef BAR_TABLE_REF = TableRef.of(null, null, "barTable");

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
    FlightClient flightClient;
    FlightSqlClient flightSqlClient;

    @Override
    protected Builder testComponentBuilder() {
        return DaggerFlightSqlTest_MyComponent.builder();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
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
        super.tearDown();
    }

    @Test
    public void listFlights() {
        assertThat(flightClient.listFlights(Criteria.ALL)).isEmpty();
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
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME);
        {
            final SchemaResult schemaResult = flightSqlClient.getCatalogsSchema();
            assertThat(schemaResult.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getCatalogs();
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 0, 0, true);
        }
        unpackable(CommandGetCatalogs.getDescriptor(), CommandGetCatalogs.class);
    }

    @Test
    public void getSchemas() throws Exception {
        final Schema expectedSchema = flatTableSchema(CATALOG_NAME, DB_SCHEMA_NAME);
        {
            final SchemaResult schemasSchema = flightSqlClient.getSchemasSchema();
            assertThat(schemasSchema.getSchema()).isEqualTo(expectedSchema);
        }
        for (final FlightInfo info : new FlightInfo[] {
                flightSqlClient.getSchemas(null, null),
                flightSqlClient.getSchemas("DoesNotExist", null),
                flightSqlClient.getSchemas(null, ""),
                flightSqlClient.getSchemas(null, "%"),
                flightSqlClient.getSchemas(null, "SomeSchema"),
        }) {
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 0, 0, true);
        }
        unpackable(CommandGetDbSchemas.getDescriptor(), CommandGetDbSchemas.class);
    }

    @Test
    public void getTables() throws Exception {
        setFooTable();
        setFoodTable();
        setBarTable();
        for (final boolean includeSchema : new boolean[] {false, true}) {
            final Schema expectedSchema = includeSchema
                    ? flatTableSchema(CATALOG_NAME, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA)
                    : flatTableSchema(CATALOG_NAME, DB_SCHEMA_NAME, TABLE_NAME, TABLE_TYPE);
            {
                final SchemaResult schema = flightSqlClient.getTablesSchema(includeSchema);
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            // Any of these queries will fetch everything from query scope
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables(null, null, null, null, includeSchema),
                    flightSqlClient.getTables("", null, null, null, includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("TABLE"), includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("IRRELEVANT_TYPE", "TABLE"), includeSchema),
                    flightSqlClient.getTables(null, null, "%", null, includeSchema),
                    flightSqlClient.getTables(null, null, "%able", null, includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 3, true);
            }

            // Any of these queries will fetch foo_table and foodtable; there is no way to uniquely filter based on the
            // `_` literal
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables(null, null, "foo_table", null, includeSchema),
                    flightSqlClient.getTables(null, null, "foo_%", null, includeSchema),
                    flightSqlClient.getTables(null, null, "f%", null, includeSchema),
                    flightSqlClient.getTables(null, null, "%table", null, includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 2, true);
            }

            // Any of these queries will fetch foodtable
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables(null, null, "foodtable", null, includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 1, true);
            }

            // Any of these queries will fetch barTable
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables(null, null, "barTable", null, includeSchema),
                    flightSqlClient.getTables(null, null, "bar%", null, includeSchema),
                    flightSqlClient.getTables(null, null, "b%", null, includeSchema),
                    flightSqlClient.getTables(null, null, "%Table", null, includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 1, true);
            }

            // Any of these queries will fetch an empty table
            for (final FlightInfo info : new FlightInfo[] {
                    flightSqlClient.getTables("DoesNotExistCatalog", null, null, null, includeSchema),
                    flightSqlClient.getTables(null, null, null, List.of("IRRELEVANT_TYPE"), includeSchema),
                    flightSqlClient.getTables(null, "", null, null, includeSchema),
                    flightSqlClient.getTables(null, "%", null, null, includeSchema),
                    flightSqlClient.getTables(null, null, "", null, includeSchema),
                    flightSqlClient.getTables(null, null, "doesNotExist", null, includeSchema),
                    flightSqlClient.getTables(null, null, "%_table2", null, includeSchema),
            }) {
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 0, 0, true);
            }
        }
        unpackable(CommandGetTables.getDescriptor(), CommandGetTables.class);
    }

    @Test
    public void getTableTypes() throws Exception {
        final Schema expectedSchema = flatTableSchema(TABLE_TYPE);
        {
            final SchemaResult schema = flightSqlClient.getTableTypesSchema();
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getTableTypes();
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 1, 1, true);
        }
        unpackable(CommandGetTableTypes.getDescriptor(), CommandGetTableTypes.class);
    }

    @Test
    public void select1() throws Exception {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        {
            final SchemaResult schema = flightSqlClient.getExecuteSchema("SELECT 1 as Foo");
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.execute("SELECT 1 as Foo");
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 1, 1, false);
        }
        unpackable(CommandStatementQuery.getDescriptor(), CommandStatementQuery.class);
    }

    @Test
    public void select1Prepared() throws Exception {
        final Schema expectedSchema = new Schema(
                List.of(new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null)));
        try (final PreparedStatement prepared = flightSqlClient.prepare("SELECT 1 as Foo")) {
            assertThat(prepared.getResultSetSchema()).isEqualTo(FlightSqlResolver.DATASET_SCHEMA_SENTINEL);
            {
                final SchemaResult schema = prepared.fetchSchema();
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo info = prepared.execute();
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 1, false);
            }
            unpackable(CommandPreparedStatementQuery.getDescriptor(), CommandPreparedStatementQuery.class);
        }
    }

    @Test
    public void selectStarFromQueryScopeTable() throws Exception {
        setFooTable();

        final Schema expectedSchema = flatTableSchema(
                new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null));
        {
            final SchemaResult schema = flightSqlClient.getExecuteSchema("SELECT * FROM foo_table");
            assertThat(schema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.execute("SELECT * FROM foo_table");
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 1, 3, false);
        }
        // The Flight SQL resolver will maintain state to ensure results are resolvable, even if the underlying table
        // goes away between flightInfo and doGet.
        {
            final FlightInfo info = flightSqlClient.execute("SELECT * FROM foo_table");
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            removeFooTable();
            consume(info, 1, 3, false);
        }
        unpackable(CommandStatementQuery.getDescriptor(), CommandStatementQuery.class);

    }

    @Test
    public void selectStarPreparedFromQueryScopeTable() throws Exception {
        setFooTable();
        {
            final Schema expectedSchema = flatTableSchema(
                    new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null));
            try (final PreparedStatement prepared = flightSqlClient.prepare("SELECT * FROM foo_table")) {
                assertThat(prepared.getResultSetSchema()).isEqualTo(FlightSqlResolver.DATASET_SCHEMA_SENTINEL);
                {
                    final SchemaResult schema = prepared.fetchSchema();
                    assertThat(schema.getSchema()).isEqualTo(expectedSchema);
                }
                {
                    final FlightInfo info = prepared.execute();
                    assertThat(info.getSchema()).isEqualTo(expectedSchema);
                    consume(info, 1, 3, false);
                }
                // The Flight SQL resolver will maintain state to ensure results are resolvable, even if the underlying
                // table goes away between flightInfo and doGet.
                {
                    final FlightInfo info = prepared.execute();
                    assertThat(info.getSchema()).isEqualTo(expectedSchema);
                    removeFooTable();
                    consume(info, 1, 3, false);
                }
                // The states in _not_ maintained by the PreparedStatement state though, and will not be available for
                // the next execute
                expectException(prepared::execute, FlightStatusCode.NOT_FOUND, "Object 'foo_table' not found");
                unpackable(CommandPreparedStatementQuery.getDescriptor(), CommandPreparedStatementQuery.class);
            }
            unpackable(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT, ActionCreatePreparedStatementRequest.class);
            unpackable(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT, ActionClosePreparedStatementRequest.class);
        }
    }

    @Test
    public void preparedStatementIsLazy() throws Exception {
        try (final PreparedStatement prepared = flightSqlClient.prepare("SELECT * FROM foo_table")) {
            assertThat(prepared.getResultSetSchema()).isEqualTo(FlightSqlResolver.DATASET_SCHEMA_SENTINEL);
            expectException(prepared::fetchSchema, FlightStatusCode.NOT_FOUND, "Object 'foo_table' not found");
            expectException(prepared::execute, FlightStatusCode.NOT_FOUND, "Object 'foo_table' not found");
            // If the state-of-the-world changes, this will be reflected in new calls against the prepared statement.
            // This also implies that we won't error out at the start of prepare call if the table doesn't exist.
            //
            // We could introduce some sort of reference-based Transactional model (orthogonal to a snapshot-based
            // Transactional model) which would ensure schema consistency, but that would be an effort outside of a
            // PreparedStatement.
            setFooTable();
            final Schema expectedSchema = flatTableSchema(
                    new Field("Foo", new FieldType(true, MinorType.INT.getType(), null, DEEPHAVEN_INT), null));
            {
                final SchemaResult schema = prepared.fetchSchema();
                assertThat(schema.getSchema()).isEqualTo(expectedSchema);
            }
            {
                final FlightInfo info = prepared.execute();
                assertThat(info.getSchema()).isEqualTo(expectedSchema);
                consume(info, 1, 3, false);
            }
        }
    }

    @Test
    public void selectQuestionMark() {
        queryError("SELECT ?", FlightStatusCode.INVALID_ARGUMENT, "Illegal use of dynamic parameter");
    }

    @Test
    public void selectFooParam() {
        setFooTable();
        queryError("SELECT Foo FROM foo_table WHERE Foo = ?", FlightStatusCode.INVALID_ARGUMENT,
                "Flight SQL: query parameters are not supported");
    }

    @Test
    public void selectTableDoesNotExist() {
        queryError("SELECT * FROM my_table", FlightStatusCode.NOT_FOUND, "Object 'my_table' not found");
    }

    @Test
    public void selectColumnDoesNotExist() {
        setFooTable();
        queryError("SELECT BadColumn FROM foo_table", FlightStatusCode.NOT_FOUND,
                "Column 'BadColumn' not found in any table");
    }

    @Test
    public void selectFunctionDoesNotExist() {
        setFooTable();
        queryError("SELECT my_function(Foo) FROM foo_table", FlightStatusCode.INVALID_ARGUMENT,
                "No match found for function signature");
    }

    @Test
    public void badSqlQuery() {
        queryError("this is not SQL", FlightStatusCode.INVALID_ARGUMENT, "Flight SQL: query can't be parsed");
    }

    @Test
    public void executeSubstrait() {
        getSchemaUnimplemented(() -> flightSqlClient.getExecuteSubstraitSchema(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.executeSubstrait(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        misbehave(CommandStatementSubstraitPlan.getDefaultInstance(), CommandStatementSubstraitPlan.getDescriptor());
        unpackable(CommandStatementSubstraitPlan.getDescriptor(), CommandStatementSubstraitPlan.class);
    }

    @Test
    public void executeSubstraitUpdate() {
        // Note: this is the same descriptor as the executeSubstrait
        getSchemaUnimplemented(() -> flightSqlClient.getExecuteSubstraitSchema(fakePlan()),
                CommandStatementSubstraitPlan.getDescriptor());
        expectUnpublishable(() -> flightSqlClient.executeSubstraitUpdate(fakePlan()));
        unpackable(CommandStatementSubstraitPlan.getDescriptor(), CommandStatementSubstraitPlan.class);
    }

    @Test
    public void insert1() {
        expectUnpublishable(() -> flightSqlClient.executeUpdate("INSERT INTO fake(name) VALUES('Smith')"));
        unpackable(CommandStatementUpdate.getDescriptor(), CommandStatementUpdate.class);
    }

    private void queryError(String query, FlightStatusCode expectedCode, String expectedMessage) {
        expectException(() -> flightSqlClient.getExecuteSchema(query), expectedCode, expectedMessage);
        expectException(() -> flightSqlClient.execute(query), expectedCode, expectedMessage);
        try (final PreparedStatement prepared = flightSqlClient.prepare(query)) {
            assertThat(prepared.getResultSetSchema()).isEqualTo(FlightSqlResolver.DATASET_SCHEMA_SENTINEL);
            expectException(prepared::fetchSchema, expectedCode, expectedMessage);
            expectException(prepared::execute, expectedCode, expectedMessage);
        }
    }

    @Test
    public void insertPrepared() {
        setFooTable();
        try (final PreparedStatement prepared = flightSqlClient.prepare("INSERT INTO foo_table(Foo) VALUES(42)")) {
            expectException(prepared::fetchSchema, FlightStatusCode.INVALID_ARGUMENT,
                    "Flight SQL: Unsupported calcite type 'org.apache.calcite.rel.logical.LogicalTableModify'");
            expectException(prepared::execute, FlightStatusCode.INVALID_ARGUMENT,
                    "Flight SQL: Unsupported calcite type 'org.apache.calcite.rel.logical.LogicalTableModify'");
        }
        try (final PreparedStatement prepared = flightSqlClient.prepare("INSERT INTO foo_table(MyArg) VALUES(42)")) {
            expectException(prepared::fetchSchema, FlightStatusCode.INVALID_ARGUMENT,
                    "Flight SQL: Unknown target column 'MyArg'");
            expectException(prepared::execute, FlightStatusCode.INVALID_ARGUMENT,
                    "Flight SQL: Unknown target column 'MyArg'");
        }
        try (final PreparedStatement prepared = flightSqlClient.prepare("INSERT INTO x(Foo) VALUES(42)")) {
            expectException(prepared::fetchSchema, FlightStatusCode.NOT_FOUND, "Flight SQL: Object 'x' not found");
            expectException(prepared::execute, FlightStatusCode.NOT_FOUND, "Flight SQL: Object 'x' not found");
        }
    }

    @Test
    public void getSqlInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getSqlInfoSchema(), CommandGetSqlInfo.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getSqlInfo(), CommandGetSqlInfo.getDescriptor());
        misbehave(CommandGetSqlInfo.getDefaultInstance(), CommandGetSqlInfo.getDescriptor());
        unpackable(CommandGetSqlInfo.getDescriptor(), CommandGetSqlInfo.class);
    }

    @Test
    public void getXdbcTypeInfo() {
        getSchemaUnimplemented(() -> flightSqlClient.getXdbcTypeInfoSchema(), CommandGetXdbcTypeInfo.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getXdbcTypeInfo(), CommandGetXdbcTypeInfo.getDescriptor());
        misbehave(CommandGetXdbcTypeInfo.getDefaultInstance(), CommandGetXdbcTypeInfo.getDescriptor());
        unpackable(CommandGetXdbcTypeInfo.getDescriptor(), CommandGetXdbcTypeInfo.class);
    }

    @Test
    public void getCrossReference() {
        setFooTable();
        setBarTable();
        getSchemaUnimplemented(() -> flightSqlClient.getCrossReferenceSchema(),
                CommandGetCrossReference.getDescriptor());
        commandUnimplemented(() -> flightSqlClient.getCrossReference(FOO_TABLE_REF, BAR_TABLE_REF),
                CommandGetCrossReference.getDescriptor());
        misbehave(CommandGetCrossReference.getDefaultInstance(), CommandGetCrossReference.getDescriptor());
        unpackable(CommandGetCrossReference.getDescriptor(), CommandGetCrossReference.class);
    }

    @Test
    public void getPrimaryKeys() throws Exception {
        setFooTable();
        final Schema expectedSchema =
                flatTableSchema(CATALOG_NAME, DB_SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, KEY_NAME, KEY_SEQUENCE);
        {
            final SchemaResult exportedKeysSchema = flightSqlClient.getPrimaryKeysSchema();
            assertThat(exportedKeysSchema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getPrimaryKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 0, 0, true);
        }
        // Note: the info must remain valid even if the server state goes away.
        {
            final FlightInfo info = flightSqlClient.getPrimaryKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            removeFooTable();
            // resolve should still be OK
            consume(info, 0, 0, true);
        }
        expectException(() -> flightSqlClient.getPrimaryKeys(BAR_TABLE_REF), FlightStatusCode.NOT_FOUND,
                "Flight SQL: table not found");

        // Note: misbehaving clients who fudge tickets directly will not get errors; but they will also not learn any
        // information on whether the tables actually exist or not since the returned table is always empty.
        for (final CommandGetPrimaryKeys command : new CommandGetPrimaryKeys[] {
                CommandGetPrimaryKeys.newBuilder().setTable("DoesNotExist").build(),
                CommandGetPrimaryKeys.newBuilder().setCatalog("Catalog").setDbSchema("DbSchema")
                        .setTable("DoesNotExist").build()
        }) {
            final Ticket ticket =
                    ProtocolExposer.fromProtocol(FlightSqlTicketHelper.ticketCreator().visit(command));
            try (final FlightStream stream = flightSqlClient.getStream(ticket)) {
                consume(stream, 0, 0);
            }
        }
        unpackable(CommandGetPrimaryKeys.getDescriptor(), CommandGetPrimaryKeys.class);
    }

    @Test
    public void getExportedKeys() throws Exception {
        setFooTable();
        final Schema expectedSchema = flatTableSchema(PK_CATALOG_NAME, PK_DB_SCHEMA_NAME, PK_TABLE_NAME, PK_COLUMN_NAME,
                FK_CATALOG_NAME, FK_DB_SCHEMA_NAME, FK_TABLE_NAME, FK_COLUMN_NAME, KEY_SEQUENCE, FK_KEY_NAME,
                PK_KEY_NAME, UPDATE_RULE, DELETE_RULE);
        {
            final SchemaResult exportedKeysSchema = flightSqlClient.getExportedKeysSchema();
            assertThat(exportedKeysSchema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getExportedKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 0, 0, true);
        }
        // Note: the info must remain valid even if the server state goes away.
        {
            final FlightInfo info = flightSqlClient.getExportedKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            removeFooTable();
            // resolve should still be OK
            consume(info, 0, 0, true);
        }
        expectException(() -> flightSqlClient.getExportedKeys(BAR_TABLE_REF), FlightStatusCode.NOT_FOUND,
                "Flight SQL: table not found");

        // Note: misbehaving clients who fudge tickets directly will not get errors; but they will also not learn any
        // information on whether the tables actually exist or not since the returned table is always empty.
        for (final CommandGetExportedKeys command : new CommandGetExportedKeys[] {
                CommandGetExportedKeys.newBuilder().setTable("DoesNotExist").build(),
                CommandGetExportedKeys.newBuilder().setCatalog("Catalog").setDbSchema("DbSchema")
                        .setTable("DoesNotExist").build()
        }) {
            final Ticket ticket =
                    ProtocolExposer.fromProtocol(FlightSqlTicketHelper.ticketCreator().visit(command));
            try (final FlightStream stream = flightSqlClient.getStream(ticket)) {
                consume(stream, 0, 0);
            }
        }
        unpackable(CommandGetExportedKeys.getDescriptor(), CommandGetExportedKeys.class);
    }

    @Test
    public void getImportedKeys() throws Exception {
        setFooTable();
        final Schema expectedSchema = flatTableSchema(PK_CATALOG_NAME, PK_DB_SCHEMA_NAME, PK_TABLE_NAME, PK_COLUMN_NAME,
                FK_CATALOG_NAME, FK_DB_SCHEMA_NAME, FK_TABLE_NAME, FK_COLUMN_NAME, KEY_SEQUENCE, FK_KEY_NAME,
                PK_KEY_NAME, UPDATE_RULE, DELETE_RULE);
        {
            final SchemaResult importedKeysSchema = flightSqlClient.getImportedKeysSchema();
            assertThat(importedKeysSchema.getSchema()).isEqualTo(expectedSchema);
        }
        {
            final FlightInfo info = flightSqlClient.getImportedKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            consume(info, 0, 0, true);
        }
        // Note: the info must remain valid even if the server state goes away.
        {
            final FlightInfo info = flightSqlClient.getImportedKeys(FOO_TABLE_REF);
            assertThat(info.getSchema()).isEqualTo(expectedSchema);
            removeFooTable();
            // resolve should still be OK
            consume(info, 0, 0, true);
        }

        expectException(() -> flightSqlClient.getImportedKeys(BAR_TABLE_REF), FlightStatusCode.NOT_FOUND,
                "Flight SQL: table not found");

        // Note: misbehaving clients who fudge tickets directly will not get errors; but they will also not learn any
        // information on whether the tables actually exist or not since the returned table is always empty.
        for (final CommandGetImportedKeys command : new CommandGetImportedKeys[] {
                CommandGetImportedKeys.newBuilder().setTable("DoesNotExist").build(),
                CommandGetImportedKeys.newBuilder().setCatalog("Catalog").setDbSchema("DbSchema")
                        .setTable("DoesNotExist").build()
        }) {
            final Ticket ticket =
                    ProtocolExposer.fromProtocol(FlightSqlTicketHelper.ticketCreator().visit(command));
            try (final FlightStream stream = flightSqlClient.getStream(ticket)) {
                consume(stream, 0, 0);
            }
        }
        unpackable(CommandGetImportedKeys.getDescriptor(), CommandGetImportedKeys.class);
    }

    @Test
    public void commandStatementIngest() {
        // Note: if we actually want to test out FlightSqlClient behavior more directly, we can set up scaffolding to
        // call FlightSqlClient#executeIngest.
        final FlightDescriptor ingestCommand =
                FlightDescriptor.command(Any.pack(CommandStatementIngest.getDefaultInstance()).toByteArray());
        getSchemaUnimplemented(() -> flightClient.getSchema(ingestCommand), CommandStatementIngest.getDescriptor());
        commandUnimplemented(() -> flightClient.getInfo(ingestCommand), CommandStatementIngest.getDescriptor());
        misbehave(CommandStatementIngest.getDefaultInstance(), CommandStatementIngest.getDescriptor());
        unpackable(CommandStatementIngest.getDescriptor(), CommandStatementIngest.class);
    }

    @Test
    public void unknownCommandLooksLikeFlightSql() {
        final String typeUrl = "type.googleapis.com/arrow.flight.protocol.sql.CommandLooksRealButDoesNotExist";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        getSchemaUnknown(() -> flightClient.getSchema(descriptor), typeUrl);
        commandUnknown(() -> flightClient.getInfo(descriptor), typeUrl);
    }

    @Test
    public void unknownCommand() {
        // Note: this should likely be tested in the context of Flight, not Flight SQL
        final String typeUrl = "type.googleapis.com/com.example.SomeRandomCommand";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        expectException(() -> flightClient.getSchema(descriptor), FlightStatusCode.INVALID_ARGUMENT,
                "no resolver for command");
        expectException(() -> flightClient.getInfo(descriptor), FlightStatusCode.INVALID_ARGUMENT,
                "no resolver for command");
    }

    @Test
    public void prepareSubstrait() {
        actionUnimplemented(() -> flightSqlClient.prepare(fakePlan()),
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN);
        unpackable(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN,
                ActionCreatePreparedSubstraitPlanRequest.class);
    }

    @Test
    public void beginTransaction() {
        actionUnimplemented(() -> flightSqlClient.beginTransaction(), FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION, ActionBeginTransactionRequest.class);
    }

    @Test
    public void commit() {
        actionUnimplemented(() -> flightSqlClient.commit(fakeTxn()), FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION, ActionEndTransactionRequest.class);
    }

    @Test
    public void rollbackTxn() {
        actionUnimplemented(() -> flightSqlClient.rollback(fakeTxn()), FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION, ActionEndTransactionRequest.class);
    }

    @Test
    public void beginSavepoint() {
        actionUnimplemented(() -> flightSqlClient.beginSavepoint(fakeTxn(), "fakeName"),
                FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT, ActionBeginSavepointRequest.class);
    }

    @Test
    public void release() {
        actionUnimplemented(() -> flightSqlClient.release(fakeSavepoint()), FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT, ActionEndSavepointRequest.class);
    }

    @Test
    public void rollbackSavepoint() {
        actionUnimplemented(() -> flightSqlClient.rollback(fakeSavepoint()), FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        unpackable(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT, ActionEndSavepointRequest.class);
    }

    @Test
    public void cancelQuery() {
        final FlightInfo info = flightSqlClient.execute("SELECT 1");
        actionUnimplemented(() -> flightSqlClient.cancelQuery(info), FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY);
        unpackable(FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY, ActionCancelQueryRequest.class);
    }

    @Test
    public void cancelFlightInfo() {
        // Note: this should likely be tested in the context of Flight, not Flight SQL
        final FlightInfo info = flightSqlClient.execute("SELECT 1");
        actionNoResolver(() -> flightClient.cancelFlightInfo(new CancelFlightInfoRequest(info)),
                FlightConstants.CANCEL_FLIGHT_INFO.getType());
    }

    @Test
    public void unknownAction() {
        // Note: this should likely be tested in the context of Flight, not Flight SQL
        final String type = "SomeFakeAction";
        final Action action = new Action(type, new byte[0]);
        actionNoResolver(() -> doAction(action), type);
    }

    private Result doAction(Action action) {
        final Iterator<Result> it = flightClient.doAction(action);
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final Result result = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return result;
    }

    private void misbehave(Message message, Descriptor descriptor) {
        final Ticket ticket = ProtocolExposer.fromProtocol(Flight.Ticket.newBuilder()
                .setTicket(
                        ByteString.copyFrom(new byte[] {(byte) TICKET_PREFIX}).concat(Any.pack(message).toByteString()))
                .build());
        expectException(() -> flightSqlClient.getStream(ticket).next(), FlightStatusCode.INVALID_ARGUMENT,
                "Flight SQL: Invalid ticket");
    }

    private static FlightDescriptor unpackableCommand(Descriptor descriptor) {
        return unpackableCommand("type.googleapis.com/" + descriptor.getFullName());
    }

    private static FlightDescriptor unpackableCommand(String typeUrl) {
        return FlightDescriptor.command(
                Any.newBuilder().setTypeUrl(typeUrl).setValue(ByteString.copyFrom(new byte[1])).build().toByteArray());
    }

    private void getSchemaUnimplemented(Runnable r, Descriptor command) {
        // right now our server impl routes all getSchema through their respective commands
        commandUnimplemented(r, command);
    }

    private void commandUnimplemented(Runnable r, Descriptor command) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("Flight SQL: command '%s' is unimplemented", command.getFullName()));
    }

    private void getSchemaUnknown(Runnable r, String command) {
        // right now our server impl routes all getSchema through their respective commands
        commandUnknown(r, command);
    }

    private void commandUnknown(Runnable r, String command) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("Flight SQL: command '%s' is unknown", command));
    }

    private void unpackable(Descriptor descriptor, Class<?> clazz) {
        final FlightDescriptor flightDescriptor = unpackableCommand(descriptor);
        getSchemaUnpackable(() -> flightClient.getSchema(flightDescriptor), clazz);
        commandUnpackable(() -> flightClient.getInfo(flightDescriptor), clazz);
    }


    private void unpackable(ActionType type, Class<?> actionProto) {
        {
            final Action action = new Action(type.getType(), Any.getDefaultInstance().toByteArray());
            expectException(() -> doAction(action), FlightStatusCode.INVALID_ARGUMENT, String.format(
                    "Flight SQL: Invalid action, provided message cannot be unpacked as %s", actionProto.getName()));
        }
        {
            final Action action = new Action(type.getType(), new byte[] {-1});
            expectException(() -> doAction(action), FlightStatusCode.INVALID_ARGUMENT, "Flight SQL: Invalid action");
        }
    }

    private void getSchemaUnpackable(Runnable r, Class<?> clazz) {
        commandUnpackable(r, clazz);
    }

    private void commandUnpackable(Runnable r, Class<?> clazz) {
        expectUnpackableCommand(r, clazz);
    }

    private void expectUnpackableCommand(Runnable r, Class<?> clazz) {
        expectException(r, FlightStatusCode.INVALID_ARGUMENT,
                String.format("Flight SQL: Invalid command, provided message cannot be unpacked as %s",
                        clazz.getName()));
    }

    private void expectUnpublishable(Runnable r) {
        expectException(r, FlightStatusCode.INVALID_ARGUMENT, "Flight SQL descriptors cannot be published to");
    }

    private void actionUnimplemented(Runnable r, ActionType actionType) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("Flight SQL: Action type '%s' is unimplemented", actionType.getType()));
    }

    private void actionNoResolver(Runnable r, String actionType) {
        expectException(r, FlightStatusCode.UNIMPLEMENTED,
                String.format("No action resolver found for action type '%s'", actionType));
    }

    private static void expectException(Runnable r, FlightStatusCode code, String messagePart) {
        try {
            r.run();
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            assertThat(e.status().code()).isEqualTo(code);
            assertThat(e).hasMessageContaining(messagePart);
        }
    }

    private static FlightEndpoint endpoint(FlightInfo info) {
        assertThat(info.getEndpoints()).hasSize(1);
        return info.getEndpoints().get(0);
    }

    private static Schema flatTableSchema(Field... fields) {
        return new Schema(List.of(fields), FLAT_ATTRIBUTES);
    }

    private static void setFooTable() {
        setSimpleTable("foo_table", "Foo");
    }

    private static void setFoodTable() {
        setSimpleTable("foodtable", "Food");
    }

    private static void setBarTable() {
        setSimpleTable("barTable", "Bar");
    }

    private static void removeFooTable() {
        removeTable("foo_table");
    }

    private static void removeFoodTable() {
        removeTable("foodtable");
    }

    private static void removeBarTable() {
        removeTable("barTable");
    }

    private static void setSimpleTable(String tableName, String columnName) {
        final TableDefinition td = TableDefinition.of(ColumnDefinition.ofInt(columnName));
        final Table table = TableTools.newTable(td, TableTools.intCol(columnName, 1, 2, 3));
        ExecutionContext.getContext().getQueryScope().putParam(tableName, table);
    }

    private static void removeTable(String tableName) {
        ExecutionContext.getContext().getQueryScope().putParam(tableName, null);
    }

    private void consume(FlightInfo info, int expectedFlightCount, int expectedNumRows, boolean expectReusable)
            throws Exception {
        final FlightEndpoint endpoint = endpoint(info);
        if (expectReusable) {
            assertThat(endpoint.getExpirationTime()).isPresent();
        } else {
            assertThat(endpoint.getExpirationTime()).isEmpty();
        }
        try (final FlightStream stream = flightSqlClient.getStream(endpoint.getTicket())) {
            consume(stream, expectedFlightCount, expectedNumRows);
        }
        if (expectReusable) {
            try (final FlightStream stream = flightSqlClient.getStream(endpoint.getTicket())) {
                consume(stream, expectedFlightCount, expectedNumRows);
            }
        } else {
            try (final FlightStream stream = flightSqlClient.getStream(endpoint.getTicket())) {
                consumeNotFound(stream);
            }
        }
    }

    private static void consume(FlightStream stream, int expectedFlightCount, int expectedNumRows) {
        int numRows = 0;
        int flightCount = 0;
        while (stream.next()) {
            ++flightCount;
            numRows += stream.getRoot().getRowCount();
        }
        assertThat(flightCount).isEqualTo(expectedFlightCount);
        assertThat(numRows).isEqualTo(expectedNumRows);
    }

    private static void consumeNotFound(FlightStream stream) {
        expectException(stream::next, FlightStatusCode.NOT_FOUND,
                "Unable to find Flight SQL query. Flight SQL tickets should be resolved promptly and resolved at most once.");
    }

    private static SubstraitPlan fakePlan() {
        return new SubstraitPlan("fake".getBytes(StandardCharsets.UTF_8), "1");
    }

    private static Transaction fakeTxn() {
        return new Transaction("fake".getBytes(StandardCharsets.UTF_8));
    }

    private static Savepoint fakeSavepoint() {
        return new Savepoint("fake".getBytes(StandardCharsets.UTF_8));
    }
}
