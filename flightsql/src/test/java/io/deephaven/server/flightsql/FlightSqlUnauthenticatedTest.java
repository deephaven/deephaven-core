//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.deephaven.server.runner.DeephavenApiServerTestBase.TestComponent.Builder;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.Savepoint;
import org.apache.arrow.flight.sql.FlightSqlClient.SubstraitPlan;
import org.apache.arrow.flight.sql.FlightSqlClient.Transaction;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

// using JUnit4 so we can inherit properly from DeephavenApiServerTestBase
@RunWith(JUnit4.class)
public class FlightSqlUnauthenticatedTest extends DeephavenApiServerTestBase {

    private static final TableRef FOO_TABLE_REF = TableRef.of(null, null, "foo_table");
    public static final TableRef BAR_TABLE_REF = TableRef.of(null, null, "bar_table");

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

    private BufferAllocator bufferAllocator;
    private FlightClient flightClient;
    private FlightSqlClient flightSqlClient;

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
    public void listActions() {
        // Note: this should likely be tested in the context of Flight, not Flight SQL
        assertThat(flightClient.listActions()).isEmpty();
    }

    @Test
    public void listFlights() {
        // Note: this should likely be tested in the context of Flight, not Flight SQL
        assertThat(flightClient.listFlights(Criteria.ALL)).isEmpty();
    }

    @Test
    public void getCatalogs() {
        unauthenticated(() -> flightSqlClient.getCatalogsSchema());
        unauthenticated(() -> flightSqlClient.getCatalogs());
    }

    @Test
    public void getSchemas() {
        unauthenticated(() -> flightSqlClient.getSchemasSchema());
        unauthenticated(() -> flightSqlClient.getSchemas(null, null));
    }

    @Test
    public void getTables() throws Exception {
        unauthenticated(() -> flightSqlClient.getTablesSchema(false));
        unauthenticated(() -> flightSqlClient.getTablesSchema(true));
        unauthenticated(() -> flightSqlClient.getTables(null, null, null, null, false));
        unauthenticated(() -> flightSqlClient.getTables(null, null, null, null, true));
    }

    @Test
    public void getTableTypes() throws Exception {
        unauthenticated(() -> flightSqlClient.getTableTypesSchema());
        unauthenticated(() -> flightSqlClient.getTableTypes());
    }

    @Test
    public void select1() throws Exception {
        unauthenticated(() -> flightSqlClient.getExecuteSchema("SELECT 1 as Foo"));
        unauthenticated(() -> flightSqlClient.execute("SELECT 1 as Foo"));
    }

    @Test
    public void select1Prepared() throws Exception {
        unauthenticated(() -> flightSqlClient.prepare("SELECT 1 as Foo"));
    }

    @Test
    public void executeSubstrait() {
        unauthenticated(() -> flightSqlClient.getExecuteSubstraitSchema(fakePlan()));
        unauthenticated(() -> flightSqlClient.executeSubstrait(fakePlan()));
    }

    @Test
    public void executeUpdate() {
        // We are unable to hook in earlier atm than
        // io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver.DoPutObserver
        // so we are unable to provide Flight SQL-specific error message. This could be remedied in the future with an
        // update to TicketResolver.
        try {
            flightSqlClient.executeUpdate("INSERT INTO fake(name) VALUES('Smith')");
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            assertThat(e.status().code()).isEqualTo(FlightStatusCode.UNAUTHENTICATED);
            assertThat(e).hasMessage("");
        }
    }

    @Test
    public void executeSubstraitUpdate() {
        // We are unable to hook in earlier atm than
        // io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver.DoPutObserver
        // so we are unable to provide Flight SQL-specific error message. This could be remedied in the future with an
        // update to TicketResolver.
        try {
            flightSqlClient.executeSubstraitUpdate(fakePlan());
            failBecauseExceptionWasNotThrown(FlightRuntimeException.class);
        } catch (FlightRuntimeException e) {
            assertThat(e.status().code()).isEqualTo(FlightStatusCode.UNAUTHENTICATED);
            assertThat(e).hasMessage("");
        }
    }

    @Test
    public void getSqlInfo() {
        unauthenticated(() -> flightSqlClient.getSqlInfoSchema());
        unauthenticated(() -> flightSqlClient.getSqlInfo());
    }

    @Test
    public void getXdbcTypeInfo() {
        unauthenticated(() -> flightSqlClient.getXdbcTypeInfoSchema());
        unauthenticated(() -> flightSqlClient.getXdbcTypeInfo());
    }

    @Test
    public void getCrossReference() {
        unauthenticated(() -> flightSqlClient.getCrossReferenceSchema());
        unauthenticated(() -> flightSqlClient.getCrossReference(FOO_TABLE_REF, BAR_TABLE_REF));
    }

    @Test
    public void getPrimaryKeys() {
        unauthenticated(() -> flightSqlClient.getPrimaryKeysSchema());
        unauthenticated(() -> flightSqlClient.getPrimaryKeys(FOO_TABLE_REF));
    }

    @Test
    public void getExportedKeys() {
        unauthenticated(() -> flightSqlClient.getExportedKeysSchema());
        unauthenticated(() -> flightSqlClient.getExportedKeys(FOO_TABLE_REF));
    }

    @Test
    public void getImportedKeys() {
        unauthenticated(() -> flightSqlClient.getImportedKeysSchema());
        unauthenticated(() -> flightSqlClient.getImportedKeys(FOO_TABLE_REF));
    }

    @Test
    public void commandStatementIngest() {
        // This is a real newer Flight SQL command.
        // Once we upgrade to newer Flight SQL, we can change this to Unimplemented and use the proper APIs.
        final String typeUrl = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementIngest";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        unauthenticated(() -> flightClient.getSchema(descriptor));
        unauthenticated(() -> flightClient.getInfo(descriptor));
    }

    @Test
    public void unknownCommandLooksLikeFlightSql() {
        final String typeUrl = "type.googleapis.com/arrow.flight.protocol.sql.CommandLooksRealButDoesNotExist";
        final FlightDescriptor descriptor = unpackableCommand(typeUrl);
        unauthenticated(() -> flightClient.getSchema(descriptor));
        unauthenticated(() -> flightClient.getInfo(descriptor));
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
        unauthenticated(() -> flightSqlClient.prepare(fakePlan()));
    }

    @Test
    public void beginTransaction() {
        unauthenticated(() -> flightSqlClient.beginTransaction());
    }

    @Test
    public void commit() {
        unauthenticated(() -> flightSqlClient.commit(fakeTxn()));
    }

    @Test
    public void rollbackTxn() {
        unauthenticated(() -> flightSqlClient.rollback(fakeTxn()));
    }

    @Test
    public void beginSavepoint() {
        unauthenticated(() -> flightSqlClient.beginSavepoint(fakeTxn(), "fakeName"));
    }

    @Test
    public void release() {
        unauthenticated(() -> flightSqlClient.release(fakeSavepoint()));
    }

    @Test
    public void rollbackSavepoint() {
        unauthenticated(() -> flightSqlClient.rollback(fakeSavepoint()));
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

    private static FlightDescriptor unpackableCommand(String typeUrl) {
        return FlightDescriptor.command(
                Any.newBuilder().setTypeUrl(typeUrl).setValue(ByteString.copyFrom(new byte[1])).build().toByteArray());
    }

    private void unauthenticated(Runnable r) {
        expectException(r, FlightStatusCode.UNAUTHENTICATED, "Flight SQL: Must be authenticated");
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
