//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.sql.Sql;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.type.Type;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.impl.Flight.Result;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.server.session.FlightSqlTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.server.session.FlightSqlTicketHelper.TICKET_PREFIX;
import static org.apache.arrow.flight.sql.FlightSqlUtils.unpackOrThrow;

@Singleton
public final class FlightSqlTicketResolver extends TicketResolverBase {

    @VisibleForTesting
    static final String CREATE_PREPARED_STATEMENT_ACTION_TYPE = "CreatePreparedStatement";

    @VisibleForTesting
    static final String CLOSE_PREPARED_STATEMENT_ACTION_TYPE = "ClosePreparedStatement";

    private static final String FLIGHT_SQL_COMMAND_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementQuery";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL =
            FLIGHT_SQL_COMMAND_PREFIX + "CommandPreparedStatementQuery";

    @VisibleForTesting
    static final String COMMAND_GET_TABLE_TYPES_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetTableTypes";

    @VisibleForTesting
    static final String COMMAND_GET_CATALOGS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetCatalogs";

    @VisibleForTesting
    static final String COMMAND_GET_DB_SCHEMAS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetDbSchemas";

    @VisibleForTesting
    static final String COMMAND_GET_TABLES_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetTables";

    /**
     * Note: FlightSqlUtils.FLIGHT_SQL_ACTIONS is not all the actions, see
     * <a href="https://github.com/apache/arrow/pull/43718">Add all ActionTypes to FlightSqlUtils.FLIGHT_SQL_ACTIONS</a>
     */
    private static final Set<String> FLIGHT_SQL_ACTION_TYPES = Stream.of(
            FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT,
            FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION,
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
            FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT,
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN,
            FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY,
            FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT,
            FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION)
            .map(ActionType::getType)
            .collect(Collectors.toSet());

    @VisibleForTesting
    static final TableDefinition GET_TABLE_TYPES_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("table_type"));

    @VisibleForTesting
    static final TableDefinition GET_CATALOGS_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("catalog_name"));

    @VisibleForTesting
    static final TableDefinition GET_DB_SCHEMAS_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("catalog_name"),
            ColumnDefinition.ofString("db_schema_name"));

    @VisibleForTesting
    static final TableDefinition GET_TABLES_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("catalog_name"),
            ColumnDefinition.ofString("db_schema_name"),
            ColumnDefinition.ofString("table_name"),
            ColumnDefinition.ofString("table_type"),
            ColumnDefinition.of("table_schema", Type.byteType().arrayType()));

    // Unable to depends on TicketRouter, would be a circular dependency atm (since TicketRouter depends on all of the
    // TicketResolvers).
    // private final TicketRouter router;
    private final ScopeTicketResolver scopeTicketResolver;

    @Inject
    public FlightSqlTicketResolver(final AuthorizationProvider authProvider,
            final ScopeTicketResolver scopeTicketResolver) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    // TODO: we should probably plumb optional TicketResolver support that allows efficient
    // io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema without needing to go through flightInfoFor

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }
        if (descriptor.getType() != DescriptorType.CMD) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    String.format("Unsupported descriptor type '%s'", descriptor.getType()));
        }
        return session.<Flight.FlightInfo>nonExport().submit(() -> {
            final Table table = execute(session, descriptor);
            final ExportObject<Table> sse = session.newServerSideExport(table);
            final int exportId = sse.getExportIdInt();
            return TicketRouter.getFlightInfo(table, descriptor,
                    FlightSqlTicketHelper.exportIdToFlightTicket(exportId));
        });
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // todo: should we list all of them?
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without an active session");
        }
        return session.getExport(FlightSqlTicketHelper.ticketToExportId(ticket, logId));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // this general interface does not make sense to me
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': SQL tickets cannot be published to");
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': SQL descriptors cannot be published to");
    }

    @Override
    public boolean supportsCommand(FlightDescriptor descriptor) {
        try {
            return Any.parseFrom(descriptor.getCmd()).getTypeUrl().startsWith(FLIGHT_SQL_COMMAND_PREFIX);
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
    }

    @Override
    public boolean supportsDoActionType(String type) {
        // todo: should we support all types, and then throw more appropriate error in doAction?
        return FLIGHT_SQL_ACTION_TYPES.contains(type);
    }

    @Override
    public void doAction(@Nullable SessionState session, Flight.Action actionRequest,
            StreamObserver<Result> responseObserver) {
        // todo: catch exceptions
        switch (actionRequest.getType()) {
            case CREATE_PREPARED_STATEMENT_ACTION_TYPE: {
                final ActionCreatePreparedStatementRequest request =
                        unpack(actionRequest, ActionCreatePreparedStatementRequest.class);
                final ActionCreatePreparedStatementResult response = createPreparedStatement(session, request);
                safelyComplete(responseObserver, response);
                return;
            }
            case CLOSE_PREPARED_STATEMENT_ACTION_TYPE: {
                final ActionClosePreparedStatementRequest request =
                        unpack(actionRequest, ActionClosePreparedStatementRequest.class);
                closePreparedStatement(session, request);
                // no responses
                GrpcUtil.safelyComplete(responseObserver);
                return;
            }
        }
        GrpcUtil.safelyError(responseObserver, Code.UNIMPLEMENTED,
                String.format("FlightSql action '%s' is not implemented", actionRequest.getType()));
    }

    private ActionCreatePreparedStatementResult createPreparedStatement(@Nullable SessionState session,
            ActionCreatePreparedStatementRequest request) {
        if (request.hasTransactionId()) {
            throw new IllegalArgumentException("Transactions not supported");
        }
        // Hack, we are just passing the SQL through the "handle"
        return ActionCreatePreparedStatementResult.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8(request.getQuery()))
                .build();
    }

    private void closePreparedStatement(@Nullable SessionState session, ActionClosePreparedStatementRequest request) {
        // todo: release the server exports?
    }

    private Table execute(SessionState sessionState, final Flight.FlightDescriptor descriptor) {
        final Any any = parseOrThrow(descriptor.getCmd());
        switch (any.getTypeUrl()) {
            case COMMAND_STATEMENT_QUERY_TYPE_URL:
                return execute(sessionState, unpackOrThrow(any, CommandStatementQuery.class));
            case COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                return execute(sessionState, unpackOrThrow(any, CommandPreparedStatementQuery.class));
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return execute(unpackOrThrow(any, CommandGetTableTypes.class));
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return execute(unpackOrThrow(any, CommandGetCatalogs.class));
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return execute(unpackOrThrow(any, CommandGetDbSchemas.class));
            case COMMAND_GET_TABLES_TYPE_URL:
                return execute(unpackOrThrow(any, CommandGetTables.class));
        }
        throw new UnsupportedOperationException("todo");
    }

    private Table execute(SessionState sessionState, CommandStatementQuery query) {
        if (query.hasTransactionId()) {
            throw new IllegalArgumentException("Transactions not supported");
        }
        return executSqlQuery(sessionState, query.getQuery());
    }

    private Table execute(SessionState sessionState, CommandPreparedStatementQuery query) {
        // Hack, we are just passing the SQL through the "handle"
        return executSqlQuery(sessionState, query.getPreparedStatementHandle().toStringUtf8());
    }

    private Table executSqlQuery(SessionState sessionState, String sql) {
        // See SQLTODO(catalog-reader-implementation)
        // final QueryScope queryScope = sessionState.getExecutionContext().getQueryScope();
        // Note: ScopeTicketResolver uses from ExecutionContext.getContext() instead of session...
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        // noinspection unchecked,rawtypes
        final Map<String, Table> queryScopeTables =
                (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
        final TableSpec tableSpec = Sql.parseSql(sql, queryScopeTables, TicketTable::fromQueryScopeField, null);
        // Note: this is doing io.deephaven.server.session.TicketResolver.Authorization.transform, but not
        // io.deephaven.auth.ServiceAuthWiring
        return tableSpec.logic()
                .create(new TableCreatorScopeTickets(TableCreatorImpl.INSTANCE, scopeTicketResolver, sessionState));
    }

    private Table execute(CommandGetTableTypes request) {
        return TableTools.newTable(GET_TABLE_TYPES_DEFINITION,
                TableTools.stringCol("table_type", "TABLE"));
    }

    private Table execute(CommandGetCatalogs request) {
        return TableTools.newTable(GET_CATALOGS_DEFINITION);
    }

    private Table execute(CommandGetDbSchemas request) {
        return TableTools.newTable(GET_DB_SCHEMAS_DEFINITION);
    }

    private Table execute(CommandGetTables request) {
        return TableTools.newTable(GET_TABLES_DEFINITION);
    }

    private static void safelyComplete(StreamObserver<Result> responseObserver, com.google.protobuf.Message response) {
        GrpcUtil.safelyComplete(responseObserver, pack(response));
    }

    private static <T extends com.google.protobuf.Message> T unpack(Action action, Class<T> clazz) {
        // A more efficient version of
        // org.apache.arrow.flight.sql.FlightSqlUtils.unpackAndParseOrThrow
        // TODO: should we do statusruntimeexception instead?
        final Any any = parseOrThrow(action.getBody());
        return unpackOrThrow(any, clazz);
    }

    private static Result pack(com.google.protobuf.Message message) {
        return Result.newBuilder().setBody(Any.pack(message).toByteString()).build();
    }

    private static Any parseOrThrow(ByteString data) {
        // A more efficient version of
        // org.apache.arrow.flight.sql.FlightSqlUtils.parseOrThrow
        // TODO: should we do statusruntimeexception instead?
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Received invalid message from remote.")
                    .withCause(e)
                    .toRuntimeException();
        }
    }
}
