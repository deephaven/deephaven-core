//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.sql.Sql;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.type.Type;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.ActionResolver;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.apache.arrow.flight.impl.Flight.Result;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.server.flightsql.FlightSqlTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.server.flightsql.FlightSqlTicketHelper.TICKET_PREFIX;

@Singleton
public final class FlightSqlResolver extends TicketResolverBase implements ActionResolver {

    @VisibleForTesting
    static final String CREATE_PREPARED_STATEMENT_ACTION_TYPE = "CreatePreparedStatement";

    @VisibleForTesting
    static final String CLOSE_PREPARED_STATEMENT_ACTION_TYPE = "ClosePreparedStatement";

    @VisibleForTesting
    static final String BEGIN_SAVEPOINT_ACTION_TYPE = "BeginSavepoint";

    @VisibleForTesting
    static final String END_SAVEPOINT_ACTION_TYPE = "EndSavepoint";

    @VisibleForTesting
    static final String BEGIN_TRANSACTION_ACTION_TYPE = "BeginTransaction";

    @VisibleForTesting
    static final String END_TRANSACTION_ACTION_TYPE = "EndTransaction";

    @VisibleForTesting
    static final String CANCEL_QUERY_ACTION_TYPE = "CancelQuery";

    @VisibleForTesting
    static final String CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE = "CreatePreparedSubstraitPlan";

    /**
     * Note: FlightSqlUtils.FLIGHT_SQL_ACTIONS is not all the actions, see
     * <a href="https://github.com/apache/arrow/pull/43718">Add all ActionTypes to FlightSqlUtils.FLIGHT_SQL_ACTIONS</a>
     *
     * <p>
     * It is unfortunate that there is no proper prefix or namespace for these action types, which would make it much
     * easier to route correctly.
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

    private static final Set<ActionType> SUPPORTED_FLIGHT_SQL_ACTION_TYPES = Set.of(
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
            FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);

    private static final String FLIGHT_SQL_COMMAND_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementQuery";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_UPDATE_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementUpdate";

    // Need to update to newer FlightSql version for this
    // @VisibleForTesting
    // static final String COMMAND_STATEMENT_INGEST_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementIngest";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL =
            FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementSubstraitPlan";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL =
            FLIGHT_SQL_COMMAND_PREFIX + "CommandPreparedStatementQuery";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL =
            FLIGHT_SQL_COMMAND_PREFIX + "CommandPreparedStatementUpdate";

    @VisibleForTesting
    static final String COMMAND_GET_TABLE_TYPES_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetTableTypes";

    @VisibleForTesting
    static final String COMMAND_GET_CATALOGS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetCatalogs";

    @VisibleForTesting
    static final String COMMAND_GET_DB_SCHEMAS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetDbSchemas";

    @VisibleForTesting
    static final String COMMAND_GET_TABLES_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetTables";

    @VisibleForTesting
    static final String COMMAND_GET_SQL_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetSqlInfo";

    @VisibleForTesting
    static final String COMMAND_GET_CROSS_REFERENCE_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetCrossReference";

    @VisibleForTesting
    static final String COMMAND_GET_EXPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetExportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_IMPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetImportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_PRIMARY_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetPrimaryKeys";

    @VisibleForTesting
    static final String COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandGetXdbcTypeInfo";

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

    @VisibleForTesting
    static final TableDefinition GET_TABLES_DEFINITION_NO_SCHEMA = TableDefinition.of(
            ColumnDefinition.ofString("catalog_name"),
            ColumnDefinition.ofString("db_schema_name"),
            ColumnDefinition.ofString("table_name"),
            ColumnDefinition.ofString("table_type"));

    // Unable to depends on TicketRouter, would be a circular dependency atm (since TicketRouter depends on all of the
    // TicketResolvers).
    // private final TicketRouter router;
    private final ScopeTicketResolver scopeTicketResolver;

    @Inject
    public FlightSqlResolver(final AuthorizationProvider authProvider,
            final ScopeTicketResolver scopeTicketResolver) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    // We should probably plumb optional TicketResolver support that allows efficient
    // io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema without needing to go through flightInfoFor

    @Override
    public ExportObject<FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }
        if (descriptor.getType() != DescriptorType.CMD) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    String.format("Unsupported descriptor type '%s'", descriptor.getType()));
        }
        // Doing as much validation outside of the export as we can.
        final Any any = parseOrThrow(descriptor.getCmd());
        final Supplier<Table> command = supplier(session, any, comm(any));
        return session.<Flight.FlightInfo>nonExport().submit(() -> {
            final Table table = command.get();
            final ExportObject<Table> sse = session.newServerSideExport(table);
            final int exportId = sse.getExportIdInt();
            return TicketRouter.getFlightInfo(table, descriptor,
                    FlightSqlTicketHelper.exportIdToFlightTicket(exportId));
        });
    }

    private static <T> Supplier<Table> supplier(SessionState sessionState, Any any, Command<T> command) {
        final T request = command.parse(any);
        command.validate(request);
        return () -> command.execute(sessionState, request);
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {

    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no FlightSQL tickets can exist without an active session");
        }
        return session.getExport(FlightSqlTicketHelper.ticketToExportId(ticket, logId));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // this general interface does not make sense
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL tickets cannot be published to");
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL descriptors cannot be published to");
    }

    @Override
    public boolean supportsCommand(FlightDescriptor descriptor) {
        // No good way to check if this is a valid command without parsing to Any first.
        final Any any;
        try {
            any = Any.parseFrom(descriptor.getCmd());
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
        return any.getTypeUrl().startsWith(FLIGHT_SQL_COMMAND_PREFIX);
    }

    @Override
    public boolean supportsDoActionType(String type) {
        return FLIGHT_SQL_ACTION_TYPES.contains(type);
    }

    @Override
    public void forAllFlightActionType(@Nullable SessionState session, Consumer<ActionType> visitor) {
        visitor.accept(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        visitor.accept(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
    }

    @Override
    public void doAction(@Nullable SessionState session, Flight.Action actionRequest, Consumer<Result> visitor) {
        final String type = actionRequest.getType();
        switch (type) {
            case CREATE_PREPARED_STATEMENT_ACTION_TYPE: {
                final ActionCreatePreparedStatementRequest request =
                        unpack(actionRequest, ActionCreatePreparedStatementRequest.class);
                final ActionCreatePreparedStatementResult response = createPreparedStatement(session, request);
                visitor.accept(pack(response));
                return;
            }
            case CLOSE_PREPARED_STATEMENT_ACTION_TYPE: {
                final ActionClosePreparedStatementRequest request =
                        unpack(actionRequest, ActionClosePreparedStatementRequest.class);
                closePreparedStatement(session, request);
                // no response
                return;
            }
            case BEGIN_SAVEPOINT_ACTION_TYPE:
            case END_SAVEPOINT_ACTION_TYPE:
            case BEGIN_TRANSACTION_ACTION_TYPE:
            case END_TRANSACTION_ACTION_TYPE:
            case CANCEL_QUERY_ACTION_TYPE:
            case CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE:
                // TODO: try to parse request to make sure it's valid?
                throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                        String.format("FlightSQL Action type '%s' is unimplemented", type));
        }
        // Should not get to this point, should not be routed here if it's unknown
        throw Exceptions.statusRuntimeException(Code.INTERNAL,
                String.format("FlightSQL Action type '%s' is unknown", type));
    }

    private ActionCreatePreparedStatementResult createPreparedStatement(@Nullable SessionState session,
            ActionCreatePreparedStatementRequest request) {
        if (request.hasTransactionId()) {
            throw transactionIdsNotSupported();
        }
        // We should consider executing the sql here, attaching the ticket as the handle, in that way we can properly
        // release it during closePreparedStatement.
        return ActionCreatePreparedStatementResult.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8(request.getQuery()))
                .build();
    }

    private void closePreparedStatement(@Nullable SessionState session, ActionClosePreparedStatementRequest request) {

    }

    private Command<?> comm(Any any) {
        final String typeUrl = any.getTypeUrl();
        switch (typeUrl) {
            case COMMAND_STATEMENT_QUERY_TYPE_URL:
                return new CommandStatementQueryImpl();
            case COMMAND_STATEMENT_UPDATE_TYPE_URL:
                return new UnsupportedCommand<>(CommandStatementUpdate.class);
            case COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL:
                return new UnsupportedCommand<>(CommandStatementSubstraitPlan.class);
            case COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                return new CommandPreparedStatementQueryImpl();
            case COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL:
                return new UnsupportedCommand<>(CommandPreparedStatementUpdate.class);
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return new CommandGetTableTypesImpl();
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return new CommandGetCatalogsImpl();
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return new CommandGetDbSchemasImpl();
            case COMMAND_GET_TABLES_TYPE_URL:
                return new CommandGetTablesImpl();
            case COMMAND_GET_SQL_INFO_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetSqlInfo.class);
            case COMMAND_GET_CROSS_REFERENCE_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetCrossReference.class);
            case COMMAND_GET_EXPORTED_KEYS_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetExportedKeys.class);
            case COMMAND_GET_IMPORTED_KEYS_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetImportedKeys.class);
            case COMMAND_GET_PRIMARY_KEYS_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetPrimaryKeys.class);
            case COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL:
                return new UnsupportedCommand<>(CommandGetXdbcTypeInfo.class);
        }
        throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                String.format("FlightSQL command '%s' is unknown", typeUrl));
    }

    private Table execute(SessionState sessionState, CommandPreparedStatementQuery query) {
        // Hack, we are just passing the SQL through the "handle"
        return executeSqlQuery(sessionState, query.getPreparedStatementHandle().toStringUtf8());
    }

    private Table executeSqlQuery(SessionState sessionState, String sql) {
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
        return request.getIncludeSchema()
                ? TableTools.newTable(GET_TABLES_DEFINITION)
                : TableTools.newTable(GET_TABLES_DEFINITION_NO_SCHEMA);
    }

    private static StatusRuntimeException commandNotSupported(Descriptor descriptor) {
        throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                String.format("FlightSQL command '%s' is unimplemented", descriptor.getFullName()));
    }

    private static StatusRuntimeException transactionIdsNotSupported() {
        return Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "FlightSQL transaction ids are not supported");
    }

    private static <T extends com.google.protobuf.Message> T unpack(Action action, Class<T> clazz) {
        // A more efficient DH version of org.apache.arrow.flight.sql.FlightSqlUtils.unpackAndParseOrThrow
        final Any any = parseOrThrow(action.getBody());
        return unpackOrThrow(any, clazz);
    }

    private static Result pack(com.google.protobuf.Message message) {
        return Result.newBuilder().setBody(Any.pack(message).toByteString()).build();
    }

    private static Any parseOrThrow(ByteString data) {
        // A more efficient DH version of org.apache.arrow.flight.sql.FlightSqlUtils.parseOrThrow
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            // Same details as from org.apache.arrow.flight.sql.FlightSqlUtils.parseOrThrow
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Received invalid message from remote.");
        }
    }

    private static <T extends Message> T unpackOrThrow(Any source, Class<T> as) {
        // DH version of org.apache.arrow.flight.sql.FlightSqlUtils.unpackOrThrow
        try {
            return source.unpack(as);
        } catch (final InvalidProtocolBufferException e) {
            // Same details as from org.apache.arrow.flight.sql.FlightSqlUtils.unpackOrThrow
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("Provided message cannot be unpacked as " + as.getName() + ": " + e)
                    .withCause(e));
        }
    }


    interface Command<T> {
        T parse(Any any);

        void validate(T command);

        Table execute(SessionState sessionState, T command);
    }

    static abstract class CommandBase<T extends Message> implements Command<T> {
        private final Class<T> clazz;

        public CommandBase(Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public T parse(Any any) {
            return unpackOrThrow(any, clazz);
        }

        @Override
        public void validate(T command) {

        }
    }

    static class UnsupportedCommand<T extends Message> extends CommandBase<T> {
        public UnsupportedCommand(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void validate(T command) {
            throw commandNotSupported(command.getDescriptorForType());
        }

        @Override
        public Table execute(SessionState sessionState, T command) {
            throw new IllegalStateException();
        }
    }

    class CommandStatementQueryImpl extends CommandBase<CommandStatementQuery> {

        public CommandStatementQueryImpl() {
            super(CommandStatementQuery.class);
        }

        @Override
        public void validate(CommandStatementQuery command) {
            if (command.hasTransactionId()) {
                throw transactionIdsNotSupported();
            }
        }

        @Override
        public Table execute(SessionState sessionState, CommandStatementQuery command) {
            return executeSqlQuery(sessionState, command.getQuery());
        }
    }

    class CommandPreparedStatementQueryImpl extends CommandBase<CommandPreparedStatementQuery> {
        public CommandPreparedStatementQueryImpl() {
            super(CommandPreparedStatementQuery.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandPreparedStatementQuery command) {
            return FlightSqlResolver.this.execute(sessionState, command);
        }
    }

    class CommandGetTableTypesImpl extends CommandBase<CommandGetTableTypes> {
        public CommandGetTableTypesImpl() {
            super(CommandGetTableTypes.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetTableTypes command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    class CommandGetCatalogsImpl extends CommandBase<CommandGetCatalogs> {
        public CommandGetCatalogsImpl() {
            super(CommandGetCatalogs.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetCatalogs command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    class CommandGetDbSchemasImpl extends CommandBase<CommandGetDbSchemas> {
        public CommandGetDbSchemasImpl() {
            super(CommandGetDbSchemas.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetDbSchemas command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    class CommandGetTablesImpl extends CommandBase<CommandGetTables> {
        public CommandGetTablesImpl() {
            super(CommandGetTables.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetTables command) {
            return FlightSqlResolver.this.execute(command);
        }
    }
}
