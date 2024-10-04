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
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
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
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.apache.arrow.flight.impl.Flight.Result;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
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

    private static final String CATALOG_NAME = "catalog_name";
    private static final String DB_SCHEMA_NAME = "db_schema_name";
    private static final String TABLE_TYPE = "table_type";
    private static final String TABLE_NAME = "table_name";
    private static final String TABLE_SCHEMA = "table_schema";

    private static final String TABLE_TYPE_TABLE = "TABLE";

    @VisibleForTesting
    static final TableDefinition GET_TABLE_TYPES_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString(TABLE_TYPE));

    @VisibleForTesting
    static final TableDefinition GET_CATALOGS_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString(CATALOG_NAME));

    @VisibleForTesting
    static final TableDefinition GET_DB_SCHEMAS_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString(CATALOG_NAME),
            ColumnDefinition.ofString(DB_SCHEMA_NAME));

    @VisibleForTesting
    static final TableDefinition GET_TABLES_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString(CATALOG_NAME),
            ColumnDefinition.ofString(DB_SCHEMA_NAME),
            ColumnDefinition.ofString(TABLE_NAME),
            ColumnDefinition.ofString(TABLE_TYPE),
            ColumnDefinition.of(TABLE_SCHEMA, Type.byteType().arrayType()));

    @VisibleForTesting
    static final TableDefinition GET_TABLES_DEFINITION_NO_SCHEMA = TableDefinition.of(
            ColumnDefinition.ofString(CATALOG_NAME),
            ColumnDefinition.ofString(DB_SCHEMA_NAME),
            ColumnDefinition.ofString(TABLE_NAME),
            ColumnDefinition.ofString(TABLE_TYPE));

    // Unable to depends on TicketRouter, would be a circular dependency atm (since TicketRouter depends on all of the
    // TicketResolvers).
    // private final TicketRouter router;
    private final ScopeTicketResolver scopeTicketResolver;

    @Inject
    public FlightSqlResolver(
            final AuthorizationProvider authProvider,
            final ScopeTicketResolver scopeTicketResolver) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    private static <T> Supplier<Table> supplier(SessionState sessionState, CommandHandler<T> handler, Any any) {
        final T command = handler.parse(any);
        handler.validate(command);
        return () -> handler.execute(sessionState, command);
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

    // ---------------------------------------------------------------------------------------------------------------

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
        final Supplier<Table> command = supplier(session, commandHandler(any), any);
        return session.<Flight.FlightInfo>nonExport().submit(() -> {
            final Table table = command.get();
            // Note: the only way we clean up these tables is when the session is cleaned up.
            final ExportObject<Table> sse = session.newServerSideExport(table);
            final int exportId = sse.getExportIdInt();
            return TicketRouter.getFlightInfo(table, descriptor,
                    FlightSqlTicketHelper.exportIdToFlightTicket(exportId));
        });
    }

    private CommandHandler<?> commandHandler(Any any) {
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
                TableTools.stringCol(TABLE_TYPE, TABLE_TYPE_TABLE));
    }

    private Table execute(CommandGetCatalogs request) {
        return TableTools.newTable(GET_CATALOGS_DEFINITION);
    }

    private Table execute(CommandGetDbSchemas request) {
        return TableTools.newTable(GET_DB_SCHEMAS_DEFINITION);
    }

    private Table execute(CommandGetTables request) {
        final boolean includeSchema = request.getIncludeSchema();
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        final Map<String, Table> queryScopeTables =
                (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
        final int size = queryScopeTables.size();
        final String[] catalogNames = new String[size];
        final String[] dbSchemaNames = new String[size];
        final String[] tableNames = new String[size];
        final String[] tableTypes = new String[size];
        final byte[][] tableSchemas = includeSchema ? new byte[size][] : null;
        int ix = 0;
        for (Entry<String, Table> e : queryScopeTables.entrySet()) {
            catalogNames[ix] = null;
            dbSchemaNames[ix] = null;
            tableNames[ix] = e.getKey();
            tableTypes[ix] = TABLE_TYPE_TABLE;
            if (includeSchema) {
                tableSchemas[ix] = BarrageUtil.schemaBytesFromTable(e.getValue()).toByteArray();
            }
            ++ix;
        }
        final ColumnHolder<String> c1 = TableTools.stringCol(CATALOG_NAME, catalogNames);
        final ColumnHolder<String> c2 = TableTools.stringCol(DB_SCHEMA_NAME, dbSchemaNames);
        final ColumnHolder<String> c3 = TableTools.stringCol(TABLE_NAME, tableNames);
        final ColumnHolder<String> c4 = TableTools.stringCol(TABLE_TYPE, tableTypes);
        final ColumnHolder<byte[]> c5 = includeSchema
                ? new ColumnHolder<>(TABLE_SCHEMA, byte[].class, byte.class, false, tableSchemas)
                : null;
        return includeSchema
                ? TableTools.newTable(GET_TABLES_DEFINITION, c1, c2, c3, c4, c5)
                : TableTools.newTable(GET_TABLES_DEFINITION_NO_SCHEMA, c1, c2, c3, c4);
    }

    interface CommandHandler<T> {
        T parse(Any any);

        void validate(T command);

        Table execute(SessionState sessionState, T command);
    }

    static abstract class CommandBase<T extends Message> implements CommandHandler<T> {
        private final Class<T> clazz;

        public CommandBase(Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public final T parse(Any any) {
            return unpackOrThrow(any, clazz);
        }

        @Override
        public void validate(T command) {

        }
    }

    private static StatusRuntimeException commandNotSupported(Descriptor descriptor) {
        throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                String.format("FlightSQL command '%s' is unimplemented", descriptor.getFullName()));
    }

    final static class UnsupportedCommand<T extends Message> extends CommandBase<T> {
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

    final class CommandStatementQueryImpl extends CommandBase<CommandStatementQuery> {

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

    final class CommandPreparedStatementQueryImpl extends CommandBase<CommandPreparedStatementQuery> {
        public CommandPreparedStatementQueryImpl() {
            super(CommandPreparedStatementQuery.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandPreparedStatementQuery command) {
            return FlightSqlResolver.this.execute(sessionState, command);
        }
    }

    final class CommandGetTableTypesImpl extends CommandBase<CommandGetTableTypes> {
        public CommandGetTableTypesImpl() {
            super(CommandGetTableTypes.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetTableTypes command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    final class CommandGetCatalogsImpl extends CommandBase<CommandGetCatalogs> {
        public CommandGetCatalogsImpl() {
            super(CommandGetCatalogs.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetCatalogs command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    final class CommandGetDbSchemasImpl extends CommandBase<CommandGetDbSchemas> {
        public CommandGetDbSchemasImpl() {
            super(CommandGetDbSchemas.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetDbSchemas command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    final class CommandGetTablesImpl extends CommandBase<CommandGetTables> {
        public CommandGetTablesImpl() {
            super(CommandGetTables.class);
        }

        @Override
        public Table execute(SessionState sessionState, CommandGetTables command) {
            return FlightSqlResolver.this.execute(command);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

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
    public void doAction(@Nullable SessionState session, Flight.Action request, Consumer<Result> visitor) {
        executeAction(session, action(request), request, visitor);
    }

    private <Request, Response extends Message> void executeAction(
            @Nullable SessionState session,
            ActionHandler<Request, Response> handler,
            Action request,
            Consumer<Result> visitor) {
        handler.execute(session, handler.parse(request), new ResultVisitorAdapter<>(visitor));
    }

    private ActionHandler<?, ? extends Message> action(Action action) {
        final String type = action.getType();
        switch (type) {
            case CREATE_PREPARED_STATEMENT_ACTION_TYPE:
                return new CreatePreparedStatementImpl();
            case CLOSE_PREPARED_STATEMENT_ACTION_TYPE:
                return new ClosePreparedStatementImpl();
            case BEGIN_SAVEPOINT_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT,
                        ActionBeginSavepointRequest.class);
            case END_SAVEPOINT_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT,
                        ActionEndSavepointRequest.class);
            case BEGIN_TRANSACTION_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION,
                        ActionBeginTransactionRequest.class);
            case END_TRANSACTION_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION,
                        ActionEndTransactionRequest.class);
            case CANCEL_QUERY_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY, ActionCancelQueryRequest.class);
            case CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE:
                return new UnsupportedAction<>(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN,
                        ActionCreatePreparedSubstraitPlanRequest.class);
        }
        // Should not get to this point, should not be routed here if it's unknown
        throw Exceptions.statusRuntimeException(Code.INTERNAL,
                String.format("FlightSQL Action type '%s' is unknown", type));
    }

    private static <T extends com.google.protobuf.Message> T unpack(Action action, Class<T> clazz) {
        // A more efficient DH version of org.apache.arrow.flight.sql.FlightSqlUtils.unpackAndParseOrThrow
        final Any any = parseOrThrow(action.getBody());
        return unpackOrThrow(any, clazz);
    }

    private static Result pack(com.google.protobuf.Message message) {
        return Result.newBuilder().setBody(Any.pack(message).toByteString()).build();
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
        // no-op; eventually, we may want to release the export
    }

    interface ActionHandler<Request, Response> {
        Request parse(Flight.Action action);

        void execute(SessionState session, Request request, Consumer<Response> visitor);
    }

    static abstract class ActionBase<Request extends com.google.protobuf.Message, Response>
            implements ActionHandler<Request, Response> {

        final ActionType type;
        private final Class<Request> clazz;

        public ActionBase(ActionType type, Class<Request> clazz) {
            this.type = Objects.requireNonNull(type);
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public final Request parse(Action action) {
            if (!type.getType().equals(action.getType())) {
                // should be routed correctly earlier
                throw new IllegalStateException();
            }
            return unpack(action, clazz);
        }
    }

    final class CreatePreparedStatementImpl
            extends ActionBase<ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult> {
        public CreatePreparedStatementImpl() {
            super(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT, ActionCreatePreparedStatementRequest.class);
        }

        @Override
        public void execute(SessionState session, ActionCreatePreparedStatementRequest request,
                Consumer<ActionCreatePreparedStatementResult> visitor) {
            visitor.accept(createPreparedStatement(session, request));
        }
    }

    // Faking it as Empty message so it types check
    final class ClosePreparedStatementImpl extends ActionBase<ActionClosePreparedStatementRequest, Empty> {
        public ClosePreparedStatementImpl() {
            super(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT, ActionClosePreparedStatementRequest.class);
        }

        @Override
        public void execute(SessionState session, ActionClosePreparedStatementRequest request,
                Consumer<Empty> visitor) {
            closePreparedStatement(session, request);
            // no responses
        }
    }

    static final class UnsupportedAction<Request extends Message, Response> extends ActionBase<Request, Response> {
        public UnsupportedAction(ActionType type, Class<Request> clazz) {
            super(type, clazz);
        }

        @Override
        public void execute(SessionState session, Request request, Consumer<Response> visitor) {
            throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                    String.format("FlightSQL Action type '%s' is unimplemented", type.getType()));
        }
    }

    private static class ResultVisitorAdapter<Response extends Message> implements Consumer<Response> {
        private final Consumer<Result> delegate;

        public ResultVisitorAdapter(Consumer<Result> delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void accept(Response response) {
            delegate.accept(pack(response));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static StatusRuntimeException transactionIdsNotSupported() {
        return Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "FlightSQL transaction ids are not supported");
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
}
