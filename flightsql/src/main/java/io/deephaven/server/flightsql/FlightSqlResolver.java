//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.deephaven.engine.context.EmptyQueryScope;
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
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.hash.KeyedLongObjectKey.BasicStrict;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.type.Type;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.ActionResolver;
import io.deephaven.server.session.CommandResolver;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.sql.SqlParseException;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.apache.arrow.flight.impl.Flight.FlightEndpoint;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.apache.arrow.flight.impl.Flight.Ticket;
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
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.server.flightsql.FlightSqlTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.server.flightsql.FlightSqlTicketHelper.TICKET_PREFIX;

/**
 * A <a href="https://arrow.apache.org/docs/format/FlightSql.html">FlightSQL</a> resolver.
 *
 * <p>
 * Supported commands: {@link CommandStatementQuery}, {@link CommandPreparedStatementQuery}, {@link CommandGetTables},
 * {@link CommandGetCatalogs}, {@link CommandGetDbSchemas}, and {@link CommandGetTableTypes}.
 *
 * <p>
 * Supported actions: {@link FlightSqlUtils#FLIGHT_SQL_CREATE_PREPARED_STATEMENT} and
 * {@link FlightSqlUtils#FLIGHT_SQL_CLOSE_PREPARED_STATEMENT}.
 */
@Singleton
public final class FlightSqlResolver extends TicketResolverBase implements ActionResolver, CommandResolver {

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

    private static final String FLIGHT_SQL_TYPE_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandStatementQuery";

    // This is a server-implementation detail, but happens to be the same scheme that FlightSQL
    // org.apache.arrow.flight.sql.FlightSqlProducer uses
    static final String TICKET_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "TicketStatementQuery";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_UPDATE_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandStatementUpdate";

    // Need to update to newer FlightSql version for this
    // @VisibleForTesting
    // static final String COMMAND_STATEMENT_INGEST_TYPE_URL = FLIGHT_SQL_COMMAND_PREFIX + "CommandStatementIngest";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL =
            FLIGHT_SQL_TYPE_PREFIX + "CommandStatementSubstraitPlan";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL =
            FLIGHT_SQL_TYPE_PREFIX + "CommandPreparedStatementQuery";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL =
            FLIGHT_SQL_TYPE_PREFIX + "CommandPreparedStatementUpdate";

    @VisibleForTesting
    static final String COMMAND_GET_TABLE_TYPES_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetTableTypes";

    @VisibleForTesting
    static final String COMMAND_GET_CATALOGS_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetCatalogs";

    @VisibleForTesting
    static final String COMMAND_GET_DB_SCHEMAS_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetDbSchemas";

    @VisibleForTesting
    static final String COMMAND_GET_TABLES_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetTables";

    @VisibleForTesting
    static final String COMMAND_GET_SQL_INFO_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetSqlInfo";

    @VisibleForTesting
    static final String COMMAND_GET_CROSS_REFERENCE_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetCrossReference";

    @VisibleForTesting
    static final String COMMAND_GET_EXPORTED_KEYS_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetExportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_IMPORTED_KEYS_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetImportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_PRIMARY_KEYS_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetPrimaryKeys";

    @VisibleForTesting
    static final String COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "CommandGetXdbcTypeInfo";

    private static final String CATALOG_NAME = "catalog_name";
    private static final String DB_SCHEMA_NAME = "db_schema_name";
    private static final String TABLE_TYPE = "table_type";
    private static final String TABLE_NAME = "table_name";
    private static final String TABLE_SCHEMA = "table_schema";

    private static final String TABLE_TYPE_TABLE = "TABLE";

    // This should probably be less than the session refresh window
    private static final Duration TICKET_DURATION = Duration.ofMinutes(1);

    private static final Logger log = LoggerFactory.getLogger(FlightSqlResolver.class);

    private static final KeyedLongObjectKey<QueryBase> QUERY_KEY = new BasicStrict<>() {
        @Override
        public long getLongKey(QueryBase queryBase) {
            return queryBase.handleId;
        }
    };

    private static final KeyedLongObjectKey<PreparedStatement> PREPARED_STATEMENT_KEY = new BasicStrict<>() {
        @Override
        public long getLongKey(PreparedStatement preparedStatement) {
            return preparedStatement.handleId;
        }
    };

    @VisibleForTesting
    static final Schema DATASET_SCHEMA_SENTINEL = new Schema(List.of(Field.nullable("DO_NOT_USE", Utf8.INSTANCE)));

    private static final ByteString DATASET_SCHEMA_SENTINEL_BYTES = serializeMetadata(DATASET_SCHEMA_SENTINEL);

    // Unable to depends on TicketRouter, would be a circular dependency atm (since TicketRouter depends on all of the
    // TicketResolvers).
    // private final TicketRouter router;
    private final ScopeTicketResolver scopeTicketResolver;

    private final AtomicLong handleIdGenerator;

    private final KeyedLongObjectHashMap<QueryBase> queries;
    private final KeyedLongObjectHashMap<PreparedStatement> preparedStatements;
    private final ScheduledExecutorService scheduler;

    @Inject
    public FlightSqlResolver(
            final AuthorizationProvider authProvider,
            final ScopeTicketResolver scopeTicketResolver,
            final ScheduledExecutorService scheduler) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.scopeTicketResolver = Objects.requireNonNull(scopeTicketResolver);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.handleIdGenerator = new AtomicLong(100_000_000);
        this.queries = new KeyedLongObjectHashMap<>(QUERY_KEY);
        this.preparedStatements = new KeyedLongObjectHashMap<>(PREPARED_STATEMENT_KEY);
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public boolean handlesCommand(Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != DescriptorType.CMD) {
            throw new IllegalStateException("descriptor is not a command");
        }
        // No good way to check if this is a valid command without parsing to Any first.
        final Any command = parse(descriptor.getCmd()).orElse(null);
        return command != null && command.getTypeUrl().startsWith(FLIGHT_SQL_TYPE_PREFIX);
    }

    // We should probably plumb optional TicketResolver support that allows efficient
    // io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema without needing to go through flightInfoFor

    @Override
    public ExportObject<FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != DescriptorType.CMD) {
            // TODO: we should extract a PathResolver (like CommandResolver) so this can be elevated to a server
            // implementation issue instead of user facing error
            throw error(Code.FAILED_PRECONDITION,
                    String.format("Unsupported descriptor type '%s'", descriptor.getType()));
        }
        // todo: scope, nugget?
//        final CommandHandler commandHandler = commandHandler(session, command.getTypeUrl(), false);
//        final TicketHandler ticketHandler = commandHandler.initialize(command);
//        final FlightInfo info = ticketHandler.flightInfo(descriptor);
//        return SessionState.wrapAsExport(info);

        return session.<FlightInfo>nonExport().submit(() -> {
            final Any command = parseOrThrow(descriptor.getCmd());
            final CommandHandler commandHandler = commandHandler(session, command.getTypeUrl(), false);
            final TicketHandler ticketHandler = commandHandler.initialize(command);
            return ticketHandler.flightInfo(descriptor);
        });

//        final FlightInfo info = ticketHandler.flightInfo(descriptor);
//        return SessionState.wrapAsExport(info);
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
//        // todo: scope, nugget?
//        final Any message = FlightSqlTicketHelper.unpackTicket(ticket, logId);
//        final TicketHandler handler = ticketHandler(session, message);
//        final Table table = handler.takeTable(session);
//        // noinspection unchecked
//        return (ExportObject<T>) SessionState.wrapAsExport(table);

        //noinspection unchecked
        return (ExportObject<T>) session.<Table>nonExport().submit(() -> {
            final Any message = FlightSqlTicketHelper.unpackTicket(ticket, logId);
            final TicketHandler handler = ticketHandler(session, message);
            return handler.takeTable(session);
        });
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public void listActions(@Nullable SessionState session, Consumer<ActionType> visitor) {
        if (session != null) {
            visitor.accept(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
            visitor.accept(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
        }
    }

    @Override
    public boolean handlesActionType(String type) {
        // There is no prefix for FlightSQL action types, so the best we can do is a set-based lookup. This also means
        // that this resolver will not be able to respond with an appropriately scoped error message for new FlightSQL
        // action types (io.deephaven.server.flightsql.FlightSqlResolver.UnsupportedAction).
        return FLIGHT_SQL_ACTION_TYPES.contains(type);
    }

    @Override
    public void doAction(@Nullable SessionState session, org.apache.arrow.flight.Action action,
            Consumer<org.apache.arrow.flight.Result> visitor) {
        if (!handlesActionType(action.getType())) {
            throw new IllegalStateException(String.format("FlightSQL does not handle type '%s'", action.getType()));
        }
        executeAction(session, action(action), action, visitor);
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // This is a bit different than the other resolvers; a ticket may be a very long byte string here since it
        // may represent a command.
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {

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
        throw error(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL tickets cannot be published to");
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw error(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL descriptors cannot be published to");
    }

    // ---------------------------------------------------------------------------------------------------------------

    interface CommandHandler {

        TicketHandler initialize(Any any);
    }

    interface TicketHandler {

        FlightInfo flightInfo(FlightDescriptor descriptor);

        Table takeTable(SessionState session);
    }

    private CommandHandler commandHandler(SessionState session, String typeUrl, boolean fromTicket) {
        switch (typeUrl) {
            case COMMAND_STATEMENT_QUERY_TYPE_URL:
                if (fromTicket) {
                    // This should not happen with well-behaved clients; or it means there is an bug in our
                    // command/ticket logic
                    throw error(Code.INVALID_ARGUMENT, "Invalid ticket; please ensure client is using opaque ticket");
                }
                return new CommandStatementQueryImpl(session);
            case COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                if (session == null) {
                    throw noAuthForPrepared();
                }
                return new CommandPreparedStatementQueryImpl(session);
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return CommandGetTableTypesImpl.INSTANCE;
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return CommandGetCatalogsImpl.INSTANCE;
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return CommandGetDbSchemasImpl.INSTANCE;
            case COMMAND_GET_TABLES_TYPE_URL:
                return CommandGetTablesImpl.INSTANCE;
            case COMMAND_STATEMENT_UPDATE_TYPE_URL:
                return new UnsupportedCommand<>(CommandStatementUpdate.class);
            case COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL:
                return new UnsupportedCommand<>(CommandStatementSubstraitPlan.class);
            case COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL:
                return new UnsupportedCommand<>(CommandPreparedStatementUpdate.class);
            case COMMAND_GET_SQL_INFO_TYPE_URL:
                // Need dense_union support to implement this.
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
        throw error(Code.UNIMPLEMENTED,
                String.format("FlightSQL command '%s' is unknown", typeUrl));
    }

    private TicketHandler ticketHandler(SessionState session, Any message) {
        final String typeUrl = message.getTypeUrl();
        if (TICKET_STATEMENT_QUERY_TYPE_URL.equals(typeUrl)) {
            final TicketStatementQuery ticketStatementQuery = unpackOrThrow(message, TicketStatementQuery.class);
            final TicketHandler ticketHandler = queries.get(id(ticketStatementQuery));
            if (ticketHandler == null) {
                throw error(Code.NOT_FOUND,
                        "Unable to find FlightSQL query. FlightSQL tickets should be resolved promptly and resolved at most once.");
            }
            return ticketHandler;
        }
        final CommandHandler commandHandler = commandHandler(session, typeUrl, true);
        try {
            return commandHandler.initialize(message);
        } catch (StatusRuntimeException e) {
            // This should not happen with well-behaved clients; or it means there is an bug in our command/ticket logic
            throw error(Code.INVALID_ARGUMENT,
                    "Invalid ticket; please ensure client is using an opaque ticket", e);
        }
    }

    private Table executeSqlQuery(SessionState session, String sql) {
        // See SQLTODO(catalog-reader-implementation)
        // final QueryScope queryScope = sessionState.getExecutionContext().getQueryScope();
        // Note: ScopeTicketResolver uses from ExecutionContext.getContext() instead of session...
        final QueryScope queryScope =
                session == null ? EmptyQueryScope.INSTANCE : ExecutionContext.getContext().getQueryScope();
        // noinspection unchecked,rawtypes
        final Map<String, Table> queryScopeTables =
                (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
        final TableSpec tableSpec = Sql.parseSql(sql, queryScopeTables, TicketTable::fromQueryScopeField, null);
        // Note: this is doing io.deephaven.server.session.TicketResolver.Authorization.transform, but not
        // io.deephaven.auth.ServiceAuthWiring
        return tableSpec.logic()
                .create(new TableCreatorScopeTickets(TableCreatorImpl.INSTANCE, scopeTicketResolver, session));
    }

    /**
     * This is the base class for "easy" commands; that is, commands that have a fixed schema and are cheap to
     * initialize.
     */
    private static abstract class CommandHandlerFixedBase<T extends Message> implements CommandHandler {
        private final Class<T> clazz;

        public CommandHandlerFixedBase(Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        void check(T command) {

        }

        long totalRecords() {
            return -1;
        }

        abstract Ticket ticket(T command);

        abstract ByteString schemaBytes(T command);

        abstract Table table(T command);

        Timestamp expirationTime() {
            final Instant expire = Instant.now().plus(TICKET_DURATION);
            return Timestamp.newBuilder()
                    .setSeconds(expire.getEpochSecond())
                    .setNanos(expire.getNano())
                    .build();
        }

        @Override
        public final TicketHandler initialize(Any any) {
            final T command = unpackOrThrow(any, clazz);
            check(command);
            return new TicketHandlerFixed(command);
        }

        private class TicketHandlerFixed implements TicketHandler {
            private final T command;

            private TicketHandlerFixed(T command) {
                this.command = Objects.requireNonNull(command);
            }

            @Override
            public FlightInfo flightInfo(FlightDescriptor descriptor) {
                return FlightInfo.newBuilder()
                        .setFlightDescriptor(descriptor)
                        .setSchema(schemaBytes(command))
                        .addEndpoint(FlightEndpoint.newBuilder()
                                .setTicket(ticket(command))
                                .setExpirationTime(expirationTime())
                                .build())
                        .setTotalRecords(totalRecords())
                        .setTotalBytes(-1)
                        .build();
            }

            @Override
            public Table takeTable(SessionState session) {
                final Table table = CommandHandlerFixedBase.this.table(command);
                final long totalRecords = totalRecords();
                if (totalRecords != -1) {
                    if (table.isRefreshing()) {
                        throw new IllegalStateException(
                                "TicketHandler implementation error; should only override totalRecords for non-refreshing tables");
                    }
                    if (table.size() != totalRecords) {
                        throw new IllegalStateException(
                                "Ticket handler implementation error; totalRecords does not match the table size");
                    }
                }
                return table;
            }
        }
    }

    static final class UnsupportedCommand<T extends Message> extends CommandHandlerFixedBase<T> {
        UnsupportedCommand(Class<T> clazz) {
            super(clazz);
        }

        @Override
        void check(T command) {
            final Descriptor descriptor = command.getDescriptorForType();
            throw error(Code.UNIMPLEMENTED,
                    String.format("FlightSQL command '%s' is unimplemented", descriptor.getFullName()));
        }

        @Override
        Ticket ticket(T command) {
            throw new UnsupportedOperationException();
        }

        @Override
        ByteString schemaBytes(T command) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table table(T command) {
            throw new UnsupportedOperationException();
        }
    }

    public static long id(TicketStatementQuery query) {
        if (query.getStatementHandle().size() != 8) {
            throw error(Code.INVALID_ARGUMENT, "Invalid FlightSQL ticket handle");
        }
        return query.getStatementHandle()
                .asReadOnlyByteBuffer()
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    abstract class QueryBase implements CommandHandler, TicketHandler {
        private final long handleId;
        protected final SessionState session;

        private ScheduledFuture<?> watchdog;

        private boolean initialized;
        // protected ByteString schemaBytes;
        protected Table table;

        QueryBase(SessionState session) {
            this.handleId = handleIdGenerator.getAndIncrement();
            this.session = session;
            queries.put(handleId, this);
        }

        @Override
        public final TicketHandler initialize(Any any) {
            try {
                return initializeImpl(any);
            } catch (Throwable t) {
                close();
                throw t;
            }
        }

        private synchronized QueryBase initializeImpl(Any any) {
            if (initialized) {
                throw new IllegalStateException("initialize on Query should only be called once");
            }
            initialized = true;
            // TODO: nugget, scopes.
            // TODO: some attribute to set on table to force the schema / schemaBytes?
            // TODO: query scope, exex context
            execute(any);
            if (table == null) {
                throw new IllegalStateException(
                        "QueryBase implementation has a bug, should have set table");
            }
            watchdog = scheduler.schedule(this::onWatchdog, 5, TimeUnit.SECONDS);
            return this;
        }

        // responsible for setting table and schemaBytes
        protected abstract void execute(Any any);

        protected void executeImpl(String sql) {
            try {
                table = executeSqlQuery(session, sql);
            } catch (SqlParseException e) {
                throw error(Code.INVALID_ARGUMENT, "FlightSQL query can't be parsed", e);
            } catch (UnsupportedOperationException e) {
                if (e.getMessage().contains("org.apache.calcite.rex.RexDynamicParam")) {
                    throw queryParametersNotSupported(e);
                }
                throw e;
            } catch (CalciteContextException e) {
                // See org.apache.calcite.runtime.CalciteResource for the various messages we might encounter
                final Throwable cause = e.getCause();
                if (cause instanceof SqlValidatorException) {
                    if (cause.getMessage().contains("not found")) {
                        throw error(Code.NOT_FOUND, cause.getMessage(), cause);
                    }
                    throw error(Code.INVALID_ARGUMENT, cause.getMessage(), cause);
                }
                throw e;
            }
        }

        @Override
        public synchronized final FlightInfo flightInfo(FlightDescriptor descriptor) {
            return TicketRouter.getFlightInfo(table, descriptor, ticket());
        }

        @Override
        public synchronized final Table takeTable(SessionState session) {
            try {
                if (this.session != session) {
                    // TODO: what if original session is null? (should not be allowed?)
                    throw error(Code.UNAUTHENTICATED, "Must use same session for queries");
                }
                return table;
            } finally {
                close();
            }
        }

        public void close() {
            closeImpl(true);
        }

        private void onWatchdog() {
            log.debug().append("Watchdog cleaning up query ").append(handleId).endl();
            closeImpl(false);
        }

        private synchronized void closeImpl(boolean cancelWatchdog) {
            queries.remove(handleId, this);
            table = null;
            if (cancelWatchdog && watchdog != null) {
                watchdog.cancel(true);
            }
        }

        private ByteString handle() {
            final ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            bb.putLong(handleId);
            bb.flip();
            return ByteStringAccess.wrap(bb);
        }

        private Ticket ticket() {
            return FlightSqlTicketHelper.ticketFor(TicketStatementQuery.newBuilder()
                    .setStatementHandle(handle())
                    .build());
        }
    }


    final class CommandStatementQueryImpl extends QueryBase {

        CommandStatementQueryImpl(SessionState session) {
            super(session);
        }

        @Override
        public void execute(Any any) {
            final CommandStatementQuery command = unpackOrThrow(any, CommandStatementQuery.class);
            if (command.hasTransactionId()) {
                throw transactionIdsNotSupported();
            }
            executeImpl(command.getQuery());
        }
    }

    final class CommandPreparedStatementQueryImpl extends QueryBase {

        PreparedStatement prepared;

        CommandPreparedStatementQueryImpl(SessionState session) {
            super(Objects.requireNonNull(session));
        }

        @Override
        public void execute(Any any) {
            final CommandPreparedStatementQuery command = unpackOrThrow(any, CommandPreparedStatementQuery.class);
            prepared = getPreparedStatement(session, command.getPreparedStatementHandle());
            // Assumed this is not actually parameterized.
            final String sql = prepared.parameterizedQuery();
            executeImpl(sql);
            prepared.attach(this);
        }

        @Override
        public void close() {
            closeImpl(true);
        }

        private void closeImpl(boolean detach) {
            if (detach && prepared != null) {
                prepared.detach(this);
            }
            super.close();
        }

    }

    private static final class CommandStaticTable<T extends Message> extends CommandHandlerFixedBase<T> {
        private final Table table;
        private final Function<T, Ticket> f;
        private final ByteString schemaBytes;

        CommandStaticTable(Class<T> clazz, Table table, Function<T, Ticket> f) {
            super(clazz);
            if (table.isRefreshing()) {
                throw new IllegalArgumentException("Expected static table");
            }
            this.table = Objects.requireNonNull(table);
            this.f = Objects.requireNonNull(f);
            this.schemaBytes = BarrageUtil.schemaBytesFromTable(table);
        }

        @Override
        Ticket ticket(T command) {
            return f.apply(command);
        }

        @Override
        ByteString schemaBytes(T command) {
            return schemaBytes;
        }

        @Override
        Table table(T command) {
            return table;
        }

        @Override
        long totalRecords() {
            return table.size();
        }
    }

    @VisibleForTesting
    static final class CommandGetTableTypesImpl {
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(ColumnDefinition.ofString(TABLE_TYPE));
        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE =
                TableTools.newTable(DEFINITION, ATTRIBUTES, TableTools.stringCol(TABLE_TYPE, TABLE_TYPE_TABLE));

        public static final CommandHandler INSTANCE =
                new CommandStaticTable<>(CommandGetTableTypes.class, TABLE, FlightSqlTicketHelper::ticketFor);
    }

    @VisibleForTesting
    static final class CommandGetCatalogsImpl {
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(ColumnDefinition.ofString(CATALOG_NAME));
        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);

        public static final CommandHandler INSTANCE =
                new CommandStaticTable<>(CommandGetCatalogs.class, TABLE, FlightSqlTicketHelper::ticketFor);
    }

    @VisibleForTesting
    static final class CommandGetDbSchemasImpl extends CommandHandlerFixedBase<CommandGetDbSchemas> {

        public static final CommandGetDbSchemasImpl INSTANCE = new CommandGetDbSchemasImpl();

        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME));

        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);
        private static final ByteString SCHEMA_BYTES = BarrageUtil.schemaBytesFromTable(TABLE);

        private static final FieldDescriptor GET_DB_SCHEMAS_FILTER_PATTERN =
                CommandGetDbSchemas.getDescriptor().findFieldByNumber(2);

        private CommandGetDbSchemasImpl() {
            super(CommandGetDbSchemas.class);
        }

        @Override
        void check(CommandGetDbSchemas command) {
            // Note: even though we technically support this field right now since we _always_ return empty, this is a
            // defensive check in case there is a time in the future where we have catalogs and forget to update this
            // method.
            if (command.hasDbSchemaFilterPattern()) {
                throw error(Code.INVALID_ARGUMENT,
                        String.format("FlightSQL %s not supported at this time", GET_DB_SCHEMAS_FILTER_PATTERN));
            }
        }

        @Override
        long totalRecords() {
            return 0;
        }

        @Override
        Ticket ticket(CommandGetDbSchemas command) {
            return FlightSqlTicketHelper.ticketFor(command);
        }

        @Override
        ByteString schemaBytes(CommandGetDbSchemas command) {
            return SCHEMA_BYTES;
        }

        @Override
        public Table table(CommandGetDbSchemas command) {
            return TABLE;
        }
    }

    @VisibleForTesting
    static final class CommandGetTablesImpl extends CommandHandlerFixedBase<CommandGetTables> {

        public static final CommandGetTablesImpl INSTANCE = new CommandGetTablesImpl();

        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME),
                ColumnDefinition.ofString(TABLE_NAME),
                ColumnDefinition.ofString(TABLE_TYPE),
                ColumnDefinition.of(TABLE_SCHEMA, Type.byteType().arrayType()));

        @VisibleForTesting
        static final TableDefinition DEFINITION_NO_SCHEMA = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME),
                ColumnDefinition.ofString(TABLE_NAME),
                ColumnDefinition.ofString(TABLE_TYPE));

        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final ByteString SCHEMA_BYTES =
                BarrageUtil.schemaBytesFromTableDefinition(DEFINITION, ATTRIBUTES, true);
        private static final ByteString SCHEMA_BYTES_NO_SCHEMA =
                BarrageUtil.schemaBytesFromTableDefinition(DEFINITION_NO_SCHEMA, ATTRIBUTES, true);

        private static final FieldDescriptor GET_TABLES_DB_SCHEMA_FILTER_PATTERN =
                CommandGetTables.getDescriptor().findFieldByNumber(2);

        private static final FieldDescriptor GET_TABLES_TABLE_NAME_FILTER_PATTERN =
                CommandGetTables.getDescriptor().findFieldByNumber(3);

        CommandGetTablesImpl() {
            super(CommandGetTables.class);
        }

        @Override
        void check(CommandGetTables command) {
            if (command.hasDbSchemaFilterPattern()) {
                throw error(Code.INVALID_ARGUMENT,
                        String.format("FlightSQL %s not supported at this time", GET_TABLES_DB_SCHEMA_FILTER_PATTERN));
            }
            if (command.hasTableNameFilterPattern()) {
                throw error(Code.INVALID_ARGUMENT,
                        String.format("FlightSQL %s not supported at this time", GET_TABLES_TABLE_NAME_FILTER_PATTERN));
            }
        }

        @Override
        Ticket ticket(CommandGetTables command) {
            return FlightSqlTicketHelper.ticketFor(command);
        }

        @Override
        ByteString schemaBytes(CommandGetTables command) {
            return command.getIncludeSchema()
                    ? SCHEMA_BYTES
                    : SCHEMA_BYTES_NO_SCHEMA;
        }

        @Override
        public Table table(CommandGetTables request) {
            // already validated we don't have filter patterns
            final boolean hasNullCatalog = !request.hasCatalog() || request.getCatalog().isEmpty();
            final boolean hasTableTypeTable =
                    request.getTableTypesCount() == 0 || request.getTableTypesList().contains(TABLE_TYPE_TABLE);
            final boolean includeSchema = request.getIncludeSchema();
            return hasNullCatalog && hasTableTypeTable
                    ? getTables(includeSchema, ExecutionContext.getContext().getQueryScope(), ATTRIBUTES)
                    : getTablesEmpty(includeSchema, ATTRIBUTES);
        }

        private static Table getTablesEmpty(boolean includeSchema, @NotNull Map<String, Object> attributes) {
            Objects.requireNonNull(attributes);
            return includeSchema
                    ? TableTools.newTable(DEFINITION, attributes)
                    : TableTools.newTable(DEFINITION_NO_SCHEMA, attributes);
        }

        private static Table getTables(boolean includeSchema, @NotNull QueryScope queryScope,
                @NotNull Map<String, Object> attributes) {
            Objects.requireNonNull(attributes);
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
                    ? TableTools.newTable(DEFINITION, attributes, c1, c2, c3, c4, c5)
                    : TableTools.newTable(DEFINITION_NO_SCHEMA, attributes, c1, c2, c3, c4);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private <Request, Response extends Message> void executeAction(
            @Nullable SessionState session,
            ActionHandler<Request, Response> handler,
            org.apache.arrow.flight.Action request,
            Consumer<org.apache.arrow.flight.Result> visitor) {
        handler.execute(session, handler.parse(request), new ResultVisitorAdapter<>(visitor));
    }

    private ActionHandler<?, ? extends Message> action(org.apache.arrow.flight.Action action) {
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
        // Should not get here unless handlesActionType is implemented incorrectly.
        throw new IllegalStateException(String.format("Unexpected FlightSQL Action type '%s'", type));
    }

    private static <T extends com.google.protobuf.Message> T unpack(org.apache.arrow.flight.Action action,
            Class<T> clazz) {
        // A more efficient DH version of org.apache.arrow.flight.sql.FlightSqlUtils.unpackAndParseOrThrow
        final Any any = parseOrThrow(action.getBody());
        return unpackOrThrow(any, clazz);
    }

    private static org.apache.arrow.flight.Result pack(com.google.protobuf.Message message) {
        return new org.apache.arrow.flight.Result(Any.pack(message).toByteArray());
    }

    private PreparedStatement getPreparedStatement(SessionState session, ByteString handle) {
        final long id = preparedStatementHandleId(handle);
        final PreparedStatement preparedStatement = preparedStatements.get(id);
        if (preparedStatement == null) {
            throw error(Code.NOT_FOUND, "Unknown Prepared Statement");
        }
        preparedStatement.verifyOwner(session);
        return preparedStatement;
    }

    interface ActionHandler<Request, Response> {
        Request parse(org.apache.arrow.flight.Action action);

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
        public final Request parse(org.apache.arrow.flight.Action action) {
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
            if (session == null) {
                throw noAuthForPrepared();
            }
            if (request.hasTransactionId()) {
                throw transactionIdsNotSupported();
            }
            // It could be good to parse the query at this point in time to ensure it's valid and _not_ parameterized;
            // we will need to dig into Calcite further to explore this possibility. For now, we will error out either
            // when the client tries to do a DoPut for the parameter value, or during the Ticket execution, if the query
            // is invalid.
            final PreparedStatement prepared = new PreparedStatement(session, request.getQuery());

            // Note: we are providing a fake dataset schema here since the FlightSQL JDBC driver uses the results as an
            // indication of whether the query is a SELECT or UPDATE, see
            // org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler.PreparedStatement.getType. There should
            // likely be some better way the driver could be implemented...
            //
            // Regardless, the client is not allowed to assume correctness of the returned schema. For example, the
            // parameterized query `SELECT ?` is undefined at this point in time. We may need to re-examine this if we
            // eventually support non-trivial parameterized queries (it may be necessary to set setParameterSchema, even
            // if we don't know exactly what they will be).
            //
            // There does seem to be some conflicting guidance on whether dataset_schema is actually required or not.
            //
            // From the FlightSql.proto:
            //
            // > If a result set generating query was provided, dataset_schema contains the schema of the result set.
            // It should be an IPC-encapsulated Schema, as described in Schema.fbs. For some queries, the schema of the
            // results may depend on the schema of the parameters. The server should provide its best guess as to the
            // schema at this point. Clients must not assume that this schema, if provided, will be accurate.
            //
            // From https://arrow.apache.org/docs/format/FlightSql.html#query-execution
            //
            // > The response will contain an opaque handle used to identify the prepared statement. It may also contain
            // two optional schemas: the Arrow schema of the result set, and the Arrow schema of the bind parameters (if
            // any). Because the schema of the result set may depend on the bind parameters, the schemas may not
            // necessarily be provided here as a result, or if provided, they may not be accurate. Clients should not
            // assume the schema provided here will be the schema of any data actually returned by executing the
            // prepared statement.
            //
            // > Some statements may have bind parameters without any specific type. (As a trivial example for SQL,
            // consider SELECT ?.) It is not currently specified how this should be handled in the bind parameter schema
            // above. We suggest either using a union type to enumerate the possible types, or using the NA (null) type
            // as a wildcard/placeholder.
            final ActionCreatePreparedStatementResult response = ActionCreatePreparedStatementResult.newBuilder()
                    .setPreparedStatementHandle(prepared.handle())
                    .setDatasetSchema(DATASET_SCHEMA_SENTINEL_BYTES)
                    // .setParameterSchema(...)
                    .build();
            visitor.accept(response);
        }
    }

    private static ByteString serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteStringAccess.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
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
            if (session == null) {
                throw noAuthForPrepared();
            }
            final PreparedStatement prepared = getPreparedStatement(session, request.getPreparedStatementHandle());
            prepared.close();
            // no responses
        }
    }

    static final class UnsupportedAction<Request extends Message, Response> extends ActionBase<Request, Response> {
        public UnsupportedAction(ActionType type, Class<Request> clazz) {
            super(type, clazz);
        }

        @Override
        public void execute(SessionState session, Request request, Consumer<Response> visitor) {
            throw error(Code.UNIMPLEMENTED,
                    String.format("FlightSQL Action type '%s' is unimplemented", type.getType()));
        }
    }

    private static class ResultVisitorAdapter<Response extends Message> implements Consumer<Response> {
        private final Consumer<org.apache.arrow.flight.Result> delegate;

        public ResultVisitorAdapter(Consumer<org.apache.arrow.flight.Result> delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void accept(Response response) {
            delegate.accept(pack(response));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static StatusRuntimeException transactionIdsNotSupported() {
        return error(Code.INVALID_ARGUMENT, "FlightSQL transaction ids are not supported");
    }

    private static StatusRuntimeException queryParametersNotSupported(RuntimeException cause) {
        return error(Code.INVALID_ARGUMENT, "FlightSQL query parameters are not supported", cause);
    }

    private static StatusRuntimeException noAuthForPrepared() {
        return error(Code.UNAUTHENTICATED, "Must have an authenticated session to use prepared statements");
    }

    private static StatusRuntimeException error(Code code, String message) {
        // todo: io.deephaven.proto.util.Exceptions.statusRuntimeException sets trailers, this doesn't?
        return code
                .toStatus()
                .withDescription(message)
                .asRuntimeException();
    }

    private static StatusRuntimeException error(Code code, String message, Throwable cause) {
        // todo: io.deephaven.proto.util.Exceptions.statusRuntimeException sets trailers, this doesn't?
        return code
                .toStatus()
                .withDescription(message)
                .withCause(cause)
                .asRuntimeException();
    }

    private static Optional<Any> parse(ByteString data) {
        try {
            return Optional.of(Any.parseFrom(data));
        } catch (final InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    private static Optional<Any> parse(byte[] data) {
        try {
            return Optional.of(Any.parseFrom(data));
        } catch (final InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    private static Any parseOrThrow(ByteString data) {
        return parse(data).orElseThrow(() -> error(Code.INVALID_ARGUMENT, "Received invalid message from remote."));
    }

    private static Any parseOrThrow(byte[] data) {
        return parse(data).orElseThrow(() -> error(Code.INVALID_ARGUMENT, "Received invalid message from remote."));
    }

    private static <T extends Message> T unpackOrThrow(Any source, Class<T> as) {
        // DH version of org.apache.arrow.flight.sql.FlightSqlUtils.unpackOrThrow
        try {
            return source.unpack(as);
        } catch (final InvalidProtocolBufferException e) {
            // Same details as from org.apache.arrow.flight.sql.FlightSqlUtils.unpackOrThrow
            throw error(Code.INVALID_ARGUMENT,
                    "Provided message cannot be unpacked as " + as.getName() + ": " + e, e);
        }
    }

    private static ByteString preparedStatementHandle(long handleId) {
        final ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(handleId);
        bb.flip();
        return ByteStringAccess.wrap(bb);
    }

    private static long preparedStatementHandleId(ByteString handle) {
        if (handle.size() != 8) {
            throw error(Code.INVALID_ARGUMENT, "Invalid Prepared Statement handle");
        }
        return handle.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private class PreparedStatement {
        private final long handleId;
        private final SessionState session;
        private final String parameterizedQuery;
        private final Set<CommandPreparedStatementQueryImpl> queries;
        private final Closeable onSessionClosedCallback;

        PreparedStatement(SessionState session, String parameterizedQuery) {
            this.handleId = handleIdGenerator.getAndIncrement();
            this.session = Objects.requireNonNull(session);
            this.parameterizedQuery = Objects.requireNonNull(parameterizedQuery);
            this.queries = new HashSet<>();
            preparedStatements.put(handleId, this);
            this.session.addOnCloseCallback(onSessionClosedCallback = this::onSessionClosed);
        }

        public String parameterizedQuery() {
            return parameterizedQuery;
        }

        public ByteString handle() {
            return preparedStatementHandle(handleId);
        }

        public void verifyOwner(SessionState session) {
            // todo throw error if not same session
            if (!this.session.equals(session)) {
                // TODO: what if original session is null? (should not be allowed?)
                throw error(Code.UNAUTHENTICATED, "Must use same session for Prepared queries");
            }
        }

        public synchronized void attach(CommandPreparedStatementQueryImpl query) {
            queries.add(query);
        }

        public synchronized void detach(CommandPreparedStatementQueryImpl query) {
            queries.remove(query);
        }

        public void close() {
            closeImpl();
            session.removeOnCloseCallback(onSessionClosedCallback);
        }

        private void onSessionClosed() {
            log.debug().append("onSessionClosed: removing prepared statement ").append(handleId).endl();
            closeImpl();
        }

        private synchronized void closeImpl() {
            if (!preparedStatements.remove(handleId, this)) {
                return;
            }
            for (CommandPreparedStatementQueryImpl query : queries) {
                query.closeImpl(false);
            }
            queries.clear();
        }
    }
}
