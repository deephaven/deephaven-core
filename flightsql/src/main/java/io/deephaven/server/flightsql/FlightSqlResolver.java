//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
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
import io.deephaven.proto.backplane.grpc.ExportNotification;
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
import io.deephaven.sql.UnsupportedSqlOperation;
import io.deephaven.util.SafeCloseable;
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
import org.apache.arrow.flight.sql.FlightSqlProducer;
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
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.validate.SqlValidatorException;
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
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.server.flightsql.FlightSqlTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.server.flightsql.FlightSqlTicketHelper.TICKET_PREFIX;

/**
 * A <a href="https://arrow.apache.org/docs/format/FlightSql.html">FlightSQL</a> resolver. This supports the read-only
 * querying of the global query scope, which is presented simply with the query scope variables names as the table names
 * without a catalog and schema name.
 *
 * <p>
 * This implementation does not currently follow the FlightSQL protocol to exact specification. Namely, all the returned
 * {@link Schema Flight schemas} have nullable {@link Field fields}, and some of the fields on specific commands have
 * different types (see {@link #flightInfoFor(SessionState, FlightDescriptor, String)} for specifics).
 *
 * <p>
 * All commands, actions, and resolution must be called by authenticated users.
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
    private static final String FLIGHT_SQL_COMMAND_TYPE_PREFIX = FLIGHT_SQL_TYPE_PREFIX + "Command";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementQuery";

    // This is a server-implementation detail, but happens to be the same scheme that FlightSQL
    // org.apache.arrow.flight.sql.FlightSqlProducer uses
    static final String TICKET_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_TYPE_PREFIX + "TicketStatementQuery";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_UPDATE_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementUpdate";

    // Need to update to newer FlightSql version for this
    // @VisibleForTesting
    // static final String COMMAND_STATEMENT_INGEST_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementIngest";

    @VisibleForTesting
    static final String COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementSubstraitPlan";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "PreparedStatementQuery";

    @VisibleForTesting
    static final String COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "PreparedStatementUpdate";

    @VisibleForTesting
    static final String COMMAND_GET_TABLE_TYPES_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetTableTypes";

    @VisibleForTesting
    static final String COMMAND_GET_CATALOGS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetCatalogs";

    @VisibleForTesting
    static final String COMMAND_GET_DB_SCHEMAS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetDbSchemas";

    @VisibleForTesting
    static final String COMMAND_GET_TABLES_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetTables";

    @VisibleForTesting
    static final String COMMAND_GET_SQL_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetSqlInfo";

    @VisibleForTesting
    static final String COMMAND_GET_CROSS_REFERENCE_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetCrossReference";

    @VisibleForTesting
    static final String COMMAND_GET_EXPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetExportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_IMPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetImportedKeys";

    @VisibleForTesting
    static final String COMMAND_GET_PRIMARY_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetPrimaryKeys";

    @VisibleForTesting
    static final String COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetXdbcTypeInfo";

    private static final String CATALOG_NAME = "catalog_name";
    private static final String PK_CATALOG_NAME = "pk_catalog_name";
    private static final String FK_CATALOG_NAME = "fk_catalog_name";

    private static final String DB_SCHEMA_NAME = "db_schema_name";
    private static final String PK_DB_SCHEMA_NAME = "pk_db_schema_name";
    private static final String FK_DB_SCHEMA_NAME = "fk_db_schema_name";

    private static final String TABLE_NAME = "table_name";
    private static final String PK_TABLE_NAME = "pk_table_name";
    private static final String FK_TABLE_NAME = "fk_table_name";

    private static final String COLUMN_NAME = "column_name";
    private static final String PK_COLUMN_NAME = "pk_column_name";
    private static final String FK_COLUMN_NAME = "fk_column_name";

    private static final String KEY_NAME = "key_name";
    private static final String PK_KEY_NAME = "pk_key_name";
    private static final String FK_KEY_NAME = "fk_key_name";

    private static final String TABLE_TYPE = "table_type";
    private static final String KEY_SEQUENCE = "key_sequence";
    private static final String TABLE_SCHEMA = "table_schema";
    private static final String UPDATE_RULE = "update_rule";
    private static final String DELETE_RULE = "delete_rule";

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

    // Need dense_union support to implement this.
    private static final UnsupportedCommand GET_SQL_INFO_HANDLER =
            new UnsupportedCommand(CommandGetSqlInfo.getDescriptor(), CommandGetSqlInfo.class);
    private static final UnsupportedCommand STATEMENT_UPDATE_HANDLER =
            new UnsupportedCommand(CommandStatementUpdate.getDescriptor(), CommandStatementUpdate.class);
    private static final UnsupportedCommand GET_CROSS_REFERENCE_HANDLER =
            new UnsupportedCommand(CommandGetCrossReference.getDescriptor(), CommandGetCrossReference.class);
    private static final UnsupportedCommand STATEMENT_SUBSTRAIT_PLAN_HANDLER =
            new UnsupportedCommand(CommandStatementSubstraitPlan.getDescriptor(), CommandStatementSubstraitPlan.class);
    private static final UnsupportedCommand PREPARED_STATEMENT_UPDATE_HANDLER = new UnsupportedCommand(
            CommandPreparedStatementUpdate.getDescriptor(), CommandPreparedStatementUpdate.class);
    private static final UnsupportedCommand GET_XDBC_TYPE_INFO_HANDLER =
            new UnsupportedCommand(CommandGetXdbcTypeInfo.getDescriptor(), CommandGetXdbcTypeInfo.class);

    // Unable to depends on TicketRouter, would be a circular dependency atm (since TicketRouter depends on all the
    // TicketResolvers).
    // private final TicketRouter router;
    private final ScopeTicketResolver scopeTicketResolver;

    private final AtomicLong handleIdGenerator;

    private final KeyedLongObjectHashMap<QueryBase> queries;
    private final KeyedLongObjectHashMap<PreparedStatement> preparedStatements;
    private final ScheduledExecutorService scheduler;
    private final LivenessScope scope;

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
        this.scope = new LivenessScope(false);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Returns {@code true} if the given command {@code descriptor} appears to be a valid FlightSQL command; that is, it
     * is parsable as an {@code Any} protobuf message with the type URL prefixed with
     * {@value FLIGHT_SQL_COMMAND_TYPE_PREFIX}.
     *
     * @param descriptor the descriptor
     * @return {@code true} if the given command appears to be a valid FlightSQL command
     */
    @Override
    public boolean handlesCommand(Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != DescriptorType.CMD) {
            throw new IllegalStateException("descriptor is not a command");
        }
        // No good way to check if this is a valid command without parsing to Any first.
        final Any command = parse(descriptor.getCmd()).orElse(null);
        return command != null && command.getTypeUrl().startsWith(FLIGHT_SQL_COMMAND_TYPE_PREFIX);
    }

    // We should probably plumb optional TicketResolver support that allows efficient
    // io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema without needing to go through flightInfoFor

    /**
     * Executes the given {@code descriptor} command. Only supports authenticated access.
     *
     * <p>
     * {@link CommandStatementQuery}: Executes the given SQL query. The returned Flight info should be promptly
     * resolved, and resolved at most once. Transactions are not currently supported.
     *
     * <p>
     * {@link CommandPreparedStatementQuery}: Executes the prepared SQL query (must be executed within the scope of a
     * {@link FlightSqlUtils#FLIGHT_SQL_CREATE_PREPARED_STATEMENT} /
     * {@link FlightSqlUtils#FLIGHT_SQL_CLOSE_PREPARED_STATEMENT}). The returned Flight info should be promptly
     * resolved, and resolved at most once.
     *
     * <p>
     * {@link CommandGetTables}: Retrieve the tables authorized for the user. The {@value TABLE_NAME},
     * {@value TABLE_TYPE}, and (optional) {@value TABLE_SCHEMA} fields will be out-of-spec as nullable columns (the
     * returned data for these columns will never be {@code null}).
     *
     * <p>
     * {@link CommandGetCatalogs}: Retrieves the catalogs authorized for the user. The {@value CATALOG_NAME} field will
     * be out-of-spec as a nullable column (the returned data for this column will never be {@code null}). Currently,
     * always an empty table.
     *
     * <p>
     * {@link CommandGetDbSchemas}: Retrieves the catalogs and schemas authorized for the user. The
     * {@value DB_SCHEMA_NAME} field will be out-of-spec as a nullable (the returned data for this column will never be
     * {@code null}). Currently, always an empty table.
     *
     * <p>
     * {@link CommandGetTableTypes}: Retrieves the table types authorized for the user. The {@value TABLE_TYPE} field
     * will be out-of-spec as a nullable (the returned data for this column will never be {@code null}). Currently,
     * always a table with a single row with value {@value TABLE_TYPE_TABLE}.
     *
     * <p>
     * {@link CommandGetPrimaryKeys}: Retrieves the primary keys for a table if the user is authorized. If the table
     * does not exist (or the user is not authorized), a {@link Code#NOT_FOUND} exception will be thrown. The
     * {@value TABLE_NAME}, {@value COLUMN_NAME}, and {@value KEY_SEQUENCE} will be out-of-spec as nullable columns (the
     * returned data for these columns will never be {@code null}). Currently, always an empty table.
     *
     * <p>
     * {@link CommandGetImportedKeys}: Retrieves the imported keys for a table if the user is authorized. If the table
     * does not exist (or the user is not authorized), a {@link Code#NOT_FOUND} exception will be thrown. The
     * {@value PK_TABLE_NAME}, {@value PK_COLUMN_NAME}, {@value FK_TABLE_NAME}, {@value FK_COLUMN_NAME}, and
     * {@value KEY_SEQUENCE} will be out-of-spec as nullable columns (the returned data for these columns will never be
     * {@code null}). The {@value UPDATE_RULE} and {@value DELETE_RULE} will be out-of-spec as nullable {@code int8}
     * types instead of {@code uint8} (the returned data for these columns will never be {@code null}). Currently,
     * always an empty table.
     *
     * <p>
     * {@link CommandGetExportedKeys}: Retrieves the exported keys for a table if the user is authorized. If the table
     * does not exist (or the user is not authorized), a {@link Code#NOT_FOUND} exception will be thrown. The
     * {@value PK_TABLE_NAME}, {@value PK_COLUMN_NAME}, {@value FK_TABLE_NAME}, {@value FK_COLUMN_NAME}, and
     * {@value KEY_SEQUENCE} will be out-of-spec as nullable columns (the returned data for these columns will never be
     * {@code null}). The {@value UPDATE_RULE} and {@value DELETE_RULE} will be out-of-spec as nullable {@code int8}
     * types instead of {@code uint8} (the returned data for these columns will never be {@code null}). Currently,
     * always an empty table.
     *
     * <p>
     * All other commands will throw an {@link Code#UNIMPLEMENTED} exception.
     *
     * @param session the session
     * @param descriptor the flight descriptor to retrieve a ticket for
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the flight info for the given {@code descriptor} command
     */
    @Override
    public ExportObject<FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw unauthenticatedError();
        }
        if (descriptor.getType() != DescriptorType.CMD) {
            // We _should_ be able to eventually elevate this to an IllegalStateException since we should be able to
            // pass along context that FlightSQL does not support any PATH-based Descriptors. This may involve
            // extracting a PathResolver interface (like CommandResolver) and potentially breaking
            // io.deephaven.server.session.TicketResolverBase.flightDescriptorRoute
            throw error(Code.FAILED_PRECONDITION, "FlightSQL only supports Command-based descriptors");
        }
        final Any command = parseOrThrow(descriptor.getCmd());
        if (!command.getTypeUrl().startsWith(FLIGHT_SQL_COMMAND_TYPE_PREFIX)) {
            // If we get here, there is an error with io.deephaven.server.session.TicketRouter.getCommandResolver /
            // handlesCommand
            throw new IllegalStateException(String.format("Unexpected command typeUrl '%s'", command.getTypeUrl()));
        }
        return session.<FlightInfo>nonExport().submit(() -> getInfo(session, descriptor, command));
    }

    private FlightInfo getInfo(final SessionState session, final FlightDescriptor descriptor, final Any command) {
        final CommandHandler commandHandler = commandHandler(session, command.getTypeUrl(), true);
        final TicketHandler ticketHandler = commandHandler.initialize(command);
        try {
            return ticketHandler.getInfo(descriptor);
        } catch (Throwable t) {
            if (ticketHandler instanceof TicketHandlerReleasable) {
                ((TicketHandlerReleasable) ticketHandler).release();
            }
            throw t;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Only supports authenticated access.
     *
     * @param session the user session context
     * @param ticket (as ByteByffer) the ticket to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the exported table
     * @param <T> the type, must be Table
     */
    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        if (session == null) {
            throw unauthenticatedError();
        }
        final Any message = FlightSqlTicketHelper.unpackTicket(ticket, logId);

        final ExportObject<TicketHandler> ticketHandler = session.<TicketHandler>nonExport()
                .submit(() -> ticketHandlerForResolve(session, message));

        // noinspection unchecked
        return (ExportObject<T>) new TableResolver(ticketHandler, session).submit();
    }

    private static class TableResolver implements Callable<Table>, Runnable, SessionState.ExportErrorHandler {
        private final ExportObject<TicketHandler> export;
        private final SessionState session;
        private TicketHandler handler;

        public TableResolver(ExportObject<TicketHandler> export, SessionState session) {
            this.export = Objects.requireNonNull(export);
            this.session = Objects.requireNonNull(session);
        }

        public ExportObject<Table> submit() {
            // We need to provide clean handoff of the Table for Liveness management between the resolver and the
            // export; as such, we _can't_ unmanage the Table during a call to TicketHandler.resolve, so we must rely
            // on onSuccess / onError callbacks (after export has started managing the Table).
            return session.<Table>nonExport()
                    .require(export)
                    .onSuccess(this)
                    .onError(this)
                    .submit((Callable<Table>) this);
        }

        // submit
        @Override
        public Table call() {
            handler = export.get();
            if (!handler.isAuthorized(session)) {
                throw new IllegalStateException("Expected TicketHandler to already be authorized for session");
            }
            return handler.resolve();
        }

        // onSuccess
        @Override
        public void run() {
            if (handler == null) {
                // Should only be run onSuccess, so export.get() must have succeeded
                throw new IllegalStateException();
            }
            release();
        }

        @Override
        public void onError(ExportNotification.State resultState, String errorContext, @Nullable Exception cause,
                @Nullable String dependentExportId) {
            if (handler == null) {
                return;
            }
            release();
        }

        private void release() {
            if (!(handler instanceof TicketHandlerReleasable)) {
                return;
            }
            ((TicketHandlerReleasable) handler).release();
        }
    }


    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // This general interface does not make sense; resolution should always be done against a _ticket_. Nothing
        // calls io.deephaven.server.session.TicketRouter.resolve(SessionState, FlightDescriptor, String)
        throw new IllegalStateException();
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Supports unauthenticated access. When unauthenticated, will not return any actions types. When authenticated,
     * will return the action types the user is authorized to access. Currently, supports
     * {@link FlightSqlUtils#FLIGHT_SQL_CREATE_PREPARED_STATEMENT} and
     * {@link FlightSqlUtils#FLIGHT_SQL_CLOSE_PREPARED_STATEMENT}.
     *
     * @param session the session
     * @param visitor the visitor
     */
    @Override
    public void listActions(@Nullable SessionState session, Consumer<ActionType> visitor) {
        if (session == null) {
            return;
        }
        visitor.accept(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        visitor.accept(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
    }

    /**
     * Returns {@code true} if {@code type} is a known FlightSQL action type (even if this implementation does not
     * implement it).
     *
     * @param type the action type
     * @return if {@code type} is a known FlightSQL action type
     */
    @Override
    public boolean handlesActionType(String type) {
        // There is no prefix for FlightSQL action types, so the best we can do is a set-based lookup. This also means
        // that this resolver will not be able to respond with an appropriately scoped error message for new FlightSQL
        // action types (io.deephaven.server.flightsql.FlightSqlResolver.UnsupportedAction).
        return FLIGHT_SQL_ACTION_TYPES.contains(type);
    }

    /**
     * Executes the given {@code action}. Only supports authenticated access. Currently, supports
     * {@link FlightSqlUtils#FLIGHT_SQL_CREATE_PREPARED_STATEMENT} and
     * {@link FlightSqlUtils#FLIGHT_SQL_CLOSE_PREPARED_STATEMENT}; all other action types will throw an
     * {@link Code#UNIMPLEMENTED} exception. Transactions are not currently supported.
     *
     * @param session the session
     * @param action the action
     * @param visitor the visitor
     */
    @Override
    public void doAction(@Nullable SessionState session, org.apache.arrow.flight.Action action,
            Consumer<org.apache.arrow.flight.Result> visitor) {
        if (session == null) {
            throw unauthenticatedError();
        }
        if (!handlesActionType(action.getType())) {
            // If we get here, there is an error with io.deephaven.server.session.ActionRouter.doAction /
            // handlesActionType
            throw new IllegalStateException(String.format("Unexpected action type '%s'", action.getType()));
        }
        executeAction(session, action(action), action, visitor);
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Supports unauthenticated access. When unauthenticated, will not return any Flight info. When authenticated, this
     * may return Flight info the user is authorized to access. Currently, no Flight info is returned.
     *
     * @param session optional session that the resolver can use to filter which flights a visitor sees
     * @param visitor the callback to invoke per descriptor path
     */
    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        if (session == null) {
            return;
        }
        // Potential support for listing here in the future
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Publishing to FlightSQL descriptors is not currently supported. Throws a {@link Code#FAILED_PRECONDITION} error.
     */
    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        if (session == null) {
            throw unauthenticatedError();
        }
        throw error(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL descriptors cannot be published to");
    }

    /**
     * Publishing to FlightSQL tickets is not currently supported. Throws a {@link Code#FAILED_PRECONDITION} error.
     */
    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        if (session == null) {
            throw unauthenticatedError();
        }
        throw error(Code.FAILED_PRECONDITION,
                "Could not publish '" + logId + "': FlightSQL tickets cannot be published to");
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // This is a bit different from the other resolvers; a ticket may be a very long byte string here since it
        // may represent a command.
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    // ---------------------------------------------------------------------------------------------------------------

    interface CommandHandler {

        TicketHandler initialize(Any any);
    }

    interface TicketHandler {

        boolean isAuthorized(SessionState session);

        FlightInfo getInfo(FlightDescriptor descriptor);

        Table resolve();
    }

    interface TicketHandlerReleasable extends TicketHandler {

        void release();
    }

    private CommandHandler commandHandler(SessionState session, String typeUrl, boolean forFlightInfo) {
        switch (typeUrl) {
            case COMMAND_STATEMENT_QUERY_TYPE_URL:
                if (!forFlightInfo) {
                    // This should not happen with well-behaved clients; or it means there is a bug in our
                    // command/ticket logic
                    throw error(Code.INVALID_ARGUMENT, "Invalid ticket; please ensure client is using opaque ticket");
                }
                return new CommandStatementQueryImpl(session);
            case COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                return new CommandPreparedStatementQueryImpl(session);
            case COMMAND_GET_TABLES_TYPE_URL:
                return new CommandGetTablesImpl();
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return CommandGetTableTypesConstants.HANDLER;
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return CommandGetCatalogsConstants.HANDLER;
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return CommandGetDbSchemasConstants.HANDLER;
            case COMMAND_GET_PRIMARY_KEYS_TYPE_URL:
                return commandGetPrimaryKeysHandler;
            case COMMAND_GET_IMPORTED_KEYS_TYPE_URL:
                return commandGetImportedKeysHandler;
            case COMMAND_GET_EXPORTED_KEYS_TYPE_URL:
                return commandGetExportedKeysHandler;
            case COMMAND_GET_SQL_INFO_TYPE_URL:
                return GET_SQL_INFO_HANDLER;
            case COMMAND_STATEMENT_UPDATE_TYPE_URL:
                return STATEMENT_UPDATE_HANDLER;
            case COMMAND_GET_CROSS_REFERENCE_TYPE_URL:
                return GET_CROSS_REFERENCE_HANDLER;
            case COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL:
                return STATEMENT_SUBSTRAIT_PLAN_HANDLER;
            case COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL:
                return PREPARED_STATEMENT_UPDATE_HANDLER;
            case COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL:
                return GET_XDBC_TYPE_INFO_HANDLER;
        }
        throw error(Code.UNIMPLEMENTED, String.format("command '%s' is unknown", typeUrl));
    }

    private TicketHandler ticketHandlerForResolve(SessionState session, Any message) {
        final String typeUrl = message.getTypeUrl();
        if (TICKET_STATEMENT_QUERY_TYPE_URL.equals(typeUrl)) {
            final TicketStatementQuery ticketStatementQuery = unpackOrThrow(message, TicketStatementQuery.class);
            final TicketHandler ticketHandler = queries.get(id(ticketStatementQuery));
            if (ticketHandler == null) {
                throw error(Code.NOT_FOUND,
                        "Unable to find FlightSQL query. FlightSQL tickets should be resolved promptly and resolved at most once.");
            }
            if (!ticketHandler.isAuthorized(session)) {
                throw error(Code.PERMISSION_DENIED, "Must be the owner to resolve");
            }
            return ticketHandler;
        }
        final CommandHandler commandHandler = commandHandler(session, typeUrl, false);
        try {
            return commandHandler.initialize(message);
        } catch (StatusRuntimeException e) {
            // This should not happen with well-behaved clients; or it means there is a bug in our command/ticket logic
            throw error(Code.INVALID_ARGUMENT,
                    "Invalid ticket; please ensure client is using an opaque ticket", e);
        }
    }

    private Table executeSqlQuery(SessionState session, String sql) {
        // See SQLTODO(catalog-reader-implementation)
        // final QueryScope queryScope = sessionState.getExecutionContext().getQueryScope();
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        // noinspection unchecked,rawtypes
        final Map<String, Table> queryScopeTables =
                (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
        final TableSpec tableSpec = Sql.parseSql(sql, queryScopeTables, TicketTable::fromQueryScopeField, null);
        // Note: this is doing io.deephaven.server.session.TicketResolver.Authorization.transform, but not
        // io.deephaven.auth.ServiceAuthWiring
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            // TODO: computeEnclosed
            return tableSpec.logic()
                    .create(new TableCreatorScopeTickets(TableCreatorImpl.INSTANCE, scopeTicketResolver, session));
        }
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

        /**
         * This is called as the first part of {@link TicketHandler#getInfo(FlightDescriptor)} for the handler returned
         * from {@link #initialize(Any)}. It can be used as an early signal to let clients know that the command is not
         * supported, or one of the arguments is not valid.
         */
        void checkForGetInfo(T command) {

        }

        /**
         * This is called as the first part of {@link TicketHandler#resolve()} for the handler returned from
         * {@link #initialize(Any)}.
         */
        void checkForResolve(T command) {

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

        /**
         * The handler. Will invoke {@link #checkForGetInfo(Message)} as the first part of
         * {@link TicketHandler#getInfo(FlightDescriptor)}. Will invoke {@link #checkForResolve(Message)} as the first
         * part of {@link TicketHandler#resolve()}.
         */
        @Override
        public final TicketHandler initialize(Any any) {
            final T command = unpackOrThrow(any, clazz);
            return new TicketHandlerFixed(command);
        }

        private class TicketHandlerFixed implements TicketHandler {
            private final T command;

            private TicketHandlerFixed(T command) {
                this.command = Objects.requireNonNull(command);
            }

            @Override
            public boolean isAuthorized(SessionState session) {
                return true;
            }

            @Override
            public FlightInfo getInfo(FlightDescriptor descriptor) {
                checkForGetInfo(command);
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
            public Table resolve() {
                checkForResolve(command);
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

    private static final class UnsupportedCommand implements CommandHandler, TicketHandler {
        private final Descriptor descriptor;
        private final Class<? extends Message> clazz;

        UnsupportedCommand(Descriptor descriptor, Class<? extends Message> clazz) {
            this.descriptor = Objects.requireNonNull(descriptor);
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public TicketHandler initialize(Any any) {
            unpackOrThrow(any, clazz);
            return this;
        }

        @Override
        public boolean isAuthorized(SessionState session) {
            return true;
        }

        @Override
        public FlightInfo getInfo(FlightDescriptor descriptor) {
            throw error(Code.UNIMPLEMENTED,
                    String.format("command '%s' is unimplemented", this.descriptor.getFullName()));
        }

        @Override
        public Table resolve() {
            throw error(Code.INVALID_ARGUMENT, String.format(
                    "client is misbehaving, should use getInfo for command '%s'", this.descriptor.getFullName()));
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

    // TODO: consider this owning Table instead (SingletonLivenessManager or has + patch from Ryan)
    abstract class QueryBase implements CommandHandler, TicketHandlerReleasable {
        private final long handleId;
        protected final SessionState session;

        private ScheduledFuture<?> watchdog;

        private boolean initialized;
        private boolean resolved;
        protected Table table;

        QueryBase(SessionState session) {
            this.handleId = handleIdGenerator.getAndIncrement();
            this.session = Objects.requireNonNull(session);
            queries.put(handleId, this);
        }

        @Override
        public final TicketHandlerReleasable initialize(Any any) {
            try {
                return initializeImpl(any);
            } catch (Throwable t) {
                release();
                throw t;
            }
        }

        private synchronized QueryBase initializeImpl(Any any) {
            if (initialized) {
                throw new IllegalStateException("initialize on Query should only be called once");
            }
            initialized = true;
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
            } catch (UnsupportedSqlOperation e) {
                if (e.clazz() == RexDynamicParam.class) {
                    throw queryParametersNotSupported(e);
                }
                throw error(Code.INVALID_ARGUMENT, String.format("Unsupported calcite type '%s'", e.clazz().getName()),
                        e);
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
        public final boolean isAuthorized(SessionState session) {
            return this.session.equals(session);
        }

        @Override
        public synchronized final FlightInfo getInfo(FlightDescriptor descriptor) {
            return TicketRouter.getFlightInfo(table, descriptor, ticket());
        }

        @Override
        public synchronized final Table resolve() {
            if (resolved) {
                throw error(Code.FAILED_PRECONDITION, "Should only resolve once");
            }
            resolved = true;
            if (table == null) {
                throw error(Code.FAILED_PRECONDITION, "Should resolve table quicker");
            }
            return table;
        }

        @Override
        public void release() {
            cleanup(true);
        }

        private void onWatchdog() {
            log.debug().append("Watchdog cleaning up query ").append(handleId).endl();
            cleanup(false);
        }

        private synchronized void cleanup(boolean cancelWatchdog) {
            if (cancelWatchdog && watchdog != null) {
                watchdog.cancel(true);
                watchdog = null;
            }
            if (table != null) {
                scope.unmanage(table);
                table = null;
            }
            queries.remove(handleId, this);
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
            super(session);
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
        public void release() {
            releaseImpl(true);
        }

        private void releaseImpl(boolean detach) {
            if (detach && prepared != null) {
                prepared.detach(this);
            }
            super.release();
        }
    }

    private static class CommandStaticTable<T extends Message> extends CommandHandlerFixedBase<T> {
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
    static final class CommandGetTableTypesConstants {

        /**
         * Models return type for {@link CommandGetTableTypes},
         * {@link FlightSqlProducer.Schemas#GET_TABLE_TYPES_SCHEMA}.
         *
         * <pre>
         * table_type: utf8 not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(TABLE_TYPE) // out-of-spec
        );
        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE =
                TableTools.newTable(DEFINITION, ATTRIBUTES, TableTools.stringCol(TABLE_TYPE, TABLE_TYPE_TABLE));

        public static final CommandHandler HANDLER =
                new CommandStaticTable<>(CommandGetTableTypes.class, TABLE, FlightSqlTicketHelper::ticketFor);
    }

    @VisibleForTesting
    static final class CommandGetCatalogsConstants {

        /**
         * Models return type for {@link CommandGetCatalogs}, {@link FlightSqlProducer.Schemas#GET_CATALOGS_SCHEMA}.
         * 
         * <pre>
         * catalog_name: utf8 not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME) // out-of-spec
        );
        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);

        public static final CommandHandler HANDLER =
                new CommandStaticTable<>(CommandGetCatalogs.class, TABLE, FlightSqlTicketHelper::ticketFor);
    }

    @VisibleForTesting
    static final class CommandGetDbSchemasConstants {

        /**
         * Models return type for {@link CommandGetDbSchemas}, {@link FlightSqlProducer.Schemas#GET_SCHEMAS_SCHEMA}.
         * 
         * <pre>
         * catalog_name: utf8,
         * db_schema_name: utf8 not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME) // out-of-spec
        );
        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);
        public static final CommandHandler HANDLER =
                new CommandStaticTable<>(CommandGetDbSchemas.class, TABLE, FlightSqlTicketHelper::ticketFor);
    }

    @VisibleForTesting
    static final class CommandGetKeysConstants {

        /**
         * Models return type for {@link CommandGetImportedKeys} / {@link CommandGetExportedKeys},
         * {@link FlightSqlProducer.Schemas#GET_IMPORTED_KEYS_SCHEMA},
         * {@link FlightSqlProducer.Schemas#GET_EXPORTED_KEYS_SCHEMA}.
         * 
         * <pre>
         * pk_catalog_name: utf8,
         * pk_db_schema_name: utf8,
         * pk_table_name: utf8 not null,
         * pk_column_name: utf8 not null,
         * fk_catalog_name: utf8,
         * fk_db_schema_name: utf8,
         * fk_table_name: utf8 not null,
         * fk_column_name: utf8 not null,
         * key_sequence: int32 not null,
         * fk_key_name: utf8,
         * pk_key_name: utf8,
         * update_rule: uint8 not null,
         * delete_rule: uint8 not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(PK_CATALOG_NAME),
                ColumnDefinition.ofString(PK_DB_SCHEMA_NAME),
                ColumnDefinition.ofString(PK_TABLE_NAME), // out-of-spec
                ColumnDefinition.ofString(PK_COLUMN_NAME), // out-of-spec
                ColumnDefinition.ofString(FK_CATALOG_NAME),
                ColumnDefinition.ofString(FK_DB_SCHEMA_NAME),
                ColumnDefinition.ofString(FK_TABLE_NAME), // out-of-spec
                ColumnDefinition.ofString(FK_COLUMN_NAME), // out-of-spec
                ColumnDefinition.ofInt(KEY_SEQUENCE), // out-of-spec
                ColumnDefinition.ofString(FK_KEY_NAME), // yes, this does come _before_ the PK version
                ColumnDefinition.ofString(PK_KEY_NAME),
                ColumnDefinition.ofByte(UPDATE_RULE), // out-of-spec
                ColumnDefinition.ofByte(DELETE_RULE) // out-of-spec
        );

        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);
    }

    @VisibleForTesting
    static final class CommandGetPrimaryKeysConstants {

        /**
         * Models return type for {@link CommandGetPrimaryKeys} /
         * {@link FlightSqlProducer.Schemas#GET_PRIMARY_KEYS_SCHEMA}.
         * 
         * <pre>
         * catalog_name: utf8,
         * db_schema_name: utf8,
         * table_name: utf8 not null,
         * column_name: utf8 not null,
         * key_name: utf8,
         * key_sequence: int32 not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME),
                ColumnDefinition.ofString(TABLE_NAME), // out-of-spec
                ColumnDefinition.ofString(COLUMN_NAME), // out-of-spec
                ColumnDefinition.ofString(KEY_NAME),
                ColumnDefinition.ofInt(KEY_SEQUENCE) // out-of-spec
        );

        private static final Map<String, Object> ATTRIBUTES = Map.of();
        private static final Table TABLE = TableTools.newTable(DEFINITION, ATTRIBUTES);
    }

    private boolean hasTable(String catalog, String dbSchema, String table) {
        if (catalog != null && !catalog.isEmpty()) {
            return false;
        }
        if (dbSchema != null && !dbSchema.isEmpty()) {
            return false;
        }
        final Object obj;
        {
            final QueryScope scope = ExecutionContext.getContext().getQueryScope();
            try {
                obj = scope.readParamValue(table);
            } catch (QueryScope.MissingVariableException e) {
                return false;
            }
        }
        if (!(obj instanceof Table)) {
            return false;
        }
        return authorization.transform((Table) obj) != null;
    }

    private final CommandHandler commandGetPrimaryKeysHandler = new CommandStaticTable<>(CommandGetPrimaryKeys.class,
            CommandGetPrimaryKeysConstants.TABLE, FlightSqlTicketHelper::ticketFor) {
        @Override
        void checkForGetInfo(CommandGetPrimaryKeys command) {
            if (CommandGetPrimaryKeys.getDefaultInstance().equals(command)) {
                // TODO: Plumb through io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema
                // We need to pretend that CommandGetPrimaryKeys.getDefaultInstance() is a valid command until we can
                // plumb getSchema through to the resolvers.
                return;
            }
            if (!hasTable(
                    command.hasCatalog() ? command.getCatalog() : null,
                    command.hasDbSchema() ? command.getDbSchema() : null,
                    command.getTable())) {
                throw tableNotFound();
            }
        }

        // No need to check at resolve time since there is no actual state involved. If Deephaven exposes the notion
        // of keys, this will need to behave more like QueryBase where there is a handle-based ticket and some sort
        // state maintained. It is also incorrect to perform the same checkForFlightInfo at resolve time because the
        // state of the server may have changed between getInfo and doGet/doExchange, and getInfo should still be valid
        // for client.
        // @Override
        // void checkForResolve(CommandGetPrimaryKeys command) {
        // }
    };

    private final CommandHandler commandGetImportedKeysHandler = new CommandStaticTable<>(CommandGetImportedKeys.class,
            CommandGetKeysConstants.TABLE, FlightSqlTicketHelper::ticketFor) {
        @Override
        void checkForGetInfo(CommandGetImportedKeys command) {
            if (CommandGetImportedKeys.getDefaultInstance().equals(command)) {
                // TODO: Plumb through io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema
                // We need to pretend that CommandGetImportedKeys.getDefaultInstance() is a valid command until we can
                // plumb getSchema through to the resolvers.
                return;
            }
            if (!hasTable(
                    command.hasCatalog() ? command.getCatalog() : null,
                    command.hasDbSchema() ? command.getDbSchema() : null,
                    command.getTable())) {
                throw tableNotFound();
            }
        }

        // No need to check at resolve time since there is no actual state involved. If Deephaven exposes the notion
        // of keys, this will need to behave more like QueryBase where there is a handle-based ticket and some sort
        // state maintained. It is also incorrect to perform the same checkForFlightInfo at resolve time because the
        // state of the server may have changed between getInfo and doGet/doExchange, and getInfo should still be valid
        // for client.
        // @Override
        // void checkForResolve(CommandGetImportedKeys command) {
        // }
    };

    private final CommandHandler commandGetExportedKeysHandler = new CommandStaticTable<>(CommandGetExportedKeys.class,
            CommandGetKeysConstants.TABLE, FlightSqlTicketHelper::ticketFor) {
        @Override
        void checkForGetInfo(CommandGetExportedKeys command) {
            if (CommandGetExportedKeys.getDefaultInstance().equals(command)) {
                // TODO: Plumb through io.deephaven.server.arrow.FlightServiceGrpcImpl.getSchema
                // We need to pretend that CommandGetExportedKeys.getDefaultInstance() is a valid command until we can
                // plumb getSchema through to the resolvers.
                return;
            }
            if (!hasTable(
                    command.hasCatalog() ? command.getCatalog() : null,
                    command.hasDbSchema() ? command.getDbSchema() : null,
                    command.getTable())) {
                throw tableNotFound();
            }
        }

        // No need to check at resolve time since there is no actual state involved. If Deephaven exposes the notion
        // of keys, this will need to behave more like QueryBase where there is a handle-based ticket and some sort
        // state maintained. It is also incorrect to perform the same checkForFlightInfo at resolve time because the
        // state of the server may have changed between getInfo and doGet/doExchange, and getInfo should still be valid
        // for client.
        // @Override
        // void checkForResolve(CommandGetExportedKeys command) {
        //
        // }
    };

    @VisibleForTesting
    static final class CommandGetTablesConstants {

        /**
         * Models return type for {@link CommandGetTables} / {@link FlightSqlProducer.Schemas#GET_TABLES_SCHEMA}.
         * 
         * <pre>
         * catalog_name: utf8,
         * db_schema_name: utf8,
         * table_name: utf8 not null,
         * table_type: utf8 not null,
         * table_schema: bytes not null
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME),
                ColumnDefinition.ofString(TABLE_NAME), // out-of-spec
                ColumnDefinition.ofString(TABLE_TYPE), // out-of-spec
                ColumnDefinition.of(TABLE_SCHEMA, Type.byteType().arrayType()) // out-of-spec
        );

        /**
         * Models return type for {@link CommandGetTables} /
         * {@link FlightSqlProducer.Schemas#GET_TABLES_SCHEMA_NO_SCHEMA}.
         * 
         * <pre>
         * catalog_name: utf8,
         * db_schema_name: utf8,
         * table_name: utf8 not null,
         * table_type: utf8 not null,
         * </pre>
         */
        @VisibleForTesting
        static final TableDefinition DEFINITION_NO_SCHEMA = TableDefinition.of(
                ColumnDefinition.ofString(CATALOG_NAME),
                ColumnDefinition.ofString(DB_SCHEMA_NAME), // out-of-spec
                ColumnDefinition.ofString(TABLE_NAME), // out-of-spec
                ColumnDefinition.ofString(TABLE_TYPE));

        private static final Map<String, Object> ATTRIBUTES = Map.of();

        private static final ByteString SCHEMA_BYTES_NO_SCHEMA =
                BarrageUtil.schemaBytesFromTableDefinition(DEFINITION_NO_SCHEMA, ATTRIBUTES, true);

        private static final ByteString SCHEMA_BYTES =
                BarrageUtil.schemaBytesFromTableDefinition(DEFINITION, ATTRIBUTES, true);
    }

    private class CommandGetTablesImpl extends CommandHandlerFixedBase<CommandGetTables> {

        CommandGetTablesImpl() {
            super(CommandGetTables.class);
        }

        @Override
        Ticket ticket(CommandGetTables command) {
            return FlightSqlTicketHelper.ticketFor(command);
        }

        @Override
        ByteString schemaBytes(CommandGetTables command) {
            return command.getIncludeSchema()
                    ? CommandGetTablesConstants.SCHEMA_BYTES
                    : CommandGetTablesConstants.SCHEMA_BYTES_NO_SCHEMA;
        }

        @Override
        public Table table(CommandGetTables request) {
            // A not present `catalog` means "don't filter based on catalog".
            // An empty `catalog` string explicitly means "only return tables that don't have a catalog".
            // In our case (since we don't expose catalogs ATM), we can combine them.
            final boolean hasCatalog = request.hasCatalog() && !request.getCatalog().isEmpty();

            // `table_types` is a set that the user wants to include, empty means "include all".
            final boolean hasTableTypeTable =
                    request.getTableTypesCount() == 0 || request.getTableTypesList().contains(TABLE_TYPE_TABLE);

            final boolean includeSchema = request.getIncludeSchema();
            if (hasCatalog || !hasTableTypeTable || request.hasDbSchemaFilterPattern()) {
                return getTablesEmpty(includeSchema, CommandGetTablesConstants.ATTRIBUTES);
            }
            final Predicate<String> tableNameFilter = request.hasTableNameFilterPattern()
                    ? flightSqlFilterPredicate(request.getTableNameFilterPattern())
                    : x -> true;
            return getTables(includeSchema, ExecutionContext.getContext().getQueryScope(),
                    CommandGetTablesConstants.ATTRIBUTES, tableNameFilter);
        }

        private Table getTablesEmpty(boolean includeSchema, Map<String, Object> attributes) {
            return includeSchema
                    ? TableTools.newTable(CommandGetTablesConstants.DEFINITION, attributes)
                    : TableTools.newTable(CommandGetTablesConstants.DEFINITION_NO_SCHEMA, attributes);
        }

        private Table getTables(boolean includeSchema, QueryScope queryScope, Map<String, Object> attributes,
                Predicate<String> tableNameFilter) {
            Objects.requireNonNull(attributes);
            final Map<String, Table> queryScopeTables =
                    (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
            final int size = queryScopeTables.size();
            final String[] catalogNames = new String[size];
            final String[] dbSchemaNames = new String[size];
            final String[] tableNames = new String[size];
            final String[] tableTypes = new String[size];
            final byte[][] tableSchemas = includeSchema ? new byte[size][] : null;
            int count = 0;
            for (Entry<String, Table> e : queryScopeTables.entrySet()) {
                final Table table = authorization.transform(e.getValue());
                if (table == null) {
                    continue;
                }
                final String tableName = e.getKey();
                if (!tableNameFilter.test(tableName)) {
                    continue;
                }
                catalogNames[count] = null;
                dbSchemaNames[count] = null;
                tableNames[count] = tableName;
                tableTypes[count] = TABLE_TYPE_TABLE;
                if (includeSchema) {
                    tableSchemas[count] = BarrageUtil.schemaBytesFromTable(table).toByteArray();
                }
                ++count;
            }
            final ColumnHolder<String> c1 = TableTools.stringCol(CATALOG_NAME, catalogNames);
            final ColumnHolder<String> c2 = TableTools.stringCol(DB_SCHEMA_NAME, dbSchemaNames);
            final ColumnHolder<String> c3 = TableTools.stringCol(TABLE_NAME, tableNames);
            final ColumnHolder<String> c4 = TableTools.stringCol(TABLE_TYPE, tableTypes);
            final ColumnHolder<byte[]> c5 = includeSchema
                    ? new ColumnHolder<>(TABLE_SCHEMA, byte[].class, byte.class, false, tableSchemas)
                    : null;
            final Table newTable = includeSchema
                    ? TableTools.newTable(CommandGetTablesConstants.DEFINITION, attributes, c1, c2, c3, c4, c5)
                    : TableTools.newTable(CommandGetTablesConstants.DEFINITION_NO_SCHEMA, attributes, c1, c2, c3, c4);
            return count == size
                    ? newTable
                    : newTable.head(count);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private <Request, Response extends Message> void executeAction(
            final SessionState session,
            final ActionHandler<Request, Response> handler,
            final org.apache.arrow.flight.Action request,
            final Consumer<org.apache.arrow.flight.Result> visitor) {
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
        final Any any = parseOrThrow(action.getBody());
        return unpackOrThrow(any, clazz);
    }

    private static org.apache.arrow.flight.Result pack(com.google.protobuf.Message message) {
        return new org.apache.arrow.flight.Result(Any.pack(message).toByteArray());
    }

    private PreparedStatement getPreparedStatement(SessionState session, ByteString handle) {
        Objects.requireNonNull(session);
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
        public void execute(
                final SessionState session,
                final ActionCreatePreparedStatementRequest request,
                final Consumer<ActionCreatePreparedStatementResult> visitor) {
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
        public void execute(
                final SessionState session,
                final ActionClosePreparedStatementRequest request,
                final Consumer<Empty> visitor) {
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

    private static StatusRuntimeException unauthenticatedError() {
        return error(Code.UNAUTHENTICATED, "Must be authenticated");
    }

    private static StatusRuntimeException deniedError() {
        return error(Code.PERMISSION_DENIED, "Must be authorized");
    }

    private static StatusRuntimeException tableNotFound() {
        return error(Code.NOT_FOUND, "table not found");
    }

    private static StatusRuntimeException transactionIdsNotSupported() {
        return error(Code.INVALID_ARGUMENT, "transaction ids are not supported");
    }

    private static StatusRuntimeException queryParametersNotSupported(RuntimeException cause) {
        return error(Code.INVALID_ARGUMENT, "query parameters are not supported", cause);
    }

    private static StatusRuntimeException error(Code code, String message) {
        return code
                .toStatus()
                .withDescription("FlightSQL: " + message)
                .asRuntimeException();
    }

    private static StatusRuntimeException error(Code code, String message, Throwable cause) {
        return code
                .toStatus()
                .withDescription("FlightSQL: " + message)
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
            this.session = Objects.requireNonNull(session);
            this.parameterizedQuery = Objects.requireNonNull(parameterizedQuery);
            this.handleId = handleIdGenerator.getAndIncrement();
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
            if (!this.session.equals(session)) {
                throw error(Code.UNAUTHENTICATED, "Must use same session");
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
                query.releaseImpl(false);
            }
            queries.clear();
        }
    }

    /**
     * The Arrow "specification" for filter pattern leaves a lot to be desired. In totality:
     *
     * <pre>
     * In the pattern string, two special characters can be used to denote matching rules:
     *    - "%" means to match any substring with 0 or more characters.
     *    - "_" means to match any one character.
     * </pre>
     *
     * There does not seem to be any potential for escaping, which means that underscores can't explicitly be matched
     * against, which is a common pattern used in Deephaven table names. As mentioned below, it also follows that an
     * empty string should only explicitly match against an empty string.
     */
    private static Predicate<String> flightSqlFilterPredicate(String flightSqlPattern) {
        // This is the technically correct, although likely represents a FlightSQL client mis-use, as the results will
        // be empty (unless an empty db_schema_name is allowed).
        //
        // Unlike the "catalog" field in CommandGetDbSchemas (/ CommandGetTables) where an empty string means
        // "retrieves those without a catalog", an empty filter pattern does not seem to be meant to match the
        // respective field where the value is not present.
        //
        // The Arrow schema for CommandGetDbSchemas explicitly points out that the returned db_schema_name is not null,
        // which implies that filter patterns are not meant to match against fields where the value is not present
        // (null).
        if (flightSqlPattern.isEmpty()) {
            // If Deephaven supports catalog / db_schema_name in the future and db_schema_name can be empty, we'd need
            // to match on that.
            // return String::isEmpty;
            return x -> false;
        }
        if ("%".equals(flightSqlPattern)) {
            return x -> true;
        }
        if (flightSqlPattern.indexOf('%') == -1 && flightSqlPattern.indexOf('_') == -1) {
            // If there are no special characters, search for an exact match; this case was explicitly seen via the
            // FlightSQL JDBC driver.
            return flightSqlPattern::equals;
        }
        final int L = flightSqlPattern.length();
        final StringBuilder pattern = new StringBuilder();
        final StringBuilder quoted = new StringBuilder();
        final Runnable appendQuoted = () -> {
            if (quoted.length() != 0) {
                pattern.append(Pattern.quote(quoted.toString()));
                quoted.setLength(0);
            }
        };
        for (int i = 0; i < L; ++i) {
            final char c = flightSqlPattern.charAt(i);
            if (c == '%') {
                appendQuoted.run();
                pattern.append(".*");
            } else if (c == '_') {
                appendQuoted.run();
                pattern.append('.');
            } else {
                quoted.append(c);
            }
        }
        appendQuoted.run();
        final Pattern p = Pattern.compile(pattern.toString());
        return x -> p.matcher(x).matches();
    }
}
