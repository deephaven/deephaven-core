//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.sql.Sql;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.ActionResolver;
import io.deephaven.server.session.CommandResolver;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.util.Scheduler;
import io.deephaven.sql.SqlParseException;
import io.deephaven.sql.UnsupportedSqlOperation;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightEndpoint;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.apache.arrow.flight.impl.Flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.deephaven.server.flightsql.FlightSqlErrorHelper.error;

/**
 * A <a href="https://arrow.apache.org/docs/format/FlightSql.html">Flight SQL</a> resolver. This supports the read-only
 * querying of the global query scope, which is presented simply with the query scope variables names as the table names
 * without a catalog and schema name.
 *
 * <p>
 * This implementation does not currently follow the Flight SQL protocol to exact specification. Namely, all the
 * returned {@link Schema Flight schemas} have nullable {@link Field fields}, and some of the fields on specific
 * commands have different types (see {@link #flightInfoFor(SessionState, FlightDescriptor, String)} for specifics).
 *
 * <p>
 * All commands, actions, and resolution must be called by authenticated users.
 */
@Singleton
public final class FlightSqlResolver implements ActionResolver, CommandResolver {

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
    private static final Duration FIXED_TICKET_EXPIRE_DURATION = Duration.ofMinutes(1);
    private static final long QUERY_WATCHDOG_TIMEOUT_MILLIS = Duration
            .parse(Configuration.getInstance().getStringWithDefault("FlightSQL.queryTimeout", "PT5s")).toMillis();

    private static final Logger log = LoggerFactory.getLogger(FlightSqlResolver.class);

    private static final KeyedObjectKey<ByteString, QueryBase> QUERY_KEY =
            new KeyedObjectKey.BasicAdapter<>(QueryBase::handleId);

    private static final KeyedObjectKey<ByteString, PreparedStatement> PREPARED_STATEMENT_KEY =
            new KeyedObjectKey.BasicAdapter<>(PreparedStatement::handleId);

    @VisibleForTesting
    static final Schema DATASET_SCHEMA_SENTINEL = new Schema(List.of(Field.nullable("DO_NOT_USE", Utf8.INSTANCE)));

    private final Scheduler scheduler;
    private final Authorization authorization;
    private final KeyedObjectHashMap<ByteString, QueryBase> queries;
    private final KeyedObjectHashMap<ByteString, PreparedStatement> preparedStatements;

    @Inject
    public FlightSqlResolver(
            final AuthorizationProvider authProvider,
            final Scheduler scheduler) {
        this.authorization = Objects.requireNonNull(authProvider.getTicketResolverAuthorization());
        this.scheduler = Objects.requireNonNull(scheduler);
        this.queries = new KeyedObjectHashMap<>(QUERY_KEY);
        this.preparedStatements = new KeyedObjectHashMap<>(PREPARED_STATEMENT_KEY);
    }

    /**
     * The Flight SQL ticket route, equal to {@value FlightSqlTicketHelper#TICKET_PREFIX}.
     *
     * @return the Flight SQL ticket route
     */
    @Override
    public byte ticketRoute() {
        return FlightSqlTicketHelper.TICKET_PREFIX;
    }

    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Returns {@code true} if the given command {@code descriptor} appears to be a valid Flight SQL command; that is,
     * it is parsable as an {@code Any} protobuf message with the type URL prefixed with
     * {@value FlightSqlSharedConstants#FLIGHT_SQL_COMMAND_TYPE_PREFIX}.
     *
     * @param descriptor the descriptor
     * @return {@code true} if the given command appears to be a valid Flight SQL command
     */
    @Override
    public boolean handlesCommand(Flight.FlightDescriptor descriptor) {
        return FlightSqlCommandHelper.handlesCommand(descriptor);
    }

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
        return FlightSqlCommandHelper.visit(descriptor, new GetFlightInfoImpl(session, descriptor), logId);
    }

    private class GetFlightInfoImpl extends FlightSqlCommandHelper.CommandVisitorBase<ExportObject<FlightInfo>> {
        private final SessionState session;
        private final FlightDescriptor descriptor;

        public GetFlightInfoImpl(SessionState session, FlightDescriptor descriptor) {
            this.session = Objects.requireNonNull(session);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        @Override
        public ExportObject<FlightInfo> visitDefault(Descriptor descriptor, Object command) {
            return submit(new UnsupportedCommand<>(descriptor), command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetCatalogs command) {
            return submit(CommandGetCatalogsConstants.HANDLER, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetDbSchemas command) {
            return submit(CommandGetDbSchemasConstants.HANDLER, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetTableTypes command) {
            return submit(CommandGetTableTypesConstants.HANDLER, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetImportedKeys command) {
            return submit(commandGetImportedKeysHandler, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetExportedKeys command) {
            return submit(commandGetExportedKeysHandler, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetPrimaryKeys command) {
            return submit(commandGetPrimaryKeysHandler, command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandGetTables command) {
            return submit(new CommandGetTablesImpl(), command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandStatementQuery command) {
            return submit(new CommandStatementQueryImpl(session), command);
        }

        @Override
        public ExportObject<FlightInfo> visit(CommandPreparedStatementQuery command) {
            return submit(new CommandPreparedStatementQueryImpl(session), command);
        }

        private <T> ExportObject<FlightInfo> submit(CommandHandler<T> handler, T command) {
            return session.<FlightInfo>nonExport().submit(() -> getInfo(handler, command));
        }

        private <T> FlightInfo getInfo(CommandHandler<T> handler, T command) {
            final QueryPerformanceRecorder qpr = QueryPerformanceRecorder.getInstance();
            try (final QueryPerformanceNugget ignore =
                    qpr.getNugget(String.format("FlightSQL.getInfo/%s", command.getClass().getSimpleName()))) {
                return flightInfo(handler, command);
            }
        }

        private <T> FlightInfo flightInfo(CommandHandler<T> handler, T command) {
            final TicketHandler ticketHandler = handler.execute(command);
            try {
                return ticketHandler.getInfo(descriptor);
            } catch (Throwable t) {
                if (ticketHandler instanceof TicketHandlerReleasable) {
                    ((TicketHandlerReleasable) ticketHandler).release();
                }
                throw t;
            }
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
        final ExportObject<Table> tableExport = FlightSqlTicketHelper.visit(ticket, new ResolveImpl(session), logId);
        // noinspection unchecked
        return (ExportObject<T>) tableExport;
    }

    private class ResolveImpl implements FlightSqlTicketHelper.TicketVisitor<ExportObject<Table>> {
        private final SessionState session;

        public ResolveImpl(SessionState session) {
            this.session = Objects.requireNonNull(session);
        }

        @Override
        public ExportObject<Table> visit(CommandGetCatalogs ticket) {
            return submit(CommandGetCatalogsConstants.HANDLER, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetDbSchemas ticket) {
            return submit(CommandGetDbSchemasConstants.HANDLER, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetTableTypes ticket) {
            return submit(CommandGetTableTypesConstants.HANDLER, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetImportedKeys ticket) {
            return submit(commandGetImportedKeysHandler, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetExportedKeys ticket) {
            return submit(commandGetExportedKeysHandler, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetPrimaryKeys ticket) {
            return submit(commandGetPrimaryKeysHandler, ticket);
        }

        @Override
        public ExportObject<Table> visit(CommandGetTables ticket) {
            return submit(commandGetTables, ticket);
        }

        private <C extends Message> ExportObject<Table> submit(CommandHandlerFixedBase<C> fixed, C command) {
            // We know this is a trivial execute, okay to do on RPC thread
            return submit(fixed.execute(command));
        }

        @Override
        public ExportObject<Table> visit(TicketStatementQuery ticket) {
            final TicketHandler ticketHandler = queries.get(ticket.getStatementHandle());
            if (ticketHandler == null) {
                throw error(Code.NOT_FOUND,
                        "Unable to find Flight SQL query. Flight SQL tickets should be resolved promptly and resolved at most once.");
            }
            if (!ticketHandler.isOwner(session)) {
                // We should not be concerned about returning "NOT_FOUND" here; the handleId is sufficiently random that
                // it is much more likely an authentication setup issue.
                throw permissionDeniedWithHelpfulMessage();
            }
            return submit(ticketHandler);
        }

        // Note: we could be more efficient and do the static table resolution on thread instead of submitting. For
        // simplicity purposes for now, we will submit all of them for resolution.
        private ExportObject<Table> submit(TicketHandler handler) {
            return new TableResolver(session, handler).submit();
        }
    }

    private static class TableResolver implements SessionState.ExportErrorHandler {
        private final SessionState session;
        private final TicketHandler handler;

        public TableResolver(SessionState session, TicketHandler handler) {
            this.handler = Objects.requireNonNull(handler);
            this.session = Objects.requireNonNull(session);
        }

        public ExportObject<Table> submit() {
            // We need to provide clean handoff of the Table for Liveness management between the resolver and the
            // export; as such, we _can't_ unmanage the Table during a call to TicketHandler.resolve, so we must rely
            // on onSuccess / onError callbacks (after export has started managing the Table).
            return session.<Table>nonExport()
                    .onSuccess(this::onSuccess)
                    .onError(this)
                    .submit(handler::resolve);
        }

        private void onSuccess() {
            release();
        }

        @Override
        public void onError(ExportNotification.State resultState, String errorContext, @Nullable Exception cause,
                @Nullable String dependentExportId) {
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
        // noinspection DataFlowIssue
        throw Assert.statementNeverExecuted();
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
     * Returns {@code true} if {@code type} is a known Flight SQL action type (even if this implementation does not
     * implement it).
     *
     * @param type the action type
     * @return if {@code type} is a known Flight SQL action type
     */
    @Override
    public boolean handlesActionType(String type) {
        return FlightSqlActionHelper.handlesAction(type);
    }

    /**
     * Executes the given {@code action}. Only supports authenticated access. Currently, supports
     * {@link FlightSqlUtils#FLIGHT_SQL_CREATE_PREPARED_STATEMENT} and
     * {@link FlightSqlUtils#FLIGHT_SQL_CLOSE_PREPARED_STATEMENT}; all other action types will throw an
     * {@link Code#UNIMPLEMENTED} exception. Transactions are not currently supported.
     *
     * @param session the session
     * @param action the action
     * @param observer the observer
     */
    @Override
    public void doAction(
            @Nullable final SessionState session,
            final Action action,
            final StreamObserver<Result> observer) {
        // If false, there is an error with io.deephaven.server.session.ActionRouter.doAction / handlesActionType
        Assert.eqTrue(handlesActionType(action.getType()), "handlesActionType(action.getType())");
        if (session == null) {
            throw unauthenticatedError();
        }
        executeAction(session, FlightSqlActionHelper.visit(action, new ActionHandlerVisitor()), observer);
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
     * Publishing to Flight SQL descriptors is not currently supported. Throws a {@link Code#FAILED_PRECONDITION} error.
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
                "Could not publish '" + logId + "': Flight SQL descriptors cannot be published to");
    }

    /**
     * Publishing to Flight SQL tickets is not currently supported. Throws a {@link Code#FAILED_PRECONDITION} error.
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
                "Could not publish '" + logId + "': Flight SQL tickets cannot be published to");
    }

    // ---------------------------------------------------------------------------------------------------------------

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // This is a bit different from the other resolvers; a ticket may be a very long byte string here since it
        // may represent a command.
        return FlightSqlTicketHelper.toReadableString(ticket, logId);
    }

    // ---------------------------------------------------------------------------------------------------------------

    interface CommandHandler<C> {

        TicketHandler execute(C command);
    }

    interface TicketHandler {

        boolean isOwner(SessionState session);

        FlightInfo getInfo(FlightDescriptor descriptor);

        Table resolve();
    }

    interface TicketHandlerReleasable extends TicketHandler {

        void release();
    }

    private Table executeSqlQuery(String sql) {
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final QueryScope queryScope = executionContext.getQueryScope();
        // We aren't managing the liveness of Tables that come verbatim (authorization un-transformed) from the query
        // scope (we are ensuring that any transformed, or operation created, tables don't escape to a higher-layer's
        // liveness scope). In the case where they either are already not live, or become not live by the time the
        // operation logic is executed, an appropriate exception will be thrown. While this is a liveness race, it isn't
        // technically much different than a liveness race possible via ScopeTicketResolver.resolve.
        //
        // The proper way to do this would be to re-model the table execution logic of GrpcTableOperation (gRPC) into a
        // QST form, whereby table dependencies are presented as properly-scoped, liveness-managed Exports for the
        // duration of the operation.
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            // See SQLTODO(catalog-reader-implementation)
            // Unfortunately, we must do authorization.transform on all the query scope tables up-front; to make this
            // on-demand, we need to implement a Calcite Catalog Reader (non-trivial). Technically, parseSql only needs
            // to know the definitions of the tables; we could consider a table-specific authorization interface that
            // presents the specialization Authorization.transformedDefinition(Table) to make this cheaper in a lot of
            // cases.
            final Map<String, Table> queryScopeTables =
                    queryScope.toMap(o -> queryScopeAuthorizedTableMapper(queryScope, o), (n, t) -> t != null);
            final TableSpec tableSpec =
                    Sql.parseSql(sql, queryScopeTables, TableCreatorScopeTickets::ticketTable, null);
            final TableCreator<Table> tableCreator =
                    new TableCreatorScopeTickets(TableCreatorImpl.INSTANCE, queryScopeTables);
            // We could consider doing finer-grained sharedLock in the future; right now, taking it for the whole
            // operation if any of the TicketTable sources are refreshing.
            final List<Table> refreshingTables = new ArrayList<>();
            for (final TableSpec node : ParentsVisitor.reachable(List.of(tableSpec))) {
                // Of the source tables, SQL can produce a NewTable or a TicketTable (until we introduce custom
                // functions, where we could conceivable have it produce EmptyTable, TimeTable, etc).
                if (!(node instanceof TicketTable)) {
                    continue;
                }
                final Table sourceTable = tableCreator.of((TicketTable) node);
                if (sourceTable.isRefreshing()) {
                    refreshingTables.add(sourceTable);
                }
            }
            final UpdateGraph updateGraph = refreshingTables.isEmpty()
                    ? null
                    : NotificationQueue.Dependency.getUpdateGraph(null, refreshingTables.toArray(new Table[0]));
            // Note: Authorization.transform has already been performed, but we are _not_ doing
            // io.deephaven.auth.ServiceAuthWiring checks.
            // TODO(deephaven-core#6307): Declarative server-side table execution logic that preserves authorization
            // logic
            try (
                    final SafeCloseable ignored0 =
                            updateGraph == null ? null : executionContext.withUpdateGraph(updateGraph).open();
                    final SafeCloseable ignored1 =
                            updateGraph == null ? null : updateGraph.sharedLock().lockCloseable()) {
                final Table table = tableSpec.logic().create(tableCreator);
                if (table.isRefreshing()) {
                    table.retainReference();
                }
                return table;
            }
        }
    }

    private Table queryScopeTableMapper(QueryScope queryScope, Object object) {
        if (object == null) {
            return null;
        }
        object = queryScope.unwrapObject(object);
        if (!(object instanceof Table)) {
            return null;
        }
        return (Table) object;
    }

    private Table queryScopeAuthorizedTableMapper(QueryScope queryScope, Object object) {
        final Table table = queryScopeTableMapper(queryScope, object);
        return table == null ? null : authorization.transform(table);
    }

    /**
     * This is the base class for "easy" commands; that is, commands that have a fixed schema and are cheap to
     * initialize.
     */
    static abstract class CommandHandlerFixedBase<T extends Message> implements CommandHandler<T> {

        /**
         * This is called as the first part of {@link TicketHandler#getInfo(FlightDescriptor)} for the handler returned
         * from {@link #execute(T)}. It can be used as an early signal to let clients know that the command is not
         * supported, or one of the arguments is not valid.
         */
        void checkForGetInfo(T command) {

        }

        /**
         * This is called as the first part of {@link TicketHandler#resolve()} for the handler returned from
         * {@link #execute(T)}.
         */
        void checkForResolve(T command) {
            // This is provided for completeness, but the current implementations don't use it.
            //
            // The callers that override checkForGetInfo, for example, all involve table names; if that table exists and
            // they are authorized, they will get back a ticket that upon resolve will be an (empty) table. Otherwise,
            // they will get a NOT_FOUND exception at getFlightInfo time.
            //
            // In this context, it is incorrect to do the same check at resolve time because we need to ensure that
            // getFlightInfo / doGet (/ doExchange) appears stateful - it would be incorrect to return getFlightInfo
            // with the semantics "this table exists" and then potentially throw a NOT_FOUND at resolve time.
            //
            // If Deephaven Flight SQL implements CommandGetExportedKeys, CommandGetImportedKeys, or
            // CommandGetPrimaryKeys, we'll likely need to "upgrade" the implementation to a properly stateful one like
            // QueryBase with handle-based tickets.
        }

        long totalRecords() {
            return -1;
        }

        abstract Ticket ticket(T command);

        abstract ByteString schemaBytes(T command);

        abstract Table table(T command);

        /**
         * The handler. Will invoke {@link #checkForGetInfo(Message)} as the first part of
         * {@link TicketHandler#getInfo(FlightDescriptor)}. Will invoke {@link #checkForResolve(Message)} as the first
         * part of {@link TicketHandler#resolve()}.
         */
        @Override
        public final TicketHandler execute(T command) {
            return new TicketHandlerFixed(command);
        }

        private class TicketHandlerFixed implements TicketHandler {
            private final T command;

            private TicketHandlerFixed(T command) {
                this.command = Objects.requireNonNull(command);
            }

            @Override
            public boolean isOwner(SessionState session) {
                return true;
            }

            @Override
            public FlightInfo getInfo(FlightDescriptor descriptor) {
                checkForGetInfo(command);
                // Note: the presence of expirationTime is mainly a way to let clients know that they can retry a DoGet
                // / DoExchange with an existing ticket without needing to do a new getFlightInfo first (for at least
                // the amount of time as specified by expirationTime). Given that the tables resolved via this code-path
                // are "easy" (in a lot of the cases, they are static empty tables or "easily" computable)...
                // We are not setting an expiration timestamp for the SQL queries - they are only meant to be resolvable
                // once; this is different from the watchdog concept.
                return FlightInfo.newBuilder()
                        .setFlightDescriptor(descriptor)
                        .setSchema(schemaBytes(command))
                        .addEndpoint(FlightEndpoint.newBuilder()
                                .setTicket(ticket(command))
                                .setExpirationTime(timestamp(Instant.now().plus(FIXED_TICKET_EXPIRE_DURATION)))
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
                    // If true, TicketHandler implementation error; should only override totalRecords for
                    // non-refreshing tables
                    Assert.eqFalse(table.isRefreshing(), "table.isRefreshing()");
                    // If false, Ticket handler implementation error; totalRecords does not match the table size
                    Assert.eq(table.size(), "table.size()", totalRecords, "totalRecords");
                }
                return table;
            }
        }
    }

    private static final class UnsupportedCommand<T> implements CommandHandler<T>, TicketHandler {
        private final Descriptor descriptor;

        UnsupportedCommand(Descriptor descriptor) {
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        @Override
        public TicketHandler execute(T command) {
            return this;
        }

        @Override
        public boolean isOwner(SessionState session) {
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

    abstract class QueryBase<C> implements CommandHandler<C>, TicketHandlerReleasable {
        private final ByteString handleId;
        protected final SessionState session;

        private boolean initialized;
        private boolean resolved;
        private Table table;

        QueryBase(SessionState session) {
            this.handleId = randomHandleId();
            this.session = Objects.requireNonNull(session);
            queries.put(handleId, this);
        }

        public ByteString handleId() {
            return handleId;
        }

        @Override
        public final TicketHandlerReleasable execute(C command) {
            try {
                return executeImpl(command);
            } catch (Throwable t) {
                release();
                throw t;
            }
        }

        private synchronized QueryBase<C> executeImpl(C command) {
            Assert.eqFalse(initialized, "initialized");
            initialized = true;
            executeSql(command);
            Assert.neqNull(table, "table");
            // Note: we aren't currently providing a way to proactively cleanup query watchdogs - given their
            // short-lived nature, they will execute "quick enough" for most use cases.
            scheduler.runAfterDelay(QUERY_WATCHDOG_TIMEOUT_MILLIS, this::onWatchdog);
            return this;
        }

        // responsible for setting table and schemaBytes
        protected abstract void executeSql(C command);

        protected void executeSql(String sql) {
            try {
                table = executeSqlQuery(sql);
            } catch (SqlParseException e) {
                throw error(Code.INVALID_ARGUMENT, "query can't be parsed", e);
            } catch (UnsupportedSqlOperation e) {
                if (e.clazz() == RexDynamicParam.class) {
                    throw queryParametersNotSupported(e);
                }
                throw error(Code.INVALID_ARGUMENT,
                        String.format("Unsupported calcite type '%s'", e.clazz().getName()),
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

        // ----------------------------------------------------------------------------------------------------------

        @Override
        public final boolean isOwner(SessionState session) {
            return this.session.equals(session);
        }

        @Override
        public final synchronized FlightInfo getInfo(FlightDescriptor descriptor) {
            return TicketRouter.getFlightInfo(table, descriptor, ticket());
        }

        @Override
        public final synchronized Table resolve() {
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
        public synchronized void release() {
            if (!queries.remove(handleId, this)) {
                return;
            }
            doRelease();
        }

        private void doRelease() {
            if (table != null) {
                if (table.isRefreshing()) {
                    table.dropReference();
                }
                table = null;
            }
        }

        // ----------------------------------------------------------------------------------------------------------

        private synchronized void onWatchdog() {
            if (!queries.remove(handleId, this)) {
                return;
            }
            log.debug().append("Watchdog cleaning up query handleId=")
                    .append(ByteStringAsHex.INSTANCE, handleId)
                    .endl();
            doRelease();
        }

        private Ticket ticket() {
            return FlightSqlTicketHelper.ticketCreator().visit(TicketStatementQuery.newBuilder()
                    .setStatementHandle(handleId)
                    .build());
        }
    }

    final class CommandStatementQueryImpl extends QueryBase<CommandStatementQuery> {

        CommandStatementQueryImpl(SessionState session) {
            super(session);
        }

        @Override
        public void executeSql(CommandStatementQuery command) {
            if (command.hasTransactionId()) {
                throw transactionIdsNotSupported();
            }
            executeSql(command.getQuery());
        }
    }

    final class CommandPreparedStatementQueryImpl extends QueryBase<CommandPreparedStatementQuery> {

        private PreparedStatement prepared;

        CommandPreparedStatementQueryImpl(SessionState session) {
            super(session);
        }

        @Override
        public void executeSql(CommandPreparedStatementQuery command) {
            prepared = getPreparedStatement(session, command.getPreparedStatementHandle());
            // Assumed this is not actually parameterized.
            final String sql = prepared.parameterizedQuery();
            executeSql(sql);
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

        CommandStaticTable(Table table, Function<T, Ticket> f) {
            super();
            Assert.eqFalse(table.isRefreshing(), "table.isRefreshing()");
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

        public static final CommandHandlerFixedBase<CommandGetTableTypes> HANDLER = new CommandStaticTable<>(
                TABLE, FlightSqlTicketHelper.ticketCreator()::visit);
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

        public static final CommandHandlerFixedBase<CommandGetCatalogs> HANDLER =
                new CommandStaticTable<>(TABLE, FlightSqlTicketHelper.ticketCreator()::visit);
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
        public static final CommandHandlerFixedBase<CommandGetDbSchemas> HANDLER = new CommandStaticTable<>(
                TABLE, FlightSqlTicketHelper.ticketCreator()::visit);
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
        final QueryScope scope = ExecutionContext.getContext().getQueryScope();
        try {
            obj = scope.readParamValue(table);
        } catch (QueryScope.MissingVariableException e) {
            return false;
        }
        if (!(obj instanceof Table)) {
            return false;
        }
        return !authorization.isDeniedAccess(obj);
    }

    private final CommandHandlerFixedBase<CommandGetPrimaryKeys> commandGetPrimaryKeysHandler =
            new CommandStaticTable<>(CommandGetPrimaryKeysConstants.TABLE,
                    FlightSqlTicketHelper.ticketCreator()::visit) {
                @Override
                void checkForGetInfo(CommandGetPrimaryKeys command) {
                    if (CommandGetPrimaryKeys.getDefaultInstance().equals(command)) {
                        // We need to pretend that CommandGetPrimaryKeys.getDefaultInstance() is a valid command until
                        // we can plumb getSchema through to the resolvers.
                        // TODO(deephaven-core#6218): feat: expose getSchema to TicketResolvers
                        return;
                    }
                    if (!hasTable(
                            command.hasCatalog() ? command.getCatalog() : null,
                            command.hasDbSchema() ? command.getDbSchema() : null,
                            command.getTable())) {
                        throw tableNotFound();
                    }
                }
            };

    private final CommandHandlerFixedBase<CommandGetImportedKeys> commandGetImportedKeysHandler =
            new CommandStaticTable<>(CommandGetKeysConstants.TABLE, FlightSqlTicketHelper.ticketCreator()::visit) {
                @Override
                void checkForGetInfo(CommandGetImportedKeys command) {
                    if (CommandGetImportedKeys.getDefaultInstance().equals(command)) {
                        // We need to pretend that CommandGetImportedKeys.getDefaultInstance() is a valid command until
                        // we can plumb getSchema through to the resolvers.
                        // TODO(deephaven-core#6218): feat: expose getSchema to TicketResolvers
                        return;
                    }
                    if (!hasTable(
                            command.hasCatalog() ? command.getCatalog() : null,
                            command.hasDbSchema() ? command.getDbSchema() : null,
                            command.getTable())) {
                        throw tableNotFound();
                    }
                }
            };

    private final CommandHandlerFixedBase<CommandGetExportedKeys> commandGetExportedKeysHandler =
            new CommandStaticTable<>(CommandGetKeysConstants.TABLE, FlightSqlTicketHelper.ticketCreator()::visit) {
                @Override
                void checkForGetInfo(CommandGetExportedKeys command) {
                    if (CommandGetExportedKeys.getDefaultInstance().equals(command)) {
                        // We need to pretend that CommandGetExportedKeys.getDefaultInstance() is a valid command until
                        // we can plumb getSchema through to the resolvers.
                        // TODO(deephaven-core#6218): feat: expose getSchema to TicketResolvers
                        return;
                    }
                    if (!hasTable(
                            command.hasCatalog() ? command.getCatalog() : null,
                            command.hasDbSchema() ? command.getDbSchema() : null,
                            command.getTable())) {
                        throw tableNotFound();
                    }
                }
            };

    private final CommandHandlerFixedBase<CommandGetTables> commandGetTables = new CommandGetTablesImpl();

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
                ColumnDefinition.fromGenericType(TABLE_SCHEMA, Schema.class) // out-of-spec
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

        @Override
        Ticket ticket(CommandGetTables command) {
            return FlightSqlTicketHelper.ticketCreator().visit(command);
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
            // Note: _not_ using queryScopeAuthorizedTable mapper; we can have a more efficient implementation when
            // !includeSchema that only needs to check authorization.isDeniedAccess.
            final Map<String, Table> queryScopeTables = queryScope.toMap(
                    o -> queryScopeTableMapper(queryScope, o),
                    (tableName, table) -> table != null && tableNameFilter.test(tableName));
            final int size = queryScopeTables.size();
            final String[] catalogNames = new String[size];
            final String[] dbSchemaNames = new String[size];
            final String[] tableNames = new String[size];
            final String[] tableTypes = new String[size];
            final Schema[] tableSchemas = includeSchema ? new Schema[size] : null;
            int count = 0;
            for (Entry<String, Table> e : queryScopeTables.entrySet()) {
                final String tableName = e.getKey();
                final Schema schema;
                if (includeSchema) {
                    final Table table = authorization.transform(e.getValue());
                    if (table == null) {
                        continue;
                    }
                    schema = BarrageUtil.schemaFromTable(table);
                } else {
                    if (authorization.isDeniedAccess(e.getValue())) {
                        continue;
                    }
                    schema = null;
                }
                catalogNames[count] = null;
                dbSchemaNames[count] = null;
                tableNames[count] = tableName;
                tableTypes[count] = TABLE_TYPE_TABLE;
                if (includeSchema) {
                    tableSchemas[count] = schema;
                }
                ++count;
            }
            final ColumnHolder<String> c1 = TableTools.stringCol(CATALOG_NAME, catalogNames);
            final ColumnHolder<String> c2 = TableTools.stringCol(DB_SCHEMA_NAME, dbSchemaNames);
            final ColumnHolder<String> c3 = TableTools.stringCol(TABLE_NAME, tableNames);
            final ColumnHolder<String> c4 = TableTools.stringCol(TABLE_TYPE, tableTypes);
            final ColumnHolder<Schema> c5 = includeSchema
                    ? new ColumnHolder<>(TABLE_SCHEMA, Schema.class, null, false, tableSchemas)
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

    private <Response extends Message> void executeAction(
            final SessionState session,
            final ActionHandler<Response> handler,
            final StreamObserver<Result> observer) {
        // If there was complicated logic going on (actual building of tables), or needed to block, we would instead use
        // exports or some other mechanism of doing this work off-thread. For now, it's simple enough that we can do it
        // all on this RPC thread.
        handler.execute(session, new SafelyOnNextConsumer<>(observer));
        GrpcUtil.safelyComplete(observer);
    }

    private class ActionHandlerVisitor
            extends FlightSqlActionHelper.ActionVisitorBase<ActionHandler<? extends Message>> {
        @Override
        public ActionHandler<? extends Message> visit(ActionCreatePreparedStatementRequest action) {
            return new CreatePreparedStatementImpl(action);
        }

        @Override
        public ActionHandler<? extends Message> visit(ActionClosePreparedStatementRequest action) {
            return new ClosePreparedStatementImpl(action);
        }

        @Override
        public ActionHandler<? extends Message> visitDefault(ActionType actionType, Object action) {
            return new UnsupportedAction<>(actionType);
        }
    }

    private static org.apache.arrow.flight.Result pack(com.google.protobuf.Message message) {
        return new org.apache.arrow.flight.Result(Any.pack(message).toByteArray());
    }

    private PreparedStatement getPreparedStatement(SessionState session, ByteString handle) {
        Objects.requireNonNull(session);
        final PreparedStatement preparedStatement = preparedStatements.get(handle);
        if (preparedStatement == null) {
            throw error(Code.NOT_FOUND, "Unknown Prepared Statement");
        }
        preparedStatement.verifyOwner(session);
        return preparedStatement;
    }

    interface ActionHandler<Response> {

        void execute(SessionState session, Consumer<Response> visitor);
    }

    static abstract class ActionBase<Request extends com.google.protobuf.Message, Response>
            implements ActionHandler<Response> {

        final ActionType type;
        final Request request;

        public ActionBase(Request request, ActionType type) {
            this.type = Objects.requireNonNull(type);
            this.request = Objects.requireNonNull(request);
        }
    }

    final class CreatePreparedStatementImpl
            extends ActionBase<ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult> {
        public CreatePreparedStatementImpl(ActionCreatePreparedStatementRequest request) {
            super(request, FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        }

        @Override
        public void execute(
                final SessionState session,
                final Consumer<ActionCreatePreparedStatementResult> visitor) {
            if (request.hasTransactionId()) {
                throw transactionIdsNotSupported();
            }
            // It could be good to parse the query at this point in time to ensure it's valid and _not_ parameterized;
            // we will need to dig into Calcite further to explore this possibility. For now, we will error out either
            // when the client tries to do a DoPut for the parameter value, or during the Ticket execution, if the query
            // is invalid.
            final PreparedStatement prepared = new PreparedStatement(session, request.getQuery());

            // Note: we are providing a fake dataset schema here since the Flight SQL JDBC driver uses the results as an
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
            final ByteString datasetSchemaBytes;
            try {
                datasetSchemaBytes = serializeToByteString(DATASET_SCHEMA_SENTINEL);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            final ActionCreatePreparedStatementResult response = ActionCreatePreparedStatementResult.newBuilder()
                    .setPreparedStatementHandle(prepared.handleId())
                    .setDatasetSchema(datasetSchemaBytes)
                    // .setParameterSchema(...)
                    .build();
            visitor.accept(response);
        }
    }

    // Faking it as Empty message so it types check
    final class ClosePreparedStatementImpl extends ActionBase<ActionClosePreparedStatementRequest, Empty> {
        public ClosePreparedStatementImpl(ActionClosePreparedStatementRequest request) {
            super(request, FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
        }

        @Override
        public void execute(
                final SessionState session,
                final Consumer<Empty> visitor) {
            final PreparedStatement prepared = getPreparedStatement(session, request.getPreparedStatementHandle());
            prepared.close();
            // no responses
        }
    }

    static final class UnsupportedAction<Response> implements ActionHandler<Response> {
        private final ActionType type;

        public UnsupportedAction(ActionType type) {
            this.type = Objects.requireNonNull(type);
        }

        @Override
        public void execute(SessionState session, Consumer<Response> visitor) {
            throw error(Code.UNIMPLEMENTED,
                    String.format("Action type '%s' is unimplemented", type.getType()));
        }
    }

    private static class SafelyOnNextConsumer<Response extends Message> implements Consumer<Response> {
        private final StreamObserver<Result> delegate;

        public SafelyOnNextConsumer(StreamObserver<Result> delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void accept(Response response) {
            GrpcUtil.safelyOnNext(delegate, pack(response));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static StatusRuntimeException unauthenticatedError() {
        return error(Code.UNAUTHENTICATED, "Must be authenticated");
    }

    private static StatusRuntimeException permissionDeniedWithHelpfulMessage() {
        return error(Code.PERMISSION_DENIED,
                "Must use the original session; is the client echoing the authentication token properly? Some clients may need to explicitly enable cookie-based authentication with the header x-deephaven-auth-cookie-request=true (namely, Java Flight SQL JDBC drivers, and maybe others).");
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

    /*
     * The random number generator used by this class to create random based UUIDs. In a holder class to defer
     * initialization until needed.
     */
    private static class Holder {
        static final SecureRandom SECURE_RANDOM = new SecureRandom();
    }

    private static ByteString randomHandleId() {
        // While we don't _rely_ on security through obscurity, we don't want to have a simple incrementing counter
        // since it would be trivial to deduce other users' handleIds.
        final byte[] handleIdBytes = new byte[16];
        Holder.SECURE_RANDOM.nextBytes(handleIdBytes);
        return ByteStringAccess.wrap(handleIdBytes);
    }

    private enum ByteStringAsHex implements LogOutput.ObjFormatter<ByteString> {
        INSTANCE;

        @Override
        public void format(LogOutput logOutput, ByteString bytes) {
            logOutput.append("0x").append(ByteHelper.byteBufToHex(bytes.asReadOnlyByteBuffer()));
        }
    }

    private class PreparedStatement {
        private final ByteString handleId;
        private final SessionState session;
        private final String parameterizedQuery;
        private final Set<CommandPreparedStatementQueryImpl> queries;
        private final Closeable onSessionClosedCallback;

        PreparedStatement(SessionState session, String parameterizedQuery) {
            this.session = Objects.requireNonNull(session);
            this.parameterizedQuery = Objects.requireNonNull(parameterizedQuery);
            this.handleId = randomHandleId();
            this.queries = new HashSet<>();
            preparedStatements.put(handleId, this);
            this.session.addOnCloseCallback(onSessionClosedCallback = this::onSessionClosed);
        }

        public ByteString handleId() {
            return handleId;
        }

        public String parameterizedQuery() {
            return parameterizedQuery;
        }

        public void verifyOwner(SessionState session) {
            if (!this.session.equals(session)) {
                // We should not be concerned about returning "NOT_FOUND" here; the handleId is sufficiently random that
                // it is much more likely an authentication setup issue.
                throw permissionDeniedWithHelpfulMessage();
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
            log.debug()
                    .append("onSessionClosed: removing prepared statement handleId=")
                    .append(ByteStringAsHex.INSTANCE, handleId)
                    .endl();
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
     *
     * <p>
     * The <a href=
     * "https://github.com/apache/arrow/blob/apache-arrow-18.0.0/java/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowDatabaseMetadata.java#L1253-L1277">flight-sql-jdbc-core
     * implement of sqlToRegexLike</a> uses a similar approach, but appears more fragile as it is doing manual escaping
     * of regex as opposed to {@link Pattern#quote(String)}.
     */
    @VisibleForTesting
    static Predicate<String> flightSqlFilterPredicate(String flightSqlPattern) {
        // TODO(deephaven-core#6403): Flight SQL filter pattern improvements
        // This is the technically correct, although likely represents a Flight SQL client mis-use, as the results will
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
            // Flight SQL JDBC driver.
            return flightSqlPattern::equals;
        }
        final StringBuilder pattern = new StringBuilder();
        final StringBuilder quoted = new StringBuilder();
        final Runnable appendQuoted = () -> {
            if (quoted.length() != 0) {
                pattern.append(Pattern.quote(quoted.toString()));
                quoted.setLength(0);
            }
        };
        try (final IntStream codePoints = flightSqlPattern.codePoints()) {
            final PrimitiveIterator.OfInt it = codePoints.iterator();
            while (it.hasNext()) {
                final int codePoint = it.nextInt();
                if (Character.isBmpCodePoint(codePoint)) {
                    final char c = (char) codePoint;
                    if (c == '%') {
                        appendQuoted.run();
                        pattern.append(".*");
                    } else if (c == '_') {
                        appendQuoted.run();
                        pattern.append('.');
                    } else {
                        quoted.append(c);
                    }
                } else {
                    quoted.appendCodePoint(codePoint);
                }
            }
        }
        appendQuoted.run();
        final Pattern p = Pattern.compile(pattern.toString());
        return x -> p.matcher(x).matches();
    }

    private static ByteString serializeToByteString(Schema schema) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ArrowIpcUtil.serialize(outputStream, schema);
        return ByteStringAccess.wrap(outputStream.toByteArray());
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
