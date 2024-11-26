//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.annotations.VisibleForTesting;
import io.grpc.Status;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedSubstraitPlanRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndSavepointRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class FlightSqlActionHelper {

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

    interface ActionVisitor<T> {

        T visit(ActionCreatePreparedStatementRequest action);

        T visit(ActionClosePreparedStatementRequest action);

        T visit(ActionBeginSavepointRequest action);

        T visit(ActionEndSavepointRequest action);

        T visit(ActionBeginTransactionRequest action);

        T visit(ActionEndTransactionRequest action);

        T visit(@SuppressWarnings("deprecation") ActionCancelQueryRequest action);

        T visit(ActionCreatePreparedSubstraitPlanRequest action);
    }

    public static boolean handlesAction(String type) {
        // There is no prefix for Flight SQL action types, so the best we can do is a set-based lookup. This also means
        // that this resolver will not be able to respond with an appropriately scoped error message for new Flight SQL
        // action types (io.deephaven.server.flightsql.FlightSqlResolver.UnsupportedAction).
        return FLIGHT_SQL_ACTION_TYPES.contains(type);
    }

    public static <T> T visit(Action action, ActionVisitor<T> visitor) {
        final String type = action.getType();
        switch (type) {
            case CREATE_PREPARED_STATEMENT_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionCreatePreparedStatementRequest.class));
            case CLOSE_PREPARED_STATEMENT_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionClosePreparedStatementRequest.class));
            case BEGIN_SAVEPOINT_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionBeginSavepointRequest.class));
            case END_SAVEPOINT_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionEndSavepointRequest.class));
            case BEGIN_TRANSACTION_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionBeginTransactionRequest.class));
            case END_TRANSACTION_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionEndTransactionRequest.class));
            case CANCEL_QUERY_ACTION_TYPE:
                // noinspection deprecation
                return visitor.visit(unpack(action.getBody(), ActionCancelQueryRequest.class));
            case CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE:
                return visitor.visit(unpack(action.getBody(), ActionCreatePreparedSubstraitPlanRequest.class));
        }
        // noinspection DataFlowIssue
        throw Assert.statementNeverExecuted();
    }

    private static <T extends com.google.protobuf.Message> T unpack(byte[] body, Class<T> clazz) {
        final Any any = parseActionOrThrow(body);
        return unpackActionOrThrow(any, clazz);
    }

    private static Any parseActionOrThrow(byte[] data) {
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            throw FlightSqlErrorHelper.error(Status.Code.INVALID_ARGUMENT, "Invalid action");
        }
    }

    private static <T extends Message> T unpackActionOrThrow(Any source, Class<T> clazz) {
        try {
            return source.unpack(clazz);
        } catch (final InvalidProtocolBufferException e) {
            throw FlightSqlErrorHelper.error(Status.Code.INVALID_ARGUMENT,
                    "Invalid action, provided message cannot be unpacked as " + clazz.getName(), e);
        }
    }

    public static abstract class ActionVisitorBase<T> implements ActionVisitor<T> {

        public abstract T visitDefault(ActionType actionType, Object action);

        @Override
        public T visit(ActionCreatePreparedStatementRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT, action);
        }

        @Override
        public T visit(ActionClosePreparedStatementRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT, action);
        }

        @Override
        public T visit(ActionBeginSavepointRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT, action);
        }

        @Override
        public T visit(ActionEndSavepointRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT, action);
        }

        @Override
        public T visit(ActionBeginTransactionRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION, action);
        }

        @Override
        public T visit(ActionEndTransactionRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION, action);
        }

        @Override
        public T visit(@SuppressWarnings("deprecation") ActionCancelQueryRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY, action);
        }

        @Override
        public T visit(ActionCreatePreparedSubstraitPlanRequest action) {
            return visitDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN, action);
        }
    }
}
