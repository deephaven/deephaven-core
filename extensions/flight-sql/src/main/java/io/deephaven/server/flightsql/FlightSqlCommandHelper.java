//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
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

import java.util.Optional;

import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_CATALOGS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_CROSS_REFERENCE_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_DB_SCHEMAS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_EXPORTED_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_IMPORTED_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_PRIMARY_KEYS_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_SQL_INFO_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_TABLES_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_TABLE_TYPES_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_STATEMENT_QUERY_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.COMMAND_STATEMENT_UPDATE_TYPE_URL;
import static io.deephaven.server.flightsql.FlightSqlResolver.FLIGHT_SQL_COMMAND_TYPE_PREFIX;

final class FlightSqlCommandHelper {

    interface CommandVisitor<T> {

        T visit(CommandGetCatalogs command);

        T visit(CommandGetDbSchemas command);

        T visit(CommandGetTableTypes command);

        T visit(CommandGetImportedKeys command);

        T visit(CommandGetExportedKeys command);

        T visit(CommandGetPrimaryKeys command);

        T visit(CommandGetTables command);

        T visit(CommandStatementQuery command);

        T visit(CommandPreparedStatementQuery command);

        T visit(CommandGetSqlInfo command);

        T visit(CommandStatementUpdate command);

        T visit(CommandGetCrossReference command);

        T visit(CommandStatementSubstraitPlan command);

        T visit(CommandPreparedStatementUpdate command);

        T visit(CommandGetXdbcTypeInfo command);
    }

    public static boolean handlesCommand(FlightDescriptor descriptor) {
        if (descriptor.getType() != FlightDescriptor.DescriptorType.CMD) {
            throw new IllegalStateException("descriptor is not a command");
        }
        // No good way to check if this is a valid command without parsing to Any first.
        final Any command = parse(descriptor.getCmd()).orElse(null);
        return command != null && command.getTypeUrl().startsWith(FLIGHT_SQL_COMMAND_TYPE_PREFIX);
    }

    public static <T> T visit(FlightDescriptor descriptor, CommandVisitor<T> visitor, String logId) {
        if (descriptor.getType() != FlightDescriptor.DescriptorType.CMD) {
            // If we get here, there is an error with io.deephaven.server.session.TicketRouter.getPathResolver /
            // handlesPath
            throw new IllegalStateException("Flight SQL only supports Command-based descriptors");
        }
        final Any command = parse(descriptor.getCmd()).orElse(null);
        if (command == null) {
            // If we get here, there is an error with io.deephaven.server.session.TicketRouter.getCommandResolver /
            // handlesCommand
            throw new IllegalStateException("Received invalid message from remote.");
        }
        final String typeUrl = command.getTypeUrl();
        if (!typeUrl.startsWith(FLIGHT_SQL_COMMAND_TYPE_PREFIX)) {
            // If we get here, there is an error with io.deephaven.server.session.TicketRouter.getCommandResolver /
            // handlesCommand
            throw new IllegalStateException(String.format("Unexpected command typeUrl '%s'", typeUrl));
        }
        switch (typeUrl) {
            case COMMAND_STATEMENT_QUERY_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementQuery.class, logId));
            case COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                return visitor.visit(unpack(command, CommandPreparedStatementQuery.class, logId));
            case COMMAND_GET_TABLES_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetTables.class, logId));
            case COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetTableTypes.class, logId));
            case COMMAND_GET_CATALOGS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetCatalogs.class, logId));
            case COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetDbSchemas.class, logId));
            case COMMAND_GET_PRIMARY_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetPrimaryKeys.class, logId));
            case COMMAND_GET_IMPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetImportedKeys.class, logId));
            case COMMAND_GET_EXPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetExportedKeys.class, logId));
            case COMMAND_GET_SQL_INFO_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetSqlInfo.class, logId));
            case COMMAND_STATEMENT_UPDATE_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementUpdate.class, logId));
            case COMMAND_GET_CROSS_REFERENCE_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetCrossReference.class, logId));
            case COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementSubstraitPlan.class, logId));
            case COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL:
                return visitor.visit(unpack(command, CommandPreparedStatementUpdate.class, logId));
            case COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetXdbcTypeInfo.class, logId));
        }
        throw FlightSqlErrorHelper.error(Status.Code.UNIMPLEMENTED, String.format("command '%s' is unknown", typeUrl));
    }

    private static Optional<Any> parse(ByteString data) {
        try {
            return Optional.of(Any.parseFrom(data));
        } catch (final InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    private static <T extends Message> T unpack(Any command, Class<T> clazz, String logId) {
        try {
            return command.unpack(clazz);
        } catch (InvalidProtocolBufferException e) {
            throw FlightSqlErrorHelper.error(Status.Code.FAILED_PRECONDITION, String
                    .format("Invalid command, provided message cannot be unpacked as %s, %s", clazz.getName(), logId));
        }
    }
}
