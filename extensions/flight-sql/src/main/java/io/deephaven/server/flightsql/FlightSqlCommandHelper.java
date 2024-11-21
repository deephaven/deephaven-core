//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.deephaven.base.verify.Assert;
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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;

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

        T visit(CommandStatementIngest command);
    }

    public static boolean handlesCommand(FlightDescriptor descriptor) {
        // If not CMD, there is an error with io.deephaven.server.session.TicketRouter.getPathResolver / handlesPath
        Assert.eq(descriptor.getType(), "descriptor.getType()", FlightDescriptor.DescriptorType.CMD, "CMD");
        // No good way to check if this is a valid command without parsing to Any first.
        final Any command = parseOrNull(descriptor.getCmd());
        return command != null
                && command.getTypeUrl().startsWith(FlightSqlSharedConstants.FLIGHT_SQL_COMMAND_TYPE_PREFIX);
    }

    public static <T> T visit(FlightDescriptor descriptor, CommandVisitor<T> visitor, String logId) {
        // If not CMD, there is an error with io.deephaven.server.session.TicketRouter.getPathResolver / handlesPath
        Assert.eq(descriptor.getType(), "descriptor.getType()", FlightDescriptor.DescriptorType.CMD, "CMD");
        final Any command = parseOrNull(descriptor.getCmd());
        // If null, there is an error with io.deephaven.server.session.TicketRouter.getCommandResolver / handlesCommand
        Assert.neqNull(command, "command");
        final String typeUrl = command.getTypeUrl();
        // If not true, there is an error with io.deephaven.server.session.TicketRouter.getCommandResolver /
        // handlesCommand
        Assert.eqTrue(typeUrl.startsWith(FlightSqlSharedConstants.FLIGHT_SQL_COMMAND_TYPE_PREFIX),
                "typeUrl.startsWith");
        switch (typeUrl) {
            case FlightSqlSharedConstants.COMMAND_STATEMENT_QUERY_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementQuery.class, logId));
            case FlightSqlSharedConstants.COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL:
                return visitor.visit(unpack(command, CommandPreparedStatementQuery.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_TABLES_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetTables.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_TABLE_TYPES_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetTableTypes.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_CATALOGS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetCatalogs.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_DB_SCHEMAS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetDbSchemas.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_PRIMARY_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetPrimaryKeys.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_IMPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetImportedKeys.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_EXPORTED_KEYS_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetExportedKeys.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_SQL_INFO_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetSqlInfo.class, logId));
            case FlightSqlSharedConstants.COMMAND_STATEMENT_UPDATE_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementUpdate.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_CROSS_REFERENCE_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetCrossReference.class, logId));
            case FlightSqlSharedConstants.COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementSubstraitPlan.class, logId));
            case FlightSqlSharedConstants.COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL:
                return visitor.visit(unpack(command, CommandPreparedStatementUpdate.class, logId));
            case FlightSqlSharedConstants.COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL:
                return visitor.visit(unpack(command, CommandGetXdbcTypeInfo.class, logId));
            case FlightSqlSharedConstants.COMMAND_STATEMENT_INGEST_TYPE_URL:
                return visitor.visit(unpack(command, CommandStatementIngest.class, logId));
        }
        throw FlightSqlErrorHelper.error(Status.Code.UNIMPLEMENTED, String.format("command '%s' is unknown", typeUrl));
    }

    private static Any parseOrNull(ByteString data) {
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            return null;
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

    public static abstract class CommandVisitorBase<T> implements CommandVisitor<T> {
        public abstract T visitDefault(Descriptor descriptor, Object command);

        @Override
        public T visit(CommandGetCatalogs command) {
            return visitDefault(CommandGetCatalogs.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetDbSchemas command) {
            return visitDefault(CommandGetDbSchemas.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetTableTypes command) {
            return visitDefault(CommandGetTableTypes.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetImportedKeys command) {
            return visitDefault(CommandGetImportedKeys.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetExportedKeys command) {
            return visitDefault(CommandGetExportedKeys.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetPrimaryKeys command) {
            return visitDefault(CommandGetPrimaryKeys.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetTables command) {
            return visitDefault(CommandGetTables.getDescriptor(), command);
        }

        @Override
        public T visit(CommandStatementQuery command) {
            return visitDefault(CommandStatementQuery.getDescriptor(), command);
        }

        @Override
        public T visit(CommandPreparedStatementQuery command) {
            return visitDefault(CommandPreparedStatementQuery.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetSqlInfo command) {
            return visitDefault(CommandGetSqlInfo.getDescriptor(), command);
        }

        @Override
        public T visit(CommandStatementUpdate command) {
            return visitDefault(CommandStatementUpdate.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetCrossReference command) {
            return visitDefault(CommandGetCrossReference.getDescriptor(), command);
        }

        @Override
        public T visit(CommandStatementSubstraitPlan command) {
            return visitDefault(CommandStatementSubstraitPlan.getDescriptor(), command);
        }

        @Override
        public T visit(CommandPreparedStatementUpdate command) {
            return visitDefault(CommandPreparedStatementUpdate.getDescriptor(), command);
        }

        @Override
        public T visit(CommandGetXdbcTypeInfo command) {
            return visitDefault(CommandGetXdbcTypeInfo.getDescriptor(), command);
        }

        @Override
        public T visit(CommandStatementIngest command) {
            return visitDefault(CommandStatementIngest.getDescriptor(), command);
        }
    }
}
