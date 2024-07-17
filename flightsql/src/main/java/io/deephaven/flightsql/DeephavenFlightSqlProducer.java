//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.flightsql;

import com.google.protobuf.Any;
import io.deephaven.engine.sql.Sql;
import io.deephaven.engine.table.Table;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;

public class DeephavenFlightSqlProducer {

    public static Table processCommand(final Flight.FlightDescriptor descriptor, final String logId) {
        final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCmd().toByteArray());
        if (command.is(FlightSql.CommandStatementQuery.class)) {
            FlightSql.CommandStatementQuery request =
                    FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandStatementQuery.class);
            final String query = request.getQuery();

            Table table;
            try {
                table = Sql.evaluate(query);
                return table;
            } catch (Exception e) {
                throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Sql statement: " + query + "\nCaused By: " + e.toString())
                        .toRuntimeException();
            }

        } else if (command.is(FlightSql.CommandStatementSubstraitPlan.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetCatalogs.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetDbSchemas.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetTables.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetTableTypes.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetSqlInfo.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetPrimaryKeys.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetExportedKeys.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetImportedKeys.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetCrossReference.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        } else if (command.is(FlightSql.CommandGetXdbcTypeInfo.class)) {
            throw CallStatus.UNIMPLEMENTED
                    .withDescription("Substrait plan is not implemented")
                    .toRuntimeException();
        }

        throw CallStatus.INVALID_ARGUMENT
                .withDescription("Unrecognized request: " + command.getTypeUrl())
                .toRuntimeException();
    }
}
