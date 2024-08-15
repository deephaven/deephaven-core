//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlightSqlTicketResolverTest {
    @Test
    public void actionTypes() {
        checkActionType(FlightSqlTicketResolver.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        checkActionType(FlightSqlTicketResolver.CLOSE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
    }

    @Test
    public void commandTypeUrls() {
        checkPackedType(FlightSqlTicketResolver.COMMAND_STATEMENT_QUERY_TYPE_URL,
                CommandStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlTicketResolver.COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL,
                CommandPreparedStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlTicketResolver.COMMAND_GET_TABLE_TYPES_TYPE_URL,
                CommandGetTableTypes.getDefaultInstance());
        checkPackedType(FlightSqlTicketResolver.COMMAND_GET_CATALOGS_TYPE_URL,
                CommandGetCatalogs.getDefaultInstance());
        checkPackedType(FlightSqlTicketResolver.COMMAND_GET_DB_SCHEMAS_TYPE_URL,
                CommandGetDbSchemas.getDefaultInstance());
        checkPackedType(FlightSqlTicketResolver.COMMAND_GET_TABLES_TYPE_URL,
                CommandGetTables.getDefaultInstance());
    }

    @Test
    void definitions() {
        checkDefinition(FlightSqlTicketResolver.GET_TABLE_TYPES_DEFINITION, Schemas.GET_TABLE_TYPES_SCHEMA);
        checkDefinition(FlightSqlTicketResolver.GET_CATALOGS_DEFINITION, Schemas.GET_CATALOGS_SCHEMA);
        checkDefinition(FlightSqlTicketResolver.GET_DB_SCHEMAS_DEFINITION, Schemas.GET_SCHEMAS_SCHEMA);
        // TODO: we can't use the straight schema b/c it's BINARY not byte[], and we don't know how to natively map
        // checkDefinition(FlightSqlTicketResolver.GET_TABLES_DEFINITION, Schemas.GET_TABLES_SCHEMA);
    }

    private static void checkActionType(String actionType, ActionType expected) {
        assertThat(actionType).isEqualTo(expected.getType());
    }

    private static void checkPackedType(String typeUrl, Message expected) {
        assertThat(typeUrl).isEqualTo(Any.pack(expected).getTypeUrl());
    }

    private static void checkDefinition(TableDefinition definition, Schema expected) {
        assertThat(definition).isEqualTo(BarrageUtil.convertArrowSchema(expected).tableDef);
    }
}
