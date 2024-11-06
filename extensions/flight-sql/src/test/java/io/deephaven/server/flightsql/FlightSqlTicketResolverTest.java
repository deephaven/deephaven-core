//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetCatalogsConstants;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetDbSchemasConstants;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetKeysConstants;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetPrimaryKeysConstants;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetTableTypesConstants;
import io.deephaven.server.flightsql.FlightSqlResolver.CommandGetTablesConstants;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.FlightSqlUtils;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FlightSqlTicketResolverTest {
    @Test
    public void actionTypes() {
        checkActionType(FlightSqlActionHelper.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT);
        checkActionType(FlightSqlActionHelper.CLOSE_PREPARED_STATEMENT_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT);
        checkActionType(FlightSqlActionHelper.BEGIN_SAVEPOINT_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT);
        checkActionType(FlightSqlActionHelper.END_SAVEPOINT_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT);
        checkActionType(FlightSqlActionHelper.BEGIN_TRANSACTION_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION);
        checkActionType(FlightSqlActionHelper.END_TRANSACTION_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION);
        checkActionType(FlightSqlActionHelper.CANCEL_QUERY_ACTION_TYPE, FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY);
        checkActionType(FlightSqlActionHelper.CREATE_PREPARED_SUBSTRAIT_PLAN_ACTION_TYPE,
                FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN);
    }

    @Test
    public void packedTypeUrls() {
        checkPackedType(FlightSqlSharedConstants.COMMAND_STATEMENT_QUERY_TYPE_URL,
                CommandStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_STATEMENT_UPDATE_TYPE_URL,
                CommandStatementUpdate.getDefaultInstance());
        // Need to update to newer FlightSql version for this
        // checkPackedType(FlightSqlTicketResolver.COMMAND_STATEMENT_INGEST_TYPE_URL,
        // CommandStatementIngest.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL,
                CommandStatementSubstraitPlan.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL,
                CommandPreparedStatementQuery.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL,
                CommandPreparedStatementUpdate.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_TABLE_TYPES_TYPE_URL,
                CommandGetTableTypes.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_CATALOGS_TYPE_URL,
                CommandGetCatalogs.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_DB_SCHEMAS_TYPE_URL,
                CommandGetDbSchemas.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_TABLES_TYPE_URL,
                CommandGetTables.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_SQL_INFO_TYPE_URL,
                CommandGetSqlInfo.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_CROSS_REFERENCE_TYPE_URL,
                CommandGetCrossReference.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_EXPORTED_KEYS_TYPE_URL,
                CommandGetExportedKeys.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_IMPORTED_KEYS_TYPE_URL,
                CommandGetImportedKeys.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_PRIMARY_KEYS_TYPE_URL,
                CommandGetPrimaryKeys.getDefaultInstance());
        checkPackedType(FlightSqlSharedConstants.COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL,
                CommandGetXdbcTypeInfo.getDefaultInstance());
        checkPackedType(FlightSqlTicketHelper.TICKET_STATEMENT_QUERY_TYPE_URL,
                TicketStatementQuery.getDefaultInstance());
    }

    @Test
    void getTableTypesSchema() {
        isSimilar(CommandGetTableTypesConstants.DEFINITION, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Test
    void getCatalogsSchema() {
        isSimilar(CommandGetCatalogsConstants.DEFINITION, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Test
    void getDbSchemasSchema() {
        isSimilar(CommandGetDbSchemasConstants.DEFINITION, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Disabled("Deephaven is unable to serialize byte as uint8")
    @Test
    void getImportedKeysSchema() {
        isSimilar(CommandGetKeysConstants.DEFINITION, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Disabled("Deephaven is unable to serialize byte as uint8")
    @Test
    void getExportedKeysSchema() {
        isSimilar(CommandGetKeysConstants.DEFINITION, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Disabled("Arrow Java Flight SQL has a bug in ordering, not the same as documented in the protobuf spec, see https://github.com/apache/arrow/issues/44521")
    @Test
    void getPrimaryKeysSchema() {
        isSimilar(CommandGetPrimaryKeysConstants.DEFINITION, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Test
    void getTablesSchema() {
        isSimilar(CommandGetTablesConstants.DEFINITION, Schemas.GET_TABLES_SCHEMA);
        isSimilar(CommandGetTablesConstants.DEFINITION_NO_SCHEMA, Schemas.GET_TABLES_SCHEMA_NO_SCHEMA);
    }

    private static void checkActionType(String actionType, ActionType expected) {
        assertThat(actionType).isEqualTo(expected.getType());
    }

    private static void checkPackedType(String typeUrl, Message expected) {
        assertThat(typeUrl).isEqualTo(Any.pack(expected).getTypeUrl());
    }

    private static void isSimilar(TableDefinition definition, Schema expected) {
        isSimilar(BarrageUtil.toSchema(definition, Map.of(), true), expected);
    }

    private static void isSimilar(Schema actual, Schema expected) {
        assertThat(actual.getFields()).hasSameSizeAs(expected.getFields());
        int L = actual.getFields().size();
        for (int i = 0; i < L; ++i) {
            isSimilar(actual.getFields().get(i), expected.getFields().get(i));
        }
    }

    private static void isSimilar(Field actual, Field expected) {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getChildren()).isEqualTo(expected.getChildren());
        isSimilar(actual.getFieldType(), expected.getFieldType());
    }

    private static void isSimilar(FieldType actual, FieldType expected) {
        assertThat(actual.getType()).isEqualTo(expected.getType());
        assertThat(actual.getDictionary()).isEqualTo(expected.getDictionary());
    }
}
