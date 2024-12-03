//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

final class FlightSqlSharedConstants {
    static final String FLIGHT_SQL_TYPE_PREFIX = "type.googleapis.com/arrow.flight.protocol.sql.";

    static final String FLIGHT_SQL_COMMAND_TYPE_PREFIX = FLIGHT_SQL_TYPE_PREFIX + "Command";

    static final String COMMAND_GET_XDBC_TYPE_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetXdbcTypeInfo";

    static final String COMMAND_GET_PRIMARY_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetPrimaryKeys";

    static final String COMMAND_GET_IMPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetImportedKeys";

    static final String COMMAND_GET_EXPORTED_KEYS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetExportedKeys";

    static final String COMMAND_GET_CROSS_REFERENCE_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetCrossReference";

    static final String COMMAND_GET_SQL_INFO_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetSqlInfo";

    static final String COMMAND_GET_TABLES_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetTables";

    static final String COMMAND_GET_DB_SCHEMAS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetDbSchemas";

    static final String COMMAND_GET_CATALOGS_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetCatalogs";

    static final String COMMAND_GET_TABLE_TYPES_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "GetTableTypes";

    static final String COMMAND_PREPARED_STATEMENT_UPDATE_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "PreparedStatementUpdate";

    static final String COMMAND_PREPARED_STATEMENT_QUERY_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "PreparedStatementQuery";

    static final String COMMAND_STATEMENT_SUBSTRAIT_PLAN_TYPE_URL =
            FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementSubstraitPlan";

    static final String COMMAND_STATEMENT_UPDATE_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementUpdate";

    static final String COMMAND_STATEMENT_QUERY_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementQuery";

    static final String COMMAND_STATEMENT_INGEST_TYPE_URL = FLIGHT_SQL_COMMAND_TYPE_PREFIX + "StatementIngest";
}
