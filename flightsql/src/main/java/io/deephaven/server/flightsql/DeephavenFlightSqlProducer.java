//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import com.google.protobuf.ProtocolStringList;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.sql.Sql;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.qst.column.Column;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DeephavenFlightSqlProducer {

    public static Table processCommand(final Flight.FlightDescriptor descriptor, final String logId) {
        final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCmd().toByteArray());
        if (command.is(FlightSql.CommandStatementQuery.class)) {
            return getTableCommandStatementQuery(command);
        } else if (command.is(FlightSql.CommandStatementSubstraitPlan.class)) {
            throwUnimplemented("Substrait plan");
        } else if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
            throwUnimplemented("Prepared Statement");
        } else if (command.is(FlightSql.CommandGetCatalogs.class)) {
            return getTableCommandGetCatalogs();
        } else if (command.is(FlightSql.CommandGetDbSchemas.class)) {
            return getTableCommandGetDbSchema();
        } else if (command.is(FlightSql.CommandGetTables.class)) {
            return getTableCommandGetTables(command);
        } else if (command.is(FlightSql.CommandGetTableTypes.class)) {
            return TableFactory.newTable(Column.of("table_type", String.class, "TABLE"));
        } else if (command.is(FlightSql.CommandGetSqlInfo.class)) {
            throwUnimplemented("SQL Info");
        } else if (command.is(FlightSql.CommandGetPrimaryKeys.class)) {
            // TODO: Implement this, return empty table?
            throwUnimplemented("Primary Keys");
        } else if (command.is(FlightSql.CommandGetExportedKeys.class)) {
            // TODO: Implement this, return empty table?
            throwUnimplemented("Exported Keys");
        } else if (command.is(FlightSql.CommandGetImportedKeys.class)) {
            // TODO: Implement this, return empty table?
            throwUnimplemented("Imported Keys");
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

    private static void throwUnimplemented(String feature) {
        throw CallStatus.UNIMPLEMENTED
                .withDescription(feature + " is not implemented")
                .toRuntimeException();
    }

    private static Table getTableCommandGetTables(Any command) {
        FlightSql.CommandGetTables request = FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandGetTables.class);
        final String catalog = request.hasCatalog() ? request.getCatalog() : null;
        final String schemaFilterPattern =
                request.hasDbSchemaFilterPattern() ? request.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                request.hasTableNameFilterPattern() ? request.getTableNameFilterPattern() : null;
        final ProtocolStringList protocolStringList = request.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);


        if (catalog != null) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Catalog is not supported")
                    .toRuntimeException();
        } else if (schemaFilterPattern != null) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("DbSchema is not supported")
                    .toRuntimeException();
        } else if (tableFilterPattern != null) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Table name filter is not supported")
                    .toRuntimeException();
        } else if (tableTypes != null && !Arrays.equals(tableTypes, new String[] {"TABLE"})) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Table types are not supported")
                    .toRuntimeException();
        }

        Schema schemaToUse = FlightSqlProducer.Schemas.GET_TABLES_SCHEMA;
        boolean includeSchema = request.getIncludeSchema();
        if (!includeSchema) {
            schemaToUse = FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        List<String> tableNames = new ArrayList<>();
        List<byte[]> tableSchemaBytes = new ArrayList<>();

        queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table).forEach((name, table) -> {
            tableNames.add(name);
            if (includeSchema) {
                tableSchemaBytes.add(BarrageUtil
                        .schemaBytesFromTableDefinition(((Table) table).getDefinition(), Collections.emptyMap(), false)
                        .toByteArray());
            }
        });
        int rowCount = tableNames.size();
        String[] nullStringArray = new String[rowCount];
        Arrays.fill(nullStringArray, null);
        String[] tableTypeArray = new String[rowCount];
        Arrays.fill(tableTypeArray, "TABLE");
        Column catalogColumn = Column.of("catalog_name", String.class, nullStringArray);
        Column dbSchemaColumn = Column.of("db_schema_name", String.class, nullStringArray);
        Column tableNameColumn = Column.of("table_name", String.class, tableNames.toArray(new String[0]));
        Column tableTypeColumn = Column.of("table_type", String.class, tableTypeArray);
        if (request.getIncludeSchema()) {
            Column tableSchemaColumn = Column.of("table_schema", byte[].class, tableSchemaBytes.toArray(new byte[0][]));
            return TableFactory.newTable(catalogColumn, dbSchemaColumn, tableNameColumn, tableTypeColumn,
                    tableSchemaColumn);
        }
        return TableFactory.newTable(catalogColumn, dbSchemaColumn, tableNameColumn, tableTypeColumn);
    }

    @NotNull
    private static Table getTableCommandGetDbSchema() {
        final BarrageUtil.ConvertedArrowSchema result =
                BarrageUtil.convertArrowSchema(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
        final Table resultTable = BarrageTable.make(null, result.tableDef, result.attributes, null);
        return resultTable;
    }

    @NotNull
    private static Table getTableCommandGetCatalogs() {
        final BarrageUtil.ConvertedArrowSchema result =
                BarrageUtil.convertArrowSchema(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA);
        final Table resultTable = BarrageTable.make(null, result.tableDef, result.attributes, null);
        return resultTable;
    }

    private static Table getTableCommandStatementQuery(Any command) {
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
    }
}
