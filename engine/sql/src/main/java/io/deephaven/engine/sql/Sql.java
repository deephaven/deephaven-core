//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.sql;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutput.ObjFormatter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.Graphviz;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableHeader.Builder;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.type.Type;
import io.deephaven.sql.Scope;
import io.deephaven.sql.ScopeStaticImpl;
import io.deephaven.sql.SqlAdapter;
import io.deephaven.sql.TableInformation;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.ScriptApi;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

/**
 * Experimental SQL execution. Subject to change.
 */
public final class Sql {
    private static final Logger log = LoggerFactory.getLogger(Sql.class);

    @ScriptApi
    public static Table evaluate(String sql) {
        return evaluate(sql, currentScriptSessionNamedTables());
    }

    @ScriptApi
    public static TableSpec dryRun(String sql) {
        return dryRun(sql, currentScriptSessionNamedTables());
    }

    @InternalUseOnly
    public static TableSpec parseSql(String sql, Map<String, Table> scope, Function<String, TicketTable> ticketFunction,
            Map<TicketTable, Table> out) {
        return SqlAdapter.parseSql(sql, scope(scope, out, ticketFunction));
    }

    private static Table evaluate(String sql, Map<String, Table> scope) {
        final Map<TicketTable, Table> map = new HashMap<>(scope.size());
        final TableSpec tableSpec = parseSql(sql, scope, Sql::sqlref, map);
        log.debug().append("Executing. Graphviz representation:").nl().append(ToGraphvizDot.INSTANCE, tableSpec).endl();
        return tableSpec.logic().create(new TableCreatorTicketInterceptor(TableCreatorImpl.INSTANCE, map));
    }

    private static TableSpec dryRun(String sql, Map<String, Table> scope) {
        final TableSpec tableSpec = parseSql(sql, scope, Sql::sqlref, null);
        log.info().append("Dry run. Graphviz representation:").nl().append(ToGraphvizDot.INSTANCE, tableSpec).endl();
        return tableSpec;
    }

    private static TicketTable sqlref(String tableName) {
        // The TicketTable can technically be anything unique (incrementing number, random, ...), but for
        // visualization purposes it makes sense to use the (already unique) table name.
        return TicketTable.of(("sqlref/" + tableName).getBytes(StandardCharsets.UTF_8));
    }

    private static Scope scope(Map<String, Table> scope, Map<TicketTable, Table> out,
            Function<String, TicketTable> ticketFunction) {
        final ScopeStaticImpl.Builder builder = ScopeStaticImpl.builder();
        for (Entry<String, Table> e : scope.entrySet()) {
            final String tableName = e.getKey();
            final Table table = e.getValue();
            final TicketTable spec = ticketFunction.apply(tableName);
            final List<String> qualifiedName = List.of(tableName);
            final TableHeader header = adapt(table.getDefinition());
            builder.addTables(TableInformation.of(qualifiedName, header, spec));
            if (out != null) {
                out.put(spec, table);
            }
        }
        return builder.build();
    }

    private static Map<String, Table> currentScriptSessionNamedTables() {
        // getVariables() is inefficient
        // See SQLTODO(catalog-reader-implementation)
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        // noinspection unchecked,rawtypes
        return (Map<String, Table>) (Map) queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table);
    }

    private static TableHeader adapt(TableDefinition tableDef) {
        final Builder builder = TableHeader.builder();
        for (ColumnDefinition<?> cd : tableDef.getColumns()) {
            builder.addHeaders(adapt(cd));
        }
        return builder.build();
    }

    private static ColumnHeader<?> adapt(ColumnDefinition<?> columnDef) {
        return ColumnHeader.of(columnDef.getName(), Type.find(columnDef.getDataType()));
    }

    private enum ToGraphvizDot implements ObjFormatter<TableSpec> {
        INSTANCE;

        @Override
        public void format(LogOutput logOutput, TableSpec tableSpec) {
            logOutput.append(Graphviz.toDot(tableSpec));
        }
    }
}
