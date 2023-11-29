/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.api.filter.Filter;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;

@Command(name = "filter-table", mixinStandardHelpOptions = true,
        description = "Filter table using raw strings", version = "0.1.0")
class FilterTable extends SingleSessionExampleBase {

    enum Type {
        AND, OR
    }

    @Option(names = {"--filter-type"},
            description = "The filter type, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "AND")
    Type type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Parameters(arity = "1+", paramLabel = "FILTER", description = "Raw-string filters")
    List<String> filters;

    @Override
    protected void execute(Session session) throws Exception {
        final Filter filter = type == Type.AND ? Filter.and(Filter.from(filters)) : Filter.or(Filter.from(filters));
        final TableSpec filtered = ticket.ticketId().table().where(filter);
        try (final TableHandle handle = session.executeAsync(filtered).getOrCancel()) {
            session.publish("filter_table_results", handle).get();
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new FilterTable()).execute(args);
        System.exit(execute);
    }
}
