//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Upserts a key and value into a key-backed input table on the server, creating the table on first use. An empty value
 * deletes the key instead.
 */
@Command(name = "kv-input-table", mixinStandardHelpOptions = true,
        description = "Add to Input Table", version = "0.1.0")
class KeyValueInputTable implements Callable<Void> {

    public static final ColumnHeader<String> KEY_HEADER = ColumnHeader.ofString("Key");

    public static final ColumnHeader<String> VALUE_HEADER = ColumnHeader.ofString("Value");

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-n", "--name"}, description = "The scoped table name, defaults to '${DEFAULT-VALUE}'",
            defaultValue = "kv_table")
    String scopedTableName;

    @Parameters(arity = "1", paramLabel = "KEY", description = "Key.")
    String key;

    @Parameters(arity = "1", paramLabel = "VALUE", description = "Value.")
    String value;

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read and written over Flight
        final BufferAllocator allocator = new RootAllocator();
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data)
        try (final FlightSession flight = factory.newFlightSession()) {
            if (!checkExists(flight)) {
                createKeyBackedInputTable(flight);
            }
            if (value.isEmpty()) {
                deleteFromInputTable(flight, allocator);
            } else {
                addToInputTable(flight, allocator);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private boolean checkExists(FlightSession flight) {
        List<String> expected = Arrays.asList("scope", scopedTableName);
        boolean exists = false;
        for (FlightInfo flightInfo : flight.list()) {
            if (expected.equals(flightInfo.getDescriptor().getPath())) {
                exists = true;
                break;
            }
        }
        return exists;
    }

    private void createKeyBackedInputTable(FlightSession flight)
            throws InterruptedException, ExecutionException, TimeoutException, TableHandleException {
        final TableHeader header = TableHeader.of(KEY_HEADER, VALUE_HEADER);
        // A key-backed input table upserts rows by key; it is published so later runs find it by name
        final TableSpec spec = InMemoryKeyBackedInputTable.of(header, Collections.singletonList(KEY_HEADER.name()));
        try (final TableHandle handle = flight.session().execute(spec)) {
            // publicly expose
            flight.session().publish(scopedTableName, handle).get(5, TimeUnit.SECONDS);
        }
    }

    private void addToInputTable(FlightSession flight, BufferAllocator allocator)
            throws InterruptedException, ExecutionException, TimeoutException {
        final NewTable newRow = KEY_HEADER.header(VALUE_HEADER).row(key, value).newTable();
        flight.addToInputTable(new ScopeId(scopedTableName), newRow, allocator)
                .get(5, TimeUnit.SECONDS);
    }

    private void deleteFromInputTable(FlightSession flight, BufferAllocator allocator)
            throws InterruptedException, ExecutionException, TimeoutException {
        final NewTable deleteRow = KEY_HEADER.row(key).newTable();
        flight.deleteFromInputTable(new ScopeId(scopedTableName), deleteRow, allocator)
                .get(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new KeyValueInputTable()).execute(args));
    }
}
