package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightInfo;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Command(name = "kv-input-table", mixinStandardHelpOptions = true,
        description = "Add to Input Table", version = "0.1.0")
class KeyValueInputTable extends FlightExampleBase {

    public static final ColumnHeader<String> KEY_HEADER = ColumnHeader.ofString("Key");

    public static final ColumnHeader<String> VALUE_HEADER = ColumnHeader.ofString("Value");

    @Option(names = {"-n", "--name"}, description = "The scoped table name, defaults to '${DEFAULT-VALUE}'",
            defaultValue = "kv_table")
    String scopedTableName;

    @Parameters(arity = "1", paramLabel = "KEY", description = "Key.")
    String key;

    @Parameters(arity = "1", paramLabel = "VALUE", description = "Value.")
    String value;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        if (!checkExists(flight)) {
            createKeyBackedInputTable(flight);
        }
        if (value.isEmpty()) {
            deleteFromInputTable(flight);
        } else {
            addToInputTable(flight);
        }
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
        final TableSpec spec = InMemoryKeyBackedInputTable.of(header, Collections.singletonList(KEY_HEADER.name()));
        try (final TableHandle handle = flight.session().execute(spec)) {
            // publicly expose
            flight.session().publish(scopedTableName, handle).get(5, TimeUnit.SECONDS);
        }
    }

    private void addToInputTable(FlightSession flight)
            throws InterruptedException, ExecutionException, TimeoutException {
        final NewTable newRow = KEY_HEADER.header(VALUE_HEADER).row(key, value).newTable();
        flight.addToInputTable(new ScopeId(scopedTableName), newRow, bufferAllocator)
                .get(5, TimeUnit.SECONDS);
    }

    private void deleteFromInputTable(FlightSession flight)
            throws InterruptedException, ExecutionException, TimeoutException {
        final NewTable deleteRow = KEY_HEADER.row(key).newTable();
        flight.deleteFromInputTable(new ScopeId(scopedTableName), deleteRow, bufferAllocator)
                .get(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new KeyValueInputTable()).execute(args);
        System.exit(execute);
    }
}
