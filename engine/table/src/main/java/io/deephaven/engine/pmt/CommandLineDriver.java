package io.deephaven.engine.pmt;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.impl.BaseUpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Toy that reads data from the stdin and applies it to a table.
 * <p>
 * After each update cycle, the table is validated and the update is printed.
 */
public class CommandLineDriver {
    boolean inBundle = false;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new CommandLineDriver().start();
    }

    private CommandLineDriver() {
    }

    private void start() throws ExecutionException, InterruptedException {
        final Scanner scanner = new Scanner(System.in);

        final TableDefinition definition = TableDefinition.from(
                Arrays.asList("Sentinel", "Value"),
                Arrays.asList(int.class, String.class)
        );

        final MutableObject<ArrayBackedPositionalMutableTable> tableHolder = new MutableObject<>();
        final MutableObject<TableUpdateListener> listenerHolder = new MutableObject<>();
        final MutableObject<TableUpdateValidator> validatorHolder = new MutableObject<>();
        final MutableObject<TableUpdateListener> validatorListenerHolder = new MutableObject<>();
        AsyncClientErrorNotifier.setReporter(t -> {
            t.printStackTrace(System.err);
            System.exit(1);
        });

        final PeriodicUpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder(BaseUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME).build();
        final ExecutionContext executionContext;
        try {
            executionContext = ExecutionContext.newBuilder()
                    .markSystemic()
                    .newQueryLibrary()
                    .newQueryScope()
                    .setQueryCompiler(QueryCompilerImpl.create(
                            Files.createTempDirectory("CommandLineDriver").resolve("cache").toFile(),
                            ClassLoader.getSystemClassLoader()
                    ))
                    .setOperationInitializer(OperationInitializer.NON_PARALLELIZABLE)
                    .setUpdateGraph(updateGraph)
                    .build();
        } catch (IOException ex) {
            throw new RuntimeException("Could not create query copmiler", ex);
        }

        try (final SafeCloseable contextCloseable = executionContext.open()) {

            updateGraph.sharedLock().doLocked(() -> {
                final ArrayBackedPositionalMutableTable arrayBackedPositionalMutableTable = new ArrayBackedPositionalMutableTable(definition);
                tableHolder.setValue(arrayBackedPositionalMutableTable);

                final TableUpdateListener listener = new InstrumentedTableUpdateListenerAdapter(arrayBackedPositionalMutableTable, true) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        System.out.println("Update: " + upstream);
                    }
                };
                arrayBackedPositionalMutableTable.addUpdateListener(listener);
                listenerHolder.setValue(listener);

                final TableUpdateValidator tuv = TableUpdateValidator.make(arrayBackedPositionalMutableTable);
                validatorHolder.setValue(tuv);
                final QueryTable validatorResult = tuv.getResultTable();
                final InstrumentedTableUpdateListenerAdapter validatorListener = new InstrumentedTableUpdateListenerAdapter("Validator Result Listener", validatorResult, false) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        // uninteresting, we just want the failure behavior
                    }
                };
                validatorResult.addUpdateListener(validatorListener);
                validatorListenerHolder.setValue(validatorListener);
            });

            final ArrayBackedPositionalMutableTable arrayBackedPositionalMutableTable = tableHolder.getValue();

            help(System.out);

            LOOP:
            while (true) {
                final String in = scanner.nextLine();
                final String[] splits = in.split("\\s+");
                final String command = splits[0];
                switch (command) {
                    case "":
                        break;
                    case "quit":
                    case "exit":
                        break LOOP;
                    case "start":
                        doStart(arrayBackedPositionalMutableTable);
                        break;
                    case "end":
                        doEnd(arrayBackedPositionalMutableTable).get();
                        TableTools.showWithRowSet(arrayBackedPositionalMutableTable);
                        break;
                    case "endasync":
                        doEnd(arrayBackedPositionalMutableTable);
                        break;
                    case "add":
                        doAdd(splits, arrayBackedPositionalMutableTable);
                        break;
                    case "delete":
                        doDelete(splits, arrayBackedPositionalMutableTable);
                        break;
                    case "set":
                        doSet(splits, in, arrayBackedPositionalMutableTable);
                        break;
                    case "show":
                        TableTools.showWithRowSet(arrayBackedPositionalMutableTable);
                        break;
                    default:
                        System.err.println("Unknown: " + command);
                        help(System.err);
                }
            }
        }
    }

    private void help(PrintStream stream) {
        stream.println("quit");
        stream.println("\texit the driver");
        stream.println("start");
        stream.println("\tstart a bundle, done automatically for add, delete, and set\n");
        stream.println("end");
        stream.println("\tend a bundle, wait for the UpdateGraph to process it, then print out the new table\n");
        stream.println("endasync");
        stream.println("\tend a bundle, but don't wait");
        stream.println("add row count");
        stream.println("\tadd a blank segment of rows");
        stream.println("delete row count");
        stream.println("\tdelete a segment of rows");
        stream.println("set row column value");
        stream.println("\tset an individual cell");
        stream.println("show");
        stream.println("\tshow the result table");
        stream.println();
    }

    private void doStart(ArrayBackedPositionalMutableTable table) {
        inBundle = true;
        table.startBundle();
    }

    private Future<Void> doEnd(ArrayBackedPositionalMutableTable table) {
        if (inBundle) {
            inBundle = false;
            return table.endBundle();
        } else {
            return new ImmediateVoidFuture();
        }
    }

    private void doAdd(String[] splits, ArrayBackedPositionalMutableTable table) {
        if (splits.length < 3) {
            System.err.println("add start count");
            return;
        }
        if (!table.inBundle) {
            doStart(table);
        }
        final long position = Long.parseLong(splits[1]);
        final long count = Long.parseLong(splits[2]);
        // should we have a Future or something else as a return?
        table.addRow(position, count);
    }

    private void doDelete(String[] splits, ArrayBackedPositionalMutableTable table) {
        if (splits.length < 3) {
            System.err.println("delete start count");
            return;
        }
        if (!table.inBundle) {
            doStart(table);
        }
        final long position = Long.parseLong(splits[1]);
        final long count = Long.parseLong(splits[2]);
        // should we have a Future or something else as a return?
        table.deleteRow(position, count);
    }

    private void doSet(String[] splits, String in, ArrayBackedPositionalMutableTable table) {
        if (!table.inBundle) {
            doStart(table);
        }
        if (splits.length < 4) {
            System.err.println("set row column value");
            return;
        }
        final long row = Long.parseLong(splits[1]);
        final int column = Integer.parseInt(splits[2]);
        final String value = in.split("\\s+", 4)[3];
        final Object objectValue = parseObjectValue(table, column, value);
        table.setCell(row, column, objectValue);
    }

    private Object parseObjectValue(ArrayBackedPositionalMutableTable table, int column, String value) {
        final Object objectValue;
        final Class<?> columnType = table.getDefinition().getColumnTypes().get(column);
        if (int.class.equals(columnType)) {
            objectValue = Integer.parseInt(value);
        } else if (short.class.equals(columnType)) {
            objectValue = Short.parseShort(value);
        } else if (long.class.equals(columnType)) {
            objectValue = Long.parseLong(value);
        } else if (byte.class.equals(columnType)) {
            objectValue = Byte.parseByte(value);
        } else if (char.class.equals(columnType)) {
            objectValue = value.charAt(0);
        } else if (float.class.equals(columnType)) {
            objectValue = Float.parseFloat(value);
        } else if (double.class.equals(columnType)) {
            objectValue = Double.parseDouble(value);
        } else if (String.class.equals(columnType)) {
            objectValue = value;
        } else {
            throw new UnsupportedOperationException("Unknown type: " + columnType);
        }
        return objectValue;
    }

    private static class ImmediateVoidFuture implements Future<Void> {

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }
    }
}
