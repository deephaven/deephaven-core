package io.deephaven.client.examples;

import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.ExportRequest;
import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.client.impl.ExportsRequest;
import io.deephaven.client.impl.ExportsRequest.Builder;
import io.deephaven.client.impl.Session;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Command(name = "publish-table", mixinStandardHelpOptions = true,
        description = "Publish a QST as a globally scoped variable", version = "0.1.0")
class PublishTables extends SingleSessionExampleBase {

    @Parameters(arity = "1..*", paramLabel = "QST",
            description = "QST file(s) to publish. May be in the form <PATH> or <KEY>=<PATH>.",
            converter = LabeledTableConverter.class)
    LabeledTable[] tables;

    @Override
    protected void execute(Session session) throws Exception {
        LabeledTables labeledTables = LabeledTables.of(tables);

        // note: if we export the same table multiple times, that's OK - we just get an increase
        // ref count

        Listener listener = Listener.logging();
        Builder builder = ExportsRequest.builder();
        for (LabeledTable labeledTable : labeledTables) {
            builder.addRequests(ExportRequest.of(labeledTable.table(), listener));
        }
        List<Export> exports = session.export(builder.build());
        CountDownLatch latch = new CountDownLatch(labeledTables.size());
        int ix = 0;
        for (LabeledTable labeledTable : labeledTables) {
            Export export = exports.get(ix);
            session.publish(labeledTable.label(), export).whenComplete((unused, throwable) -> {
                export.close();
                if (throwable != null) {
                    StringWriter stringWriter = new StringWriter();
                    stringWriter.append("Error publishing '").append(labeledTable.label())
                            .append("'").append(System.lineSeparator());
                    throwable.printStackTrace(new PrintWriter(stringWriter));
                    System.err.println(stringWriter);
                }
                latch.countDown();
            });
            ++ix;
        }
        latch.await();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new PublishTables()).execute(args);
        System.exit(execute);
    }
}
