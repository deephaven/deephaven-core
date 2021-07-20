package io.deephaven.graphviz;

import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Converts a list of QSTs into the GraphViz DOT format.
 */
@Command(name = "graphviz", mixinStandardHelpOptions = true,
    description = "Converts a list of QSTs into the GraphViz DOT format", version = "0.1.0")
class GraphVizMain implements Callable<Void> {

    @Option(names = {"-o", "--out"}, paramLabel = "OUT",
        description = "The output file. If none specified, outputs to stdout.")
    Path output;

    @Parameters(arity = "1..*", paramLabel = "QST",
        description = "QST file(s) to process. May be in the form <PATH> or <KEY>=<PATH>.",
        converter = LabeledTableConverter.class)
    LabeledTable[] inputTables;

    private String toDotFormat() {
        return GraphVizBuilder.of(LabeledTables.of(inputTables));
    }

    @Override
    public Void call() throws Exception {
        String dotFormat = toDotFormat();
        if (output != null) {
            Files.write(output, dotFormat.getBytes(StandardCharsets.UTF_8));
        } else {
            System.out.println(dotFormat);
        }
        return null;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GraphVizMain()).execute(args);
        System.exit(execute);
    }
}
