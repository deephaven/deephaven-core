package io.deephaven.graphviz;

import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.engine.GraphvizJdkEngine;
import guru.nidi.graphviz.engine.Renderer;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.LabeledTables;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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

    @Option(names = {"-f", "--format"}, paramLabel = "FORMAT",
            description = "The output format, one of: [ ${COMPLETION-CANDIDATES} ]. Defaults to DOT.",
            defaultValue = "DOT")
    Format format;

    @Parameters(arity = "1..*", paramLabel = "QST",
            description = "QST file(s) to process. May be in the form <PATH> or <KEY>=<PATH>.",
            converter = LabeledTableConverter.class)
    LabeledTable[] inputTables;

    private Renderer render() {
        return Graphviz.fromGraph(GraphVizBuilder.of(LabeledTables.of(inputTables))).render(format);
    }

    @Override
    public Void call() throws Exception {
        Renderer render = render();
        if (output != null) {
            render.toFile(output.toFile());
        } else {
            System.out.println(render);
        }
        return null;
    }

    public static void main(String[] args) {
        Graphviz.useEngine(new GraphvizJdkEngine());
        int execute = new CommandLine(new GraphVizMain()).execute(args);
        System.exit(execute);
    }
}
