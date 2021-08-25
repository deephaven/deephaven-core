package io.deephaven.graphviz;

import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.qst.table.ViewTable;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphVizBuilderTest {

    @Test
    void example1() throws IOException {
        // Each step in the output must depend on the previous output to ensure canonical ordering
        // for unit testing; otherwise we are testing the specific ordering of
        // io.deephaven.qst.table.ParentsVisitor.postOrderList
        TableSpec t1 = TimeTable.of(Duration.ofSeconds(1)).view("I=i");
        TableSpec t2 = t1.tail(10);
        TableSpec t3 = t2.head(1);
        TableSpec t4 = TableSpec.merge(t2, t3);
        TableSpec t5 = TableSpec.merge(t4, t4);
        check(t5, "example-1.dot");
    }

    @Test
    void example1Labeled() throws IOException {
        // Each step in the output must depend on the previous output to ensure canonical ordering
        // for unit testing; otherwise we are testing the specific ordering of
        // io.deephaven.qst.table.ParentsVisitor.postOrderList
        TableSpec t1 = TimeTable.of(Duration.ofSeconds(1)).view("I=i");
        TableSpec t2 = t1.tail(10);
        TableSpec t3 = t2.head(1);
        TableSpec t4 = TableSpec.merge(t2, t3);
        TableSpec t5 = TableSpec.merge(t4, t4);
        check(LabeledTables.builder().putMap("t1", t1).putMap("t2", t2).putMap("t3", t3)
                .putMap("t4", t4).putMap("t5", t5).build(), "example-1-labeled.dot");
    }

    @Test
    void example2() throws IOException {
        TableSpec needsEscaping = TableSpec.empty(1).view("I=`some \"\n string stuff\uD83D\uDC7D`");
        check(needsEscaping, "example-2.dot");
    }

    private static void check(TableSpec t, String resource) throws IOException {
        String expected = getResource(resource);
        MutableGraph graph = GraphVizBuilder.of(Collections.singleton(t));
        assertThat(Graphviz.fromGraph(graph).render(Format.DOT).toString()).isEqualTo(expected);
    }

    private static void check(LabeledTables tables, String resource) throws IOException {
        String expected = getResource(resource);
        MutableGraph graph = GraphVizBuilder.of(tables);
        assertThat(Graphviz.fromGraph(graph).render(Format.DOT).toString()).isEqualTo(expected);
    }

    private static String getResource(String resourceName) throws IOException {
        try (
                InputStream in =
                        Objects.requireNonNull(GraphVizBuilderTest.class.getResourceAsStream(resourceName));
                InputStreamReader inReader = new InputStreamReader(in);
                BufferedReader reader = new BufferedReader(inReader)) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }
}
