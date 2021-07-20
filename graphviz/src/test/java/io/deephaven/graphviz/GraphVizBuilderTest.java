package io.deephaven.graphviz;

import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.table.TimeTable;
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
    void ij() throws IOException {
        Table i = TimeTable.of(Duration.ofMillis(1234L)).view("I=i").tail(1);
        Table j = TimeTable.of(Duration.ofMillis(4321)).view("J=i").tail(1);
        Table ij = i.naturalJoin(j, Collections.emptyList(), Collections.emptyList());

        check(ij, "ij.dot");
    }

    @Test
    void ijLabeled() throws IOException {
        Table i = TimeTable.of(Duration.ofMillis(1234L)).view("I=i").tail(1);
        Table j = TimeTable.of(Duration.ofMillis(4321)).view("J=i").tail(1);
        Table ij = i.naturalJoin(j, Collections.emptyList(), Collections.emptyList());

        LabeledTables ijLabeled =
            LabeledTables.builder().putMap("i", i).putMap("j", j).putMap("ij", ij).build();
        check(ijLabeled, "ij-labeled.dot");
    }

    private static void check(Table t, String resource) throws IOException {
        String expected = getResource(resource);
        String actual = GraphVizBuilder.of(Collections.singleton(t));
        assertThat(actual).isEqualTo(expected);
    }

    private static void check(LabeledTables tables, String resource) throws IOException {
        String expected = getResource(resource);
        String actual = GraphVizBuilder.of(tables);
        assertThat(actual).isEqualTo(expected);
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
