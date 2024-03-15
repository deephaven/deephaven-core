//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphvizTest {

    @Test
    void deepWalk() {
        final TableSpec deepWalk = last(ParentsVisitorTest.createDeepWalk(ParentsVisitorTest.DEEPWALK_SIZE));
        // we expect one line per node and one link for the one parent
        final long factor = 2;
        final long lineCount = Graphviz.toDot(deepWalk).lines().count();
        assertThat(lineCount).isBetween(
                ParentsVisitorTest.DEEPWALK_SIZE * factor - 10,
                ParentsVisitorTest.DEEPWALK_SIZE * factor + 10);
    }

    @Test
    void heavilyBranched() {
        final TableSpec heavilyBranched =
                last(ParentsVisitorTest.createHeavilyBranchedTable(ParentsVisitorTest.HEAVILY_BRANCHED_SIZE));
        // This is a malicious case, but we are checking here that we don't run out of memory.
        // we expect one line per node and two links for the two parents
        final long factor = 3;
        final long lineCount = Graphviz.toDot(heavilyBranched).lines().count();
        assertThat(lineCount).isBetween(
                ParentsVisitorTest.HEAVILY_BRANCHED_SIZE * factor - 10,
                ParentsVisitorTest.HEAVILY_BRANCHED_SIZE * factor + 10);
    }

    // While we don't really want to test all of the cases exactly (b/c we don't guarantee stability), we can at least
    // make a few test cases based on the current behavior to illustrate what it looks like.

    @Test
    void simpleString() {
        assertThat(Graphviz.toDot(TableSpec.empty(1024).head(42).view("I=(long)ii")))
                .isEqualTo("digraph {\n" +
                        "\"op_0\" [\"label\"=\"empty(1024)\"]\n" +
                        "\"op_1\" [\"label\"=\"head(42)\"]\n" +
                        "\"op_2\" [\"label\"=\"view(I=(long)ii)\"]\n" +
                        "\"op_1\" -> \"op_0\"\n" +
                        "\"op_2\" -> \"op_1\"\n" +
                        "}");
    }

    @Test
    void joinString() {
        assertThat(Graphviz.toDot(TableSpec.empty(1).join(TableSpec.empty(2))))
                .isEqualTo("digraph {\n" +
                        "\"op_0\" [\"label\"=\"empty(1)\"]\n" +
                        "\"op_1\" [\"label\"=\"empty(2)\"]\n" +
                        "\"op_2\" [\"label\"=\"join([],[])\"]\n" +
                        "\"op_2\" -> \"op_0\" [\"label\"=\"left\"]\n" +
                        "\"op_2\" -> \"op_1\" [\"label\"=\"right\"]\n" +
                        "}");
    }

    private static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }
}
