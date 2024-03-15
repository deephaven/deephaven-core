//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FriendlyStringTest {

    @Test
    void deepWalk() {
        final TableSpec deepWalk = last(ParentsVisitorTest.createDeepWalk(ParentsVisitorTest.DEEPWALK_SIZE));
        assertThat(FriendlyString.of(deepWalk).lines().count()).isLessThan(20);
    }

    @Test
    void heavilyBranched() {
        final TableSpec heavilyBranched =
                last(ParentsVisitorTest.createHeavilyBranchedTable(ParentsVisitorTest.HEAVILY_BRANCHED_SIZE));
        // This is a malicious case, but we are checking here that we don't run out of memory.
        assertThat(FriendlyString.of(heavilyBranched).lines().count()).isLessThan(10000);
    }

    // While we don't really want to test all of the cases exactly (b/c we don't guarantee stability), we can at least
    // make a few test cases based on the current behavior to illustrate the "human-friendly" aspect it's going for.

    @Test
    void simpleString() {
        assertThat(FriendlyString.of(TableSpec.empty(1024).head(42).view("I=(long)ii")))
                .isEqualTo("empty(1024)\n" +
                        ".head(42)\n" +
                        ".view(I=(long)ii)");
    }

    @Test
    void joinString() {
        assertThat(FriendlyString.of(TableSpec.empty(1).join(TableSpec.empty(2))))
                .isEqualTo("[\n" +
                        "empty(1),\n" +
                        "empty(2),\n" +
                        "]\n" +
                        ".join([],[])");
    }

    private static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }
}
