package io.deephaven.qst.table;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class ParentsVisitorTest {


    private static final Table S1 = Table.empty(42);

    private static final Table S2 = S1.head(6);

    private static final Table S3 = S2.tail(4);

    private static final Table S4 = S3.view("I=i");

    private void checkDepth(Table table, List<Table> depthFirst) {
        assertThat(new ArrayList<>(ParentsVisitor.depthFirst(Collections.singleton(table))))
            .isEqualTo(depthFirst);
    }

    private void checkDepth(Table table, List<Table> depthFirst, int maxDepth) {
        assertThat(
            new ArrayList<>(ParentsVisitor.depthFirst(Collections.singleton(table), maxDepth)))
                .isEqualTo(depthFirst);
    }

    @Test
    void checkDepth() {
        checkDepth(S1, Collections.singletonList(S1));
        checkDepth(S2, Arrays.asList(S1, S2));
        checkDepth(S3, Arrays.asList(S1, S2, S3));
        checkDepth(S4, Arrays.asList(S1, S2, S3, S4));
    }

    @Test
    void checkDepth0() {
        checkDepth(S1, Collections.singletonList(S1), 0);
        checkDepth(S2, Collections.singletonList(S2), 0);
        checkDepth(S3, Collections.singletonList(S3), 0);
        checkDepth(S4, Collections.singletonList(S4), 0);
    }

    @Test
    void checkDepth1() {
        checkDepth(S1, Collections.singletonList(S1), 1);
        checkDepth(S2, Arrays.asList(S1, S2), 1);
        checkDepth(S3, Arrays.asList(S2, S3), 1);
        checkDepth(S4, Arrays.asList(S3, S4), 1);
    }

    @Test
    void checkDepth2() {
        checkDepth(S1, Collections.singletonList(S1), 2);
        checkDepth(S2, Arrays.asList(S1, S2), 2);
        checkDepth(S3, Arrays.asList(S1, S2, S3), 2);
        checkDepth(S4, Arrays.asList(S2, S3, S4), 2);
    }

    @Test
    void checkDepth3() {
        checkDepth(S1, Collections.singletonList(S1), 3);
        checkDepth(S2, Arrays.asList(S1, S2), 3);
        checkDepth(S3, Arrays.asList(S1, S2, S3), 3);
        checkDepth(S4, Arrays.asList(S1, S2, S3, S4), 3);
    }

    @Test
    void checkDepth4() {
        checkDepth(S1, Collections.singletonList(S1), 4);
        checkDepth(S2, Arrays.asList(S1, S2), 4);
        checkDepth(S3, Arrays.asList(S1, S2, S3), 4);
        checkDepth(S4, Arrays.asList(S1, S2, S3, S4), 4);
    }


    /**
     * This is a table that branches at every level except the leaf. It is meant to exercise the
     * depth-first implementation. Naive implementations may need to search every single path
     * through the DAG; but that is not feasible (2^64 paths).
     */
    private static Table heavilyBranchedTable() {
        Table current = Table.empty(1);
        for (int i = 0; i < 64; ++i) {
            current = Table.merge(current, current);
        }
        return current;
    }

    @Test
    void heavilyBranchedTableDepthFirstWalk() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            int[] count = {0};
            ParentsVisitor.depthFirstWalk(Collections.singleton(heavilyBranchedTable()),
                table -> count[0]++);
            assertThat(count[0]).isEqualTo(65);
        });
    }
}
