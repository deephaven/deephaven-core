package io.deephaven.qst.table;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.deephaven.qst.table.ParentsVisitor.getParents;
import static io.deephaven.qst.table.ParentsVisitor.postOrderList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class ParentsVisitorTest {

    private static final int HEAVILY_BRANCHED_SIZE = 128;

    private static final int DEEPWALK_SIZE = 8192;

    private static final TableSpec S1 = TableSpec.empty(42);

    private static final TableSpec S2 = S1.head(6);

    private static final TableSpec S3 = S2.tail(4);

    private static final TableSpec S4 = S3.view("I=i");

    @Test
    void exactlyOnePostOrderChain() {
        canonicalOrder(S1, S1);
        canonicalOrder(S2, S1, S2);
        canonicalOrder(S3, S1, S2, S3);
        canonicalOrder(S4, S1, S2, S3, S4);
    }

    @Test
    void exactlyOnePostOrderBranch1() {
        TableSpec t1 = TableSpec.empty(1);
        TableSpec t2 = t1.head(2);
        TableSpec t3 = TableSpec.merge(Arrays.asList(t1, t2));
        canonicalOrder(t3, t1, t2, t3);
    }

    @Test
    void exactlyOnePostOrderBranch2() {
        TableSpec t1 = TableSpec.empty(1);
        TableSpec t2 = t1.head(2);
        TableSpec t3 = TableSpec.merge(t2, t1);
        canonicalOrder(t3, t1, t2, t3);
    }

    @Test
    void exactlyOnePostOrderBranch3() {
        TableSpec t1 = TableSpec.empty(1);
        TableSpec t2 = t1.head(1);
        TableSpec t3 = TableSpec.merge(t1, t2);
        TableSpec t4 = TableSpec.merge(t1, t2, t3);
        TableSpec t5 = TableSpec.merge(t1, t2, t3, t4);
        TableSpec t6 = TableSpec.merge(t1, t2, t3, t4, t5);
        canonicalOrder(t6, t1, t2, t3, t4, t5, t6);
    }

    @Test
    void exactlyOnePostOrderBranch4() {
        TableSpec t1 = TableSpec.empty(1);
        TableSpec t2 = t1.head(1);
        TableSpec t3 = TableSpec.merge(t2, t1);
        TableSpec t4 = TableSpec.merge(t3, t2, t1);
        TableSpec t5 = TableSpec.merge(t4, t3, t2, t1);
        TableSpec t6 = TableSpec.merge(t5, t4, t3, t2, t1);
        canonicalOrder(t6, t1, t2, t3, t4, t5, t6);
    }

    @Test
    void duplicatedInputs() {
        canonicalOrder(Arrays.asList(S4, S4), Arrays.asList(S1, S2, S3, S4));
    }

    @Test
    void multipleInputsSameChain1() {
        canonicalOrder(Arrays.asList(S4, S3, S2, S1), Arrays.asList(S1, S2, S3, S4));
    }

    @Test
    void multipleInputsSameChain2() {
        canonicalOrder(Arrays.asList(S3, S4, S2, S1), Arrays.asList(S1, S2, S3, S4));
    }

    /**
     * This is specifically designed to break recursive implementations with a very deep QST.
     */
    @Test
    void deepWalk() {
        List<TableSpec> expected = createDeepWalk(DEEPWALK_SIZE);
        TableSpec table = expected.get(expected.size() - 1);
        // recursive implementations are very likely to throw StackOverflowError
        canonicalOrder(table, expected);
    }

    @Test
    void deepWalkAllProvided() {
        List<TableSpec> expected = createDeepWalk(DEEPWALK_SIZE);
        canonicalOrder(expected, expected);
    }

    @Test
    void deepWalkAllReversed() {
        List<TableSpec> expected = createDeepWalk(DEEPWALK_SIZE);
        List<TableSpec> reversed = new ArrayList<>(expected);
        Collections.reverse(reversed);
        canonicalOrder(reversed, expected);
    }

    @Test
    void deepWalkAllShuffled() {
        List<TableSpec> expected = createDeepWalk(DEEPWALK_SIZE);
        List<TableSpec> shuffled = new ArrayList<>(expected);
        for (int i = 0; i < 10; ++i) {
            Collections.shuffle(shuffled);
            canonicalOrder(shuffled, expected);
        }
    }

    /**
     * This is specifically designed to break implementations that don't have already-visited
     * checks.
     */
    @Test
    void heavilyBranchedWalk() {
        List<TableSpec> expected = createHeavilyBranchedTable(HEAVILY_BRANCHED_SIZE);
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            canonicalOrder(expected.get(expected.size() - 1), expected);
        });
    }

    @Test
    void heavilyBranchedWalkAllProvided() {
        List<TableSpec> expected = createHeavilyBranchedTable(HEAVILY_BRANCHED_SIZE);
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            canonicalOrder(expected, expected);
        });
    }

    @Test
    void heavilyBranchedWalkAllReversed() {
        List<TableSpec> expected = createHeavilyBranchedTable(HEAVILY_BRANCHED_SIZE);
        List<TableSpec> reversed = new ArrayList<>(expected);
        Collections.reverse(reversed);
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            canonicalOrder(reversed, expected);
        });
    }

    @Test
    void heavilyBranchedWalkAllShuffled() {
        List<TableSpec> expected = createHeavilyBranchedTable(HEAVILY_BRANCHED_SIZE);
        List<TableSpec> shuffled = new ArrayList<>(expected);

        for (int i = 0; i < 10; ++i) {
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                Collections.shuffle(shuffled);
                canonicalOrder(shuffled, expected);
            });
        }
    }

    @Test
    void validPostOrderings() {
        for (TableSpec table : tables()) {
            checkValidPostOrder(postOrderList(Collections.singleton(table)));
        }
    }

    @Test
    void validPostOrderingsAllAtOnce() {
        checkValidPostOrder(postOrderList(tables()));
    }

    @Test
    void multiplePostOrderings() {
        TableSpec t1 = TableSpec.empty(1);
        TableSpec t2 = t1.head(1);
        TableSpec t3 = t1.tail(1);
        TableSpec t4 = TableSpec.merge(t2, t3);

        // Both are valid post-orderings:
        // t1, t2, t3, t4
        // t1, t3, t2, t4

        try {
            checkIsCanonicalOrder(Collections.singleton(t4));
            failBecauseExceptionWasNotThrown(AssertionError.class);
        } catch (AssertionError e) {
            // expected
        }
    }

    private static Iterable<TableSpec> tables() {
        List<TableSpec> deepWalk = createDeepWalk(DEEPWALK_SIZE);
        List<TableSpec> heavilyBranchedTable = createHeavilyBranchedTable(HEAVILY_BRANCHED_SIZE);

        return () -> Stream
            .concat(
                Stream.of(S4, heavilyBranchedTable.get(heavilyBranchedTable.size() - 1),
                    deepWalk.get(deepWalk.size() - 1)),
                TableCreatorImplTest.createTables().stream())
            .iterator();
    }

    private static void checkValidPostOrder(Iterable<TableSpec> items) {
        Set<TableSpec> visited = new HashSet<>();
        for (TableSpec item : items) {
            boolean allDependenciesSatisfied = getParents(item).allMatch(visited::contains);
            assertThat(allDependenciesSatisfied).withFailMessage("items are not in post-order")
                .isTrue();
            assertThat(visited.add(item)).withFailMessage("items are not de-duplicated").isTrue();
        }
    }

    /**
     * This is a table that branches at every level except the leaf. Naive implementations may need
     * to search every single path through the DAG; but that is not feasible (2^64 paths).
     */
    private static List<TableSpec> createHeavilyBranchedTable(int size) {
        List<TableSpec> out = new ArrayList<>(size + 1);
        TableSpec current = TableSpec.empty(1);
        out.add(current);
        for (int i = 0; i < size; ++i) {
            current = TableSpec.merge(current, current);
            out.add(current);
        }
        return out;
    }

    private static List<TableSpec> createDeepWalk(int size) {
        List<TableSpec> expected = new ArrayList<>();
        TableSpec table = TableSpec.empty(size);
        expected.add(table);
        for (int i = 0; i < size; ++i) {
            table = table.head(i);
            expected.add(table);
        }
        return expected;
    }

    private void canonicalOrder(TableSpec input, TableSpec... expectedOutputs) {
        canonicalOrder(Collections.singleton(input), Arrays.asList(expectedOutputs));
    }

    private void canonicalOrder(TableSpec input, Iterable<TableSpec> expectedOutputs) {
        canonicalOrder(Collections.singleton(input), expectedOutputs);
    }

    private void canonicalOrder(Iterable<TableSpec> inputs, Iterable<TableSpec> expectedOutputs) {
        checkValidPostOrder(expectedOutputs);
        checkIsCanonicalOrder(expectedOutputs);
        assertThat(postOrderList(inputs)).containsExactlyElementsOf(expectedOutputs);
    }

    /**
     * In general, a set of tables will have multiple valid post-orders. To check against a specific
     * order, we should ensure that there is one canonical ordering.
     *
     * <p>
     * This is a check against adding an overly-specific test that depends on a specific
     * post-ordering, which we should not do.
     */
    private static void checkIsCanonicalOrder(Iterable<TableSpec> items) {
        TableSpec prev = null;
        for (TableSpec current : items) {
            if (prev == null) {
                assertThat(getParents(current)).isEmpty();
            } else {
                assertThat(getParents(current)).contains(prev);
            }
            prev = current;
        }
    }
}
