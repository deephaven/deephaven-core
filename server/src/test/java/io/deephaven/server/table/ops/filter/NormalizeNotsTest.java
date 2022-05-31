package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;
import org.junit.Test;

import static io.deephaven.server.table.ops.filter.FilterTestUtils.*;

public class NormalizeNotsTest extends AbstractNormalizingFilterTest {

    @Test
    public void testNormalizeNots() {
        // this test effectively tests FilterDescriptor.not(), but also that the visitor
        // correctly calls it on the tree
        assertFilterEquals("two nots around a simple expression",
                not(not(invoke("foo", reference("ColumnA")))),
                invoke("foo", reference("ColumnA")));
        assertFilterEquals("two nots around a simple expression",
                not(not(eq("foo", 1))),
                eq("foo", 1));

        assertFilterEquals("two nots within a tree",
                not(and(
                        not(or(
                                invoke("methodA", reference("ColumnA")), // invoke used since it can't be rewritten to
                                                                         // handle a NOT
                                invoke("methodB", reference("ColumnA")))),
                        or(
                                invoke("methodC", reference("ColumnA")),
                                invoke("methodD", reference("ColumnA"))))),
                or(
                        or(
                                invoke("methodA", reference("ColumnA")),
                                invoke("methodB", reference("ColumnA"))),
                        and(
                                not(invoke("methodC", reference("ColumnA"))),
                                not(invoke("methodD", reference("ColumnA"))))));

        assertUnchanged("other non-flippble expression",
                or(
                        not(NormalizeFilterUtil.doIsNull(Reference.newBuilder().setColumnName("ColumnA").build())),
                        not(invoke("foo", reference("ColumnA")))));

        assertFilterEquals("flip various leaf expressions",
                not(or(
                        compare(CompareCondition.CompareOperation.LESS_THAN, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.GREATER_THAN, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.EQUALS, reference("ColumnA"), reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.EQUALS, CaseSensitivity.IGNORE_CASE,
                                reference("ColumnA"), reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.NOT_EQUALS, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.NOT_EQUALS, CaseSensitivity.IGNORE_CASE,
                                reference("ColumnA"), reference("ColumnB")),
                        in(reference("ColumnA"), reference("ColumnB")),
                        inICase(reference("ColumnA"), reference("ColumnB")),
                        notIn(reference("ColumnA"), reference("ColumnB")),
                        notInICase(reference("ColumnA"), reference("ColumnB")),
                        NormalizeFilterUtil.doContains(Reference.newBuilder().setColumnName("ColumnA").build(), "asdf",
                                CaseSensitivity.MATCH_CASE, MatchType.REGULAR))),
                and(
                        compare(CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.GREATER_THAN, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.LESS_THAN, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.NOT_EQUALS, reference("ColumnA"),
                                reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.NOT_EQUALS, CaseSensitivity.IGNORE_CASE,
                                reference("ColumnA"), reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.EQUALS, reference("ColumnA"), reference("ColumnB")),
                        compare(CompareCondition.CompareOperation.EQUALS, CaseSensitivity.IGNORE_CASE,
                                reference("ColumnA"), reference("ColumnB")),
                        notIn(reference("ColumnA"), reference("ColumnB")),
                        notInICase(reference("ColumnA"), reference("ColumnB")),
                        in(reference("ColumnA"), reference("ColumnB")),
                        inICase(reference("ColumnA"), reference("ColumnB")),
                        NormalizeFilterUtil.doContains(Reference.newBuilder().setColumnName("ColumnA").build(), "asdf",
                                CaseSensitivity.MATCH_CASE, MatchType.INVERTED)));
    }

    @Override
    protected Condition execute(Condition f) {
        return NormalizeNots.exec(f);
    }
}
