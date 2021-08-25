package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Assert;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class NormalizeNotsTest extends AbstractReplacingVisitorTest {

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
                    invoke("methodA", reference("ColumnA")), // invoke used since it can't be
                                                             // rewritten to handle a NOT
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
                not(node(FilterDescriptor.FilterOperation.CONTAINS, reference("ColumnA"),
                    literal("asdf"))),
                not(node(FilterDescriptor.FilterOperation.IS_NULL, reference("ColumnA")))));

        try {
            execute(
                not(node(FilterDescriptor.FilterOperation.SEARCH, literal("asdf"))));
            Assert.fail("Expected exception");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("Cannot not() a search", expected.getMessage());
        }

        assertFilterEquals("flip various leaf expressions",
            not(or(
                node(FilterDescriptor.FilterOperation.LT, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.GT, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.LTE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.GTE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.EQ_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NEQ, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NEQ_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.IN, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.IN_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NOT_IN, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NOT_IN_ICASE, reference("ColumnA"),
                    reference("ColumnB")))),
            and(
                node(FilterDescriptor.FilterOperation.GTE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.LTE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.GT, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.LT, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NEQ, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NEQ_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.EQ_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NOT_IN, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.NOT_IN_ICASE, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.IN, reference("ColumnA"),
                    reference("ColumnB")),
                node(FilterDescriptor.FilterOperation.IN_ICASE, reference("ColumnA"),
                    reference("ColumnB"))));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return NormalizeNots.execute(f);
    }
}
