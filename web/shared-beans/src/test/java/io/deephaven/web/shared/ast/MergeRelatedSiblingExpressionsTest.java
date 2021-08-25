package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;


public class MergeRelatedSiblingExpressionsTest extends AbstractReplacingVisitorTest {

    @Test
    public void testMergeRelatedSiblings() {
        assertUnchanged("plain IN",
                in("ColumnA", 1, 2));
        assertUnchanged("IN within AND",
                and(
                        in("ColumnA", 1),
                        in("ColumnA", 2)));

        assertUnchanged("can't merge these siblings",
                or(
                        in("ColumnA", 1),
                        eq("ColumnB", 2)));
        assertUnchanged("unrelated INs",
                or(
                        in("ColumnA", 1),
                        in("ColumnB", 2, 3, 4)));

        assertFilterEquals("merge NOT IN within AND, remove parent",
                and(
                        notIn("ColumnA", 1),
                        notIn("ColumnA", 2)),
                notIn("ColumnA", 1, 2));

        assertFilterEquals("merge only INs in OR",
                or(
                        node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnB")),
                        in("ColumnA", 1),
                        node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnC")),
                        in("ColumnA", 2)),
                or(
                        in("ColumnA", 1, 2),
                        node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnB")),
                        node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnC"))));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return MergeRelatedSiblingExpressions.execute(f);
    }
}
