package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class MakeExpressionsNullSafeTest extends AbstractReplacingVisitorTest {

    @Test
    public void testMakeExpressionsNullSafe() {
        assertUnchanged("doesnt affect EQ",
            node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), literal("A")));

        assertFilterEquals("add null checks when implicit invoke is needed",
            node(FilterDescriptor.FilterOperation.EQ_ICASE, reference("ColumnA"),
                reference("ColumnB")),
            or(
                and(
                    node(FilterDescriptor.FilterOperation.IS_NULL, reference("ColumnA")),
                    node(FilterDescriptor.FilterOperation.IS_NULL, reference("ColumnB"))),
                and(
                    not(
                        node(FilterDescriptor.FilterOperation.IS_NULL, reference("ColumnA"))),
                    invoke("equalsIgnoreCase", reference("ColumnA"), reference("ColumnB")))));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return MakeExpressionsNullSafe.execute(f);
    }
}
