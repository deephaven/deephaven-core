package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class FlipNonReferenceMatchExpressionTest extends AbstractReplacingVisitorTest {

    @Test
    public void testFlipNonReferenceMatchExpression() {
        assertUnchanged("correct form",
            in("ColumnA", 1, 2));
        assertUnchanged("correct form",
            in("ColumnA", 1));

        assertFilterEquals("all literals",
            node(FilterDescriptor.FilterOperation.IN, literals(1, 2, 3)),
            or(
                node(FilterDescriptor.FilterOperation.IN, literals(2, 1)),
                node(FilterDescriptor.FilterOperation.IN, literals(3, 1))));

        assertFilterEquals("reference on right",
            node(FilterDescriptor.FilterOperation.IN, literal(1), reference("ColumnA"), literal(4),
                literal(5)),
            or(
                in("ColumnA", 1),
                node(FilterDescriptor.FilterOperation.IN, literal(4), literal(1)),
                node(FilterDescriptor.FilterOperation.IN, literal(5), literal(1))));

        assertFilterEquals("reference on right, no OR required",
            node(FilterDescriptor.FilterOperation.IN, literal(1), reference("ColumnA")),
            in("ColumnA", 1));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return FlipNonReferenceMatchExpression.execute(f);
    }
}
