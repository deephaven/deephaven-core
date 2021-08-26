package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class ConvertInvalidInExpressionsTest extends AbstractReplacingVisitorTest {

    @Test
    public void testConvertInvalidInExpressions() {
        assertUnchanged("already correct",
            in("ColumnA", 1));
        assertUnchanged("already correct",
            in("ColumnA", 1, 2));

        assertFilterEquals("literals on both sides",
            node(FilterDescriptor.FilterOperation.IN, literals(1, 2)),
            node(FilterDescriptor.FilterOperation.EQ, literals(1, 2)));
        assertFilterEquals("references on both sides",
            node(FilterDescriptor.FilterOperation.IN, reference("ColumnA"), reference("ColumnB")),
            node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnB")));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return ConvertInvalidInExpressions.execute(f);
    }
}
