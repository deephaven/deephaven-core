package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import org.junit.Test;

import static io.deephaven.web.shared.ast.FilterTestUtils.*;

public class ConvertEqToInTest extends AbstractReplacingVisitorTest {

    @Test
    public void testConvertEqToIn() {
        assertFilterEquals("simple EQ",
            eq("ColumnA", 1),
            in("ColumnA", 1));

        assertFilterEquals("reverse EQ",
            node(FilterDescriptor.FilterOperation.EQ, literal((double) 1), reference("ColumnA")),
            in("ColumnA", 1));

        assertUnchanged("two literals",
            node(FilterDescriptor.FilterOperation.EQ, literals(1, 2)));

        assertUnchanged("two references",
            node(FilterDescriptor.FilterOperation.EQ, reference("ColumnA"), reference("ColumnB")));
    }

    @Override
    protected FilterDescriptor execute(FilterDescriptor f) {
        return ConvertEqToIn.execute(f);
    }
}
