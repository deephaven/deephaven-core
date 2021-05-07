package io.deephaven.python;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.jpy.PythonTest;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ASTHelperTest extends PythonTest {

    private ASTHelper ast;

    @Before
    public void setUp() {
        ast = ASTHelperSetup.create();
    }

    @After
    public void tearDown() {
        ast.close();
    }

    @Test
    public void extract_expression_names() {
        check("f(x[y]) + 2 - A * B", "A", "B", "f", "x", "y");
        check("FOO * BAR", "BAR", "FOO");
        check("BAR", "BAR");
        check("not X", "X");
        check("1");
        // note: while this *may* look like it should fail, we shouldn't dictate at this level what
        // constitutes a valid RHS. It's possible that we might want to map python tuple types into
        // a common java format. For now, we'll let it get thrown to the real formula parser to error
        // out if necessary
        check("(foo,bar)", "bar", "foo");
        check("None");
        check("True");
        check("False");
    }

    @Test(expected = Exception.class)
    public void not_an_expression() {
        ast.extract_expression_names("not_an_expression=1");
    }

    @Test(expected = Exception.class)
    public void junk() {
        ast.extract_expression_names("-junk-");
    }

    private void check(String formula, String... expects) {
        final List<String> results = ASTHelper
            .convertToList(ast.extract_expression_names(formula));
        assertThat(results).containsExactly(expects);
    }
}
