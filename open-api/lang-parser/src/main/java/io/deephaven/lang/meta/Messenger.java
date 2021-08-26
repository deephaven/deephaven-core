package io.deephaven.lang.meta;

import io.deephaven.lang.generated.Chunker;
import io.deephaven.lang.generated.SimpleCharStream;
import io.deephaven.lang.generated.Token;
import io.deephaven.web.shared.ide.lsp.Diagnostic;
import io.deephaven.web.shared.ide.lsp.DocumentRange;
import io.deephaven.web.shared.ide.lsp.Position;

import java.util.ArrayList;
import java.util.List;

/**
 * An instance of this object is sent to our parser, so that as it runs into invalid syntax, it can report useful
 * errors, and allow handler code to inspect the jjtree parser when it occurs.
 *
 */
public class Messenger {

    private List<Diagnostic> diagnostics = new ArrayList<>();

    public void report(int code, Chunker ast) {
        Token curToken = ast.token;
        final int pos = ast.token_source.getCurrentTokenAbsolutePosition();
        final SimpleCharStream stream = ast.token_source.stream();
        Diagnostic diagnostic = new Diagnostic();
        diagnostic.code(code);
        final DocumentRange range = new DocumentRange();
        range.start = new Position();
        range.start.line = curToken.beginLine;
        range.start.character = curToken.endColumn;
        range.end = new Position();
        range.end.line = stream.getEndLine();
        range.end.character = stream.getEndColumn();
        diagnostic.setRange(range);
        diagnostics.add(diagnostic);
    }

    public List<Diagnostic> getDiagnostics() {
        return diagnostics;
    }
}
