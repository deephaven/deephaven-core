package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerConstants;
import io.deephaven.lang.generated.ChunkerInvoke;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.generated.Token;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;

import java.util.Collection;
import java.util.Set;

/**
 * A class specifically for completing column names; to be called after the completer has discovered the name of the
 * column to match.
 *
 */
public class CompleteColumnName extends CompletionBuilder {

    private final Node node;
    private final ChunkerInvoke invoke;

    public CompleteColumnName(
            ChunkerCompleter completer,
            Node node,
            ChunkerInvoke invoke) {
        super(completer);
        this.node = node;
        this.invoke = invoke;
    }

    public void doCompletion(
            Collection<CompletionItem.Builder> results,
            CompletionRequest request,
            String colName) {
        final String src;
        final DocumentRange.Builder range;
        src = node == null ? "" : node.toSource();
        final String qt = getCompleter().getQuoteType(node);

        // handle null node by looking at invoke
        if (node == null) {
            final Token nameTok = invoke.getNameToken();
            range = replaceToken(nameTok.next, request);
            range.getStartBuilder().setCharacter(range.getStartBuilder().getCharacter() - 1);
            range.getEndBuilder().setCharacter(range.getEndBuilder().getCharacter() - 1);
            len--;
        } else {
            range = replaceNode(node, request);
        }

        StringBuilder b = new StringBuilder();
        b.append(qt);
        b.append(colName);
        // Instead of addTokens, we need to use raw strings, since we don't tokenize inside strings (yet).
        int ind = src.indexOf('=');
        if (ind == -1) {
            b.append(" = ");
            if (node != null) {
                final Token suffix = node.jjtGetLastToken().next;
                if (suffix != null && suffix.kind == ChunkerConstants.WHITESPACE) {
                    b.setLength(b.length() - 1);
                }
            }
        } else {
            // we don't want to erase anything the user has after the =
            // we also want to detect and respect their whitespace usage
            if (ind < src.length() && Character.isWhitespace(src.charAt(ind + 1))) {
                b.append(" ");
            }
            b.append(src.substring(ind));
        }
        if (node != null) {
            switch (node.jjtGetLastToken().kind) {
                case ChunkerConstants.APOS_CLOSE:
                case ChunkerConstants.QUOTE_CLOSE:
                    b.append(node.jjtGetLastToken().image);
            }
        }
        final CompletionItem.Builder result = CompletionItem.newBuilder();
        String item = b.toString();
        result.setStart(start)
                .setLength(len)
                .setLabel(item)
                .getTextEditBuilder()
                .setText(item)
                .setRange(range);
        results.add(result);
    }
}
