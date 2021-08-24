package io.deephaven.lang.completion.results;

import io.deephaven.libs.primitives.BytePrimitives;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerConstants;
import io.deephaven.lang.generated.ChunkerInvoke;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.generated.Token;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;
import io.deephaven.proto.backplane.script.grpc.TextEdit;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

/**
 * A class specifically for completing column expression; to be called after the completer has
 * discovered the a column expression with an = and the cursor is at or after =.
 *
 */
public class CompleteColumnExpression extends CompletionBuilder {

    private final Node node;
    // not currently used, but may be useful to determine if we should add any trailing `)` or not.
    private final ChunkerInvoke invoke;

    public CompleteColumnExpression(
        ChunkerCompleter completer,
        Node node,
        ChunkerInvoke invoke) {
        super(completer);
        this.node = node;
        this.invoke = invoke;
        assert node != null;
    }

    public void doCompletion(
        Collection<CompletionItem.Builder> results,
        CompletionRequest request,
        Method method) {
        final String displayCompletion;
        if (method.getDeclaringClass().getSimpleName().endsWith("Primitives")
            && method.getDeclaringClass().getPackage().equals(
                BytePrimitives.class.getPackage())) {
            // reduce massive duplication from same-named primitives methods.
            // In the future, when we have better column/type inference, we should be able to delete
            // this workaround
            displayCompletion = "*Primitives.";
        } else {
            displayCompletion = method.getDeclaringClass().getSimpleName() + ".";
        }
        String replaced = method.getName() + "(";
        String suffix = "";
        if (method.getParameterCount() == 0) {
            // hm... should consider varargs, and offer both `)` and `,` delimited options.
            replaced += ")";
            if (!node.isWellFormed()) {
                replaced += getCompleter().getQuoteType(node);
            }
        } else {
            // hrm; signal to user that there is a parameter of some kind.
            // really, this should be getting attached to additional metadata,
            // instead of trying to communicate through displayCompletion string.
            final Class<?> type0 = method.getParameterTypes()[0];
            suffix = "(" + type0.getSimpleName() + ")";
            if (!node.isWellFormed() && String.class.equals(type0)) {
                final String qt = getCompleter().getQuoteType(node);
                replaced += "\"".equals(qt) ? "`" : "\"";
            }
        }
        // need to handle null node using parent invoke, the same as CompleteColumnName
        final DocumentRange.Builder range = replaceNode(node, request);
        final String src = node.toSource();
        char c = 0, prev;
        boolean spaceBefore = false;
        boolean sawEqual = false;
        loop: for (int i = 0; i < src.length(); i++) {
            prev = c;
            c = src.charAt(i);
            if (sawEqual) {
                if (!Character.isWhitespace(toString().charAt(i))) {
                    if (spaceBefore && !Character.isWhitespace(prev)) {
                        replaced = " " + replaced;
                    }
                    break;
                }
            }
            switch (c) {
                case '\n':
                case '\r':
                    // monaco does not allow edits across lines.
                    // If we haven't reached the cursor position yet,
                    // it will be manually fixed up for us during result post-processing.
                    break loop;
                case '=':
                    spaceBefore = Character.isWhitespace(prev);
                    sawEqual = true;
                default:
                    range.getStartBuilder()
                        .setCharacter(range.getStartBuilder().getCharacter() + 1);
                    start++;
            }
        }
        CompletionItem.Builder result = CompletionItem.newBuilder();
        result.setStart(start)
            .setLength(len)
            .setLabel(displayCompletion + replaced + suffix)
            .getTextEditBuilder()
            .setText(replaced)
            .setRange(range);
        results.add(result);
    }

    public void doCompletion(
        Collection<CompletionItem.Builder> results,
        CompletionRequest request,
        String colName) {
        String replaced = colName;

        // need to handle null node using parent invoke, the same as CompleteColumnName
        final DocumentRange.Builder range = replaceNode(node, request);
        final String src = node.toSource();
        char c = 0, prev;
        boolean spaceBefore = false;
        boolean sawEqual = false;
        loop: for (int i = 0; i < src.length(); i++) {
            prev = c;
            c = src.charAt(i);
            if (sawEqual) {
                if (!Character.isWhitespace(toString().charAt(i))) {
                    if (spaceBefore && !Character.isWhitespace(prev)) {
                        replaced = " " + replaced;
                    }
                    break;
                }
            }
            switch (c) {
                case '\n':
                case '\r':
                    // monaco does not allow edits across lines.
                    // If we haven't reached the cursor position yet,
                    // it will be manually fixed up for us during result post-processing.
                    break loop;
                case '=':
                    spaceBefore = Character.isWhitespace(prev);
                    sawEqual = true;
                default:
                    range.getStartBuilder()
                        .setCharacter(range.getStartBuilder().getCharacter() + 1);
                    start++;
            }
        }

        final Token next = node.jjtGetLastToken().next;

        String withClose = replaced + getCompleter().getQuoteType(node);
        if (next == null) {
            withClose += ")";
        } else {
            switch (next.kind) {
                case ChunkerConstants.EOF:
                case ChunkerConstants.WHITESPACE:
                case ChunkerConstants.NEWLINE:
                    if (next.next == null || next.next.kind != ChunkerConstants.CLOSE_PAREN) {
                        // This could be a bit smarter w.r.t. closing quotes
                        withClose += ")";
                    }
            }
        }
        final CompletionItem.Builder result = CompletionItem.newBuilder();
        result.setStart(start)
            .setLength(len)
            .setLabel(withClose)
            .getTextEditBuilder()
            .setText(withClose)
            .setRange(range);
        results.add(result);
        // An alternate version which does not include the close quote.
        // Ideally, we just move the user's cursor position backwards, by making the main
        // completion use the code below, with an additional edit to append the closing quote.
        // We'll do this once we figure out how to control the resulting cursor position in Monaco
        // CompletionItem result = new CompletionItem(start, len, replaced, replaced, range);
    }
}
