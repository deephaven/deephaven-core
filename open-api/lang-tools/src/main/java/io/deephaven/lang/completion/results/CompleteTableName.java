package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.*;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;

import java.util.Set;
import java.util.stream.Stream;

/**
 * A class specifically for completing table names; to be called after the completer has discovered
 * the name of the table.
 *
 */
public class CompleteTableName extends CompletionBuilder {

    private final ChunkerInvoke invoke;
    private final Stream<String> matches;

    public CompleteTableName(ChunkerCompleter completer, ChunkerInvoke invoke,
        Stream<String> matches) {
        super(completer);
        this.invoke = invoke;
        this.matches = matches;
    }

    public void doCompletion(
        Node node,
        Set<CompletionItem.Builder> results,
        CompletionRequest request) {
        final int argInd = invoke.indexOfArgument(node);
        final String qt = getCompleter().getQuoteType(node);
        final DocumentRange.Builder range;
        final String[] prefix = {""}, suffix = {""};
        if (argInd == 0) {
            // The cursor is on namespace side of `,` or there is no 2nd table name argument yet.
            // We'll need to replace from the next token of the end of the namespace forward.
            Token first = node.jjtGetLastToken();
            if (first.next != null) {
                first = first.next;
            }
            Token last = first;
            if (last.kind == ChunkerConstants.WHITESPACE) {
                prefix[0] += last.image;
                last = last.next;
            }
            if (last.kind == ChunkerConstants.COMMA) {
                prefix[0] += last.image;
                last = last.next;
                if (last.kind == ChunkerConstants.WHITESPACE) {
                    prefix[0] += last.image;
                }
                assert last.next.kind != ChunkerConstants.APOS;
                assert last.next.kind != ChunkerConstants.QUOTE;
                assert last.next.kind != ChunkerConstants.TRIPLE_APOS;
                assert last.next.kind != ChunkerConstants.TRIPLE_QUOTES;
            } else {
                prefix[0] += ", ";
            }
            if (first.kind == ChunkerConstants.EOF) {
                range = placeAfter(node, request);
            } else {
                range = replaceTokens(first, last, request);
            }
        } else if (argInd == 1) {
            // The cursor is on the table name argument. Replace the string node itself.
            range = replaceNode(node, request);
            if (node instanceof ChunkerString) {
                final Token last = node.jjtGetLastToken();
                switch (last.kind) {
                    case ChunkerConstants.APOS_CLOSE:
                        if (!"'".equals(last.image)) {
                            suffix[0] = last.image;
                        }
                        break;
                    case ChunkerConstants.QUOTE_CLOSE:
                        if (!"\"".equals(last.image)) {
                            suffix[0] = last.image;
                        }
                        break;

                }
            }
        } else if (argInd > 1) {
            // The cursor is on an argument after the table name. Replace from the cursor backwards.
            matches.forEach(match -> {
                getCompleter().addMatch(results, node, match, request, qt, ")");
            });
            return;
        } else {
            assert argInd == -1;
            // cursor is on a comma near where the table name goes...
            matches.forEach(match -> {
                getCompleter().addMatch(results, node, match, request, qt, ")");
            });
            return;
        }
        matches.forEach(match -> {
            StringBuilder b = new StringBuilder();
            b.append(qt);
            b.append(match);
            b.append(qt);
            String name = b.toString();
            if (!invoke.jjtGetLastToken().image.endsWith(")")) {
                b.append(")");
            }
            b.append(suffix[0]);
            if (node != null && node.isWellFormed()) {
                len++;
                range.getEndBuilder().setCharacter(range.getEndBuilder().getCharacter() + 1);
                // may need to skip this item, in case we are suggesting the exact thing which
                // already exists.
                if (name.equals(node.toSource())) {
                    // This suggestion is a duplicate; discard it.
                    return;
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
        });
    }
}
