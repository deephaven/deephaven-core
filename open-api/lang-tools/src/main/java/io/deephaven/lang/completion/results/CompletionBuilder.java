package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionOptions;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.generated.Token;
import io.deephaven.web.shared.ide.lsp.CompletionItem;
import io.deephaven.web.shared.ide.lsp.DocumentRange;
import io.deephaven.web.shared.ide.lsp.Position;

import java.util.Set;

/**
 * Some basic tools for computing completion results.
 */
public abstract class CompletionBuilder {

    protected int start, len;
    private final ChunkerCompleter completer;

    public CompletionBuilder(ChunkerCompleter completer) {
        this.completer = completer;
    }

    public ChunkerCompleter getCompleter() {
        return completer;
    }

    protected void addTokens(StringBuilder b, Token tok, String ... suffix) {
        for (int ind = 0; ind < suffix.length; ind++) {
            final String nextToken = suffix[ind];
            String check = nextToken.trim();
            if (tok != null) {
                if (tok.image.trim().endsWith(check)) {
                    b.append(nextToken);
                    len += tok.image.length();
                    break;
                } else {
                    tok = tok.next();
                    if (tok != null) {
                        if (tok.image.matches(ChunkerCompleter.CONTAINS_NEWLINE)) {
                            // stop gobbling at newlines!
                            tok = null;
                        } else {
                            String trimmed = tok.image.trim();
                            if (trimmed.isEmpty()) {
                                b.append(tok.image);
                                len += tok.image.length();
                                ind--;
                                continue;
                            } else if (trimmed.equals(check)) {
                                // user code has the desired token, stop being helpful
                                break;
                            }
                        }
                    }
                }
            }
            if (!b.toString().trim().endsWith(check)) {
                b.append(nextToken);
            }
        }

    }

    protected DocumentRange replaceNode(Node node, CompletionRequest request) {
        start = node.getStartIndex();
        len = node.getEndIndex() - node.getStartIndex();
        final DocumentRange range = node.asRange();
        return range;
    }

    protected DocumentRange replaceToken(Token startToken, CompletionRequest request) {
        return replaceTokens(startToken, startToken, request);
    }

    protected DocumentRange replaceTokens(Token startToken, Token endToken, CompletionRequest request) {
        if (endToken == null) {
            endToken = startToken;
        }

        start = startToken.getStartIndex();
        len = endToken.getEndIndex() - start;
        final DocumentRange range = new DocumentRange(
            startToken.positionStart(),
            endToken.positionEnd()
        );

        return range;
    }

    protected DocumentRange placeAfter(Node node, CompletionRequest request) {
        start = request.getOffset();
        len = 1;
        final DocumentRange range = node.asRange();
        range.start = new Position();
        range.start.line = range.end.line;
        range.start.character = range.end.character - 1;
        return range;
    }

    protected void addMatch(Set<CompletionItem> results, Token startToken, Token endToken, String match, CompletionRequest index, CompletionOptions options) {
        if (endToken == null) {
            endToken = startToken;
        }
        StringBuilder completion = new StringBuilder();
        final String[] prefixes = options.getPrevTokens();
        final DocumentRange replacement = replaceTokens(startToken, endToken, index);
        if (prefixes != null) {
            String check = startToken.image.trim();
            for (final String prefix : prefixes) {
                if (prefix.trim().equals(check)) {
                    // keep the user's version of this token, and keep looking for required prefixes.
                    completion.append(startToken.image);
                    startToken = startToken.next;
                    check = startToken.image.trim();
                } else {
                    completion.append(prefix);
                }
            }
        }
        completion.append(match);

        final String[] suffixes = options.getNextTokens();
        if (suffixes != null && suffixes.length > 0) {
            String check = suffixes[0].trim();
            if (endToken.next != null && endToken.next.image.trim().equals(check)) {
                // the token after the end token matches the suffix.
                endToken = endToken.next;
            }
            boolean missing = false;
            for (String suffix : suffixes) {
                check = suffix.trim();
                String tokenCheck = endToken.image.trim();
                if (options.getStopTokens().contains(tokenCheck)) {
                    break;
                }
                if (!missing && tokenCheck.equals(check)) {
                    // the end token matches the suffix.  Use the user's image.
                    completion.append(endToken.image);
                    endToken = endToken.next;
                } else {
                    // the suffix is missing.  simply add it.
                    missing = true;
                    completion.append(suffix);
                }
            }
        }

        final String displayed = completion.toString();
        CompletionItem result = new CompletionItem(start, len, displayed, displayed, replacement);
        results.add(result);
    }
}
