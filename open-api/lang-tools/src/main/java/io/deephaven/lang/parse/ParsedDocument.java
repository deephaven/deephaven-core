package io.deephaven.lang.parse;

import io.deephaven.base.Lazy;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.*;
import io.deephaven.web.shared.fu.MappedIterable;
import io.deephaven.web.shared.ide.lsp.CompletionItem;
import io.deephaven.web.shared.ide.lsp.DocumentRange;
import io.deephaven.web.shared.ide.lsp.Position;
import io.deephaven.web.shared.ide.lsp.TextEdit;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Represents a parsed document.
 *
 * For now, we will be re-parsing the entire string document every time,
 * but in the future, we would like to be able to update only ranges of changed code.
 */
public class ParsedDocument {

    private static class AnchorNode extends SimpleNode {

        AnchorNode(int index) {
            super(-1);
            Token cheat = new Token(-1, "") {
                @Override
                public int compareTo(BaseToken o) {
                    if (o.getStartIndex() == index) {
                        return index - o.getEndIndex();
                    }
                    return index - o.getStartIndex();
                }
            };
            cheat.startIndex = index;
            cheat.endIndex = index;
            jjtSetFirstToken(cheat);
            jjtSetLastToken(cheat);
        }

        @Override
        public boolean containsIndex(int i) {
            return i == jjtGetFirstToken().startIndex;
        }

    }

    private static final Comparator<SimpleNode> CMP = (a, b) -> {
        if (a == b) {
            return 0;
        }
        final Token aStart = a.jjtGetFirstToken();
        final Token bStart = b.jjtGetFirstToken();

        int cmp;
        if (b instanceof AnchorNode) {
            // let the anchor node do the comparisons...
            Assert.eqFalse(a instanceof AnchorNode, "You may only use one AnchorNode at a time");
            cmp = -bStart.compareTo(aStart);
        } else {
            cmp = aStart.compareTo(bStart);
        }
        if (cmp != 0) {
            return cmp;
        }
        final Token aEnd = a.jjtGetLastToken();
        final Token bEnd = b.jjtGetLastToken();
        if (b instanceof AnchorNode) {
            cmp = -bEnd.compareTo(aEnd);
        } else {
            cmp = aEnd.compareTo(bEnd);
        }
        if (cmp != 0) {
            return cmp;
        }


        // bah.  whoever is the most derived should go _last_.
        // We will be looking backwards from cursor, so we should want the end-most item
        // to be the final node, to easily find it later...

        if (a.isChildOf(b)) {
            return 1;
        }

        assert b.isChildOf(a) :
            "Nodes occupying the same tokenspace were not in a parent-child relationship "
                + a + " does not contain " + b + " (or vice versa)";

        return -1;
    };
    /**
     * TODO: enforce clients to only send \n.  We don't want to mess around with \r\n taking up two chars.  IDS-1517-26
     */
    private static final Pattern NEW_LINE_PATTERN = Pattern.compile("\\r?\\n");
    private final ChunkerDocument doc;

    private final Lazy<Set<ChunkerStatement>> statements;
    private final String src;
    private String errorSource;
    private ParseException error;
    private final Map<DocumentRange, Position> computedPositions;
    private final Map<String, List<ChunkerAssign>> assignments;

    public ParsedDocument(ChunkerDocument doc, String document) {
        this.doc = doc;
        this.src = document;
        computedPositions = new ConcurrentHashMap<>(4);
        assignments = new ConcurrentHashMap<>(12);
        statements = new Lazy<>(()->{
            final LinkedHashSet<ChunkerStatement> stmts = new LinkedHashSet<>();
            doc.childrenAccept(new ChunkerDefaultVisitor(){
                @Override
                public Object visitChunkerStatement(ChunkerStatement node, Object data) {
                    stmts.add(node);
                    // only want to add top level statements; we don't have a good solution for nested ranges yet.
                    return null;
                }
            }, null);
            return stmts;
        });
    }

    public Node findNode(int p) {

        if (doc.jjtGetFirstToken() == doc.jjtGetLastToken() && doc.jjtGetFirstToken().kind == ChunkerConstants.EOF) {
            return doc;
        }

        // our nodes are 0-indexed...
        final Node best;
        if (p >= doc.getEndIndex()) {
            // cursor is at the end of the document.  Just find the deepest tail node.
            best = findLast(doc, Math.max(0, Math.min(p-1, doc.getEndIndex())));
        } else {
            best = findDeepest(doc, Math.max(0, p));
        }

        return best;
    }

    private Node findDeepest(Node best, int i) {
        Assert.leq(best.getStartIndex(), "node.startIndex", i);
        Assert.geq(best.getEndIndex(), "node.endIndex", i);
        // TODO: consider binary search here instead of linear;
        //  it is likely better to actually search linearly from the end
        //  since user is most likely to be editing at the end.  IDS-1517-27
        for (int c = best.jjtGetNumChildren();
             c --> 0;
        ) {
            final Node child = best.jjtGetChild(c);
            if (child.containsIndex(i) ||
                (best instanceof ChunkerStatement && i == child.getEndIndex())) {
                return findDeepest(child, i);
            }
        }
        // none of our children (if any) contained the specified index; return us.
        return best;
    }
    private Node findLast(Node best, int i) {
        Assert.leq(best.getStartIndex(), "node.startIndex", i);
        Assert.geq(best.getEndIndex(), "node.endIndex", i);

        int c = best.jjtGetNumChildren() - 1;
        final Node child = best.jjtGetChild(c);
        if (child.getEndIndex() >= i) {
            if (child.jjtGetNumChildren() == 0) {
                return child;
            }
            return findLast(child, i);
        }
        return best;
    }

    public ChunkerDocument getDoc() {
        return doc;
    }

    /**
     * When a parse fails, we do not throw away our last-good document.
     *
     * We do, however, record the failure information,
     * which you should check via {@link #isError()}.
     *
     * @param src The source with an error
     * @param e The parse exception.  May make this any exception type.
     * @return this, for chaining.  We may need to make copies later, but for now, we'll use it as-is.
     */
    public ParsedDocument withError(String src, ParseException e) {
        this.errorSource = src;
        this.error = e;
        return this;
    }

    public String getSource() {
        return errorSource == null ? src : errorSource;
    }

    public String getLastGoodSource() {
        return src;
    }

    public boolean isError() {
        return errorSource != null || error != null;
    }

    public void resetFailure() {
        this.errorSource = null;
        this.error = null;
    }

    public void logErrors(Logger log) {
        if (error != null) {
            log.warn("Invalid document; use trace logging for more details", error);
            log.trace(errorSource);
        }
    }

    @Override
    public String toString() {
        return "ParsedDocument{" +
            "doc=" + doc +
            ", errorSource='" + errorSource + '\'' +
            ", error=" + error +
            '}';
    }

    public Position findEditRange(DocumentRange replaceRange) {
        final Token end = doc.jjtGetLastToken();
        // Our document and thus, our token is 1-indexed
        // Our range which we send back through lsp protocol is 0-indexed.
        // So, this less than is really <= after correcting array indexing.
        assert replaceRange.end.line < end.endLine;
        assert replaceRange.end.character <= end.endColumn;

        // Most definitely want to cache this very expensive operation.
        return computedPositions.computeIfAbsent(replaceRange, r-> findFromNodes(replaceRange, doc));
    }

    private Position findFromNodes(DocumentRange replaceRange, Node startNode) {
        return findFromNodes(replaceRange, startNode, null);
    }
    private Position findFromNodes(DocumentRange replaceRange, Node startNode, Node endNode) {

        if (startNode.jjtGetNumChildren() == 0) {
            // we are the winner node!
            if (endNode != startNode) {
                endNode = refineEndNode(replaceRange, endNode == null ? startNode : endNode);
            }
            assert startsBefore(startNode, replaceRange.start);
            // Note, we're intentionally sending the first token of both the start and end node,
            // as we need to search forward-only when examining tokens.
            return findFromTokens(replaceRange, startNode.jjtGetFirstToken(), endNode.jjtGetFirstToken());
        } else {
            // we are going to look through children backwards,
            // as the user is most likely to be editing the end of the document.
            for (Node kid : MappedIterable.reversed(startNode.getChildren())) {
                // look backwards until we find the starting range
                // we eventually rely on tokens instead of nodes,
                // which only have forward links; thus, we must anchor to start node.
                if (endNode == null || endNode == startNode) {
                    if (startsBefore(kid, replaceRange.end)) {
                        endNode = kid;
                    }
                }
                if (startsBefore(kid, replaceRange.start)) {
                    return findFromNodes(replaceRange, kid, endNode);
                }
            }
            if (endNode != startNode) {
                endNode = refineEndNode(replaceRange, endNode == null ? startNode : endNode);
            }
            return findFromTokens(replaceRange, startNode.jjtGetFirstToken(), endNode.jjtGetFirstToken());
        }
    }

    private boolean startsBefore(Node kid, Position start) {
        return kid.jjtGetFirstToken().positionStart().lessOrEqual(start);
    }

    private Node refineEndNode(DocumentRange replaceRange, Node endNode) {
        if (endNode.jjtGetNumChildren() == 0) {
            return endNode;
        }
        for (Node kid : MappedIterable.reversed(endNode.getChildren())) {
            if (startsBefore(kid, replaceRange.end)) {
                return refineEndNode(replaceRange, kid);
            }
        }
        return endNode;
    }

    @SuppressWarnings("Duplicates")
    private Position findFromTokens(DocumentRange replaceRange, Token start, Token end) {
        // while it would be nice to actually iterate backwards here,
        // we only maintain forward links, and it would be a hassle / O(n) operation
        // to setup backlinks.
        final Position startPos = start.positionStart();
        final Position endPos = end.positionStart();
        // both asserts are >= because both start and end are the earliest token-containing-our-range we could find.
        assert replaceRange.start.greaterOrEqual(startPos);
        assert replaceRange.end.greaterOrEqual(endPos);

        int startInd = findFromToken(replaceRange.start, start, true);
        int endInd = findFromToken(replaceRange.end, end, false);
        return new Position(startInd, endInd);
    }

    private int findFromToken(Position pos, Token tok, boolean start) {
        final Token startTok = tok;
        if (start && tok.positionStart().equals(pos)) {
            return tok.tokenBegin;
        }
        if (!start && tok.positionEnd(true).equals(pos)) {
            return tok.tokenBegin + Math.min(1, tok.image.length());
        }
        while (tok.next != null) {
            if (tok.containsPosition(pos)) {
                int ind = tok.tokenBegin;//start ? tok.tokenBegin : tok.startIndex;
                final Position candidate = tok.positionStart();
                final String[] lines = NEW_LINE_PATTERN.split(tok.image);
                // multi-line tokens rare, but not illegal.
                for (int linePos = 0; linePos < lines.length; linePos++) {
                    String line = lines[linePos];
                    if (candidate.line == pos.line) {
                        // we're down to the same line.
                        ind += pos.character - candidate.character;
                        return ind;
                    } else {
                        candidate.line++;
                        candidate.character = 0;
                        // TODO: make monaco force \n only instead of \r\n, and blow up if client gives us \r\ns
                        //  so the +1 we are doing here for the line split is always valid. IDS-1517-26
                        ind += line.length() + 1;
                    }
                }
                return ind;
            } else {
                tok = tok.next;
            }
        }
        throw new IllegalArgumentException("Token " + startTok + " does not contain position " + pos);
    }

    public List<ChunkerAssign> findAssignment(CompletionRequest request, String name) {
        if (assignments.isEmpty()) {
            fillAssignments();
        }
        final List<ChunkerAssign> options = assignments.get(name), results = new ArrayList<>();
        if (options != null) {
            assert !options.isEmpty();
            final ListIterator<ChunkerAssign> itr = options.listIterator(options.size());
            while (itr.hasPrevious()) {
                final ChunkerAssign test = itr.previous();
                if (test.getStartIndex() <= request.getOffset()) {
                    results.add(test);
                }
            }
            return results;
        }
        return Collections.emptyList();
    }

    private void fillAssignments() {
        doc.jjtAccept(new ChunkerDefaultVisitor() {
            @Override
            public Object visitChunkerAssign(ChunkerAssign node, Object data) {
                assignments.computeIfAbsent(node.getName(), n->new ArrayList<>()).add(node);
                return super.visitChunkerAssign(node, data);
            }

            @Override
            public Object visitChunkerTypedAssign(ChunkerTypedAssign node, Object data) {
                assignments.computeIfAbsent(node.getName(), n->new ArrayList<>()).add(node);
                return super.visitChunkerTypedAssign(node, data);
            }
        }, null);
    }

    public void extendEnd(CompletionItem item, Position requested, Node node) {
        Token tok = node.findToken(item.textEdit.range.end);
        while (item.textEdit.range.end.lessThan(requested)) {
            tok = extendEnd(tok, requested, item);
            if (tok == null) {
                item.textEdit.range.end = requested.plus(0, 1);
                break;
            }
        }
    }

    private Token extendEnd(Token tok, Position requested, CompletionItem edit) {
        if (tok.beginLine == tok.endLine) {
            // most common case (almost everything)
            int moved = edit.textEdit.range.end.extend(tok.positionEnd());
            String txt = tok.image;
            edit.textEdit.text += txt.substring(txt.length() - moved);
            return tok.next;
        } else {
            // ick.  multi-line tokens are the devil.
            // we should probably reduce this to be only-the-newline-token,
            // and instead run more productions on the contents of multi-line strings (the other possible line-spanning token)
            throw new UnsupportedOperationException("Multi-line edits not supported yet; cannot extendEnd over " + tok);
        }
    }

    public void extendStart(CompletionItem item, Position requested, Node node) {
        throw new UnsupportedOperationException("No extendStart support yet");
    }

    public TextEdit sliceBefore(CompletionItem item, Position requested, Node node) {
        final TextEdit edit = new TextEdit();
        final DocumentRange range = item.textEdit.range;
        Token tok = node.findToken(range.start);
        Position start = tok.positionStart();
        if (start.line != requested.line || range.start.line != requested.line) {
            // not going to worry about this highly unlikely and complex corner case just yet.
            return null;
        }
        // advance the position to the start of the replacement range.
        int imageInd = 0;
        while (start.lessThan(range.start)) {
            start.character++;
            imageInd++;
        }
        edit.range = new DocumentRange(start.copy(), requested);
        StringBuilder b = new StringBuilder();
        // now, from here, gobble up the token contents as we advance the position to the requested index.
        while (start.lessThan(requested)) {
            if (tok.positionEnd(false).lessOrEqual(start)) {
                tok = tok.next;
                imageInd = 0;
                start = tok.positionStart();
            }
            start.character++;
            b.append(tok.image.charAt(imageInd++));
        }
        edit.text = b.toString();
        return edit;
    }
}
