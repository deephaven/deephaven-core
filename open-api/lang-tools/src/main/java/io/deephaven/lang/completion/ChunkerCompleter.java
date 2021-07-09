package io.deephaven.lang.completion;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.QueryScope.MissingVariableException;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.VariableProvider;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.api.HasScope;
import io.deephaven.lang.api.IsScope;
import io.deephaven.lang.completion.results.*;
import io.deephaven.lang.generated.*;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.lang.parse.LspTools;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.proto.backplane.script.grpc.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Uses a ChunkerDocument to lookup user cursor and perform autocompletion.
 */
public class ChunkerCompleter implements CompletionHandler<ParsedDocument> {

    public enum SearchDirection {
        LEFT, BOTH, RIGHT
    }

    private static final Pattern CAMEL_PATTERN = Pattern.compile("(?=\\p{Lu})");
    public static final String CONTAINS_NEWLINE = ".*\\R.*"; // \R is "any newline" in java 8
    // For now, only testing uses this property, when we want to test column expressions without the noise of static methods
    // Since we don't really expect clients to twiddle this, we only consider it as a system property.
    public static final String PROP_SUGGEST_STATIC_METHODS = "suggest.all.static.methods";

    private final Logger log;
    private final VariableProvider variables;
    private final CompletionLookups lookups;
    private String defaultQuoteType;
    private ParsedDocument doc;

    public ChunkerCompleter(final Logger log, VariableProvider variables) {
        this(log, variables, new CompletionLookups());
    }

    public ChunkerCompleter(final Logger log, VariableProvider variables, CompletionLookups lookups) {
        this.log = log;
        this.variables = variables;
        this.lookups = lookups;
    }

    public CompletableFuture<? extends Collection<CompletionFragment>> complete(String command, int offset) {
        final long start = System.nanoTime();
        CompletionParser parser = new CompletionParser();
        try {
            final ParsedDocument doc = parser.parse(command);
            // Remove the parsing time from metrics; this parsing should be done before now,
            // with a parsed document ready to go immediately.
            final long parseTime = System.nanoTime();
            return CompletableFuture.supplyAsync(()-> {
                final Set<CompletionItem> results = runCompletion(doc, offset);
                final long end = System.nanoTime();
                log.info()
                    .append("Found ")
                    .append(results.size())
                    .append(" completion items;\nparse time:\t")
                    .append(parseTime-start)
                    .append("nanos;\ncompletion time: ")
                    .append(end-parseTime)
                    .append("nanos")
                    .endl();
                return results.stream()
                    .map(this::toFragment)
                    .collect(Collectors.toList());
            });
        } catch (ParseException e) {
            final CompletableFuture<? extends Collection<CompletionFragment>> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            // TODO: better logging here; preferably writing to a system diagnostics table.  IDS-1517-16
            log.info()
                .append("Parse error in experimental completion api")
                .append(e)
                .append(" found in source\n")
                .append(command)
                .endl();
            return future;
        }
    }

    private CompletionFragment toFragment(CompletionItemOrBuilder item) {
        return new CompletionFragment(item.getStart(), item.getLength(), item.getTextEdit().getText(), item.getLabel());
    }

    @Override
    public Collection<CompletionItem.Builder> runCompletion(final ParsedDocument doc, final Position pos, final int offset) {
        final List<CompletionItem.Builder> results = new ArrayList<>();
        this.doc = doc;

        String src = doc.getSource();
        final Node node = doc.findNode(offset);
        if (node instanceof ChunkerDocument || src.trim().isEmpty()) {
            return results;
        }
        CompletionRequest req = new CompletionRequest(this, src, offset);
        searchForResults(doc, results, node, req, SearchDirection.BOTH);

        // post-process results for cases when the suggestion does not
        // actually replace the user's cursor position (it's much easier to do this
        // once in a universal fashion than to put it on each completer to figure out).
        // TODO: make tests route through this methods to check on fixRanges behavior.
        //  The CompletionFragment (autocomplete V1) methods used by tests are better
        //  for checking the result string, since they require less insanity to reconstitute
        //  a plain result string to assert upon.  IDS-1517-21
        fixRanges(doc, results, node, pos);

        if ("true".equals(System.getProperty("test.monaco.sanity"))) {
//            // We may want to manually test what monaco does with various formats of result objects,
//            // so we have this block of code hidden behind an off-by-default system property
//            results.add(new CompletionItem(0, 0, "A1", "A1", new DocumentRange(
//                pos.plus(0, -2), pos
//            )).sortText("A1"));
//
//            results.add(new CompletionItem(0, 0, "A2", "A2", new DocumentRange(
//                pos.plus(0, -2), pos.plus(0, 1)
//            )).sortText("A2"));
//
//            results.add(new CompletionItem(0, 0, "B3", "B3", new DocumentRange(
//                pos.plus(0, -2), pos.plus(0, 2)
//            )).sortText("A3"));
//
//            results.add(new CompletionItem(0, 0, "B1", "B1", new DocumentRange(
//                pos, pos
//            )).sortText("B1"));
//
//            results.add(new CompletionItem(0, 0, "B2", "B2", new DocumentRange(
//                pos, pos.plus(0, 1)
//            )).sortText("B2"));
//
//            results.add(new CompletionItem(0, 0, "A3", "A3", new DocumentRange(
//                pos, pos.plus(0, 2)
//            )).sortText("B3"));
        }

        return results;
    }
    /**
     * Part 1 of the V2 completion api; adapting our API to fit into existing CompletionHandler semantics.
     *
     * Right now we are just blindly re-parsing the whole document when using the old api,
     * which is going to be good-enough-for-now; this may also allow us to borrow the existing unit tests to some degree.
     *
     * @param doc
     * @param offset
     * @return
     */
    public Set<CompletionItem> runCompletion(ParsedDocument doc, int offset) {
        this.doc = doc;

        final Node node = doc.findNode(offset);

        String src = doc.getSource();
        if (node instanceof ChunkerDocument || src.trim().isEmpty()) {
            return Collections.emptySet();
        }
        final List<CompletionItem.Builder> results = new ArrayList<>();
        CompletionRequest req = new CompletionRequest(this, src, offset);
        searchForResults(doc, results, node, req, SearchDirection.BOTH);

        final Set<CompletionItem> built = new LinkedHashSet<>();
        for (CompletionItem.Builder result : results) {
            built.add(result.build());
        }
        return built;
    }

    /**
     * So, despite no documentation describing the exact semantics,
     * monaco is _very_ picky about the format of completions it receives.
     *
     * The list of invariants we have to obey:
     *
     * The main text edit (Range and String) is stored on the CompletionItem itself.
     * The main Range must _start_ at the user's cursor, and end at the earlier of:
     * A) The end of the text we want to replace
     * B) The end of line where the cursor is.
     *
     * Any other changes we want must be placed into the additionalTextEdits fields.
     * This includes:
     * A) Any changes on the same line as cursor that come before the cursor.
     * B) Any changes on lines where the cursor is not currently placed.
     *
     * Due to this unfortunate complexity, we are doing this in a post-processing phase (here),
     * so that individual completion provider logic only has to handle "replace text X with Y",
     * and we'll figure out the correct incantation to keep monaco happy here.
     *  @param parsed The parsed document (bag of state related to the source document).
     * @param res A set of CompletionItem to fixup.
     * @param node The original node we started the search from (for ~O(1) token searching)
     * @param cursor The position of the user's cursor (the location we need to slice from)
     */
    private void fixRanges(
            ParsedDocument parsed,
            Collection<CompletionItem.Builder> res,
            Node node,
            Position cursor
    ) {
        int ind = 0;
        for (CompletionItem.Builder item : res) {
            final Position requested = cursor.toBuilder().build();
            item.setSortText(sortable(ind++));
            final DocumentRange result = item.getTextEdit().getRange();

            if (LspTools.greaterThan(result.getStart(), requested)) {
                // The result starts after the user's cursor.
                // adjust the text edit back, stopping at the cursor.
                throw new UnsupportedOperationException("No extendStart support yet");
            } else if (LspTools.lessThan(result.getEnd(), requested)) {
                // adjust the text edit forwards, appending tokens to result.
                extendEnd(item, requested, node);
            } else if (LspTools.lessThan(result.getStart(), requested)) {
                // The result starts before the cursor.
                // Move the part up to the cursor into an additional edit.
                final TextEdit.Builder edit = sliceBefore(item, requested, node);
                if (edit == null) {
                    // could not process this edit.  TODO: We should log this case at least.  IDS-1517-31
                    continue;
                }
                item.addAdditionalTextEdits(edit);
            } else {
                assert result.getStart().equals(requested);
            }

            // now, if the result spans multiple lines, we need to break it into multiple ranges,
            // since monaco only supports same-line text edits.
            if (result.getStart().getLine() != result.getEnd().getLine()) {
                List<TextEdit> broken = new ArrayList<>();
                // TODO: also split up the additional text edits, once they actually support multiline operations.  IDS-1517-31

            }
            item.setLabel(item.getTextEdit().getText());
        }

    }
    public TextEdit.Builder sliceBefore(CompletionItem.Builder item, Position requested, Node node) {
        final TextEdit.Builder edit = TextEdit.newBuilder();
        final DocumentRange.Builder range = DocumentRange.newBuilder(item.getTextEditBuilder().getRange());
        Token tok = node.findToken(range.getStart());
        Position.Builder start = tok.positionStart();
        if (start.getLine() != requested.getLine() || range.getStart().getLine() != requested.getLine()) {
            // not going to worry about this highly unlikely and complex corner case just yet.
            return null;
        }
        // advance the position to the start of the replacement range.
        int imageInd = 0;
        while (LspTools.lessThan(start, range.getStart())) {
            start.setCharacter(start.getCharacter() + 1);
            imageInd++;
        }
        range.setStart(start.build()).setEnd(requested);
        edit.setRange(range);
        StringBuilder b = new StringBuilder();
        // now, from here, gobble up the token contents as we advance the position to the requested index.
        while (LspTools.lessThan(start, requested)) {
            if (LspTools.lessOrEqual(tok.positionEnd(false), start)) {
                // find next non-empty token
                final Token startTok = tok;
                while (tok.next != null) {
                    tok = tok.next;
                    if (!tok.image.isEmpty()) {
                        break;
                    }
                }
                if (tok != startTok) {
                    // shouldn't really happen, but this is way better than potential infinite loops of doom
                    break;
                }
                imageInd = 0;
                start = tok.positionStart();
            }
            start.setCharacter(start.getCharacter() + 1);
            if (!tok.image.isEmpty()) {
                b.append(tok.image.charAt(imageInd));
            }
            imageInd++;
        }
        edit.setText(b.toString());
        return edit;
    }

    private TextEdit.Builder extendEnd(final CompletionItem.Builder item, final Position requested, final Node node) {
        final TextEdit.Builder edit = TextEdit.newBuilder();
        final DocumentRange.Builder range = DocumentRange.newBuilder(item.getTextEditBuilder().getRange());
        Token tok = node.findToken(range.getStart());
        Position.Builder start = tok.positionStart();
        if (start.getLine() != requested.getLine() || range.getStart().getLine() != requested.getLine()) {
            // not going to worry about this highly unlikely and complex corner case just yet.
            return null;
        }
        // advance the position to the start of the replacement range.
        int imageInd = 0;
        while (LspTools.lessThan(start, range.getStart())) {
            start.setCharacter(start.getCharacter() + 1);
            imageInd++;
        }
        range.setStart(start.build()).setEnd(requested);
        edit.setRange(range);
        StringBuilder b = new StringBuilder();
        // now, from here, gobble up the token contents as we advance the position to the requested index.
        while (LspTools.lessThan(start, requested)) {
            if (LspTools.lessOrEqual(tok.positionEnd(false), start)) {
                // find next non-empty token
                final Token startTok = tok;
                while (tok.next != null) {
                    tok = tok.next;
                    if (!tok.image.isEmpty()) {
                        break;
                    }
                }
                if (tok != startTok) {
                    // shouldn't really happen, but this is way better than potential infinite loops of doom
                    break;
                }
                imageInd = 0;
                start = tok.positionStart();
            }
            start.setCharacter(start.getCharacter() + 1);
            if (!tok.image.isEmpty()) {
                b.append(tok.image.charAt(imageInd));
            }
            imageInd++;
        }
        edit.setText(b.toString());
        return edit;
    }


    private String sortable(int i) {
        StringBuilder res = new StringBuilder(Integer.toString(i, 36));
        while (res.length() < 5) {
            res.insert(0, "0");
        }
        return res.toString();
    }

    private void searchForResults(
            ParsedDocument doc,
            Collection<CompletionItem.Builder> results,
            Node node,
            CompletionRequest request,
            SearchDirection direction
    ) {
        // alright! let's figure out where the user's cursor is, and what we can help them with.
        node.jjtAccept(new ChunkerVisitor() {
            @Override
            public Object visit(SimpleNode node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerDocument(ChunkerDocument node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerStatement(ChunkerStatement node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerJavaClassDecl(ChunkerJavaClassDecl node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerAssign(ChunkerAssign node, Object data) {
                assignCompletion(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerTypedAssign(ChunkerTypedAssign node, Object data) {
                typedAssignCompletion(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerTypeDecl(ChunkerTypeDecl node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerTypeParams(ChunkerTypeParams node, Object data) {
                typeParamsCompletion(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerTypeParam(ChunkerTypeParam node, Object data) {
                typeParamCompletion(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerIdent(ChunkerIdent node, Object data) {
                identCompletion(doc, results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerNum(ChunkerNum node, Object data) {
                numCompletion(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerWhitespace(ChunkerWhitespace node, Object data) {
                whitespaceComplete(doc, results, node, request, direction);
                return null;
            }

            @Override
            public Object visitChunkerNewline(ChunkerNewline node, Object data) {
                whitespaceComplete(doc, results, node, request, direction);
                return null;
            }

            @Override
            public Object visitChunkerNew(ChunkerNew node, Object data) {
                newComplete(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerAnnotation(ChunkerAnnotation node, Object data) {
                annotationComplete(results, node, request);
                return null;
            }

            @Override
            public Object visitChunkerInvoke(ChunkerInvoke node, Object data) {
                invokeComplete(results, node, request, direction);
                return null;
            }

            @Override
            public Object visitChunkerMethodName(ChunkerMethodName node, Object data) {
                final Token tok = node.jjtGetFirstToken();
                assert tok == node.jjtGetLastToken();
                addMethodsAndVariables(results, tok, request, ((HasScope)node.jjtGetParent()).getScope(), tok.image.replace("(", ""));
                if (request.getOffset() >= tok.endIndex - 1) {
                    // The user's cursor is on the opening ( of an invocation, lets add method arguments to completion results as well.
                    ChunkerInvoke invoke = (ChunkerInvoke) node.jjtGetParent();
                    methodArgumentCompletion(invoke.getName(), results, invoke, invoke.getArgument(0), request, direction);
                }
                return null;
            }

            @Override
            public Object visitChunkerParam(ChunkerParam node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerClosure(ChunkerClosure node, Object data) {
                // not supporting completion for closures just yet; can likely offer parameter suggestions later though.
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerArray(ChunkerArray node, Object data) {
                // when we're in an array, we should suggest "anything of the same type as other array elements",
                // or otherwise look at where this array is being assigned to determine more type inference we can do for suggestion.
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerBinaryExpression(ChunkerBinaryExpression node, Object data) {
                // if we're actually in the binary expression, it's likely that we're on the operator itself.
                // for now, we'll try searching both left and right, and if we get unwanted matches, we'll reduce our scope.
                switch (direction) {
                    case BOTH:
                    case LEFT:
                        searchForResults(doc, results, node.getLeft(), request.candidate(node.getLeft().getEndIndex()), SearchDirection.LEFT);
                }
                if (node.getRight() != null) {
                    switch (direction) {
                        case BOTH:
                        case RIGHT:
                            searchForResults(doc, results, node.getRight(), request.candidate(node.getRight().getStartIndex()), SearchDirection.RIGHT);
                    }
                }
                return null;
            }

            @Override
            public Object visitChunkerString(ChunkerString node, Object data) {
                // Cursor is inside of a string; figure out where this string is being assigned.
                stringComplete(results, node, request, direction);
                return null;
            }

            @Override
            public Object visitChunkerEof(ChunkerEof node, Object data) {
                // Later on, we'll use the fact that we know the user is typing something
                // that is likely invalid, and offer suggestions like variable names, etc.
                whitespaceComplete(doc, results, node, request, SearchDirection.LEFT);
                return null;
            }
        }, null);

    }

    private void annotationComplete(Collection<CompletionItem.Builder> results, ChunkerAnnotation node, CompletionRequest offset) {
        // suggest names of annotations / arguments for groovy...
        // while python should suggest the names of decorator functions only.
    }

    private RuntimeException unsupported(Node node) {
        throw new UnsupportedOperationException("Node type " + node.getClass() + " not yet supported: " + node.toSource());
    }

    private void numCompletion(Collection<CompletionItem.Builder> results, ChunkerNum node, CompletionRequest offset) {
        // not really sure what, if anything, we'd want for numbers.
        // perhaps past history of number values entered / typed in?
    }

    private void assignCompletion(Collection<CompletionItem.Builder> results, ChunkerAssign node, CompletionRequest offset) {
        final CompleteAssignment completer = new CompleteAssignment(this, node);
        final Node value = node.getValue();
        if (value == null) {
            // no value for this assignment...  offer up anything from scope.
            FuzzyList<String> sorted = new FuzzyList<>("");

            for (String varName : variables.getVariableNames()) {
                sorted.add(varName, varName);
            }
            for (String varName : sorted) {
                completer.doCompletion(results, offset, varName, false);
            }

            // TODO: also consider offering static classes / non-void-methods or block-local-scope vars.
            //  This would get really crazy really fast w/ no filter,
            //  so maybe we'll just keep a cache of user-actually-used-these classes/methods/etc, and offer only those
            //  (possibly primed with "things we want to promote users seeing").  IDS-1517-22
        } else {
            // we only want to suggest variable names beginning with the next token
            final String startWith = value.jjtGetFirstToken().image;
            FuzzyList<String> sorted = new FuzzyList<>(startWith);
            // TODO: actually use a visitor here; really only Ident tokens should get the behavior below;
            //  Really, we should be adding all variable names like we do, then visiting all source,
            //  removing anything which occurs later than here in source, and adding any assignments which
            //  occur earlier in source-but-not-in-runtime-variable-pool. IDS-1517-23
            for (String varName : variables.getVariableNames()) {
                if (camelMatch(varName, startWith)) {
                    sorted.add(varName, varName);
                }
            }
            for (String varName : sorted) {
                completer.doCompletion(results, offset, varName, false);
            }

        }
    }

    private void typedAssignCompletion(Collection<CompletionItem.Builder> results, ChunkerTypedAssign node, CompletionRequest offset) {
    }

    private void typeParamsCompletion(Collection<CompletionItem.Builder> results, ChunkerTypeParams node, CompletionRequest offset) {
    }

    private void typeParamCompletion(Collection<CompletionItem.Builder> results, ChunkerTypeParam node, CompletionRequest offset) {

    }

    private void identCompletion(
        ParsedDocument doc,
        Collection<CompletionItem.Builder> results,
        ChunkerIdent node,
        CompletionRequest request
    ) {
        boolean onEnd = node.jjtGetFirstToken().getEndIndex() <= request.getOffset();
        if (onEnd) {
            // user cursor is on the . or at the end of a possibly.chained.expression
            // we should offer completions for our target...
            final Node target = node.getScopeTarget();
            if (target == null) {
                // offer completion by looking at our own scope, and either completing
                // from available fields/methods, or global variables
                Token replacing = findReplacement(node, request);
                addMethods(results, replacing, request, Collections.singletonList(node), "");
            } else {
                // offer completion as though cursor was after the .
                searchForResults(doc, results, target, request, SearchDirection.RIGHT);
            }
        } else {
            // user wants completion on the ident itself...
            String src = node.toSource();
            final Token tok = node.jjtGetFirstToken();
            src = src.substring(0, request.getCandidate() - tok.startIndex);
            addMethodsAndVariables(results, node.jjtGetFirstToken(), request, Collections.singletonList(node), src);
        }

    }

    private Token findReplacement(Node node, CompletionRequest request) {
        Token replacing = node.jjtGetFirstToken();
        while (replacing.next != null && replacing.next.startIndex < request.getCandidate()) {
            if (replacing.next.kind == ChunkerConstants.EOF) {
                return replacing;
            }
            replacing = replacing.next;
        }
        return replacing;
    }

    private void whitespaceComplete(
        ParsedDocument doc,
        Collection<CompletionItem.Builder> results,
        SimpleNode node,
        CompletionRequest req,
        SearchDirection direction
    ) {
        // when the cursor is on whitespace, we'll look around from here to find something to bind to...
        // for now, we'll be lazy, and just move the cursor to our non-whitespace neighbors...
        final int nextLeft = node.getStartIndex()-1;
        final int nextRight = node.getEndIndex()+1;
        switch (direction) {
            case LEFT:
                Node left = findLeftOf(node);
                if (left != null) {
                    searchForResults(doc, results, left, req.candidate(left.getEndIndex()), SearchDirection.LEFT);
                }
                break;
            case RIGHT:
                Node right = findRightOf(node);
                if (right != null) {
                    searchForResults(doc, results, right, req.candidate(right.getStartIndex()), SearchDirection.RIGHT);
                }
                break;
            case BOTH:
                // look in both directions.
                left = findLeftOf(node);
                right = findRightOf(node);
                if (left == null) {
                    if (right != null) {
                        searchForResults(doc, results, right, req.candidate(nextRight), SearchDirection.LEFT);
                    }
                } else { //  left is non-null
                    if (right == null) {

                        searchForResults(doc, results, left, req.candidate(nextLeft), SearchDirection.LEFT);
                    } else {
                        // both left and right are non-null.  Pick the closest one first.
                        if (req.getCandidate() - nextLeft > nextRight - req.getCandidate()) {
                            // right pos is closer, so we'll start there.
                            searchForResults(doc, results, right, req.candidate(nextRight), SearchDirection.RIGHT);
                            searchForResults(doc, results, left, req.candidate(nextLeft), SearchDirection.LEFT);
                        } else {
                            // cursor is closer to left side, so start there.
                            searchForResults(doc, results, left, req.candidate(nextLeft), SearchDirection.LEFT);
                            searchForResults(doc, results, right, req.candidate(nextRight), SearchDirection.RIGHT);
                        }
                    }
                }
                break;
        }
    }

    private Node findRightOf(Node node) {
        if (node == null) {
            return null;
        }
        final Node parent = node.jjtGetParent();
        if (parent == null) {
            return null;
        }
        // short-circuit for ast nodes that return values.
        if (isTerminal(parent)) {
            return parent;
        }
        final List<Node> children = parent.getChildren();
        final int index = children.indexOf(node);
        if (index == children.size()-1) {
            return findRightOf(parent.jjtGetParent());
        }
        Node next = children.get(index + 1);
        while (!isTerminal(next) && next.jjtGetNumChildren() > 0) {
            next = next.jjtGetChild(0);
        }
        return next;

    }

    private boolean isTerminal(Node node) {
        return node.isAutocompleteTerminal();
    }

    private Node findLeftOf(Node node) {
        if (node == null) {
            return null;
        }
        final Node parent = node.jjtGetParent();
        if (parent == null) {
            return null;
        }
        if (isTerminal(parent)) {
            return parent;
        }
        final List<Node> children = parent.getChildren();
        final int index = children.indexOf(node);
        if (index == 0) {
            return findLeftOf(parent.jjtGetParent());
        }
        Node next = children.get(index - 1);
        while (!isTerminal(next) && next.jjtGetNumChildren() > 0) {
            Node candidate = next.jjtGetChild(next.jjtGetNumChildren() - 1);
            if (candidate instanceof ChunkerEof || // if we found the EOF, skip back again
                (candidate == node && next.jjtGetNumChildren() > 1) // if we ran into the original target node, skip it.
            ) {
                candidate = next.jjtGetChild(next.jjtGetNumChildren()-2);
            }
            next = candidate;
        }
        return next;
    }

    private void newComplete(Collection<CompletionItem.Builder> results, ChunkerNew node, CompletionRequest offset) {
        // `new ` completion not implemented yet.  This would need to lookup matching types w/ public constructors
    }

    private void invokeComplete(
        Collection<CompletionItem.Builder> results,
        ChunkerInvoke node,
        CompletionRequest request,
        SearchDirection direction
    ) {
        // invoke completions are one of the most important to consider.
        // for now, this will be a naive replacement, but later we'll want to look at _where_
        // in the invoke the cursor is; when on the ending paren, we'd likely want to look at
        // whether we are the argument to something, and if so, do a type check, and suggest useful .coercions().
        String name = node.getName().trim();

        // Now, for our magic-named methods that we want to handle...
        boolean inArguments = node.isCursorInArguments(request.getCandidate());
        boolean inMethodName = node.isCursorOnName(request.getCandidate());
        // when the cursor is between name(andParen, both of the above will trigger.

        // Find or create a "string as first arg" that will only be used if we match a magic method name below.
        if (inArguments) {
            Node firstArg = argNode(node, request);
            methodArgumentCompletion(name, results, node, firstArg, request, direction);
        }
        if (inMethodName) {
            // hokay! when on a method name, we need to look up our scope object,
            // try to guess the type, then look at existing arguments, to use as
            // context clues...
            final List<IsScope> scope = node.getScope();
            if (scope == null || scope.isEmpty()) {
                // TODO: figure out static-import-methods-declared-in-source, or closure references,
                // or locally defined methods / functions. IDS-1517-12
            } else {
                // For now, our scope resolution will just try to get us an object variable.
                // In the future, we'll create some kind of memoized meta information;
                // for now though, we just want to be able to find variables in our binding...
                String n = node.getName();
                addMethodsAndVariables(results, node.getNameToken(), request, scope, n);
            }
        }
    }

    private void addMethods(
        Collection<CompletionItem.Builder> results,
        Token replacing,
        CompletionRequest request,
        List<IsScope> scope,
        String methodPrefix
    ) {
        Optional<Class<?>> bindingVar = resolveScopeType(request, scope);
        if (bindingVar.isPresent()) {
            final Class<?> bindingClass = bindingVar.get();
            doMethodCompletion(results, bindingClass, methodPrefix, replacing, request);
        } else {
            // log that we couldn't find the binding var;
            // in theory we should be able to resolve anything that is valid source.
            log.trace()
                .append("Unable to find binding variable for ")
                .append(methodPrefix)
                .append(" from scope ")
                .append(scope.stream().map(IsScope::getName).collect(Collectors.joining(".")))
                .append( " from request ")
                .append(request.toString())
                .endl()
                ;
        }
    }

    private void addMethodsAndVariables(
        Collection<CompletionItem.Builder> results,
        Token replacing,
        CompletionRequest request,
        List<IsScope> scope,
        String variablePrefix
    ) {
        variablePrefix = variablePrefix.trim();
        // Add any method which make sense from the existing name / ident token.
        addMethods(results, replacing, request, scope, variablePrefix);
        // Add any variables present in current scope (will show system objects)
        doVariableCompletion(results, variablePrefix, replacing, request);

        // TODO: Add completion for any assignment expressions which occur earlier than us in the document.
        //  This will show only-in-source objects, without needing to actually run user's code.  IDS-1517-18
    }

    private void doMethodCompletion(
        Collection<CompletionItem.Builder> results,
        Class<?> bindingClass,
        String methodPrefix,
        Token replacing,
        CompletionRequest request
    ) {
        // hokay!  now, we can use the invoke's name to find methods / fields in this type.
        FuzzyList<Method> sorter = new FuzzyList<>(methodPrefix);
        for (Method method : bindingClass.getMethods()) {
            if (Modifier.isPublic(method.getModifiers())) {
                // TODO we'll likely want to pick between static or instance methods, based on calling scope.
                //   IDS-1517-19
                if (camelMatch(method.getName(), methodPrefix)) {
                    sorter.add(method.getName(), method);
                }
            }
        }

        CompleteInvocation completer = new CompleteInvocation(this, replacing);
        // and now, we can suggest those methods as replacements.
        for (Method method : sorter) {
            completer.doCompletion(results, request, method);
        }
    }

    private void doVariableCompletion(Collection<CompletionItem.Builder> results, String variablePrefix, Token replacing, CompletionRequest request) {
        FuzzyList<String> sorter = new FuzzyList<>(variablePrefix);
        for (String name : variables.getVariableNames()) {
            if (!name.equals(variablePrefix) && camelMatch(name, variablePrefix)) {
                // only suggest names which are camel-case-matches (ignoring same-as-existing-variable names)
                sorter.add(name, name);
            }
        }
        CompleteVarName completer = new CompleteVarName(this, replacing);
        for (String method : sorter) {
            completer.doCompletion(results, request, method);
        }
    }

    private boolean camelMatch(String candidate, String search) {
        final String[] items = CAMEL_PATTERN.split(search);
        if (items.length == 1) {
            return candidate.startsWith(search);
        }
        for (String camel : items) {
            if (camel.isEmpty()) {
                continue;
            }
            if (Character.isLowerCase(camel.charAt(0))) {
                if (!candidate.startsWith(camel)) {
                    return false;
                }
            } else {
                // this could be more specific, but there may actually
                // be cases where user transposes part of a name,
                // and they'd actually want to see BarFoo when asking for FooBar.
                // ...this should be implemented using a result score.
                if (!candidate.contains(camel)) {
                    return false;
                }
            }
        }
        return true;
    }

    private Node argNode(ChunkerInvoke node, CompletionRequest request) {
        if (node.getArgumentCount() == 0) {
            return null;
        }
        Node best = null;
        int score = 0;
        for (Node argument : node.getArguments()) {
            if (best == null) {
                best = argument;
                score = best.distanceTo(request.getOffset());
            } else {
                final int newScore = argument.distanceTo(request.getOffset());
                if (newScore < score) {
                    best = argument;
                }
            }
        }

        return best;
    }

    private void methodArgumentCompletion(
        String name, Collection<CompletionItem.Builder> results,
        ChunkerInvoke node,
        Node replaceNode,
        CompletionRequest request,
        SearchDirection direction
    ) {
        // TODO: replace this hardcoded list of magic method names with something generated by an annotation processor. IDS-1517-32
        boolean tableReturningMethod = false;
        switch (name) {
            case "join":
            case "naturalJoin":
            case "leftJoin":
            case "exactJoin":
            case "aj":
                // TODO: joins will need special handling; IDS-1517-5 example from Charles:
                // j=l.naturalJoin(r, "InBoth,AFromLeft=BFromRight,CInLeft=DFromRight", "EInOut=FromRight,FInOut")
                // as you see, we need both the scope table l and the joined table r,
                // then we also need to handle CSV-separated column expressions.
                // To even try this using string inspection is foolish, as a `,` or `=` could easily appear inside the expression.
                // For these reasons, proper support here will have to wait until we also parse the string contents;
                // we can parse them on-demand via Chunker#MethodArgs, and just get a list of assignments.
                // might actually be better to just make Chunker#JoinArgs, to specify CSV of assignments (and allow ` strings)
                break;
            case "where":

            case "sort":
            case "sortDescending":

            case "dropColumns":

            case "select":
            case "selectDistinct":
            case "view":
            case "update":
            case "updateView":

                tableReturningMethod = true;
                maybeColumnComplete(results, node, replaceNode, request, direction);
                break;
        }
        if (node.getEndIndex() < request.getOffset()) {
            // user's cursor is actually past the end of our method...
            Class<?> scopeType;
            if (tableReturningMethod) {
                scopeType = Table.class;
            } else {
                final Optional<Class<?>> type = resolveScopeType(request, node.getScope());
                if (!type.isPresent()) {
                    return;
                }
                scopeType = type.get();
            }
            final Token rep = findReplacement(node, request);
            doMethodCompletion(results, scopeType, "", rep, request);
        }
    }

    private Optional<Class<?>> resolveScopeType(
        CompletionRequest request, List<IsScope> scope
    ) {
        if (scope == null || scope.isEmpty()) {
            return Optional.empty();
        }
        IsScope o = scope.get(0);

        final Class<?> type = variables.getVariableType(o.getName());
        if (type != null) {
            return Optional.of(type);
        }
        Optional<Class<?>> result = resolveScope(scope)
            .map(Object::getClass);
        if (result.isPresent()) {
            return result;
        }
        switch (o.getName()) {
            // This used to be where we'd intercept certain well-known-service-variables, like "db";
            // leaving this here in case we have add and such service to the OSS completer.
        }
        // Ok, maybe the user hasn't run the query yet.
        // See if there's any named assign's that have a value of db.i|t|etc

        final List<ChunkerAssign> assignments = findAssignment(doc, request, o.getName());
        for (ChunkerAssign assignment : assignments) {
            // This is pretty hacky, but our only use case here is looking for table loads, so...
            if (assignment.getValue() instanceof IsScope) {
                final List<IsScope> subscope = ((IsScope)assignment.getValue()).asScopeList();
                if (!subscope.equals(scope)) {
                    result = resolveScopeType(request, subscope);
                    if (result.isPresent()) {
                        return result;
                    }
                }
            }
        }

        return Optional.empty();
    }

    public List<ChunkerAssign> findAssignment(final ParsedDocument doc, final CompletionRequest request, final String name) {
        final Map<String, List<ChunkerAssign>> assignments = ensureAssignments(doc);
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

    private Map<String, List<ChunkerAssign>> ensureAssignments(final ParsedDocument doc) {
        Map<String, List<ChunkerAssign>> assignments = doc.getAssignments();
        if (assignments.isEmpty()) {
            doc.getDoc().jjtAccept(new ChunkerDefaultVisitor() {
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
        return assignments;
    }

    private Optional<Object> resolveScope(List<IsScope> scope) {
        if (scope == null || scope.isEmpty()) {
            return Optional.empty();
        }
        IsScope o = scope.get(0);
        // TODO: also handle static classes / variables only present in source (code not run yet)  IDS-1517-23
        try {

            final Object var = variables.getVariable(o.getName(), null);
            if (var == null) {
                return Optional.empty();
            }
            return resolveSubScope(var, scope, 1);
        } catch (MissingVariableException e) {
            return Optional.empty();
        }
    }

    private Optional<Object> resolveSubScope(Object var, List<IsScope> scope, int i) {
        if (var == null || i == scope.size()) {
            return Optional.ofNullable(var);
        }
        // keep digging into scope to find a variable.  Since we are allowing groovy here,
        // we may need to do some reflective `field access to getter method` coercion automatically...
        String nextName = scope.get(i++).getName();
        Object next = getFromVar(var, nextName);
        return resolveSubScope(next, scope, i);
    }

    private Object getFromVar(Object var, String nextName) {
        Class src;
        Object inst;
        if (var instanceof Class) {
            src = (Class) var;
            inst = null;
        } else {
            inst = var;
            src = var.getClass();
        }
        try {
            final Field field = src.getField(nextName);
            return field.get(inst);
        } catch (Exception ignored) {}

        try {
            final Field field = src.getDeclaredField(nextName);
            field.setAccessible(true);
            return field.get(inst);
        } catch (Exception ignored) {}

        // now, thanks to groovy, we can also convert to getters...
        String getterName = toBeanMethod(nextName, "get");
        try {
            final Method method = src.getMethod(getterName);
            return method.invoke(inst);
        } catch (Exception ignored) {}
        try {
            final Method method = src.getDeclaredMethod(getterName);
            method.setAccessible(true);
            return method.invoke(inst);
        } catch (Exception ignored) {}

        getterName = toBeanMethod(nextName, "is");
        try {
            final Method method = src.getMethod(getterName);
            return method.invoke(inst);
        } catch (Exception ignored) {}
        try {
            final Method method = src.getDeclaredMethod(getterName);
            method.setAccessible(true);
            return method.invoke(inst);
        } catch (Exception ignored) {}

        // give up!
        return null;
    }

    private String toBeanMethod(String nextName, String prefix) {
        if (nextName.startsWith(prefix)) {
            return nextName;
        }
        nextName = Character.toUpperCase(nextName.charAt(0)) + (nextName.length() == 1 ? "" : nextName.substring(1));
        return prefix + nextName;
    }

    private void stringComplete(
        Collection<CompletionItem.Builder> results,
        ChunkerString node,
        CompletionRequest offset,
        SearchDirection direction
    ) {
        // Alright! we are inside of a string.  Find out what method, if any, this string is contained by.
        Node parent = node.jjtGetParent();
        while (parent != null && !(parent instanceof ChunkerInvoke)) {
            parent = parent.jjtGetParent();
        }
        if (parent == null) {
            // hm, a string that's not inside a method... there might be something we can do here later,
            // but for now, let's just ignore...
        } else {
            ChunkerInvoke invoke = (ChunkerInvoke) parent;
            stringInMethodComplete(results, invoke, node, offset, direction);
        }
    }

    private void stringInMethodComplete(
        Collection<CompletionItem.Builder> results,
        ChunkerInvoke invoke,
        ChunkerString node,
        CompletionRequest offset,
        SearchDirection direction
    ) {
        methodArgumentCompletion(invoke.getName(), results, invoke, node, offset, direction);
        // Check the query library for static methods
    }

    private boolean isValidTableName(String tableName) {
        return tableName.chars().allMatch(Character::isJavaIdentifierPart);
    }

    private void maybeColumnComplete(
        Collection<CompletionItem.Builder> results,
        ChunkerInvoke invoke,
        Node node,
        CompletionRequest offset,
        SearchDirection direction
    ) {
        String str = stringLiteral(node);
        final boolean doColumnNameCompletion, doColumnExpressionCompletion;

        int equal = str.indexOf('=');

        int spacePad = 0;

        while(str.length() > spacePad && Character.isWhitespace(str.charAt(spacePad))) {
            spacePad++;
        }

        if (node == null) {
            // No node found; the invocation has no arguments
            assert invoke.getArgumentCount() == 0;

            doColumnNameCompletion = true;
            doColumnExpressionCompletion = false;
            if (invoke.isWellFormed()) {
                // we are inside a method()
            } else {
                // we are inside a method(
            }
        } else {
            // The document index of the start of the String str;
            if (node.isWellFormed()) {
                // we are inside a method(''[)] <- optional ), we don't really care, since we have an argument for reference

            } else {
                // we are inside a method('
            }
            if (equal == -1) {
                doColumnNameCompletion = true;
                doColumnExpressionCompletion = false;
            } else {
                final int strStart = node.getStartIndex() + spacePad;
                final int equalInd = strStart + equal;
                doColumnNameCompletion = equalInd >= offset.getOffset();
                doColumnExpressionCompletion = equalInd <= offset.getOffset();
            }

        }

        // first, find out columnName
        String columnName = (equal == -1 ? str : str.substring(0, equal)).trim();
        // next, find our table definition, if any...
        final List<IsScope> scope = invoke.getScope();
        TableDefinition def = findTableDefinition(scope, offset);
        if (doColumnNameCompletion) {
            if (def == null) {
                // No table definition?  We should report this as a failure (TableDoesNotExist).
                // TODO proper diagnostic messages;  IDS-1517-16
                log.info().append("No table definition found for ").append(String.valueOf(invoke.getScope())).append(" at offset ").append(offset.toString()).endl();
            } else {
                // finally, complete the column name...
                final CompleteColumnName completer = new CompleteColumnName(this, node, invoke);
                for (String colName : def.getColumnNames()) {
                    // only do column completion for not-yet complete column names...
                    if (camelMatch(colName, columnName) && colName.length() > columnName.length()) {
                        // complete the match as a string
                        completer.doCompletion(results, offset, colName);
                    }
                }
            }

        }
        if (doColumnExpressionCompletion) {
            // Users already has ColumnName=bleh (where bleh may be any string, including empty string)
            Class<?> colType = guessColumnType(columnName, invoke.getScope(), offset);
            final int methodNameStarts = equal + 1;
            final int methodNameEnds = str.length();
            final String partialMatch;
            if (str.endsWith("=")) {
                partialMatch = "";
            } else {
                partialMatch = str.substring(methodNameStarts, methodNameEnds).trim();
            }
            final CompleteColumnExpression completer = new CompleteColumnExpression(this, node, invoke);
            if (def != null) {
                // There is a table definition.  Lets try to suggest column names here.
                for (String colName : def.getColumnNames()) {
                    if (
                        // do not suggest the same column name that is already there
                        !colName.equals(partialMatch) &&
                        // only suggest column names with a camelCase match
                        camelMatch(colName, partialMatch)) {
                        completer.doCompletion(results, offset, colName);
                    }
                }

            }
            if (!partialMatch.isEmpty() || !"false".equals(System.getProperty(PROP_SUGGEST_STATIC_METHODS))) {
                // empty method name will generate MANY matches (lots of static method imports).
                // We left a system property to remove these during testing, so we don't have 30+ results,
                // when we want to test only for column name matches (above)
                for (Class cls : lookups.getStatics()) {
                    for (Method method : cls.getMethods()) {
                        // try to handle the column type here, to limit what we send...
                        // We may want to simply have this affect the score of the results,
                        // so that we'd still send back something incorrect (at least, in cases like update())
                        if (colType.isAssignableFrom(method.getReturnType())) {

                            // TODO: handle instance method calls if appropriate IDS-1517-19
                            // TODO: check binding for variables that can be referenced in column expressions (any non-typed assign before now)  IDS-1517-23
                            if (Modifier.isStatic(method.getModifiers())) {
                                if (camelMatch(method.getName(), partialMatch)) {
                                    completer.doCompletion(results, offset, method);
                                }
                            }
                        }
                    }

                }
            }

        }
    }

    private TableDefinition findTableDefinition(
        List<IsScope> scope,
        CompletionRequest offset
    ) {
        if (scope == null || scope.isEmpty()) {
            return null;
        }
        final IsScope previous = scope.get(scope.size() - 1);
        if (scope.size() == 1) {
            // previous is all we have; just lookup the table definition
            final TableDefinition definition = offset.getTableDefinition(this, doc, variables, previous.getName());
            if (definition == null) {
                // log missing definition...
            }
            return definition;
        } else if (previous instanceof ChunkerInvoke) {
            // recurse into each scope node and use the result of our parent's type to compute our own.
            // This allows us to hack in special support for statically analyzing expressions which return tables.

            return findTableDefinition(((ChunkerInvoke) previous).getScope(), offset);
        }
        return null;
    }

    private Class<?> guessColumnType(String columnName, List<IsScope> scope, CompletionRequest offset) {
        // When guessing column type, we'll prefer a perfect match against a known table-as-scope.
        // Failing that, we'll do a global lookup of all columns-by-types from all table definitions
        // And, finally, failing that, we'll do some random guessing based on "well-known column names".


        // guess table name from the scope; either a reference to a table, or a db.i|t call.
        // for now, we are not going to do complex scope inspections to guess at not-yet-run update operations.
        if (scope != null && scope.size() > 0) {
            IsScope root = scope.get(0);
            if (root instanceof ChunkerIdent) {
                if ("db".equals(root.getName())) {
                    if (scope.size() > 1) {
                        root = scope.get(1);
                    }
                } else {
                    // look for the table in binding
                    final TableDefinition def = offset.getTableDefinition(this, doc, variables, root.getName());
                    if (def != null) {
                        final ColumnDefinition col = def.getColumn(columnName);
                        if (col == null) {
                            // might happen if user did someTable.update("NewCol=123").update("NewCol=
                            // we can handle this by inspecting the scope chain, but leaving edge case out for now.
                        } else {
                            return col.getDataType();
                        }
                    }
                }
            }
        }

        switch (columnName.trim()) {
            case "Date":
                return String.class;
            case "Timestamp":
                return DBDateTime.class;
        }

        // failure; allow anything...
        return Object.class;
    }

    @Deprecated
    public void addMatch(Collection<CompletionItem.Builder> results, Node node, String match, CompletionRequest index, String ... nextTokens) {

        // IDS-1517-13
        // The mess has gone too far.  It is no longer maintainable.
        // The only code still calling into here is weird edge cases for table names and namespaces.
        // We'll fix those use cases up so we can delete this mess.

        final int start;
        int length;
        StringBuilder completion = new StringBuilder();
        String qt = getQuoteType(node);
        Token tok = node.jjtGetLastToken();
        if (tok.getEndIndex() >= index.getOffset()) {
            int s = tok.getStartIndex();
            for (Token token : node.tokensReversed(true)) {
                if (token.getStartIndex() >= index.getOffset()) {
                    completion.insert(0, token.image);
                    s = token.getStartIndex();
                } else if (token.getEndIndex() >= index.getOffset()){
                    completion.insert(0, token.image);
                    s = token.getStartIndex();
                    break;
                } else {
                    break;
                }
            }
            start = s;
            length = tok.getEndIndex() - s;
        } else {
            start = index.getOffset();
            length = 0;
            if (tok.next != null) {
                tok = tok.next;
            }
            while(tok.getEndIndex() <= index.getOffset()) {
                if (tok.image.trim().isEmpty()) {
                    if (tok.next == null) {
                        break;
                    }
                    tok = tok.next;
                    continue;
                }
                if (tok.image.equals(",")) {
                    if (tok.next == null) {
                        break;
                    }
                    tok = tok.next;
                    continue;
                }
                break;
            }
            Token quoteSearch = tok;
            if (quoteSearch.image.trim().isEmpty() && quoteSearch.next != null) {
                quoteSearch = quoteSearch.next;
            }
            switch (quoteSearch.image) {
                case "\"":
                case "'":
                case "`":
                case "'''":
                case "\"\"\"":
                    qt = quoteSearch.image;
            }
        }
        String displayed;
        if (completion.indexOf(qt) == -1) {
            completion.append(qt);
        }
        completion.append(match);
        for (int i = 0; i < nextTokens.length; i++) {
            String nextToken = nextTokens[i];

            String check = nextToken.trim();
            if (tok != null) {
                if (tok.image.trim().endsWith(check)) {
                    completion.append(nextToken);
                    break;
                } else {
                    tok = tok.next();
                    if (tok != null) {
                        if (tok.image.matches(CONTAINS_NEWLINE)) {
                            // stop gobbling at newlines!
                            tok = null;
                        } else {
                            String trimmed = tok.image.trim();
                            if (trimmed.isEmpty()) {
                                completion.append(tok.image);
                                length += tok.image.length();
                                i--;
                                continue;
                            } else if (trimmed.equals(check)) {
                                // user code has the desired token, stop being helpful
                                break;
                            }
                        }
                    }
                }
            }
            if (!completion.toString().trim().endsWith(check)) {
                completion.append(nextToken);
            }
        }
        displayed = completion.toString();

        //            + (node.isWellFormed() ? "" : nextChar);
        final DocumentRange.Builder range = LspTools.rangeFromSource(doc.getSource(), start, length);
        final CompletionItem.Builder result = CompletionItem.newBuilder();
        result.setStart(start)
                .setLength(length)
                .setLabel(displayed)
                .getTextEditBuilder()
                    .setText(displayed)
                    .setRange(range);
        results.add(result);
    }

    public String getQuoteType(Node node) {
        if (node instanceof ChunkerString) {
            return ((ChunkerString) node).getQuoteType();
        } else {
            // TODO: toSource() this node, and then check if it has ' or " or ${vars},  IDS-1517-24
            //  to decide on a better default quote type (i.e. try to guess user's style)
            return getDefaultQuoteType();
        }
    }

    public String stringLiteral(Node ns) {
        if (ns == null) {
            return "";
        }
        return (String)ns.jjtAccept(new ChunkerDefaultVisitor() {
            @Override
            public Object defaultVisit(SimpleNode node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerString(ChunkerString node, Object data) {
                // TODO: handle "multi" + "part" + "chunks"  IDS-1517-33
                return node.getRaw();
            }
        }, null);
    }

    private String getValidTablePrefix(Node ns) {
        if (ns == null) {
            return "";
        }

        return (String)ns.jjtAccept(new ChunkerDefaultVisitor() {
            @Override
            public Object defaultVisit(SimpleNode node, Object data) {
                throw unsupported(node);
            }

            @Override
            public Object visitChunkerString(ChunkerString node, Object data) {
                String str = node.getRaw();
                for (int i = 0; i < str.length(); i += 1) {
                    if (!Character.isJavaIdentifierPart(str.charAt(i))) {
                        return str.substring(0, i);
                    }
                }
                return str;
            }
        }, null);
    }

    public String getDefaultQuoteType() {
        return defaultQuoteType == null ? "\"" : defaultQuoteType;
    }

    public void setDefaultQuoteType(String defaultQuoteType) {
        this.defaultQuoteType = defaultQuoteType;
    }

    protected Map<String, TableDefinition> getReferencedTables() {
        return lookups.getReferencedTables();
    }
}
