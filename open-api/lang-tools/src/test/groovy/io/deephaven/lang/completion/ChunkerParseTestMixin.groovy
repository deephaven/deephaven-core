package io.deephaven.lang.completion

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import io.deephaven.db.util.VariableProvider
import io.deephaven.io.logger.Logger
import io.deephaven.lang.generated.Node
import io.deephaven.lang.generated.Token
import io.deephaven.lang.parse.CompletionParser
import io.deephaven.lang.parse.ParsedDocument
import io.deephaven.proto.backplane.script.grpc.CompletionItem
import io.deephaven.proto.backplane.script.grpc.CompletionItemOrBuilder
import io.deephaven.proto.backplane.script.grpc.Position
import io.deephaven.util.process.ProcessEnvironment
import spock.lang.Specification

/**
 * A collection of easily reusable test tools for autocompletion.
 */
@CompileStatic
trait ChunkerParseTestMixin {

    ParsedDocument doc

    Specification getSpec() {
        return this as Specification
    }

    /**
     */
    @SuppressWarnings("GrUnnecessaryPublicModifier") // `public` needed for type parameters.  thx groovy
    public <N extends Node> void testSearch(Integer index, Class<N> type, String textBefore, String match, String textAfter) {
        Node node = doc.findNode(index)
        assert match.length() == 1 : "Only send a single char for the match variable (you sent $match)"
        new CompletionAssertion(index, node, type, textBefore, match.charAt(0), textAfter).isValid()
        for (Node child : node.children) {
            assert !child.containsIndex(index) : "findNode should only find leaf-most nodes!; child: " + child + " contains index " + index
        }
    }

    abstract VariableProvider getVariables()

    ParsedDocument parse(String command) {
        CompletionParser p = new CompletionParser();
        doc = p.parse(command)
        return doc
    }

    String doCompletion(String command, CompletionItemOrBuilder fragment) {
        Position.Builder pos = doc.findEditRange(fragment.textEdit.range)
        return new StringBuilder(command)
            .replace(pos.line, pos.character, fragment.textEdit.text)
            .toString()
    }

    @CompileDynamic
    String doCompletion(String command, int completionPos, int resultIndex) {
        Logger log = ProcessEnvironment.getDefaultLog(CompletionHandler)

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)
        parse(command)
        Set<CompletionItem> results = completer.runCompletion(doc, completionPos)
        List<CompletionItem.Builder> result = results.toList()
        if (resultIndex >= results.size()) {
            throw new IllegalArgumentException("Invalid result index " + resultIndex +"; only had " + results.size() + " results: " + results)
        }
        return doCompletion(command, result.get(resultIndex))
    }


    @CompileDynamic
    String doCompletion(String command, int completionPos, int resultIndex) {
        Logger log = ProcessEnvironment.getDefaultLog(CompletionHandler)

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)
        parse(command)
        Set<CompletionItem> results = completer.runCompletion(doc, completionPos)
        List<CompletionItem.Builder> result = results.toList()
        if (resultIndex >= results.size()) {
            throw new IllegalArgumentException("Invalid result index " + resultIndex +"; only had " + results.size() + " results: " + results)
        }
        return doCompletion(command, result.get(resultIndex))
    }


    @CompileDynamic
    void assertAllValid(ParsedDocument parsed, String src) {
        if (!src) {
            return
        }
        // hokay! for every node, we want to assert many things:
        // a) the re-rendered source is _exactly_ the same as input
        // b) all tokens from start to end are contiguous and in order
        // c) all jjtChildren nodes are strictly contained by their parent
        // d) all children know their parent, and all parents know their children
        // e) all tokens of all children are also in their parent's token chains.
        Node doc = parsed.doc
        assert src == doc.toSource()
        boolean contiguous = checkNodes(doc)
        assert contiguous : "Node is not contiguous " + doc
    }

    @SuppressWarnings("ChangeToOperator") // changing to ++itr breaks groovy in stupid ways
    @CompileDynamic
    boolean checkNodes(Node n) {
        Iterator<Token> itr = n.tokens(true).iterator()
        assert itr.hasNext() : "Node " + n + " does not have any tokens"
        Token t = itr.next()
        int start = t.getStartIndex()
        int end = t.getEndIndex()
        assert n.jjtGetFirstToken() != null
        assert n.jjtGetLastToken() != null
        assert end >= start // eof tokens can be empty.
        assert t.image.length() + t.specialTokenLength() == end - start
        while (itr.hasNext()) {
            t = itr.next()
            assert t.containsIndex(end)
            assert t.getStartIndex() == end
            if (t.beginLine == t.endLine) {
                assert t.image.isEmpty() ? t.beginColumn == t.endColumn :
                    t.beginColumn + t.image.length() - 1 == t.endColumn
            }
            start = t.getStartIndex()
            end = t.getEndIndex()
            assert end >= start
            assert t.image.length() + t.specialTokenLength() == end - start
        }
        // ok, tokens check out.  Now, check the nodes...
        List<Node> kids = n.children
        kids.each {
            checkParentChild(n, it)
        }
        true
    }

    @CompileDynamic
    void checkParentChild(Node parent, Node child) {
        assert child != null : "Null child in parent $parent"
        assert parent == child.jjtGetParent() : "Child with incorrect parent; $child has parent ${child.jjtGetParent()} instead of $parent"
        assert parent.tokens(true).toList().containsAll(child.tokens(true).toList()) :
            "Parent tokens do not contain all child tokens: \nparent: $parent\nchild: $child"
        assert parent.startIndex <= child.startIndex
        assert parent.endIndex >= child.endIndex
        checkNodes(child)
    }
}
