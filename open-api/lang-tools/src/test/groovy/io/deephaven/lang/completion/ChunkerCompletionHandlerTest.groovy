package io.deephaven.lang.completion

import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.Table
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.util.VariableProvider
import io.deephaven.internal.log.LoggerFactory
import io.deephaven.io.logger.Logger
import io.deephaven.lang.parse.CompletionParser
import io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequest.TextDocumentContentChangeEvent
import io.deephaven.proto.backplane.script.grpc.CompletionItem
import io.deephaven.util.SafeCloseable
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Method
import java.lang.reflect.Modifier

/**
 * This test has generic "test all positions in a String" to fish out NPEs and the like,
 * as well as specific failure cases that were noted and fixed during testing.
 * The actual results are not verified, simply checking that we don't blow up on things.
 */
class ChunkerCompletionHandlerTest extends Specification implements ChunkerCompleterMixin {

    /** Makes a simple text document change that just inserts text at the given position */
    private static TextDocumentContentChangeEvent makeChange(int line, int character, String text) {
        TextDocumentContentChangeEvent.Builder result
        result = TextDocumentContentChangeEvent.newBuilder()
        result.rangeBuilder.startBuilder.setLine(line).setCharacter(character)
        result.rangeBuilder.endBuilder.setLine(line).setCharacter(character)
        result.setText(text)
        result.setRangeLength(0)
        return result.build()
    }

    private SafeCloseable executionContext;

    void setup() {
        executionContext = ExecutionContext.createForUnitTests().open();
    }

    void cleanup() {
        executionContext.close();
    }

    @Override
    Specification getSpec() {
        return this
    }

    VariableProvider mockVars(Closure configure) {
        return Mock(VariableProvider, configure)
    }

    @Unroll
    def "Complex chains of methods on tables can parse #src sanely at any index"(String src) {
        CompletionParser p = new CompletionParser()

        when:
        doc = p.parse(src)
        VariableProvider vars = Mock(VariableProvider){
            _ * getVariableType('t') >> Table
            _ * getVariableType(_) >> null
            _ * getVariable(_, _) >> null
            _ * getVariableNames() >> []
            _ * getTableDefinition('emptyTable') >> TableDefinition.of()
            0 * _
        }

        then:
        assertAllValid(doc, src)
        if (src.empty) {
            return
        }
        for (int i : 0..src.length()-1) {
            def results = performSearch(doc, i, vars)
            assert results != null : """Failed on $i ${
                src.substring(Math.max(0, i-10), i)
            }[[${src.charAt(i)}]]${
                i == src.length() ? '' : src.substring(i+1, Math.min(i+10, src.length()))
            }"""
        }

        where:
        src << sliceString(0,'''
t = emptyTable(10).update(
    "C=i")
    .updateView(""
''')
    }

    def "Methods on binding variables that were assigned a value from an emptyTable method returning Table will know it is a table"() {
        setup:
        String src = '''
t = emptyTable(10)
u = t.'''
        CompletionParser p = new CompletionParser()
        VariableProvider vars = Mock(VariableProvider){
            _ * getVariableType('t') >> Table
            _ * getVariableType(_) >> null
            _ * getVariable(_,_) >> null
            _ * getVariableNames() >> []
            0 * _
        }

        when:
        doc = p.parse(src)

        then:
        assertAllValid(doc, src)

        when:
        Set<CompletionItem> items = performSearch(doc, src.length(), vars)
        List<String> results = items.collect { this.doCompletion(src, it) }
        Set<String> seen = new HashSet<>()

        then:
        with(results) {
            for (Method m : Table.getMethods()) {
                if (Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())) {
                    if (seen.add(m.name)) {
                        results.remove(src + m.name + "(")
                    }
                }
            }
        }
        // TODO (deephaven-core#875): Auto-complete on instance should not suggest static methods. Table doesn't have
        //                            anymore, though, so we don't need to hack around that here.
        results.size() == 0
    }

    def "Completion should offer binding-scoped variables after an empty assign="() {
        given:
        CompletionParser p = new CompletionParser()
        String src = "t ="
        doc = p.parse(src)

        LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock(VariableProvider) {
            _ * getVariableNames() >> ['emptyTable']
            0 * _
        }

        when: "Cursor is at EOF, table name completion from t is returned"
        Set<CompletionItem> result = performSearch(doc, src.length(), variables)

        then: "Expect the completion result to suggest t = emptyTable."
        result.size() == 1
        doCompletion(src, result.first()) == "t =emptyTable."
    }

    def "Completion should work correctly after making edits"() {
        given:
        Logger log = LoggerFactory.getLogger(CompletionHandler)
        CompletionParser p = new CompletionParser()
        String uri = "testing://"
        String src1 = """a = 1
b = 2
c = 3
"""
        String src2 = "t = "
        p.update(uri, 0, [ makeChange(0, 0, src1) ])
        p.update(uri, 1, [ makeChange(3, 0, src2) ])
        doc = p.finish(uri)

        VariableProvider variables = Mock(VariableProvider) {
            _ * getVariableNames() >> ['emptyTable']
            0 * _
        }

        when: "Cursor is at EOF, table name completion from t is returned"
        Set<CompletionItem> result = performSearch(doc, (src1 + src2).length(), variables)

        then: "Expect the completion result to suggest t = emptyTable."
        result.size() == 1
        doCompletion((src1 + src2), result.first()) == """a = 1
b = 2
c = 3
t = emptyTable."""
    }

    def "Completion should not error out in a comment between two lines"() {
        given:
        CompletionParser p = new CompletionParser()
        String beforeCursor = """
a = 1
# hello world a."""
        String afterCursor = """
b = 2
"""
        String src = beforeCursor + afterCursor;

        doc = p.parse(src)

        LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock(VariableProvider) {
            _ * getVariableNames() >> ['emptyTable']
            0 * _
        }

        when: "Cursor is in the comment after the variablename+dot and completion is requested"
        Set<CompletionItem> result = performSearch(doc, beforeCursor.length(), variables)
        then: "Expect the completion result to suggest nothing"
        result.size() == 0
    }
    @Override
    VariableProvider getVariables() {
        return Mock(VariableProvider) {
            _ * getVariableNames() >> []
        }
    }
}
