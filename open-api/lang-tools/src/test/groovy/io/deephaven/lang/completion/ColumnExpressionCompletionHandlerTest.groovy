package io.deephaven.lang.completion

import io.deephaven.engine.util.VariableProvider
import io.deephaven.internal.log.LoggerFactory
import io.deephaven.io.logger.Logger
import io.deephaven.proto.backplane.script.grpc.CompletionItem
import io.deephaven.engine.table.Table
import io.deephaven.engine.table.TableDefinition
import io.deephaven.time.DateTime
import io.deephaven.lang.parse.CompletionParser
import spock.lang.Specification
import spock.lang.Unroll

class ColumnExpressionCompletionHandlerTest extends Specification implements ChunkerCompleterMixin {

    private static String src_(String methodName = 't', String columnName = 'Date', String completion = "las") {
        return """u = ${methodName}.update('$columnName = $completion"""
    }

    @Unroll
    def "Completion at #position should find typesafe column completion for partially completed column expressions"(int position, Set<String> completions) {
        given:

//u = t.update('Date=las
            String src = src_()
            CompletionParser p = new CompletionParser()
            doc = p.parse(src)

            Logger log = LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock(VariableProvider) {
                (0..1) * getVariableNames() >> ['t']
                (0..1) * getVariableType('t') >> Table
                (0..1) * getTableDefinition('t') >> new TableDefinition(
                        [String, DateTime], ['Date', 'DateTime']
                )
            }

            ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Run completion at cursor position $position"
            Set<String> result = completer.runCompletion(doc, position)
                .collect { this.doCompletion(src, it) }

        then: "Expect the completion result to suggest all namespaces"
            result.size() == completions.size()
            // with() inside a then will assert on each removal
            with(result){
                for (String completion : completions) {
                    result.remove(completion)
                }
            }
            result.size() == 0

        where:
            position | completions
            // between `e=`, expect method name completions, and a single column name completion, for DateTime
            19 | [
                src_('t', 'Date', "lastBusinessDateNy()'"),
                src_('t', 'Date', 'lastBusinessDateNy('),
            ]
            18 | [
                src_('t', 'Date', "lastBusinessDateNy()'"),
                src_('t', 'Date', 'lastBusinessDateNy('),
                src_('t', 'DateTime', 'las'),
            ]
    }

    @Unroll
    def "Completion should offer column name completion from when a where clause contains an unclosed String with a partial prefix (cursor position #pos)"(int pos) {
        given:
        CompletionParser p = new CompletionParser()
        String src = """
t = t.updateView ( 'D
"""
        doc = p.parse(src)

        Logger log = LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock() {
            (0..1) * getTableDefinition('t') >> new TableDefinition([String, Long, Integer], ['Date', 'Delta', 'NotMeThough'])
            (0..1) * getVariableType('t') >> Table
            (0..1) * getVariableNames() >> []
        }

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Cursor is at EOF, table name completion from t is returned"
        Set<CompletionItem.Builder> result = completer.runCompletion(doc, pos)
        result.removeIf({it.textEdit.text == 'updateView('})

//       t = t.where ( 'D
        then: "Expect the completion result to suggest all table names"
        result.size() == 2
        doCompletion(src, result.first()) == "\nt = t.updateView ( 'Date = \n"
        doCompletion(src, result.last() ) == "\nt = t.updateView ( 'Delta = \n"

        where:
        pos << [
                23,
                22,
                21,
                20,
                19,
                18, // hm, this one is no longer trying the column named
        ]
    }

    def "Completion on chained expressions should infer column names"() {
        setup:
        System.setProperty(ChunkerCompleter.PROP_SUGGEST_STATIC_METHODS, 'false')
        CompletionParser p = new CompletionParser()
        String src = """
t = t.update('A=') .update( 'B=')
"""
        doc = p.parse(src)

        Logger log = LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock(VariableProvider) {
            _ * getTableDefinition('t') >> new TableDefinition([Long, Integer], ['A1', 'A2'])
            0 * _
        }

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Cursor is on first A="
        Set<CompletionItem.Builder> result = completer.runCompletion(doc, 16)

        then: "Expect column names from T are returned"
        println(result)
        result.size() == 2
        doCompletion(src, result.first()) == "\nt = t.update('A=A1') .update( 'B=')\n"
        doCompletion(src, result.last()) == "\nt = t.update('A=A2') .update( 'B=')\n"

        when: "Cursor is after first A="
        result = completer.runCompletion(doc, 17)

        then: "Expect column names from T are returned"
        result.size() == 2
        doCompletion(src, result.first()) == "\nt = t.update('A=A1') .update( 'B=')\n"
        doCompletion(src, result.last()) == "\nt = t.update('A=A2') .update( 'B=')\n"

        when: "Cursor is on second B="
        result = completer.runCompletion(doc, 31)

        then: "Expect column names from T are returned"
        result.size() == 2
        doCompletion(src, result.first()) == "\nt = t.update('A=') .update( 'B=A1')\n"
        doCompletion(src, result.last()) == "\nt = t.update('A=') .update( 'B=A2')\n"

        when: "Cursor is after second column expression B="
        result = completer.runCompletion(doc, 32)

        then: "Expect column names from T are returned"
        result.size() == 2
        doCompletion(src, result.first()) == "\nt = t.update('A=') .update( 'B=A1')\n"
        doCompletion(src, result.last()) == "\nt = t.update('A=') .update( 'B=A2')\n"

        cleanup:
        System.clearProperty(ChunkerCompleter.PROP_SUGGEST_STATIC_METHODS)
    }

    def "Completion should infer column structure from newTable invocation"() {
        setup:
        System.setProperty(ChunkerCompleter.PROP_SUGGEST_STATIC_METHODS, 'false')
        CompletionParser p = new CompletionParser()
        String src = """
t = newTable(
  stringCol("strCol", "1", "2", "3"),
  intCol("intCol", 1, 2, 3)
)
t.where('"""
        doc = p.parse(src)

        Logger log = LoggerFactory.getLogger(CompletionHandler)
        VariableProvider variables = Mock(VariableProvider) {
            _ * getTableDefinition('t') >> null
            0 * _
        }

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Cursor is inside where('"
        Set<CompletionItem.Builder> result = completer.runCompletion(doc, src.length())

        then: "Expect column names from newTable() are returned"
        println(result)
        result.size() == 2
        doCompletion(src, result.first()) == src + "strCol = "
        doCompletion(src, result.last()) == src + "intCol = "

        cleanup:
        System.clearProperty(ChunkerCompleter.PROP_SUGGEST_STATIC_METHODS)
    }
    @Override
    VariableProvider getVariables() {
        return Mock(VariableProvider) {
            _ * getVariableNames() >> []
        }
    }
}
