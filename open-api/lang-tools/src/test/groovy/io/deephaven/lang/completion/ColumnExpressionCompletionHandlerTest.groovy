package io.deephaven.lang.completion

import io.deephaven.engine.context.QueryScope
import io.deephaven.engine.context.StandaloneQueryScope
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.TableFactory
import io.deephaven.internal.log.LoggerFactory
import io.deephaven.io.logger.Logger
import io.deephaven.lang.parse.CompletionParser
import io.deephaven.proto.backplane.script.grpc.CompletionItem
import io.deephaven.qst.column.Column
import io.deephaven.util.SafeCloseable
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.time.LocalDate

class ColumnExpressionCompletionHandlerTest extends Specification implements ChunkerCompleterMixin {

    private static String src_(String methodName = 't', String columnName = 'Date', String completion = "past") {
        return """u = ${methodName}.update('$columnName = $completion"""
    }

    private SafeCloseable executionContext;

    void setup() {
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    void cleanup() {
        executionContext.close();
    }

    @Unroll
    def "Completion at #position should find typesafe column completion for partially completed column expressions"(int position, Set<String> completions) {
        given:

//u = t.update('Date=past
            String src = src_()
            CompletionParser p = new CompletionParser()
            doc = p.parse(src)

            Logger log = LoggerFactory.getLogger(CompletionHandler)
            QueryScope variables = new StandaloneQueryScope()
            variables.putParam("t", TableFactory.newTable(
                    Column.of('Date', LocalDate.class, new LocalDate[0]),
                    Column.of('DateTime', Instant.class, new Instant[0]))
            )

            ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Run completion at cursor position $position"
            Set<String> result = completer.runCompletion(doc, position)
                .collect { this.doCompletion(src, it) }

        then: "Expect the completion result to suggest all namespaces"
            result.forEach(System.out::println)
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
            // between `e=`, expect method name completions, and a single column name completion, for Clock
            19 | [
                src_('t', 'Date', "pastDate("),
                src_('t', 'Date', "pastBusinessDate("),
                src_('t', 'Date', "pastNonBusinessDate("),
            ]
            18 | [
                src_('t', 'Date', "pastDate("),
                src_('t', 'Date', "pastBusinessDate("),
                src_('t', 'Date', "pastNonBusinessDate("),
                src_('t', 'DateTime', "past"),
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
        QueryScope variables = new StandaloneQueryScope();
        variables.putParam('t', TableFactory.newTable(
            Column.of('Date', String.class, new String[0]),
            Column.of('Delta', Long.class, new Long[0]),
            Column.of('NotMeThough', Integer.class, new Integer[0]),
        ));

        ChunkerCompleter completer = new ChunkerCompleter(log, variables)

        when: "Cursor is at EOF, table name completion from t is returned"
        Set<CompletionItem> result = completer.runCompletion(doc, pos)
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
        QueryScope variables = new StandaloneQueryScope()
        variables.putParam('t', TableFactory.newTable(
                Column.of('A1', Long.class, new Long[0]),
                Column.of('A2', Integer.class, new Integer[0]),
        ));

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
        QueryScope variables = new StandaloneQueryScope()
        variables.putParam('t', null);

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
}
