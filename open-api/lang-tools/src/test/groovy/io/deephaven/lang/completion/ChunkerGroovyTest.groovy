package io.deephaven.lang.completion

import io.deephaven.db.util.VariableProvider
import io.deephaven.lang.parse.CompletionParser
import io.deephaven.lang.parse.api.Languages
import spock.lang.Specification

class ChunkerGroovyTest extends Specification implements ChunkerParseTestMixin {

    def "Parser should handle strongly typed groovy"() {
        given:
        CompletionParser p = new CompletionParser()
        String src
        when: "Parse a variable with LHS type arguments"
        src =
"""
List<? extends CharSequence> l = new ArrayList<>()
"""

        doc = p.parse(src)
        then: "Expect to find a typed assign; something we can use to get meaningful type information from"
        assertAllValid(doc, src)

        when: "Parse an expression with complex RHS type arguments"
        src =
"""
l.add(SomeClass.<? super String>method(new Thing1<T>.Thing2<T, ? super T>()))
"""
        doc = p.parse(src)

        then: "Expect to find a simple method call taking arguments with complex arguments"
        assertAllValid(doc, src)

    }
    def "Parser should handle groovy closures"() {
        given:
        CompletionParser p = new CompletionParser()
        String src
        when: "Parse a simple closure assigned to a def"
        src = 'def job = { "1" + 2 }'
        doc = p.parse(src)
        then: "Expect to find a closure containing a binary expression as a statement"
        assertAllValid(doc, src)

        when: "Parse a closure passed as method parameters"
        src = 'List<?> val = method({ closure1() }, { })'
        doc = p.parse(src)

        then: "Expect to find closures as arguments"
        assertAllValid(doc, src)

        when: "Parse an array of closures passed as method parameters"
        src = 'val = method( [ { closure1() }, { } ] )'
        doc = p.parse(src)

        then: "Expect to find closures-in-arrays as arguments"
        assertAllValid(doc, src)

        when: "Parse a crazy combination of arrays, closures and binary expressions"
        src = '''
val = {
  List<? extends A.B> arg ->
    method(
      [{ closure1() },{}]
      ,
      { [ 1 , { -> 2 } , '"{])(}"[' ] }
      ,
      [[{ a -> return { "}][{()" } }, { 1 }(2)], { a, b -> a + b }]
    )
}
'''
        doc = p.parse(src)
        then: 'All elements come back as well formed ast nodes at the correct indices'
        assertAllValid(doc, src)
    }

    def "Parser should survive moderately complex groovy with classes, types and closures"() {
        given:
            CompletionParser p = new CompletionParser()
            p.setLanguage(Languages.LANGUAGE_GROOVY)
            String src
        when:
            src = """
class SomeClass {
    public <T> String method(Object arg) {
        return arg.toString()
    }
}
class T{}
class Thing1 <T> {
    class Thing2<T2 extends T, T1 extends T2> {
        @Override
        String toString() {
            return "hi!"
        }
    }
}
List<? extends CharSequence> l = new ArrayList<>()
Thing1<? super T> t = new Thing1<>()
l.add(new SomeClass().<? super String>method(new Thing1<? super T>.Thing2<? super T, T>(t)))
"""
        doc = p.parse(src)

        then: "Expect all ast nodes to have a valid structure"
        assertAllValid(doc, src)
        // we'll worry about pulling specific nodes out later;
        // we likely want a groovy v. python parser state later,
        // which will change the "doesn't understand keywords, chunked-only" ast structure;
        // currently, we are just extremely lax, and form chains of nodes that
        // are valid token-wise, but don't make a lot of sense on their own
        // (apart from assignments, invocations, type arguments and strings).
        // It would be better to build a visitor that rebuilds any more complex
        // nodes for the specific language being handled, than to worry about how
        // oddly parser handles classes and whatnot at this time.

    }
    @Override
    VariableProvider getVariables() {
        return Mock(VariableProvider) {
            _ * getVariableNames() >> []
        }
    }
}
