package io.deephaven.lang.completion

import groovy.text.SimpleTemplateEngine
import io.deephaven.db.util.VariableProvider
import io.deephaven.lang.generated.*
import io.deephaven.lang.parse.CompletionParser
import io.deephaven.lang.parse.LspTools
import io.deephaven.proto.backplane.script.grpc.Position
import io.deephaven.proto.backplane.script.grpc.PositionOrBuilder
import spock.lang.Specification
import spock.lang.Unroll

class ChunkerParserTest extends Specification implements ChunkerParseTestMixin {

    @Unroll
    def "Comments are visited in order when indexing a document"(String comment1, String comment2, String comment3) {
        given:
        CompletionParser p = new CompletionParser()
        String src
        when: "Parse a document with a bunch of comments"
        src =
"""$comment1$comment2$comment3
/*a comment here*/ some/*and*//*here*/./*and*//*here*/code() /*why*//*would*//*anyone*///do this
$comment3$comment2$comment1
$comment2$comment1$comment3"""
        doc = p/**/./**/parse/**/(src)
        PositionOrBuilder pos = Position.defaultInstance
        boolean hasComment = false
        then: "Expect all tokens to be iterated in correct order"
        for (Token t : new AllTokenIterableImpl(doc.doc.jjtGetFirstToken())) {
            LspTools.lessOrEqual(pos, t.positionStart())
            LspTools.lessOrEqual(t.positionStart(), (pos = t.positionEnd()))
            switch (t.kind) {
                case ChunkerConstants.MULTI_LINE_COMMENT:
                case ChunkerConstants.JAVA_DOC_COMMENT:
                case ChunkerConstants.SINGLE_LINE_COMMENT:
                    hasComment = true
            }
        }
        assert hasComment

        where:
        comment1 | comment2 | comment3
        '/* block */' | '// line2\n' | ' // line3\n'
        '/* block */' | '/*block2*/' | ' // line3\n'
        '/* block */' | '/*block2*/' | ' /* block3*/'
        '// line\n' | '/*block2*/' | ' /* block3*/'
        '// line\n' | '// line2\n' | ' /* block3*/'
        '// line\n' | '// line2\n' | ' // line3\n'
        '# python\n' | '// line2\n' | ' // line3\n'
        '# python\n' | '#python2\n' | ' // line3\n'
        '# python\n' | '#python2\n' | ' #python3\n'

    }
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

        when: "Parse an expression with complex RHS type arguments"
        src =
"""
l.add(SomeClass.<? super String>method(new Thing1<T>.Thing2<T, ? super T>()))
"""
        doc = p.parse(src)

        then: "Expect to find a simple method call taking arguments with complex arguments"

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

    def "Completion should work with many types of quotes"(String q) {
        given:
        CompletionParser p = new CompletionParser()
        String src =
"""
t = db.timeTable(${q}0:00:01$q)
g = t.update(${q}Day = Timestamp - 100000000$q) // whatever...
"""
        doc = p.parse(src)

        expect: "All found nodes are the deepest leaf in ast tree"
        assertAllValid(doc, src)
        testSearch 0, ChunkerNewline, '', '\n', ''

        testSearch 1, ChunkerIdent, '', 't', ''
        testSearch 2, ChunkerAssign, 't', ' ','= '
        testSearch 3, ChunkerAssign, 't ', '=', ' '
        testSearch 4, ChunkerAssign, 't =', ' ', ''

        testSearch 5, ChunkerIdent,  '', 'd', 'b'
        testSearch 6, ChunkerIdent,  'd', 'b', ''
        testSearch 7, ChunkerInvoke,  'db', '.', "timeTable(${q}0:00:01$q)"

        testSearch 8, ChunkerMethodName,  '', 't', "imeTable("
        testSearch 9, ChunkerMethodName,  't', 'i', "meTable("
        testSearch 10, ChunkerMethodName,  'ti', 'm', "eTable("
        // skip some boring, obvious cases
        testSearch 15, ChunkerMethodName,  'timeTab', 'l', "e("
        testSearch 16, ChunkerMethodName,  'timeTabl', 'e', "("
        testSearch 17, ChunkerMethodName,  'timeTable', '(', ""
        // now we're in the string
        testSearch 18, ChunkerString,  '', q, "0:00:01$q"
        testSearch 19, ChunkerString,  q, '0', ":00:01$q"
        testSearch 20, ChunkerString,  "${q}0", ':', "00:01$q"
        testSearch 25, ChunkerString,  "${q}0:00:0", '1', q
        testSearch 26, ChunkerString,  "${q}0:00:01", q, ''

        testSearch 27, ChunkerInvoke,  "db.timeTable(${q}0:00:01$q", ')', ''

        testSearch 28, ChunkerNewline,  '', '\n', ''

        testSearch 29, ChunkerIdent,  '', 'g', ''
        testSearch 30, ChunkerAssign,  'g', ' ', '= '
        testSearch 31, ChunkerAssign,  'g ', '=', ' '
        testSearch 32, ChunkerAssign,  'g =', ' ', ''
        testSearch 33, ChunkerIdent,  '', 't', ''
        testSearch 34, ChunkerInvoke,  't', '.', "update(${q}Day = Timestamp - 100000000$q) "
        testSearch 35, ChunkerMethodName,  '', 'u', "pdate("
        testSearch 40, ChunkerMethodName,  'updat', 'e', "("
        testSearch 41, ChunkerMethodName,  'update', '(', ""
        testSearch 42, ChunkerString,  '', q, "Day = Timestamp - 100000000$q"
        testSearch 43, ChunkerString,  q, 'D', "ay = Timestamp - 100000000$q"
        testSearch 70, ChunkerString,  "${q}Day = Timestamp - 100000000", q, ''
        testSearch 71, ChunkerInvoke,  "t.update(${q}Day = Timestamp - 100000000$q", ')', ' '
        testSearch 72, ChunkerInvoke,  "t.update(${q}Day = Timestamp - 100000000$q)", ' ', ''
        testSearch 73, ChunkerEof,  "", '/', '/ whatever...\n'

        src.length().times{
            i ->
                Node n = doc.findNode(i)
                assert n : "No node found at index $i"
                assert ! (n instanceof ChunkerDocument) : "Should never findNode() the root document"
        }
        1 + q == "1$q"

        where:
        // test with multiple forms of quotes,
        q   << [ '"' , "'" ]
    }

    def "Completion should handle unfinished documents"(String q) {
        given:
        CompletionParser p = new CompletionParser()
        String src =
"""
t = db.
"""
        doc = p.parse(src)

        expect: "All found nodes are the deepest leaf in ast tree"
        assertAllValid(doc, src)
        testSearch 0, ChunkerNewline, '', '\n', ''

        testSearch 1, ChunkerIdent, '', 't', ''
        testSearch 2, ChunkerAssign, 't', ' ','= '
        testSearch 3, ChunkerAssign, 't ', '=', ' '
        testSearch 4, ChunkerAssign, 't =', ' ', ''
        testSearch 5, ChunkerIdent, '', 'd', 'b'
        testSearch 6, ChunkerIdent, 'd', 'b', ''
        testSearch 7, ChunkerBinaryExpression, 'db', '.', ''
        testSearch 8, ChunkerNewline, '', '\n', ''

        src.length().times{
            i ->
                Node n = doc.findNode(i)
                assert n : "No node found at index $i"
                assert ! (n instanceof ChunkerDocument) : "Should never findNode() the root document"
        }
        1 + q == "1$q"

        where:
        // test with multiple forms of quotes,
        q   << [ '"' , "'" ]
    }

    @Override
    VariableProvider getVariables() {
        return Mock(VariableProvider) {
            _ * getVariableNames() >> []
        }
    }

    static String quotify(String str, String q) {
        new SimpleTemplateEngine().createTemplate(str)
                .make([ q: q]).toString()
    }
}

/*  A mess of ugly syntax that we'll want to (eventually) tolerate:
Type<? extends List<A, B>, C> var = Class.field
    .new Something<? super X<Y extends A & B, Z>>(
)

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
 */
