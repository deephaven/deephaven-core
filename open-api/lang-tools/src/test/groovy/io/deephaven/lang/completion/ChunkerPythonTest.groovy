package io.deephaven.lang.completion

import io.deephaven.db.util.VariableProvider
import io.deephaven.lang.parse.CompletionParser
import spock.lang.Specification

class ChunkerPythonTest extends Specification implements ChunkerParseTestMixin {

    def "Parser should survive moderately complex python with classes and defs"() {
        given:
            CompletionParser p = new CompletionParser()
            String src
        when:
            src = """
from .constants import *

class Type(some.Type):
    def doThings(self, ** query):
        for item in items[0]:
            if item not in results:
                results[item] = {key(): value()}

def typesafe(param: Type = None) -> int:
    add = lambda x, y: x + y
    yield add(1, 41)

"""
        doc = p.parse(src)

        then: "Expect all ast nodes to have a valid structure"
        // the goal here is merely survival without parse exceptions bubbling out.
        assertAllValid(doc, src)

    }

    @Override
    VariableProvider getVariables() {
        return Mock(VariableProvider) {
            _ * getVariableNames() >> []
        }
    }
}
