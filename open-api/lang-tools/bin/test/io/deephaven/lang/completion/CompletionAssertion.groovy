package io.deephaven.lang.completion

import groovy.transform.CompileStatic
import io.deephaven.lang.generated.Node

/**
 * Represents a given assertion about a completion document.
 */
@CompileStatic
class CompletionAssertion {

    /**
     * The node found in the document
     */
    Node node

    /**
     * The expected node type found for the given index.
     */
    Class<? extends Node> expectedType

    /**
     * Text of rendered node expected before this index
     */
    String expectedPrefix

    /**
     * Text of rendered node expected after this index
     */
    String expectedSuffix

    /**
     * The cursor index that this assertion is testing
     */
    int index

    /**
     * The character we expect to find at the matched position
     */
    char match

    CompletionAssertion (int index, Node node, Class<? extends Node> expectedType, String expectedPrefix, char match, String expectedSuffix) {
        this.index = index
        this.node = node
        this.match = match
        this.expectedType = expectedType
        this.expectedPrefix = expectedPrefix
        this.expectedSuffix = expectedSuffix
    }

    void isValid() {
        assert expectedType.isInstance(node)

        // Find and check the expected strings for our match assertions
        String str = node.toSource()
        int delta = node.startIndex
        int i = index - delta
        String before = str.substring(0, i)
        char at = str.charAt(i)
        String after = i == str.length() - 1 ? '' : str.substring(i+1)

        assert expectedPrefix == before
        assert match == at
        assert expectedSuffix.toString() == after
    }
}
