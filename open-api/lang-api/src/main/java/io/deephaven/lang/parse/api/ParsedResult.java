package io.deephaven.lang.parse.api;

import java.util.List;
import java.util.Map;

/**
 * Represents a parsed document.
 *
 * For now, we will be re-parsing the entire string document every time, but in the future, we would
 * like to be able to update only ranges of changed code.
 */
public interface ParsedResult<DocType, AssignType, NodeType> {

    NodeType findNode(int p);

    DocType getDoc();

    String getSource();

    Map<String, List<AssignType>> getAssignments();
}
