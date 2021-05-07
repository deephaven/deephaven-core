package io.deephaven.lang.completion;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A stateful object to represent a document search at a given position.
 *
 * When we search left or right from an intermediate node like whitespace, dot, comma or EOF,
 * we will create a new CompletionRequest at the new index.
 *
 * This currently uses absolute cursor positions, but we want this to be a line/column position instead,
 * so we can completely remove consideration of absolute cursors.
 *
 * This mistake was made by trying to base V2 on V1 semantics which are not really relevant
 * when considering Monaco, LSP and Javacc which all use line/column semantics.
 *
 * Absolute cursor positions are unfortunately deeply entwined in {@link ChunkerCompleter},
 * so we are leaving it in place for now.
 *
 * Note that this class also maintains a map of loaded table definitions,
 * so that repeated completions will not pay to load the same table definition more than once.
 */
public class CompletionRequest {

    private final String source;
    private final int offset;
    private final ChunkerCompleter completer;
    private int candidate;
    private final Map<String, TableDefinition> localDefs;

    public CompletionRequest(@NotNull ChunkerCompleter completer, String command, int offset) {
        this(completer, command, offset, new LinkedHashMap<>());
    }

    private CompletionRequest(@NotNull ChunkerCompleter completer, String command, int offset, Map<String, TableDefinition> localDefs) {
        this.source = command;
        this.offset = this.candidate = offset;
        this.completer = completer;
        this.localDefs = localDefs;
        Require.requirement(offset >= 0, "offset >= 0");
        Require.requirement(offset <= command.length(), "offset <= command.length()");
    }

    public String getSource() {
        return source;
    }

    public int getOffset() {
        return offset;
    }

    public String getBeforeCursor() {
        return source.substring(0, offset);
    }

    public int getCandidate() {
        return candidate;
    }

    public CompletionRequest candidate(int index) {
        final CompletionRequest req = new CompletionRequest(completer, source, offset, localDefs);
        req.candidate = index;
        return req;
    }

    public TableDefinition getTableDefinition(VariableProvider variables, String name) {
        // Each request maintains a local cache of looked-up table definitions, to avoid going to the VariableHandler unless needed
        // Note that we do NOT go to the completer.getReferencedTables map at all;
        // we don't want to cache anything local-to-script-session any further
        // than a single completion invocation (i.e. local to a CompletionRequest)
        if (localDefs.containsKey(name)) {
            // if there wasn't a table definition in the script session once, it won't magically appear again later.
            // This might seem a little excessive, but in python at least, it is non-"free" to check if binding a variable exists.
            return localDefs.get(name);
        }
        final TableDefinition result = variables.getTableDefinition(name);
        localDefs.put(name, result);
        return result;
    }

    @Override
    public String toString() {
        return "CompletionRequest{" +
            "source='" + source + '\'' +
            ", offset=" + offset +
            ", candidate=" + candidate +
            '}';
    }
}
