package io.deephaven.lang.completion;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.VariableProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.generated.ChunkerAssign;
import io.deephaven.lang.generated.ChunkerInvoke;
import io.deephaven.lang.generated.ChunkerString;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.parse.ParsedDocument;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * A stateful object to represent a document search at a given position.
 *
 * When we search left or right from an intermediate node like whitespace, dot, comma or EOF, we will create a new
 * CompletionRequest at the new index.
 *
 * This currently uses absolute cursor positions, but we want this to be a line/column position instead, so we can
 * completely remove consideration of absolute cursors.
 *
 * This mistake was made by trying to base V2 on V1 semantics which are not really relevant when considering Monaco, LSP
 * and Javacc which all use line/column semantics.
 *
 * Absolute cursor positions are unfortunately deeply entwined in {@link ChunkerCompleter}, so we are leaving it in
 * place for now.
 *
 * Note that this class also maintains a map of loaded table definitions, so that repeated completions will not pay to
 * load the same table definition more than once.
 */
public class CompletionRequest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletionRequest.class);

    private final String source;
    private final int offset;
    private final ChunkerCompleter completer;
    private int candidate;
    private final Map<String, TableDefinition> localDefs;

    public CompletionRequest(@NotNull ChunkerCompleter completer, String command, int offset) {
        this(completer, command, offset, new LinkedHashMap<>());
    }

    private CompletionRequest(@NotNull ChunkerCompleter completer, String command, int offset,
            Map<String, TableDefinition> localDefs) {
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

    public TableDefinition getTableDefinition(final ChunkerCompleter completer, final ParsedDocument doc,
            VariableProvider variables, String name) {
        // Each request maintains a local cache of looked-up table definitions, to avoid going to the VariableHandler
        // unless needed
        // Note that we do NOT go to the completer.getReferencedTables map at all;
        // we don't want to cache anything local-to-script-session any further
        // than a single completion invocation (i.e. local to a CompletionRequest)
        if (localDefs.containsKey(name)) {
            // if there wasn't a table definition in the script session once, it won't magically appear again later.
            // This might seem a little excessive, but in python at least, it is non-"free" to check if binding a
            // variable exists.
            return localDefs.get(name);
        }
        TableDefinition result = variables.getTableDefinition(name);
        if (result == null) {
            // If the result was null, we can try to search for an assign statement that is initialized w/ something we
            // _can_ grok.
            final List<ChunkerAssign> assignment = completer.findAssignment(doc, this, name);
            if (!assignment.isEmpty()) {
                // ok! there was an assignment to our table variable that occurred before user's cursor (this is,
                // of course, bad when user creates some random functions that are defined in any order).
                final ListIterator<ChunkerAssign> itr = assignment.listIterator(assignment.size());
                while (itr.hasPrevious()) {
                    final ChunkerAssign check = itr.previous();
                    result = findTableDefFromAssign(check);
                    if (result != null) {
                        break;
                    }
                }
            }
        }
        localDefs.put(name, result);
        return result;
    }

    private TableDefinition findTableDefFromAssign(final ChunkerAssign check) {
        final Node value = check.getValue();
        if (value instanceof ChunkerInvoke) {
            ChunkerInvoke invoke = (ChunkerInvoke) value;
            if ("newTable".equals(invoke.getName())) {
                // TODO: consider checking the scope of newTable... for now, too messy for ~0 value.
                return convertNewTableInvocation(invoke);
            }
        }
        return null;
    }

    private TableDefinition convertNewTableInvocation(final ChunkerInvoke invoke) {
        final TableDefinition def = new TableDefinition();
        final List<ColumnDefinition<?>> columns = new ArrayList<>();
        for (Node argument : invoke.getArguments()) {
            if (argument instanceof ChunkerInvoke) {
                final ChunkerInvoke colInvoke = ((ChunkerInvoke) argument);
                String colMethod = colInvoke.getName();
                final List<Node> colArgs = colInvoke.getArguments();
                if (colArgs.isEmpty()) {
                    // this is normal, if user has typed `newTable(stringCol(`
                    // in this case, there is no valid table definition, so we'll just skip this column.
                    // In the future, we may want to consider user-defined zero-arg functions that return
                    // ColumnDefinitions
                    continue;
                }
                final String colName = toStringLiteral(colArgs.get(0));
                if (colName == null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace()
                                .append("Unable to trace first argument back to a string literal: ")
                                .append(colInvoke.toSource())
                                .endl();
                    }
                    continue;
                }

                switch (colMethod) {
                    case "stringCol":
                        columns.add(ColumnDefinition.ofString(colName));
                        break;
                    case "dateTimeCol":
                        columns.add(ColumnDefinition.ofTime(colName));
                        break;
                    case "longCol":
                        columns.add(ColumnDefinition.ofLong(colName));
                        break;
                    case "intCol":
                        columns.add(ColumnDefinition.ofInt(colName));
                        break;
                    case "shortCol":
                        columns.add(ColumnDefinition.ofShort(colName));
                        break;
                    case "byteCol":
                        columns.add(ColumnDefinition.ofByte(colName));
                        break;
                    case "charCol":
                        columns.add(ColumnDefinition.ofChar(colName));
                        break;
                    case "doubleCol":
                        columns.add(ColumnDefinition.ofDouble(colName));
                        break;
                    case "floatCol":
                        columns.add(ColumnDefinition.ofFloat(colName));
                        break;
                    case "col":
                        // We _could_ technically try to guess from the col() varargs what the type is, but, not worth
                        // it atm.
                        columns.add(ColumnDefinition.fromGenericType(colName, Object.class));
                        break;
                    default:
                        LOGGER.warn()
                                .append("Unhandled newTable() argument ")
                                .append(argument.toSource())
                                .append(" not a recognized invocation")
                                .endl();
                        break;
                }
            } else {
                // TODO: handle ColumnDefition/etc variables
                LOGGER.warn()
                        .append("Unhandled newTable() argument ")
                        .append(argument.toSource())
                        .append(" of type ")
                        .append(argument.getClass().getName())
                        .endl();
            }
        }
        def.setColumns(columns.toArray(new ColumnDefinition[0]));
        return def;
    }

    private String toStringLiteral(final Node node) {
        if (node instanceof ChunkerString) {
            return ((ChunkerString) node).getRaw();
        } // TODO: if it's a variable name, try to trace it back to a static assignment of a string, or a binding
          // variable.
        return null;
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
