package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.TableOperationsAdapter;
import io.deephaven.qst.TableCreator.OperationsToTable;

import java.util.Objects;

/**
 * A table operations adapter that mixes-in {@linkplain StackTraceElement stack trace elements}.
 *
 * @param <TOPS>
 * @param <TABLE>
 * @see StackTraceMixInCreator
 */
public final class StackTraceMixIn<TOPS extends TableOperations<TOPS, TABLE>, TABLE> extends
        TableOperationsAdapter<StackTraceMixIn<TOPS, TABLE>, StackTraceMixIn<TOPS, TABLE>, TOPS, TABLE> {

    // Note: StackWalker available in Java 9+

    private final StackTraceMixInCreator<TOPS, TABLE> creator;
    private final OperationsToTable<TOPS, TABLE> opsToTable;
    private final StackTraceElement[] elements;

    StackTraceMixIn(StackTraceMixInCreator<TOPS, TABLE> creator,
            OperationsToTable<TOPS, TABLE> opsToTable, TOPS delegate, StackTraceElement[] elements) {
        super(delegate);
        this.opsToTable = Objects.requireNonNull(opsToTable);
        this.creator = Objects.requireNonNull(creator);
        this.elements = Objects.requireNonNull(elements);
    }

    StackTraceMixIn(StackTraceMixInCreator<TOPS, TABLE> creator,
            OperationsToTable<TOPS, TABLE> opsToTable, TOPS delegate) {
        super(delegate);
        this.opsToTable = Objects.requireNonNull(opsToTable);
        this.creator = Objects.requireNonNull(creator);
        this.elements = null;
    }

    public StackTraceElement[] elements() {
        return elements;
    }

    public TOPS ops() {
        return delegate();
    }

    TABLE table() {
        return opsToTable.of(delegate());
    }

    @Override
    protected StackTraceMixIn<TOPS, TABLE> adapt(TOPS ops) {
        return creator.add(ops);
    }

    @Override
    protected TABLE adapt(StackTraceMixIn<TOPS, TABLE> rhs) {
        if (creator != rhs.creator) {
            throw new IllegalStateException("Can't mix tables from multiple creators");
        }
        return rhs.table();
    }
}
