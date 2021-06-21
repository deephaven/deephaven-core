package io.deephaven.qst;

import io.deephaven.db.v2.QueryTable;
import io.deephaven.qst.ExportManagerDb.State.Export;
import io.deephaven.qst.manager.ExportManager;
import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.qst.manager.ExportedTableBase;
import io.deephaven.qst.table.Table;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExportManagerDb implements ExportManager {

    public static Export cast(ExportedTable table) {
        if (!(table instanceof Export)) {
            throw new IllegalArgumentException(String.format("Table was not created from %s", ExportManagerDb.class));
        }
        return (Export)table;
    }

    private final Map<Table, State> exports;

    public ExportManagerDb() {
        exports = new HashMap<>();
    }

    @Override
    public synchronized final Export export(Table table) {
        State state = exports.get(table);
        if (state != null) {
            return state.newRef();
        }
        final QueryTable queryTable = io.deephaven.db.tables.Table.of(table);
        state = new State(table, queryTable);
        exports.put(table, state);
        return state.newRef();
    }

    @Override
    public synchronized final List<Export> export(Collection<Table> tables) {
        return tables.stream().map(this::export).collect(Collectors.toList());
    }

    public class State extends io.deephaven.util.referencecounting.ReferenceCounted {
        private final Table table;
        private QueryTable canonicalTable;

        private State(Table table, QueryTable canonicalTable) {
            super(0);
            this.table = Objects.requireNonNull(table);
            this.canonicalTable = Objects.requireNonNull(canonicalTable);
        }

        Export newRef() {
            synchronized (ExportManagerDb.this) {
                incrementReferenceCount();
                return new Export((QueryTable) canonicalTable.copy());
            }
        }

        @Override
        protected void onReferenceCountAtZero() {
            canonicalTable.close();
            canonicalTable = null;
            exports.remove(table, this);
        }

        /**
         * A {@link QueryTable} export.
         */
        public class Export extends ExportedTableBase<Export> {

            private QueryTable queryTable;

            private Export(QueryTable queryTable) {
                super(table);
                this.queryTable = Objects.requireNonNull(queryTable);
            }

            /**
             * Get the query table. The ownership / lifecycle must be maintained with respect to
             * {@code this}.
             *
             * @return the query table
             */
            public QueryTable getQueryTable() {
                final QueryTable localRef = queryTable;
                // not guarding getter w/ synchronize, but this should provide some level of safety
                if (localRef == null) {
                    throw new IllegalStateException("Table has already been released");
                }
                return localRef;
            }

            @Override
            protected void releaseImpl() {
                synchronized (ExportManagerDb.this) {
                    queryTable.close();
                    queryTable = null;
                    decrementReferenceCount();
                }
            }

            @Override
            protected Export newRefImpl() {
                return State.this.newRef();
            }
        }
    }
}
