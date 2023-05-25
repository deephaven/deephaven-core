/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedMutableTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTime;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSchema;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.Clock;
import io.deephaven.qst.table.ClockSystem;
import io.deephaven.qst.table.TimeTable;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Engine-specific implementation of {@link TableCreator}.
 */
public enum TableCreatorImpl implements TableCreator<Table> {

    INSTANCE;

    @SuppressWarnings("unused")
    @AutoService(TableFactory.TableCreatorProvider.class)
    public static final class TableCreatorProvider implements TableFactory.TableCreatorProvider {

        @Override
        public TableCreator<Table> get() {
            return INSTANCE;
        }
    }

    public static Table create(TableSpec table) {
        return table.logic().create(INSTANCE);
    }

    @Override
    public final Table of(NewTable newTable) {
        return InMemoryTable.from(newTable);
    }

    @Override
    public final Table of(EmptyTable emptyTable) {
        return TableTools.emptyTable(emptyTable.size());
    }

    @Override
    public final Table of(TimeTable timeTable) {
        final io.deephaven.base.clock.Clock clock = ClockAdapter.of(timeTable.clock());
        final DateTime firstTime = timeTable.startTime().map(DateTime::of).orElse(null);
        return TableTools.timeTable(clock, firstTime, timeTable.interval().toNanos());
    }

    @Override
    public final Table of(TicketTable ticketTable) {
        throw new UnsupportedOperationException("Ticket tables can't be referenced in a static context;" +
                "no access to TicketRouter nor SessionState - see deephaven-core#1172 for more details");
    }

    @Override
    public final UpdatableTable of(InputTable inputTable) {
        return UpdatableTableAdapter.of(inputTable);
    }


    @Override
    public final Table merge(Iterable<Table> tables) {
        return TableTools.merge(StreamSupport.stream(tables.spliterator(), false).toArray(Table[]::new));
    }

    @Override
    public final Table merge(Table t1, Table t2) {
        return TableTools.merge(t1, t2);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3) {
        return TableTools.merge(t1, t2, t3);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4) {
        return TableTools.merge(t1, t2, t3, t4);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5) {
        return TableTools.merge(t1, t2, t3, t4, t5);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6) {
        return TableTools.merge(t1, t2, t3, t4, t5, t6);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7) {
        return TableTools.merge(t1, t2, t3, t4, t5, t6, t7);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7, Table t8) {
        return TableTools.merge(t1, t2, t3, t4, t5, t6, t7, t8);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7, Table t8, Table t9) {
        return TableTools.merge(t1, t2, t3, t4, t5, t6, t7, t8, t9);
    }

    @Override
    public final Table merge(Table t1, Table t2, Table t3, Table t4, Table t5, Table t6, Table t7, Table t8, Table t9,
            Table... remaining) {
        return TableTools.merge(Stream.concat(Stream.of(t1, t2, t3, t4, t5, t6, t7, t8, t9), Arrays.stream(remaining))
                .toArray(Table[]::new));
    }

    @Override
    public final Table merge(Table[] tables) {
        return TableTools.merge(tables);
    }

    static class ClockAdapter implements Clock.Visitor {

        public static io.deephaven.base.clock.Clock of(Clock provider) {
            return provider.walk(new ClockAdapter()).getOut();
        }

        private io.deephaven.base.clock.Clock out;

        public io.deephaven.base.clock.Clock getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(ClockSystem system) {
            out = io.deephaven.base.clock.Clock.system();
        }
    }

    static class UpdatableTableAdapter implements InputTable.Visitor {

        public static UpdatableTable of(InputTable inputTable) {
            return inputTable.walk(new UpdatableTableAdapter()).out();
        }

        private UpdatableTable out;

        public UpdatableTable out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
            final TableDefinition definition = DefinitionAdapter.of(inMemoryAppendOnly.schema());
            out = AppendOnlyArrayBackedMutableTable.make(definition);
        }

        @Override
        public void visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
            final TableDefinition definition = DefinitionAdapter.of(inMemoryKeyBacked.schema());
            final String[] keyColumnNames = inMemoryKeyBacked.keys().toArray(String[]::new);
            out = KeyedArrayBackedMutableTable.make(definition, keyColumnNames);
        }
    }

    static class DefinitionAdapter implements TableSchema.Visitor {

        public static TableDefinition of(TableSchema schema) {
            return schema.walk(new DefinitionAdapter()).out();
        }

        private TableDefinition out;

        public TableDefinition out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(TableSpec spec) {
            out = create(spec).getDefinition();
        }

        @Override
        public void visit(TableHeader header) {
            out = TableDefinition.from(header);
        }
    }
}
