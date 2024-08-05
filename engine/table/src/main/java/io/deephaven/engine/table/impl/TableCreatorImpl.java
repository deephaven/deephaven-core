//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.auto.service.AutoService;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.MultiJoinFactory;
import io.deephaven.engine.table.MultiJoinInput;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable;
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.BlinkInputTable;
import io.deephaven.qst.table.Clock;
import io.deephaven.qst.table.ClockSystem;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InMemoryAppendOnlyInputTable;
import io.deephaven.qst.table.InMemoryKeyBackedInputTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.TableSchema;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;
import io.deephaven.stream.TablePublisher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
        return io.deephaven.engine.table.impl.TimeTable.newBuilder()
                .registrar(ExecutionContext.getContext().getUpdateGraph())
                .clock(ClockAdapter.of(timeTable.clock()))
                .startTime(timeTable.startTime().orElse(null))
                .period(timeTable.interval())
                .blinkTable(timeTable.blinkTable())
                .build();
    }

    @Override
    public final Table of(TicketTable ticketTable) {
        throw new UnsupportedOperationException("Ticket tables can't be referenced in a static context;" +
                "no access to TicketRouter nor SessionState - see deephaven-core#1172 for more details");
    }

    @Override
    public final Table of(InputTable inputTable) {
        return InputTableAdapter.of(inputTable);
    }

    @Override
    public final Table multiJoin(List<io.deephaven.qst.table.MultiJoinInput<Table>> multiJoinInputs) {
        return MultiJoinFactory.of(multiJoinInputs.stream().map(TableCreatorImpl::adapt).toArray(MultiJoinInput[]::new))
                .table();
    }

    private static MultiJoinInput adapt(io.deephaven.qst.table.MultiJoinInput<Table> input) {
        return MultiJoinInput.of(input.table(), input.matches(), input.additions());
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

    enum ClockAdapter implements Clock.Visitor<io.deephaven.base.clock.Clock> {
        INSTANCE;

        public static io.deephaven.base.clock.Clock of(Clock provider) {
            return provider.walk(INSTANCE);
        }

        @Override
        public io.deephaven.base.clock.Clock visit(ClockSystem system) {
            return io.deephaven.base.clock.Clock.system();
        }
    }

    enum InputTableAdapter implements InputTable.Visitor<Table> {
        INSTANCE;

        private static final AtomicInteger blinkTableCount = new AtomicInteger();

        public static Table of(InputTable inputTable) {
            return inputTable.walk(INSTANCE);
        }

        @Override
        public UpdatableTable visit(InMemoryAppendOnlyInputTable inMemoryAppendOnly) {
            final TableDefinition definition = DefinitionAdapter.of(inMemoryAppendOnly.schema());
            return AppendOnlyArrayBackedInputTable.make(definition);
        }

        @Override
        public UpdatableTable visit(InMemoryKeyBackedInputTable inMemoryKeyBacked) {
            final TableDefinition definition = DefinitionAdapter.of(inMemoryKeyBacked.schema());
            final String[] keyColumnNames = inMemoryKeyBacked.keys().toArray(String[]::new);
            return KeyedArrayBackedInputTable.make(definition, keyColumnNames);
        }

        @Override
        public Table visit(BlinkInputTable blinkInputTable) {
            final TableDefinition definition = DefinitionAdapter.of(blinkInputTable.schema());
            return TablePublisher
                    .of(TableCreatorImpl.class.getSimpleName() + ".BLINK-" + blinkTableCount.getAndIncrement(),
                            definition, null, null)
                    .inputTable();
        }
    }

    enum DefinitionAdapter implements TableSchema.Visitor<TableDefinition> {
        INSTANCE;

        public static TableDefinition of(TableSchema schema) {
            return schema.walk(INSTANCE);
        }

        @Override
        public TableDefinition visit(TableSpec spec) {
            return create(spec).getDefinition();
        }

        @Override
        public TableDefinition visit(TableHeader header) {
            return TableDefinition.from(header);
        }
    }
}
