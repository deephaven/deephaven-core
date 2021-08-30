package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreationLogic2Inputs;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.StackTraceMixIn;
import io.deephaven.qst.table.StackTraceMixInCreator;
import io.deephaven.qst.table.TableSpec;

import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * A table handle manager that executes requests as a single batch.
 *
 * <p>
 * A batch request is executed in a single round-trip.
 *
 * <p>
 * Note: individual {@linkplain io.deephaven.api.TableOperations table operations} executed against a
 * {@linkplain TableHandle table handle} are still executed serially.
 */
class TableHandleManagerBatch extends TableHandleManagerBase {

    public static TableHandleManagerBatch of(Session session, boolean mixinStacktraces) {
        return new TableHandleManagerBatch(session, mixinStacktraces);
    }

    private final boolean mixinStacktraces;

    private TableHandleManagerBatch(Session session, boolean mixinStacktraces) {
        super(session, null);
        this.mixinStacktraces = mixinStacktraces;
    }

    @Override
    public TableHandle execute(TableSpec table) throws TableHandleException, InterruptedException {
        return TableHandle.of(session, table, lifecycle);
    }

    @Override
    public List<TableHandle> execute(Iterable<TableSpec> tables)
            throws TableHandleException, InterruptedException {
        return TableHandle.of(session, tables, lifecycle);
    }

    @Override
    public TableHandle executeLogic(TableCreationLogic logic)
            throws TableHandleException, InterruptedException {
        if (mixinStacktraces) {
            return new MixinBasic(logic).run();
        }
        return execute(TableSpec.of(logic));
    }

    @Override
    public List<TableHandle> executeLogic(Iterable<TableCreationLogic> logics)
            throws TableHandleException, InterruptedException {
        if (mixinStacktraces) {
            return new MixinIterable(logics).run();
        }
        return execute(
                () -> StreamSupport.stream(logics.spliterator(), false).map(TableSpec::of).iterator());
    }

    @Override
    public LabeledValues<TableHandle> executeLogic(TableCreationLabeledLogic logic)
            throws TableHandleException, InterruptedException {
        if (mixinStacktraces) {
            return new MixinLabeled(logic).run();
        }
        return execute(LabeledTables.of(logic));
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic1Input logic, TableHandle t1)
            throws TableHandleException, InterruptedException {
        if (mixinStacktraces) {
            return new Mixin1Handle(logic, t1).run();
        }
        final TableSpec table1 = t1.table();
        final TableSpec tableOut = logic.create(table1);
        return execute(tableOut);
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic2Inputs logic, TableHandle t1,
            TableHandle t2) throws TableHandleException, InterruptedException {
        if (mixinStacktraces) {
            return new Mixin2Handles(logic, t1, t2).run();
        }
        final TableSpec table1 = t1.table();
        final TableSpec table2 = t2.table();
        final TableSpec tableOut = logic.create(table1, table2);
        return execute(tableOut);
    }

    private abstract class MixinBase<T> {
        final StackTraceMixInCreator<TableSpec, TableSpec> creator = StackTraceMixInCreator.of();

        abstract T runImpl() throws InterruptedException, TableHandleException;

        T run() throws InterruptedException, TableHandleException {
            try {
                return runImpl();
            } catch (TableHandleException e) {
                throw mixinStacktrace(e);
            }
        }

        private TableHandleException mixinStacktrace(TableHandleException t) {
            // TODO (deephaven-core#986): ExportedTableUpdateMessage should contain first dependent ticket failure if
            // exists
            TableSpec tableThatErrored = t.handle().table();
            return creator.elements(tableThatErrored).map(t::mixinStacktrace).orElse(t);
        }
    }

    private class MixinBasic extends MixinBase<TableHandle> {
        private final TableCreationLogic logic;

        public MixinBasic(TableCreationLogic logic) {
            this.logic = Objects.requireNonNull(logic);
        }

        @Override
        protected TableHandle runImpl() throws InterruptedException, TableHandleException {
            final StackTraceMixIn<TableSpec, TableSpec> mixin = logic.create(creator);
            final TableSpec table = mixin.ops();
            return execute(table);
        }
    }

    private class MixinLabeled extends MixinBase<LabeledValues<TableHandle>> {
        private final TableCreationLabeledLogic logic;

        public MixinLabeled(TableCreationLabeledLogic logic) {
            this.logic = Objects.requireNonNull(logic);
        }

        @Override
        protected LabeledValues<TableHandle> runImpl()
                throws InterruptedException, TableHandleException {
            final LabeledValues<StackTraceMixIn<TableSpec, TableSpec>> mixins =
                    logic.create(creator);
            final LabeledTables labeledTables = LabeledTables.of(mixins.labels(),
                    () -> mixins.valuesStream().map(StackTraceMixIn::ops).iterator());
            return execute(labeledTables);
        }
    }

    private class MixinIterable extends MixinBase<List<TableHandle>> {
        private final Iterable<TableCreationLogic> logics;

        public MixinIterable(Iterable<TableCreationLogic> logics) {
            this.logics = Objects.requireNonNull(logics);
        }

        @Override
        protected List<TableHandle> runImpl() throws InterruptedException, TableHandleException {
            final Iterable<TableSpec> tables =
                    () -> StreamSupport.stream(logics.spliterator(), false).map(this::create)
                            .map(StackTraceMixIn::ops).iterator();
            return execute(tables);
        }

        private StackTraceMixIn<TableSpec, TableSpec> create(TableCreationLogic logic) {
            return logic.create(creator);
        }
    }

    private class Mixin1Handle extends MixinBase<TableHandle> {
        private final TableCreationLogic1Input logic;
        private final TableHandle t1;

        public Mixin1Handle(TableCreationLogic1Input logic, TableHandle t1) {
            this.logic = Objects.requireNonNull(logic);
            this.t1 = Objects.requireNonNull(t1);
        }

        @Override
        TableHandle runImpl() throws InterruptedException, TableHandleException {
            final StackTraceMixIn<TableSpec, TableSpec> mixin1 = creator.adapt(t1.table());
            final StackTraceMixIn<TableSpec, TableSpec> mixinOut = logic.create(mixin1);
            return execute(mixinOut.ops());
        }
    }

    private class Mixin2Handles extends MixinBase<TableHandle> {
        private final TableCreationLogic2Inputs logic;
        private final TableHandle t1;
        private final TableHandle t2;

        public Mixin2Handles(TableCreationLogic2Inputs logic, TableHandle t1, TableHandle t2) {
            this.logic = Objects.requireNonNull(logic);
            this.t1 = Objects.requireNonNull(t1);
            this.t2 = Objects.requireNonNull(t2);
        }

        @Override
        TableHandle runImpl() throws InterruptedException, TableHandleException {
            final StackTraceMixIn<TableSpec, TableSpec> mixin1 = creator.adapt(t1.table());
            final StackTraceMixIn<TableSpec, TableSpec> mixin2 = creator.adapt(t2.table());
            final StackTraceMixIn<TableSpec, TableSpec> mixinOut = logic.create(mixin1, mixin2);
            return execute(mixinOut.ops());
        }
    }
}
