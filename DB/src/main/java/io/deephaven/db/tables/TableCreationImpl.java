package io.deephaven.db.tables;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.qst.TableCreation;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeProvider;
import io.deephaven.qst.table.TimeProviderSystem;
import io.deephaven.qst.table.TimeTable;

import java.util.Collection;
import java.util.Objects;

enum TableCreationImpl implements TableCreation<Table> {
    INSTANCE;

    public static Table create(TableSpec table) {
        return TableCreation.create(INSTANCE, TableToOperationsImpl.INSTANCE, OperationsToTableImpl.INSTANCE, table);
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
        final io.deephaven.db.v2.utils.TimeProvider provider = TimeProviderAdapter
            .of(timeTable.timeProvider());
        final DBDateTime firstTime = timeTable.startTime().map(DBDateTime::of).orElse(null);
        return TableTools.timeTable(provider, firstTime, timeTable.interval().toNanos());
    }

    @Override
    public final Table merge(Collection<Table> tables) {
        return TableTools.merge(tables);
    }

    static class TimeProviderAdapter implements TimeProvider.Visitor {

        public static io.deephaven.db.v2.utils.TimeProvider of(TimeProvider provider) {
            return provider.walk(new TimeProviderAdapter()).getOut();
        }

        private static final io.deephaven.db.v2.utils.TimeProvider SYSTEM_PROVIDER = DBTimeUtils::currentTime;

        private io.deephaven.db.v2.utils.TimeProvider out;

        public io.deephaven.db.v2.utils.TimeProvider getOut() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(TimeProviderSystem system) {
            out = SYSTEM_PROVIDER;
        }
    }
}
