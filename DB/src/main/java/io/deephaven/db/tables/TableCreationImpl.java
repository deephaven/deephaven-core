package io.deephaven.db.tables;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.qst.TableCreation;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TimeProvider;
import io.deephaven.qst.table.TimeProviderSystem;
import io.deephaven.qst.table.TimeTable;

import java.util.Collection;
import java.util.Objects;

enum TableCreationImpl implements TableCreation<Table> {
    INSTANCE;

    public static QueryTable create(io.deephaven.qst.table.Table table) {
        Table queryTable = TableCreation.create(INSTANCE, TableToOperationsImpl.INSTANCE, OperationsToTableImpl.INSTANCE, table);
        return (QueryTable) queryTable;
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
        final io.deephaven.db.v2.TimeTable tt = new io.deephaven.db.v2.TimeTable(provider, firstTime, timeTable.timeout().toNanos());
        LiveTableMonitor.DEFAULT.addTable(tt);
        return tt;
    }

    @Override
    public final Table of(Collection<Table> tables) {
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
