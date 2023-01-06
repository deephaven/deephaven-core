package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Builder;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.qst.table.SnapshotTable;
import io.deephaven.qst.table.SnapshotWhenTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;
import java.util.function.Supplier;

public abstract class SnapshotWhenSessionTestBase extends TableSpecTestBase {

    static Iterable<Object[]> specs(Supplier<Builder> builder) {
        return iterable(
                snapshot(),
                snapshotTicking(builder.get()),
                snapshotTickingDoInitial(builder.get()),
                snapshotTickingStamp(builder.get()),
                snapshotTickingStampRename(builder.get()),
                snapshotTickingStampDoInitial(builder.get()));
    }

    static SnapshotTable snapshot() {
        return TimeTable.of(Duration.ofSeconds(1)).snapshot();
    }

    static SnapshotWhenTable snapshotTicking(Builder builder) {
        return TimeTable.of(Duration.ofSeconds(1)).view("Id=ii")
                .snapshotWhen(TimeTable.of(Duration.ofSeconds(2)), builder.build());
    }

    static SnapshotWhenTable snapshotTickingDoInitial(Builder builder) {
        return TimeTable.of(Duration.ofSeconds(1)).view("Id=ii")
                .snapshotWhen(TimeTable.of(Duration.ofSeconds(2)), builder.build());
    }

    static SnapshotWhenTable snapshotTickingStamp(Builder builder) {
        final SnapshotWhenOptions control = builder.addStampColumns(ColumnName.of("Timestamp")).build();
        return TimeTable.of(Duration.ofSeconds(1)).view("Id=ii")
                .snapshotWhen(TimeTable.of(Duration.ofSeconds(2)).updateView("Id=ii"), control);
    }

    static SnapshotWhenTable snapshotTickingStampRename(Builder builder) {
        final SnapshotWhenOptions control =
                builder.addStampColumns(JoinAddition.parse("SnapshotTimestamp=Timestamp")).build();
        return TimeTable.of(Duration.ofSeconds(1))
                .snapshotWhen(TimeTable.of(Duration.ofSeconds(2)), control);
    }

    static SnapshotWhenTable snapshotTickingStampDoInitial(Builder builder) {
        final SnapshotWhenOptions control = builder
                .addStampColumns(ColumnName.of("Timestamp"))
                .addFlags(Flag.INITIAL)
                .build();
        return TimeTable.of(Duration.ofSeconds(1)).view("Id=ii")
                .snapshotWhen(TimeTable.of(Duration.ofSeconds(2)).updateView("Id=ii"), control);
    }

    public SnapshotWhenSessionTestBase(TableSpec table) {
        super(table);
    }
}
