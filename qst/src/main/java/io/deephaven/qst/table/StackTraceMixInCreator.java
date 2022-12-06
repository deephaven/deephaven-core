/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

public final class StackTraceMixInCreator<TOPS extends TableOperations<TOPS, TABLE>, TABLE>
        implements TableCreator<StackTraceMixIn<TOPS, TABLE>> {

    public static StackTraceMixInCreator<TableSpec, TableSpec> of() {
        return new StackTraceMixInCreator<>(TableCreatorImpl.INSTANCE,
                TableToOperationsImpl.INSTANCE, OperationsToTableImpl.INSTANCE);
    }

    private final TableCreator<TABLE> creator;
    private final TableToOperations<TOPS, TABLE> toOps;
    private final OperationsToTable<TOPS, TABLE> toTable;
    private final Map<TOPS, StackTraceMixIn<TOPS, TABLE>> map;

    StackTraceMixInCreator(TableCreator<TABLE> creator, TableToOperations<TOPS, TABLE> toOps,
            OperationsToTable<TOPS, TABLE> toTable) {
        this.creator = Objects.requireNonNull(creator);
        this.toOps = Objects.requireNonNull(toOps);
        this.toTable = Objects.requireNonNull(toTable);
        this.map = new HashMap<>();
    }

    public Optional<StackTraceElement[]> elements(TOPS tops) {
        return Optional.ofNullable(map.get(tops)).map(StackTraceMixIn::elements);
    }

    public synchronized StackTraceMixIn<TOPS, TABLE> adapt(TOPS tops) {
        // note: no use in creating the stacktrace if we've *already* created the actual type TOPS
        return map.computeIfAbsent(tops, this::mixinNoStacktrace);
    }

    synchronized StackTraceMixIn<TOPS, TABLE> add(TOPS tops) {
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> of(NewTable newTable) {
        final TOPS tops = toOps.of(creator.of(newTable));
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> of(EmptyTable emptyTable) {
        final TOPS tops = toOps.of(creator.of(emptyTable));
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> of(TimeTable timeTable) {
        TOPS tops = toOps.of(creator.of(timeTable));
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> of(TicketTable ticketTable) {
        TOPS tops = toOps.of(creator.of(ticketTable));
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> of(InputTable inputTable) {
        TOPS tops = toOps.of(creator.of(inputTable));
        return map.computeIfAbsent(tops, this::mixin);
    }

    @Override
    public synchronized StackTraceMixIn<TOPS, TABLE> merge(
            Iterable<StackTraceMixIn<TOPS, TABLE>> stackTraceMixIns) {
        final Iterable<TABLE> tables = () -> StreamSupport
                .stream(stackTraceMixIns.spliterator(), false).map(StackTraceMixIn::table).iterator();
        final TOPS tops = toOps.of(creator.merge(tables));
        return map.computeIfAbsent(tops, this::mixin);
    }

    private StackTraceMixIn<TOPS, TABLE> mixin(TOPS tops) {
        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        return new StackTraceMixIn<>(this, toTable, tops, trimElements(stackTrace));
    }

    private StackTraceMixIn<TOPS, TABLE> mixinNoStacktrace(TOPS tops) {
        return new StackTraceMixIn<>(this, toTable, tops);
    }

    private static StackTraceElement[] trimElements(StackTraceElement[] elements) {
        int lastMixInIndex = -1;
        for (int i = 0; i < Math.min(10, elements.length); ++i) {
            if (StackTraceMixIn.class.getName().equals(elements[i].getClassName())
                    || StackTraceMixInCreator.class.getName().equals(elements[i].getClassName())) {
                lastMixInIndex = i;
            }
        }
        if (lastMixInIndex >= 0) {
            // Note: depending on the exact call site, the first call into StackTraceMixIn or
            // StackTraceMixInCreator may provide useful context. We can try to be smarter about
            // this in the future.
            return Arrays.stream(elements).skip(lastMixInIndex).toArray(StackTraceElement[]::new);
        }
        return elements;
    }
}
