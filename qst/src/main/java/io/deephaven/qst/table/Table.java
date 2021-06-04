package io.deephaven.qst.table;

public interface Table {

    static EmptyTable empty(long size) {
        return EmptyTable.of(size);
    }

    static EmptyTable empty(long size, TableHeader header) {
        return EmptyTable.of(size, header);
    }

    HeadTable head(long size);

    TailTable tail(long size);
}
