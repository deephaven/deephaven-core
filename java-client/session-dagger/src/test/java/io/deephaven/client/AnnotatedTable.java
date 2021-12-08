package io.deephaven.client;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableHeader;

import java.util.Objects;

public class AnnotatedTable {

    public static AnnotatedTable ofStatic(TableHeader header, TableCreationLogic logic, long size) {
        return new AnnotatedTable(header, logic, true, size);
    }

    public static AnnotatedTable ofDynamic(TableHeader header, TableCreationLogic logic) {
        return new AnnotatedTable(header, logic, false, 0);
    }

    private final TableHeader header;
    private final TableCreationLogic logic;
    private final boolean isStatic;
    private final long size;

    AnnotatedTable(TableHeader header, TableCreationLogic logic, boolean isStatic, long size) {
        this.header = Objects.requireNonNull(header);
        this.logic = Objects.requireNonNull(logic);
        this.isStatic = isStatic;
        this.size = size;
    }

    public TableHeader header() {
        return header;
    }

    public TableCreationLogic logic() {
        return logic;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public long size() {
        return size;
    }
}
