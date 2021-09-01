package io.deephaven.client;

import io.deephaven.qst.TableCreationLogic;

import java.util.Objects;

public class AnnotatedTable {

    public static AnnotatedTable ofStatic(TableCreationLogic logic, long size) {
        return new AnnotatedTable(logic, true, size);
    }

    public static AnnotatedTable ofDynamic(TableCreationLogic logic) {
        return new AnnotatedTable(logic, false, 0);
    }

    private final TableCreationLogic logic;
    private final boolean isStatic;
    private final long size;

    AnnotatedTable(TableCreationLogic logic, boolean isStatic, long size) {
        this.logic = Objects.requireNonNull(logic);
        this.isStatic = isStatic;
        this.size = size;
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
