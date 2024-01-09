package io.deephaven.dataadapter.consumers;

public interface ObjIntConsumer<R> extends java.util.function.ObjIntConsumer<R> {
    void accept(R record, int colValue);
}
