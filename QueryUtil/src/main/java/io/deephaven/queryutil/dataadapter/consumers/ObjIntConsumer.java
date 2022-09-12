package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjIntConsumer<R> {
    void accept(R record, int colValue);
}
