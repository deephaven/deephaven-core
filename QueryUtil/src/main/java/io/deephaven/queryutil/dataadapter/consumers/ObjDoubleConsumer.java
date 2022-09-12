package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjDoubleConsumer<R> {
    void accept(R record, double colValue);
}
