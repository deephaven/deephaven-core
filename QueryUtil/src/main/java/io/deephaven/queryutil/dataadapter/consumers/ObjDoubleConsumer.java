package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjDoubleConsumer<R> extends java.util.function.ObjDoubleConsumer<R> {
    void accept(R record, double colValue);
}
