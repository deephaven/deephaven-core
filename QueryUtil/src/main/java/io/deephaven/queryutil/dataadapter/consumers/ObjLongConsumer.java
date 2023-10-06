package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjLongConsumer<R> extends java.util.function.ObjLongConsumer<R> {
    void accept(R record, long colValue);
}
