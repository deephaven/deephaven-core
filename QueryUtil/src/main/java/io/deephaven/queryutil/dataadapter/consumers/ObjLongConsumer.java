package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjLongConsumer<R> {
    void accept(R record, long colValue);
}
