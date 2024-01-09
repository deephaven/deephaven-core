package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjFloatConsumer<R> {
    void accept(R record, float colValue);
}
