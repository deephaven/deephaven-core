package io.deephaven.dataadapter.consumers;

public interface ObjFloatConsumer<R> {
    void accept(R record, float colValue);
}
