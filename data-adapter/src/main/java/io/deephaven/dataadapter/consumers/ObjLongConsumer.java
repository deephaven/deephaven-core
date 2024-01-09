package io.deephaven.dataadapter.consumers;

public interface ObjLongConsumer<R> extends java.util.function.ObjLongConsumer<R> {
    void accept(R record, long colValue);
}
