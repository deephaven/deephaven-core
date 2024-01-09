package io.deephaven.dataadapter.consumers;

public interface ObjShortConsumer<R> {
    void accept(R record, short colValue);
}
