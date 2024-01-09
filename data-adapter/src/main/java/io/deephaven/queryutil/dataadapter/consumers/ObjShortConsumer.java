package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjShortConsumer<R> {
    void accept(R record, short colValue);
}
