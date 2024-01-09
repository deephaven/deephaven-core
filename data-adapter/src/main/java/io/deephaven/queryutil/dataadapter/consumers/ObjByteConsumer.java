package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjByteConsumer<R> {
    void accept(R record, byte colValue);
}
