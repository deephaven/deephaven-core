package io.deephaven.queryutil.dataadapter.consumers;

public interface ObjCharConsumer<R> {
    void accept(R record, char colValue);
}
