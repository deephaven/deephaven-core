package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;

public class ExcessiveHeadTail implements TableCreationLogic {

    private final long count;

    public ExcessiveHeadTail(long count) {
        this.count = count;
    }

    @Override
    public final <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        T base = c.of(TableSpec.empty(count * 2)).view("I=i");
        for (long i = 0; i < count; ++i) {
            base = base.head(2 * count - 2 * i);
            base = base.tail(2 * count - 2 * i - 1);
        }
        return base;
    }
}
