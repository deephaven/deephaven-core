/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see TableOperations#lazyUpdate(Collection)
 */
@Immutable
@NodeStyle
public abstract class LazyUpdateTable extends TableBase implements SelectableTable {

    public static Builder builder() {
        return ImmutableLazyUpdateTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends SelectableTable.Builder<LazyUpdateTable, Builder> {

    }
}
