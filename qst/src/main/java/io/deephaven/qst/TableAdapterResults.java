/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableAdapterResults.Output.Visitor;
import io.deephaven.qst.table.TableSpec;
import java.util.Map;
import java.util.Objects;

import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class TableAdapterResults<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

    public abstract Map<TableSpec, Output<TOPS, TABLE>> map();

    public interface Output<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

        <T, V extends Visitor<T, TOPS, TABLE>> T walk(V visitor);

        interface Visitor<T, TOPS extends TableOperations<TOPS, TABLE>, TABLE> {
            T visit(TOPS tops);

            T visit(TABLE table);
        }
    }

    /**
     * Used to collapse an output when the table operations and table type are the same generic type.
     *
     * @param <T> the table operations and table type
     */
    public static final class GetOutput<T extends TableOperations<T, T>> implements Visitor<T, T, T> {

        @Override
        public T visit(T t) {
            return t;
        }
    }
}
