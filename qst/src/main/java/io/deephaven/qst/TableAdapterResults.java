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

        <V extends Visitor<TOPS, TABLE>> V walk(V visitor);

        interface Visitor<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {
            void visit(TOPS tops);

            void visit(TABLE table);
        }
    }

    /**
     * Used to collapse an output when the table operations and table type are the same generic type.
     *
     * @param <T> the table operations and table type
     */
    public static class GetOutput<T extends TableOperations<T, T>> implements Visitor<T, T> {
        private T out;

        public T out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(T t) {
            out = t;
        }
    }
}
