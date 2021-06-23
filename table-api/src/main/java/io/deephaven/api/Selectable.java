package io.deephaven.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public interface Selectable {

    static Selectable parse(String x) {
        if (ColumnName.isValidColumnName(x)) {
            return ImmutableColumnName.of(x); // todo
        }
        return ColumnFormula.parse(x);
    }

    static List<Selectable> from(String... values) {
        return from(Arrays.asList(values));
    }

    static List<Selectable> from(Collection<String> values) {
        return values.stream().map(Selectable::parse).collect(Collectors.toList());
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnFormula columnFormula);
    }
}
