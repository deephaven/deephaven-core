package io.deephaven.api;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public interface JoinAddition {

    static JoinAddition parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
        }
        return ColumnAssignment.parse(x);
    }

    static List<JoinAddition> from(Collection<String> values) {
        return values.stream().map(JoinAddition::parse).collect(Collectors.toList());
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnAssignment columnAssignment);
    }
}
