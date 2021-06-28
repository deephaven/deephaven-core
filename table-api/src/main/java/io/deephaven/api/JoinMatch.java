package io.deephaven.api;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public interface JoinMatch {

    static JoinMatch parse(String x) {
        if (ColumnName.isValidParsedColumnName(x)) {
            return ColumnName.parse(x);
        }
        return ColumnMatch.parse(x);
    }

    static List<JoinMatch> from(Collection<String> values) {
        return values.stream().map(JoinMatch::parse).collect(Collectors.toList());
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName columnName);

        void visit(ColumnMatch columnMatch);
    }
}
