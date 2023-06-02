package io.deephaven.sql;

import java.util.List;
import java.util.Optional;

public interface Scope {

    Optional<TableInformation> table(List<String> qualifiedName);
}
