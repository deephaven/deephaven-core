package io.deephaven.db.tables.verify;

public class AppendOnlyAssertionFailure extends TableAssertionFailure {
    AppendOnlyAssertionFailure() {
        super("Update to table violates append-only assertion!");
    }

    AppendOnlyAssertionFailure(String description) {
        super("Update to table violates append-only assertion! (Table description: " + description + ")");
    }
}
