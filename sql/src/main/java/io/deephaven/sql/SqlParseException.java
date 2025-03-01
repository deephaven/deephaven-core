//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

public final class SqlParseException extends RuntimeException {

    SqlParseException(org.apache.calcite.sql.parser.SqlParseException cause) {
        super(cause);
    }
}
