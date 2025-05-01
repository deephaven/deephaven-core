//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

import java.time.LocalDateTime;

class PyIcebergTestUtils {
    static final Table EXPECTED_DATA = TableTools.newTable(
            TableTools.col("datetime",
                    LocalDateTime.of(2024, 11, 27, 10, 0, 0),
                    LocalDateTime.of(2022, 11, 27, 10, 0, 0),
                    LocalDateTime.of(2022, 11, 26, 10, 1, 0),
                    LocalDateTime.of(2023, 11, 26, 10, 2, 0),
                    LocalDateTime.of(2025, 11, 28, 10, 3, 0)),
            TableTools.stringCol("symbol", "AAPL", "MSFT", "GOOG", "AMZN", "MSFT"),
            TableTools.doubleCol("bid", 150.25, 150.25, 2800.75, 3400.5, 238.85),
            TableTools.doubleCol("ask", 151.0, 151.0, 2810.5, 3420.0, 250.0))
            .sort("datetime", "symbol");

}
