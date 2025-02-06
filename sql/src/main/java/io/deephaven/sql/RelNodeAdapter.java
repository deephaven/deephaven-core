//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.RelNode;

interface RelNodeAdapter {

    TableSpec table(RelNode node);
}
