//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.filter.Filter;
import org.apache.calcite.rex.RexNode;

interface RexNodeFilterAdapter {

    Filter filter(RexNode node);
}
