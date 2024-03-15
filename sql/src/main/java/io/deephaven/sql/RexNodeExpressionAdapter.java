//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.expression.Expression;
import org.apache.calcite.rex.RexNode;

interface RexNodeExpressionAdapter {

    Expression expression(RexNode node);
}
