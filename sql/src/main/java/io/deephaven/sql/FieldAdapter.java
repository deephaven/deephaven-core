//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

interface FieldAdapter extends OutputFieldAdapter {

    ColumnName input(RexInputRef inputRef, RelDataTypeField inputField);

    RexNodeFilterAdapter filterAdapter(RelNode parent);

    RexNodeExpressionAdapter expressionAdapter(RelNode parent);

    default Filter filter(RelNode parent, RexNode node) {
        return filterAdapter(parent).filter(node);
    }

    default Expression expression(RelNode parent, RexNode node) {
        return expressionAdapter(parent).expression(node);
    }
}
