//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

class RexVisitorBase<T> implements RexVisitor<T> {
    @Override
    public T visitInputRef(RexInputRef inputRef) {
        throw unsupported(inputRef);
    }

    @Override
    public T visitLocalRef(RexLocalRef localRef) {
        throw unsupported(localRef);
    }

    @Override
    public T visitLiteral(RexLiteral literal) {
        throw unsupported(literal);
    }

    @Override
    public T visitCall(RexCall call) {
        throw unsupported(call);
    }

    @Override
    public T visitOver(RexOver over) {
        throw unsupported(over);
    }

    @Override
    public T visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw unsupported(correlVariable);
    }

    @Override
    public T visitDynamicParam(RexDynamicParam dynamicParam) {
        throw unsupported(dynamicParam);
    }

    @Override
    public T visitRangeRef(RexRangeRef rangeRef) {
        throw unsupported(rangeRef);
    }

    @Override
    public T visitFieldAccess(RexFieldAccess fieldAccess) {
        throw unsupported(fieldAccess);
    }

    @Override
    public T visitSubQuery(RexSubQuery subQuery) {
        throw unsupported(subQuery);
    }

    @Override
    public T visitTableInputRef(RexTableInputRef fieldRef) {
        throw unsupported(fieldRef);
    }

    @Override
    public T visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw unsupported(fieldRef);
    }

    private UnsupportedOperationException unsupported(RexNode node) {
        return new UnsupportedOperationException(String.format("%s: %s", getClass().getName(), node.toString()));
    }
}
