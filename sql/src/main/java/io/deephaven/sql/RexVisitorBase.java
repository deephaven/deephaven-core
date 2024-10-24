//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
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
        throw unsupported(inputRef, RexInputRef.class);
    }

    @Override
    public T visitLocalRef(RexLocalRef localRef) {
        throw unsupported(localRef, RexLocalRef.class);
    }

    @Override
    public T visitLiteral(RexLiteral literal) {
        throw unsupported(literal, RexLiteral.class);
    }

    @Override
    public T visitCall(RexCall call) {
        throw unsupported(call, RexCall.class);
    }

    @Override
    public T visitOver(RexOver over) {
        throw unsupported(over, RexOver.class);
    }

    @Override
    public T visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw unsupported(correlVariable, RexCorrelVariable.class);
    }

    @Override
    public T visitDynamicParam(RexDynamicParam dynamicParam) {
        throw unsupported(dynamicParam, RexDynamicParam.class);
    }

    @Override
    public T visitRangeRef(RexRangeRef rangeRef) {
        throw unsupported(rangeRef, RexRangeRef.class);
    }

    @Override
    public T visitFieldAccess(RexFieldAccess fieldAccess) {
        throw unsupported(fieldAccess, RexFieldAccess.class);
    }

    @Override
    public T visitSubQuery(RexSubQuery subQuery) {
        throw unsupported(subQuery, RexSubQuery.class);
    }

    @Override
    public T visitTableInputRef(RexTableInputRef fieldRef) {
        throw unsupported(fieldRef, RexTableInputRef.class);
    }

    @Override
    public T visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw unsupported(fieldRef, RexPatternFieldRef.class);
    }

    @Override
    public T visitLambda(RexLambda fieldRef) {
        throw unsupported(fieldRef, RexLambda.class);
    }

    @Override
    public T visitLambdaRef(RexLambdaRef fieldRef) {
        throw unsupported(fieldRef, RexLambdaRef.class);
    }

    private <T extends RexNode> UnsupportedSqlOperation unsupported(T node, Class<T> clazz) {
        return new UnsupportedSqlOperation(
                String.format("%s: %s %s", getClass().getName(), node.getClass().getName(), node.toString()), clazz);
    }
}
