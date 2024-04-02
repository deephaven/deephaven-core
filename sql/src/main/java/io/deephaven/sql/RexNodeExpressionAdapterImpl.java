//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

final class RexNodeExpressionAdapterImpl extends RexVisitorBase<Expression> implements RexNodeExpressionAdapter {
    private static final Map<SqlOperator, BiFunction<RexNodeExpressionAdapterImpl, RexCall, Expression>> ops =
            Map.ofEntries(
                    Map.entry(SqlStdOperatorTable.PI, RexNodeExpressionAdapterImpl::pi),
                    Map.entry(SqlStdOperatorTable.RAND, RexNodeExpressionAdapterImpl::rand),
                    Map.entry(SqlStdOperatorTable.CURRENT_TIMESTAMP, RexNodeExpressionAdapterImpl::currentTimestamp),
                    Map.entry(SqlStdOperatorTable.UNARY_MINUS, RexNodeExpressionAdapterImpl::unaryMinus),
                    Map.entry(SqlStdOperatorTable.SQRT, RexNodeExpressionAdapterImpl::sqrt),
                    Map.entry(SqlStdOperatorTable.LN, RexNodeExpressionAdapterImpl::ln),
                    Map.entry(SqlStdOperatorTable.ABS, RexNodeExpressionAdapterImpl::abs),
                    Map.entry(SqlStdOperatorTable.SIN, RexNodeExpressionAdapterImpl::sin),
                    Map.entry(SqlStdOperatorTable.COS, RexNodeExpressionAdapterImpl::cos),
                    Map.entry(SqlStdOperatorTable.TAN, RexNodeExpressionAdapterImpl::tan),
                    Map.entry(SqlStdOperatorTable.ASIN, RexNodeExpressionAdapterImpl::asin),
                    Map.entry(SqlStdOperatorTable.ACOS, RexNodeExpressionAdapterImpl::acos),
                    Map.entry(SqlStdOperatorTable.ATAN, RexNodeExpressionAdapterImpl::atan),
                    Map.entry(SqlStdOperatorTable.SIGN, RexNodeExpressionAdapterImpl::sign),
                    Map.entry(SqlStdOperatorTable.ROUND, RexNodeExpressionAdapterImpl::round),
                    Map.entry(SqlStdOperatorTable.CEIL, RexNodeExpressionAdapterImpl::ceil),
                    Map.entry(SqlStdOperatorTable.FLOOR, RexNodeExpressionAdapterImpl::floor),
                    Map.entry(SqlStdOperatorTable.EXP, RexNodeExpressionAdapterImpl::exp),
                    Map.entry(SqlStdOperatorTable.PLUS, RexNodeExpressionAdapterImpl::plus),
                    Map.entry(SqlStdOperatorTable.MINUS, RexNodeExpressionAdapterImpl::minus),
                    Map.entry(SqlStdOperatorTable.MULTIPLY, RexNodeExpressionAdapterImpl::multiply),
                    Map.entry(SqlStdOperatorTable.DIVIDE, RexNodeExpressionAdapterImpl::divide),
                    Map.entry(SqlStdOperatorTable.PERCENT_REMAINDER, RexNodeExpressionAdapterImpl::percentRemainder),
                    Map.entry(SqlStdOperatorTable.POWER, RexNodeExpressionAdapterImpl::power),
                    // Unsupported
                    Map.entry(SqlStdOperatorTable.LOG10, RexNodeExpressionAdapterImpl::log10),
                    Map.entry(SqlStdOperatorTable.ATAN2, RexNodeExpressionAdapterImpl::atan2),
                    Map.entry(SqlStdOperatorTable.CBRT, RexNodeExpressionAdapterImpl::cbrt),
                    Map.entry(SqlStdOperatorTable.DEGREES, RexNodeExpressionAdapterImpl::degrees),
                    Map.entry(SqlStdOperatorTable.RADIANS, RexNodeExpressionAdapterImpl::radians));

    private final RelNode node;
    private final FieldAdapter fieldAdapter;

    RexNodeExpressionAdapterImpl(RelNode node, FieldAdapter fieldAdapter) {
        this.node = Objects.requireNonNull(node);
        this.fieldAdapter = Objects.requireNonNull(fieldAdapter);
    }

    @Override
    public Expression expression(RexNode node) {
        // SQLTODO(custom-expression)
        // At a minimum, we'll probably want a way to express creating an expression referencing query scope params:
        //
        // SELECT MyIntColumn + query_scope("MyQueryScopeVar") FROM ...
        //
        // We'll likely want to be more general in support of this, and have a way to express generic
        //
        // SELECT custom_function("com.example.Example.myFunction", MyIntColumn) FROM ...
        return node.accept(this);
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        return fieldAdapter.input(inputRef, Helper.inputField(node, inputRef.getIndex()));
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        return LiteralAdapter.of(literal);
    }

    @Override
    public Expression visitCall(RexCall call) {
        if (RexNodeFilterAdapterImpl.isFilter(call)) {
            return fieldAdapter.filter(node, call);
        }
        final BiFunction<RexNodeExpressionAdapterImpl, RexCall, Expression> bf = ops.get(call.op);
        if (bf != null) {
            return bf.apply(this, call);
        }
        throw new UnsupportedOperationException("Unsupported operator " + call.op.getName());
    }

    private Expression pi(RexCall call) {
        if (!call.operands.isEmpty()) {
            throw new IllegalArgumentException("Expected 0 argument operator");
        }
        return RawString.of("java.lang.Math.PI");
    }

    private Expression rand(RexCall call) {
        if (!call.operands.isEmpty()) {
            throw new UnsupportedOperationException("Unsupported RAND with seed");
        }
        return RawString.of("java.lang.Math.random()");
    }

    private Expression currentTimestamp(RexCall call) {
        if (!call.operands.isEmpty()) {
            throw new IllegalArgumentException("Expected 0 argument operator");
        }
        return RawString.of("java.time.Instant.now()");
    }

    private Function unaryMinus(RexCall call) {
        if (call.operands.size() != 1) {
            throw new IllegalArgumentException("Expected 1 argument operator");
        }
        return Function.builder().name("-").addArguments(expression(call.operands.get(0))).build();
    }

    private Expression sqrt(RexCall call) {
        return apply1("sqrt", call);
    }

    private Expression ln(RexCall call) {
        return apply1("log", call);
    }

    private Expression abs(RexCall call) {
        return apply1("abs", call);
    }

    private Expression sin(RexCall call) {
        return apply1("sin", call);
    }

    private Expression cos(RexCall call) {
        return apply1("cos", call);
    }

    private Expression tan(RexCall call) {
        return apply1("tan", call);
    }

    private Expression asin(RexCall call) {
        return apply1("asin", call);
    }

    private Expression acos(RexCall call) {
        return apply1("acos", call);
    }

    private Expression atan(RexCall call) {
        return apply1("atan", call);
    }

    private Expression sign(RexCall call) {
        return apply1("signum", call);
    }

    private Expression round(RexCall call) {
        return apply1("round", call);
    }

    private Expression ceil(RexCall call) {
        return apply1("ceil", call);
    }

    private Expression floor(RexCall call) {
        return apply1("floor", call);
    }

    private Expression exp(RexCall call) {
        return apply1("exp", call);
    }

    private Expression plus(RexCall call) {
        return apply2("plus", call);
    }

    private Expression minus(RexCall call) {
        return apply2("minus", call);
    }

    private Expression multiply(RexCall call) {
        return apply2("multiply", call);
    }

    private Expression divide(RexCall call) {
        return apply2("divide", call);
    }

    private Expression percentRemainder(RexCall call) {
        return apply2("remainder", call);
    }

    private Expression power(RexCall call) {
        return apply2("pow", call);
    }

    private Expression log10(RexCall call) {
        throw new UnsupportedOperationException(
                "No support for log10, see https://github.com/deephaven/deephaven-core/issues/3516");
    }

    private Expression atan2(RexCall call) {
        throw new UnsupportedOperationException(
                "No support for atan2, see https://github.com/deephaven/deephaven-core/issues/3517");
    }

    private Expression cbrt(RexCall call) {
        throw new UnsupportedOperationException(
                "No support for cube root, see https://github.com/deephaven/deephaven-core/issues/3518");
    }

    private Expression degrees(RexCall call) {
        throw new UnsupportedOperationException(
                "No support for degrees, see https://github.com/deephaven/deephaven-core/issues/3519");
    }

    private Expression radians(RexCall call) {
        throw new UnsupportedOperationException(
                "No support for radians, see https://github.com/deephaven/deephaven-core/issues/3520");
    }

    private Function apply1(String name, RexCall call) {
        if (call.operands.size() != 1) {
            throw new IllegalArgumentException("Expected 1 argument operator");
        }
        return Function.builder().name(name).addArguments(expression(call.operands.get(0))).build();
    }

    private Function apply2(String name, RexCall call) {
        if (call.operands.size() != 2) {
            throw new IllegalArgumentException("Expected 2 argument operator");
        }
        final Expression lhs = expression(call.operands.get(0));
        final Expression rhs = expression(call.operands.get(1));
        return Function.builder().name(name).addArguments(lhs, rhs).build();
    }
}
