//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

final class RexNodeFilterAdapterImpl extends RexVisitorBase<Filter> implements RexNodeFilterAdapter {

    private static final Map<SqlOperator, BiFunction<RexNodeFilterAdapterImpl, RexCall, Filter>> operators =
            Map.ofEntries(
                    Map.entry(SqlStdOperatorTable.EQUALS, RexNodeFilterAdapterImpl::eq),
                    Map.entry(SqlStdOperatorTable.GREATER_THAN, RexNodeFilterAdapterImpl::gt),
                    Map.entry(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, RexNodeFilterAdapterImpl::gte),
                    Map.entry(SqlStdOperatorTable.LESS_THAN, RexNodeFilterAdapterImpl::lt),
                    Map.entry(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, RexNodeFilterAdapterImpl::lte),
                    Map.entry(SqlStdOperatorTable.NOT_EQUALS, RexNodeFilterAdapterImpl::neq),
                    Map.entry(SqlStdOperatorTable.IS_DISTINCT_FROM, RexNodeFilterAdapterImpl::isDistinctFrom),
                    Map.entry(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, RexNodeFilterAdapterImpl::isNotDistinctFrom),
                    Map.entry(SqlStdOperatorTable.NOT, RexNodeFilterAdapterImpl::not),
                    Map.entry(SqlStdOperatorTable.OR, RexNodeFilterAdapterImpl::or),
                    Map.entry(SqlStdOperatorTable.AND, RexNodeFilterAdapterImpl::and),
                    Map.entry(SqlStdOperatorTable.IS_NOT_NULL, RexNodeFilterAdapterImpl::isNotNull),
                    Map.entry(SqlStdOperatorTable.IS_NULL, RexNodeFilterAdapterImpl::isNull));

    private static final Set<SqlReturnTypeInference> conditionReturnTypes = Set.of(
            ReturnTypes.BOOLEAN,
            ReturnTypes.BOOLEAN_NULLABLE,
            ReturnTypes.BOOLEAN_NOT_NULL,
            ReturnTypes.BOOLEAN_FORCE_NULLABLE,
            ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED);

    public static boolean isFilter(RexCall call) {
        return (call.op.getReturnTypeInference() != null
                && conditionReturnTypes.contains(call.op.getReturnTypeInference())) || operators.containsKey(call.op);
    }

    private final RelNode parent;
    private final FieldAdapter fieldAdapter;

    public RexNodeFilterAdapterImpl(RelNode parent, FieldAdapter fieldAdapter) {
        this.parent = Objects.requireNonNull(parent);
        this.fieldAdapter = Objects.requireNonNull(fieldAdapter);
    }

    private RexNodeExpressionAdapter expressionAdapter() {
        return new RexNodeExpressionAdapterImpl(parent, fieldAdapter);
    }

    @Override
    public Filter filter(RexNode node) {
        return node.accept(this);
    }

    @Override
    public Filter visitInputRef(RexInputRef inputRef) {
        return Filter.isTrue(expressionAdapter().expression(inputRef));
    }

    @Override
    public Filter visitLiteral(RexLiteral literal) {
        if (!literal.getTypeName().equals(SqlTypeName.BOOLEAN)) {
            throw new UnsupportedOperationException("filters only support boolean literals");
        }
        return RexLiteral.booleanValue(literal) ? Literal.of(true) : Literal.of(false);
    }

    @Override
    public Filter visitCall(RexCall call) {
        final BiFunction<RexNodeFilterAdapterImpl, RexCall, Filter> function = operators.get(call.op);
        if (function == null) {
            throw new UnsupportedOperationException(String.format("Operator '%s' not implemented", call.op));
        }
        return function.apply(this, call);
    }

    // SQLTODO(null-semantics-ternary-logic)
    // We may need to be explicit in how we want the engine to handle semantics around comparison operators against
    // null values. TBD whether DH engine semantics match the guarantees that SQL standard may provide (or it may be the
    // case that SQL engines to implement their own semantics in this regard, TBD).

    private Filter eq(RexCall call) {
        // SQLTODO(null-eq-semantics)
        // This is incorrect, we should create a sql_eq function that respects null sql semantics
        // return Filter.isTrue(ExpressionFunction.builder().name("sql_eq")...build());
        return apply2(FilterComparison::eq, call);
    }

    private FilterComparison neq(RexCall call) {
        // SQLTODO(null-eq-semantics)
        // This is incorrect, we should create a sql_neq function that respects null sql semantics
        // return Filter.isTrue(ExpressionFunction.builder().name("sql_neq")...build());
        return apply2(FilterComparison::neq, call);
    }

    private FilterComparison lt(RexCall call) {
        return apply2(FilterComparison::lt, call);
    }

    private FilterComparison lte(RexCall call) {
        return apply2(FilterComparison::leq, call);
    }

    private FilterComparison gt(RexCall call) {
        return apply2(FilterComparison::gt, call);
    }

    private FilterComparison gte(RexCall call) {
        return apply2(FilterComparison::geq, call);
    }

    private FilterComparison isDistinctFrom(RexCall call) {
        return apply2(FilterComparison::neq, call);
    }

    private FilterComparison isNotDistinctFrom(RexCall call) {
        return apply2(FilterComparison::eq, call);
    }

    private Filter not(RexCall call) {
        return apply1Filter(Filter::invert, call);
    }

    private Filter isNull(RexCall call) {
        return apply1Expression(Filter::isNull, call);
    }

    private Filter isNotNull(RexCall call) {
        return apply1Expression(Filter::isNotNull, call);
    }

    private Filter or(RexCall call) {
        return applyN(Filter::or, call);
    }

    private Filter and(RexCall call) {
        return applyN(Filter::and, call);
    }

    private Filter apply1Filter(java.util.function.Function<Filter, Filter> f, RexCall call) {
        if (call.operands.size() != 1) {
            throw new IllegalArgumentException("Expected 1 argument operator");
        }
        final Filter unary = filter(call.operands.get(0));
        return f.apply(unary);
    }

    private Filter apply1Expression(java.util.function.Function<Expression, Filter> f, RexCall call) {
        if (call.operands.size() != 1) {
            throw new IllegalArgumentException("Expected 1 argument operator");
        }
        final Expression unary = expressionAdapter().expression(call.operands.get(0));
        return f.apply(unary);
    }

    private FilterComparison apply2(BiFunction<Expression, Expression, FilterComparison> f, RexCall call) {
        if (call.operands.size() != 2) {
            throw new IllegalArgumentException("Expected 2 argument operator");
        }
        final Expression lhs = expressionAdapter().expression(call.operands.get(0));
        final Expression rhs = expressionAdapter().expression(call.operands.get(1));
        final FilterComparison comparison = f.apply(lhs, rhs);
        return inputRefMatchHack(call, comparison);
    }

    private Filter applyN(java.util.function.Function<List<Filter>, Filter> f, RexCall call) {
        final List<Filter> inputs = call.operands.stream().map(this::filter).collect(Collectors.toList());
        return f.apply(inputs);
    }

    private static FilterComparison inputRefMatchHack(RexCall call, FilterComparison comparison) {
        // SQLTODO(input-ref-match-hack)
        // This is a HACK to ensure that if we get a match filter, we always put the input ref from the left
        // table before the input ref from the right table.
        if (call.operands.get(0) instanceof RexInputRef && call.operands.get(1) instanceof RexInputRef) {
            if (((RexInputRef) call.operands.get(0)).getIndex() > ((RexInputRef) call.operands.get(1)).getIndex()) {
                return comparison.transpose();
            }
        }
        return comparison;
    }
}
