//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.*;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.util.annotations.InternalUseOnly;

/**
 * Converts an Engine API {@link Filter} into a gRPC {@link Condition}.
 *
 * <p>
 * Note that you may not be able to round trip a Condition and Filter using this class.
 * </p>
 */
@InternalUseOnly
public class FilterAdapter implements Filter.Visitor<Condition> {

    /**
     * Converts an Engine API {@link Filter} into a gRPC {@link Condition}.
     */
    public static Condition of(final Filter filter) {
        return filter.walk(new FilterAdapter());
    }

    private static CompareCondition.CompareOperation adapt(FilterComparison.Operator operator) {
        switch (operator) {
            case LESS_THAN:
                return CompareCondition.CompareOperation.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return CompareCondition.CompareOperation.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL;
            case EQUALS:
                return CompareCondition.CompareOperation.EQUALS;
            case NOT_EQUALS:
                return CompareCondition.CompareOperation.NOT_EQUALS;
            default:
                throw new IllegalArgumentException("Unexpected operator " + operator);
        }
    }

    @Override
    public Condition visit(FilterIsNull isNull) {
        if (!(isNull.expression() instanceof ColumnName)) {
            // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
            throw new UnsupportedOperationException("Only supports null checking a reference to a column");
        }
        return Condition.newBuilder()
                .setIsNull(IsNullCondition.newBuilder()
                        .setReference(BatchTableRequestBuilder.reference((ColumnName) isNull.expression()))
                        .build())
                .build();
    }

    @Override
    public Condition visit(FilterComparison comparison) {
        FilterComparison preferred = comparison.maybeTranspose();
        FilterComparison.Operator operator = preferred.operator();
        // Processing as single FilterIn is currently the more efficient server impl.
        // See FilterTableGrpcImpl
        // See io.deephaven.server.table.ops.filter.FilterFactory
        switch (operator) {
            case EQUALS:
                return visit(FilterIn.of(preferred.lhs(), preferred.rhs()));
            case NOT_EQUALS:
                return visit(Filter.not(FilterIn.of(preferred.lhs(), preferred.rhs())));
        }
        return Condition.newBuilder()
                .setCompare(CompareCondition.newBuilder()
                        .setOperation(adapt(operator))
                        .setLhs(BatchTableRequestBuilder.ExpressionAdapter.adapt(preferred.lhs()))
                        .setRhs(BatchTableRequestBuilder.ExpressionAdapter.adapt(preferred.rhs()))
                        .build())
                .build();
    }

    @Override
    public Condition visit(FilterIn in) {
        final InCondition.Builder builder = InCondition.newBuilder()
                .setTarget(BatchTableRequestBuilder.ExpressionAdapter.adapt(in.expression()));
        for (Expression value : in.values()) {
            builder.addCandidates(BatchTableRequestBuilder.ExpressionAdapter.adapt(value));
        }
        return Condition.newBuilder().setIn(builder).build();
    }

    @Override
    public Condition visit(FilterNot<?> not) {
        // This is a shallow simplification that removes the need for setNot when it is not needed.
        final Filter invertedFilter = not.invertFilter();
        if (not.equals(invertedFilter)) {
            return Condition.newBuilder().setNot(NotCondition.newBuilder().setFilter(of(not.filter())).build())
                    .build();
        } else {
            return of(invertedFilter);
        }
    }

    @Override
    public Condition visit(FilterOr ors) {
        OrCondition.Builder builder = OrCondition.newBuilder();
        for (Filter filter : ors) {
            builder.addFilters(of(filter));
        }
        return Condition.newBuilder().setOr(builder.build()).build();
    }

    @Override
    public Condition visit(FilterAnd ands) {
        AndCondition.Builder builder = AndCondition.newBuilder();
        for (Filter filter : ands) {
            builder.addFilters(of(filter));
        }
        return Condition.newBuilder().setAnd(builder.build()).build();
    }

    @Override
    public Condition visit(FilterSerial serial) {
        // TODO(DH-19051): integrate serial/barrier filter/selectables w/gRPC
        throw new UnsupportedOperationException("Can't build Condition with FilterSerial");
    }

    @Override
    public Condition visit(FilterBarrier barrier) {
        // TODO(DH-19051): integrate serial/barrier filter/selectables w/gRPC
        throw new UnsupportedOperationException("Can't build Condition with FilterBarrier");
    }

    @Override
    public Condition visit(FilterRespectsBarrier respectsBarrier) {
        // TODO(DH-19051): integrate serial/barrier filter/selectables w/gRPC
        throw new UnsupportedOperationException("Can't build Condition with FilterRespectsBarrier");
    }

    @Override
    public Condition visit(FilterPattern pattern) {
        // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
        throw new UnsupportedOperationException("Can't build Condition with FilterPattern");
    }

    @Override
    public Condition visit(Function function) {
        // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
        throw new UnsupportedOperationException("Can't build Condition with Function");
    }

    @Override
    public Condition visit(Method method) {
        // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
        throw new UnsupportedOperationException("Can't build Condition with Method");
    }

    @Override
    public Condition visit(boolean literal) {
        // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
        throw new UnsupportedOperationException("Can't build Condition with literal");
    }

    @Override
    public Condition visit(RawString rawString) {
        // TODO(deephaven-core#3609): Update gRPC expression / filter / literal structures
        throw new UnsupportedOperationException("Can't build Condition with raw string");
    }
}
