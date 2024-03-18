//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.api.filter.Filter;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.proto.backplane.grpc.AndCondition;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.FilterTableRequest;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.table.ops.filter.ConvertInvalidInExpressions;
import io.deephaven.server.table.ops.filter.FilterFactory;
import io.deephaven.server.table.ops.filter.FlipNonReferenceMatchExpression;
import io.deephaven.server.table.ops.filter.MakeExpressionsNullSafe;
import io.deephaven.server.table.ops.filter.MergeNestedBinaryOperations;
import io.deephaven.server.table.ops.filter.NormalizeNots;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class FilterTableGrpcImpl extends GrpcTableOperation<FilterTableRequest> {

    @Inject
    public FilterTableGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionFilter, BatchTableRequest.Operation::getFilter,
                FilterTableRequest::getResultId, FilterTableRequest::getSourceId);
    }

    @Override
    public Table create(final FilterTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        Table sourceTable = sourceTables.get(0).get();
        List<Condition> filters = request.getFiltersList();
        if (filters.isEmpty()) {
            return sourceTable;
        }

        final List<Condition> finishedConditions = finishConditions(filters);

        List<WhereFilter> whereFilters = finishedConditions.stream()
                .map(f -> FilterFactory.makeFilter(sourceTable.getDefinition(), f))
                .collect(Collectors.toList());

        // execute the filters
        return sourceTable.where(Filter.and(whereFilters));
    }

    @NotNull
    public static List<Condition> finishConditions(@NotNull final List<Condition> filters) {
        Condition filter;
        if (filters.size() == 1) {
            filter = filters.get(0);
        } else {
            filter = Condition.newBuilder()
                    .setAnd(AndCondition.newBuilder()
                            .addAllFilters(filters)
                            .build())
                    .build();
        }

        // make type info available
        // TODO (deephaven-core#733)

        // validate based on the table structure and available invocations
        // TODO (deephaven-core#733)

        // rewrite unnecessary NOT expressions away
        filter = NormalizeNots.exec(filter);

        // if a "in" expression has a non-reference on the left or reference on the right, flip it, and split
        // up values so these can be left as INs or remade into EQs, and join them together with OR/ANDs.
        filter = FlipNonReferenceMatchExpression.exec(filter);

        // merge ANDs nested in ANDs and ORs nested in ORs for a simpler structure
        filter = MergeNestedBinaryOperations.exec(filter);

        // for any "in" expression (at this point, all have a reference on the left), if they have a reference
        // value on the left it must be split into its own "equals" instead.
        filter = ConvertInvalidInExpressions.exec(filter);

        // replace any EQ-type expression with its corresponding IN-type expression. this preserves the changes
        // made above, could be moved earlier in this list, but must come before "in"/"not in"s are merged
        // TODO (deephaven-core#733)

        // within each OR/AND, find any comparable "in"/"not in" expression referring to the same column
        // on the left side and merge them into one match
        // TODO (deephaven-core#733)

        // rewrite any expressions which "should" be safe from the client needing to add null checks
        filter = MakeExpressionsNullSafe.exec(filter);

        // get a top array of filters to convert into SelectFilters
        // TODO (deephaven-core#733)

        return Collections.singletonList(filter);
    }
}
