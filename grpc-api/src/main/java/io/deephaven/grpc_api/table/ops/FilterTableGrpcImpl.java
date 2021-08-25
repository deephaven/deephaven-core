package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.ops.filter.*;
import io.deephaven.proto.backplane.grpc.*;
import org.apache.commons.text.StringEscapeUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;

@Singleton
public class FilterTableGrpcImpl extends GrpcTableOperation<FilterTableRequest> {

    @Inject
    public FilterTableGrpcImpl() {
        super(BatchTableRequest.Operation::getFilter, FilterTableRequest::getResultId, FilterTableRequest::getSourceId);
    }

    @Override
    public Table create(final FilterTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        Table sourceTable = sourceTables.get(0).get();

        List<Condition> filters = request.getFiltersList();
        if (filters.isEmpty()) {
            return sourceTable;
        }
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

        List<Condition> finishedConditions = Collections.singletonList(filter);

        // build SelectFilter[] to pass to the table
        SelectFilter[] selectFilters = finishedConditions.stream().map(f -> FilterFactory.makeFilter(sourceTable, f))
                .toArray(SelectFilter[]::new);

        // execute the filters
        return sourceTable.where(selectFilters);
    }
}
