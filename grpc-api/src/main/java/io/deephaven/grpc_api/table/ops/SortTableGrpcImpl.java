package io.deephaven.grpc_api.table.ops;


import io.deephaven.api.Selectable;
import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.SortPair;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortTableRequest;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SortTableGrpcImpl extends GrpcTableOperation<SortTableRequest> {

    @Inject
    public SortTableGrpcImpl() {
        super(BatchTableRequest.Operation::getSort, SortTableRequest::getResultId, SortTableRequest::getSourceId);
    }

    @Override
    public Table create(final SortTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table original = sourceTables.get(0).get();
        Table result = original;

        final ArrayList<String> absColumns = new ArrayList<>();
        final ArrayList<String> absViews = new ArrayList<>();
        for (int i = request.getSortsCount() - 1; i >= 0; i--) {
            if (request.getSorts(i).getIsAbsolute()) {
                final String columnName = request.getSorts(i).getColumnName();
                final String absName = "__ABS__" + columnName;
                absColumns.add(absName);
                absViews.add("__ABS__" + columnName + " = abs(" + columnName + ")");
            }
        }

        if (!absViews.isEmpty()) {
            result = result.updateView(Selectable.from(absViews));
        }

        // This loop does two optimizations:
        // 1. Consolidate all sorts into a SortPair array in order to only call one sort on the table
        // 2. Move all the reverses to the back:
        //    - For an odd number of reverses only call one reverse
        //    - For an even number of reverses do not call reverse (they cancel out)
        // As a reverse moves past a sort, the direction of the sort is reversed (e.g. asc -> desc)
        final List<SortPair> sortPairs = new ArrayList<>();
        boolean shouldReverse = false;
        for (int i = 0; i < request.getSortsCount(); i++) {
            final SortDescriptor sort = request.getSorts(i);

            int direction = 0;
            switch (sort.getDirection()) {
                case REVERSE:
                    // Toggle the reverse flag, should be true for odd number of reverses and false for an even number
                    shouldReverse = !shouldReverse;
                    continue;
                case DESCENDING:
                    direction = -1;
                    break;
                case ASCENDING:
                    direction = 1;
                    break;
                default:
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected sort direction: " + direction);
            }

            final StringBuilder columnName = new StringBuilder(sort.getColumnName());
            if (sort.getIsAbsolute()) {
                columnName.insert(0, "__ABS__");
            }

            // if should reverse is true, then a flip the direction of the sort
            if (shouldReverse) {
                direction *= -1;
            }

            if (direction == -1) {
                sortPairs.add(SortPair.descending(columnName.toString()));
            } else {
                sortPairs.add(SortPair.ascending(columnName.toString()));
            }
        }

        // Next sort if there are any sorts
        if (sortPairs.size() > 0) {
            result = result.sort(sortPairs.toArray(SortPair.ZERO_LENGTH_SORT_PAIR_ARRAY));
        }

        // All reverses were pushed to the back, so reverse if needed
        if (shouldReverse) {
            result = result.reverse();
        }

        if (!absColumns.isEmpty()) {
            result = result.dropColumns(absColumns);
            // dropColumns does not preserve key column attributes, so they need to be copied over
            final Object keyColumns = original.getAttribute(Table.KEY_COLUMNS_ATTRIBUTE);
            if (keyColumns != null) {
                result.setAttribute(Table.KEY_COLUMNS_ATTRIBUTE, keyColumns);
            }
            final Object uniqueKeys = original.getAttribute(Table.UNIQUE_KEYS_ATTRIBUTE);
            if (uniqueKeys != null) {
                result.setAttribute(Table.UNIQUE_KEYS_ATTRIBUTE, uniqueKeys);
            }
        }

        return result;
    }
}
