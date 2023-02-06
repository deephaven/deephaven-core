/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbsoluteSortColumnConventions;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.server.session.SessionState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SortTableGrpcImpl extends GrpcTableOperation<SortTableRequest> {

    @Inject
    public SortTableGrpcImpl(final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionSort, BatchTableRequest.Operation::getSort,
                SortTableRequest::getResultId, SortTableRequest::getSourceId);
    }

    @Override
    public Table create(final SortTableRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);

        final Table original = sourceTables.get(0).get();
        Table result = original;

        final List<String> absColumns = new ArrayList<>();
        final List<Selectable> absViews = new ArrayList<>();
        for (int si = request.getSortsCount() - 1; si >= 0; si--) {
            if (request.getSorts(si).getIsAbsolute()) {
                final String columnName = request.getSorts(si).getColumnName();
                final String absName = AbsoluteSortColumnConventions.baseColumnNameToAbsoluteName(columnName);
                absColumns.add(absName);
                absViews.add(AbsoluteSortColumnConventions.makeSelectable(absName, columnName));
            }
        }

        if (!absViews.isEmpty()) {
            result = result.updateView(absViews);
        }

        // This loop does two optimizations:
        // 1. Consolidate all sorts into a SortPair array in order to only call one sort on the table
        // 2. Move all the reverses to the back:
        // - For an odd number of reverses only call one reverse
        // - For an even number of reverses do not call reverse (they cancel out)
        // As a reverse moves past a sort, the direction of the sort is reversed (e.g. asc -> desc)
        final List<SortColumn> sortColumns = new ArrayList<>();
        boolean shouldReverse = false;
        for (int si = 0; si < request.getSortsCount(); si++) {
            final SortDescriptor sort = request.getSorts(si);

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
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Unexpected sort direction: " + direction);
            }

            final String columnName = sort.getIsAbsolute()
                    ? AbsoluteSortColumnConventions.baseColumnNameToAbsoluteName(sort.getColumnName())
                    : sort.getColumnName();

            // if should reverse is true, then a flip the direction of the sort
            if (shouldReverse) {
                direction *= -1;
            }

            if (direction == -1) {
                sortColumns.add(SortColumn.desc(ColumnName.of(columnName)));
            } else {
                sortColumns.add(SortColumn.asc(ColumnName.of(columnName)));
            }
        }

        // Next sort if there are any sorts
        if (sortColumns.size() > 0) {
            result = result.sort(sortColumns);
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
                ((QueryTable) result).setAttribute(Table.KEY_COLUMNS_ATTRIBUTE, keyColumns);
            }
            final Object uniqueKeys = original.getAttribute(Table.UNIQUE_KEYS_ATTRIBUTE);
            if (uniqueKeys != null) {
                ((QueryTable) result).setAttribute(Table.UNIQUE_KEYS_ATTRIBUTE, uniqueKeys);
            }
        }

        return result;
    }
}
