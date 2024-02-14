/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.engine.table.MultiJoinFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.MultiJoinInput;
import io.deephaven.proto.backplane.grpc.MultiJoinTablesRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class MultiJoinGrpcImpl extends GrpcTableOperation<MultiJoinTablesRequest> {

    @Inject
    public MultiJoinGrpcImpl(
            final TableServiceContextualAuthWiring authWiring) {
        super(authWiring::checkPermissionMultiJoinTables,
                BatchTableRequest.Operation::getMultiJoin,
                MultiJoinTablesRequest::getResultId,
                (MultiDependencyFunction<MultiJoinTablesRequest>) request -> request.getMultiJoinInputsCount() > 0
                        ? request.getMultiJoinInputsList().stream().map(MultiJoinInput::getSourceId)
                                .collect(Collectors.toList())
                        : request.getSourceIdsList());
    }

    @Override
    public void validateRequest(final MultiJoinTablesRequest request) throws StatusRuntimeException {
        if (request.getSourceIdsList().isEmpty() && request.getMultiJoinInputsList().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Cannot join zero source tables.");
        }
        if (!request.getSourceIdsList().isEmpty() && !request.getMultiJoinInputsList().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "If `multi_join_inputs` are provided, `source_ids` must remain empty.");
        }
    }

    @Override
    public Table create(final MultiJoinTablesRequest request,
            final List<SessionState.ExportObject<Table>> sourceTables) {

        final Table firstTable = sourceTables.get(0).get();
        final Table[] allTables = sourceTables.stream().map(SessionState.ExportObject::get).toArray(Table[]::new);

        // Were multiJoinInputs provided?
        if (!request.getMultiJoinInputsList().isEmpty()) {
            // Build the multiJoinInput array.
            final io.deephaven.engine.table.MultiJoinInput[] multiJoinInputs =
                    new io.deephaven.engine.table.MultiJoinInput[request.getMultiJoinInputsCount()];

            for (int i = 0; i < request.getMultiJoinInputsCount(); i++) {
                final Table table = sourceTables.get(i).get();

                final MultiJoinInput mjInput = request.getMultiJoinInputs(i);
                final String[] columnsToMatch = mjInput.getColumnsToMatchList().toArray(new String[0]);
                final String[] columnsToAdd = mjInput.getColumnsToAddList().toArray(new String[0]);

                multiJoinInputs[i] =
                        io.deephaven.engine.table.MultiJoinInput.of(table, columnsToMatch, columnsToAdd);
            }
            return firstTable.getUpdateGraph(allTables).sharedLock().computeLocked(
                    () -> MultiJoinFactory.of(multiJoinInputs).table());
        }

        // Build from the provided tables and key columns.
        final String[] columnsToMatch = request.getColumnsToMatchList().toArray(new String[0]);

        return firstTable.getUpdateGraph(allTables).sharedLock().computeLocked(
                () -> MultiJoinFactory.of(columnsToMatch, allTables).table());
    }
}
