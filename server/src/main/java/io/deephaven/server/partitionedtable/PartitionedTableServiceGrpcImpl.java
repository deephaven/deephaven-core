//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.partitionedtable;

import com.google.rpc.Code;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.GetTableRequest;
import io.deephaven.proto.backplane.grpc.MergeRequest;
import io.deephaven.proto.backplane.grpc.PartitionByRequest;
import io.deephaven.proto.backplane.grpc.PartitionByResponse;
import io.deephaven.proto.backplane.grpc.PartitionedTableServiceGrpc;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.*;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static io.deephaven.extensions.barrage.util.ExportUtil.buildTableCreationResponse;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyOnNextAndComplete;

public class PartitionedTableServiceGrpcImpl extends PartitionedTableServiceGrpc.PartitionedTableServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(PartitionedTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final PartitionedTableServiceContextualAuthWiring authWiring;
    private final TicketResolver.Authorization authorizationTransformation;

    @Inject
    public PartitionedTableServiceGrpcImpl(
            TicketRouter ticketRouter,
            SessionService sessionService,
            AuthorizationProvider authorizationProvider,
            PartitionedTableServiceContextualAuthWiring authWiring) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.authWiring = authWiring;
        this.authorizationTransformation = authorizationProvider.getTicketResolverAuthorization();
    }

    @Override
    public void partitionBy(
            @NotNull final PartitionByRequest request,
            @NotNull final StreamObserver<PartitionByResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final String description = "PartitionedTableService#partitionBy(table="
                + ticketRouter.getLogNameFor(request.getTableId(), "tableId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getTableId(), "tableId");

            session.<PartitionedTable>newExport(request.getResultId(), "resultId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(targetTable)
                    .onError(responseObserver)
                    .onSuccess((final PartitionedTable ignoredResult) -> safelyOnNextAndComplete(responseObserver,
                            PartitionByResponse.getDefaultInstance()))
                    .submit(() -> {
                        authWiring.checkPermissionPartitionBy(session.getAuthContext(), request,
                                Collections.singletonList(targetTable.get()));
                        PartitionedTable partitionedTable = targetTable.get().partitionBy(request.getDropKeys(),
                                request.getKeyColumnNamesList().toArray(String[]::new));
                        return partitionedTable;
                    });
        }
    }

    @Override
    public void merge(
            @NotNull final MergeRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final String description = "PartitionedTableService#merge(table="
                + ticketRouter.getLogNameFor(request.getPartitionedTable(), "partitionedTable") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<PartitionedTable> partitionedTable =
                    ticketRouter.resolve(session, request.getPartitionedTable(), "partitionedTable");

            session.<Table>newExport(request.getResultId(), "resultId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(partitionedTable)
                    .onError(responseObserver)
                    .onSuccess((final Table merged) -> safelyOnNextAndComplete(responseObserver,
                            buildTableCreationResponse(request.getResultId(), merged)))
                    .submit(() -> {
                        final Table table = partitionedTable.get().table();
                        authWiring.checkPermissionMerge(session.getAuthContext(), request,
                                Collections.singletonList(table));
                        Table merged;
                        if (table.isRefreshing()) {
                            merged = table.getUpdateGraph().sharedLock().computeLocked(partitionedTable.get()::merge);
                        } else {
                            merged = partitionedTable.get().merge();
                        }
                        merged = authorizationTransformation.transform(merged);
                        if (merged == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to merge table.");
                        }
                        return merged;
                    });
        }
    }

    @Override
    public void getTable(
            @NotNull final GetTableRequest request,
            @NotNull final StreamObserver<ExportedTableCreationResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final String description = "PartitionedTableService#getTable(table="
                + ticketRouter.getLogNameFor(request.getPartitionedTable(), "partitionedTable") + ", keyTable="
                + ticketRouter.getLogNameFor(request.getKeyTableTicket(), "keyTable") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<PartitionedTable> partitionedTable =
                    ticketRouter.resolve(session, request.getPartitionedTable(), "partitionedTable");
            final SessionState.ExportObject<Table> keys =
                    ticketRouter.resolve(session, request.getKeyTableTicket(), "keyTable");

            session.<Table>newExport(request.getResultId(), "resultId")
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(partitionedTable, keys)
                    .onError(responseObserver)
                    .onSuccess((final Table table) -> safelyOnNextAndComplete(responseObserver,
                            buildTableCreationResponse(request.getResultId(), table)))
                    .submit(() -> {
                        Table table;
                        Table keyTable = keys.get();
                        authWiring.checkPermissionGetTable(session.getAuthContext(), request,
                                List.of(partitionedTable.get().table(), keyTable));
                        if (!keyTable.isRefreshing()) {
                            long keyTableSize = keyTable.size();
                            if (keyTableSize != 1) {
                                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                        "Provided key table does not have one row, instead has " + keyTableSize);
                            }
                            long row = keyTable.getRowSet().firstRowKey();
                            Object[] values =
                                    partitionedTable.get().keyColumnNames().stream()
                                            .map(keyTable::getColumnSource)
                                            .map(cs -> cs.get(row))
                                            .toArray();
                            table = partitionedTable.get().constituentFor(values);
                        } else {
                            table = keyTable.getUpdateGraph().sharedLock().computeLocked(() -> {
                                long keyTableSize = keyTable.size();
                                if (keyTableSize != 1) {
                                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                            "Provided key table does not have one row, instead has " + keyTableSize);
                                }
                                Table requestedRow = partitionedTable.get().table().whereIn(keyTable,
                                        partitionedTable.get().keyColumnNames().toArray(String[]::new));
                                if (requestedRow.size() != 1) {
                                    if (requestedRow.isEmpty()) {
                                        throw Exceptions.statusRuntimeException(Code.NOT_FOUND,
                                                "Key matches zero rows in the partitioned table");
                                    } else {
                                        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                                                "Key matches more than one entry in the partitioned table: "
                                                        + requestedRow.size());
                                    }
                                }
                                return (Table) requestedRow
                                        .getColumnSource(partitionedTable.get().constituentColumnName())
                                        .get(requestedRow.getRowSet().firstRowKey());
                            });
                        }
                        table = authorizationTransformation.transform(table);
                        if (table == null) {
                            throw Exceptions.statusRuntimeException(
                                    Code.FAILED_PRECONDITION, "Not authorized to get table.");
                        }
                        return table;
                    });
        }
    }
}
