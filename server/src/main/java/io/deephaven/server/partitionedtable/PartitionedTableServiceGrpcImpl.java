package io.deephaven.server.partitionedtable;

import com.google.rpc.Code;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.util.ExportUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.GetTableRequest;
import io.deephaven.proto.backplane.grpc.MergeRequest;
import io.deephaven.proto.backplane.grpc.PartitionByRequest;
import io.deephaven.proto.backplane.grpc.PartitionByResponse;
import io.deephaven.proto.backplane.grpc.PartitionedTableServiceGrpc;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;

import java.util.function.Supplier;

import static io.deephaven.extensions.barrage.util.ExportUtil.buildTableCreationResponse;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyExecute;

public class PartitionedTableServiceGrpcImpl extends PartitionedTableServiceGrpc.PartitionedTableServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(PartitionedTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final UpdateGraphProcessor updateGraphProcessor;

    @Inject
    public PartitionedTableServiceGrpcImpl(TicketRouter ticketRouter, SessionService sessionService,
            UpdateGraphProcessor updateGraphProcessor) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.updateGraphProcessor = updateGraphProcessor;
    }

    @Override
    public void partitionBy(PartitionByRequest request, StreamObserver<PartitionByResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getTableId(), "tableId");

            session.newExport(request.getResultId(), "resultId")
                    .require(targetTable)
                    .onError(responseObserver)
                    .submit(() -> {
                        PartitionedTable partitionedTable = targetTable.get().partitionBy(request.getDropKeys(),
                                request.getKeyColumnNamesList().toArray(String[]::new));
                        safelyExecute(() -> {
                            responseObserver.onNext(PartitionByResponse.getDefaultInstance());
                            responseObserver.onCompleted();
                        });
                        return partitionedTable;
                    });

        });
    }

    @Override
    public void merge(MergeRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<PartitionedTable> partitionedTable =
                    ticketRouter.resolve(session, request.getPartitionedTable(), "partitionedTable");

            session.newExport(request.getResultId(), "resultId")
                    .require(partitionedTable)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Table merged;
                        if (partitionedTable.get().table().isRefreshing()) {
                            merged = updateGraphProcessor.sharedLock()
                                    .computeLocked(partitionedTable.get()::merge);
                        } else {
                            merged = partitionedTable.get().merge();
                        }
                        safelyExecute(() -> {
                            responseObserver.onNext(buildTableCreationResponse(request.getResultId(), merged));
                            responseObserver.onCompleted();
                        });
                        return merged;
                    });
        });

    }

    @Override
    public void getTable(GetTableRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<PartitionedTable> partitionedTable =
                    ticketRouter.resolve(session, request.getPartitionedTable(), "partitionedTable");
            SessionState.ExportObject<Table> keys =
                    ticketRouter.resolve(session, request.getKeyTableTicket(), "keyTableTicket");

            session.newExport(request.getResultId(), "resultId")
                    .require(partitionedTable, keys)
                    .onError(responseObserver)
                    .submit(() -> {
                        final Table table;
                        Table keyTable = keys.get();
                        if (!keyTable.isRefreshing()) {
                            long keyTableSize = keyTable.size();
                            if (keyTableSize != 1) {
                                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
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
                            table = updateGraphProcessor.sharedLock().computeLocked(() -> {
                                long keyTableSize = keyTable.size();
                                if (keyTableSize != 1) {
                                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                            "Provided key table does not have one row, instead has " + keyTableSize);
                                }
                                Table requestedRow = partitionedTable.get().table().whereIn(keyTable,
                                        partitionedTable.get().keyColumnNames().toArray(String[]::new));
                                if (requestedRow.size() != 1) {
                                    if (requestedRow.isEmpty()) {
                                        throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                                                "Key matches zero rows in the partitioned table");
                                    } else {
                                        throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                                                "Key matches more than one entry in the partitioned table: "
                                                        + requestedRow.size());
                                    }
                                }
                                return (Table) requestedRow
                                        .getColumnSource(partitionedTable.get().constituentColumnName())
                                        .get(requestedRow.getRowSet().firstRowKey());
                            });
                        }
                        safelyExecute(() -> {
                            responseObserver.onNext(buildTableCreationResponse(request.getResultId(), table));
                            responseObserver.onCompleted();
                        });
                        return table;
                    });
        });
    }
}
