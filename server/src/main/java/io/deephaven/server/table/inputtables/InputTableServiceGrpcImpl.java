package io.deephaven.server.table.inputtables;

import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.AddTableResponse;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableResponse;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;

public class InputTableServiceGrpcImpl extends InputTableServiceGrpc.InputTableServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(InputTableServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;

    @Inject
    public InputTableServiceGrpcImpl(TicketRouter ticketRouter, SessionService sessionService) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
    }

    @Override
    public void addTableToInputTable(AddTableRequest request, StreamObserver<AddTableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getInputTable(), "inputTable");
            SessionState.ExportObject<Table> tableToAdd =
                    ticketRouter.resolve(session, request.getTableToAdd(), "tableToAdd");

            session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver)
                    .require(targetTable, tableToAdd)
                    .submit(() -> {
                        Object inputTable = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTable instanceof MutableInputTable)) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Table can't be used as an input table");
                        }

                        MutableInputTable mutableInputTable = (MutableInputTable) inputTable;
                        Table table = tableToAdd.get();

                        // validate that the columns are compatible
                        try {
                            mutableInputTable.validateAddOrModify(table);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided tables's columns are not compatible: " + exception.getMessage());
                        }

                        // actually add the tables contents
                        try {
                            mutableInputTable.add(table);
                            GrpcUtil.safelyExecuteLocked(responseObserver, () -> {
                                responseObserver.onNext(AddTableResponse.getDefaultInstance());
                                responseObserver.onCompleted();
                            });
                        } catch (IOException ioException) {
                            throw GrpcUtil.statusRuntimeException(Code.DATA_LOSS, "Error adding table to input table");
                        }
                    });
        });
    }

    @Override
    public void deleteTableFromInputTable(DeleteTableRequest request,
            StreamObserver<DeleteTableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<Table> targetTable =
                    ticketRouter.resolve(session, request.getInputTable(), "inputTable");
            SessionState.ExportObject<Table> tableToDeleteExport =
                    ticketRouter.resolve(session, request.getTableToRemove(), "tableToDelete");

            session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver)
                    .require(targetTable, tableToDeleteExport)
                    .submit(() -> {
                        Object inputTable = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTable instanceof MutableInputTable)) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Table can't be used as an input table");
                        }

                        MutableInputTable mutableInputTable = (MutableInputTable) inputTable;

                        Table tableToDelete = tableToDeleteExport.get();

                        // validate that the columns are compatible
                        try {
                            mutableInputTable.validateDelete(tableToDelete);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided tables's columns are not compatible: " + exception.getMessage());
                        } catch (UnsupportedOperationException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Provided input table does not support delete.");
                        }

                        // actually delete the table's contents
                        try {
                            mutableInputTable.delete(tableToDelete);
                            GrpcUtil.safelyExecuteLocked(responseObserver, () -> {
                                responseObserver.onNext(DeleteTableResponse.getDefaultInstance());
                                responseObserver.onCompleted();
                            });
                        } catch (IOException ioException) {
                            throw GrpcUtil.statusRuntimeException(Code.DATA_LOSS,
                                    "Error deleting table from inputtable");
                        }
                    });
        });
    }
}
