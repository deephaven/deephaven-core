package io.deephaven.grpc_api.table.inputtables;

import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.config.MutableInputTable;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public void addTablesToInputTable(AddTableRequest request, StreamObserver<AddTableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<Table> targetTable = ticketRouter.resolve(session, request.getInputTable(), "inputTable");
            SessionState.ExportObject<Table> tableToAdd = ticketRouter.resolve(session, request.getTableToAdd(), "tableToAdd");

            session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver)
                    .require(targetTable, tableToAdd)
                    .submit(() -> {
                        Object inputTable = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTable instanceof MutableInputTable)) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Table can't be used as an input table");
                        }

                        MutableInputTable mutableInputTable = (MutableInputTable) inputTable;
                        Table table = tableToAdd.get();

                        // validate that the columns are compatible
                        try {
                            mutableInputTable.validateAddOrModify(table);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Provided tables's columns are not compatible: " + exception.getMessage());
                        }

                        // actually add the tables contents
                        try {
                            mutableInputTable.add(table);
                        } catch (IOException ioException) {
                            throw GrpcUtil.statusRuntimeException(Code.DATA_LOSS, "Error adding table to input table");
                        }
                    });
        });
    }

    @Override
    public void deleteTablesFromInputTable(DeleteTableRequest request, StreamObserver<DeleteTableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<Table> targetTable = ticketRouter.resolve(session, request.getInputTable(), "inputTable");
            SessionState.ExportObject<Table> tableToDeleteExport = ticketRouter.resolve(session, request.getTableToRemove(), "tableToDelete");

            session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver)
                    .require(targetTable, tableToDeleteExport)
                    .submit(() -> {
                        Object inputTable = targetTable.get().getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
                        if (!(inputTable instanceof MutableInputTable)) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Table can't be used as an input table");
                        }

                        MutableInputTable mutableInputTable = (MutableInputTable) inputTable;

                        Table tableToDelete = tableToDeleteExport.get();

                        // validate that the columns are compatible
                        try {
                            mutableInputTable.validateDelete(tableToDelete);
                        } catch (TableDefinition.IncompatibleTableDefinitionException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Provided tables's columns are not compatible: " + exception.getMessage());
                        } catch (UnsupportedOperationException exception) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Provided input table does not support delete.");
                        }

                        // actually delete the table's contents
                        try {
                            mutableInputTable.delete(tableToDelete);
                        } catch (IOException ioException) {
                            throw GrpcUtil.statusRuntimeException(Code.DATA_LOSS, "Error adding table to inputtable");
                        }
                    });
        });
    }
}
