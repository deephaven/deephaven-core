//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.partitionedtable;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.GetTableRequest;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.NoopTicketResolverAuthorization;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.*;

public class TestPartitionedTableService extends BaseCachedJMockTestCase {
    @Test
    public void testGetTable() {
        final PartitionedTableServiceGrpcImpl service = makeService();

        final GetTableRequest getTableRequestSingle = GetTableRequest.newBuilder().build();
        final GetTableRequest getTableRequestMerged = GetTableRequest.newBuilder()
                .setUniqueBehavior(GetTableRequest.UniqueBehavior.PERMIT_MULTIPLE_KEYS).build();

        final ExecutionContext executionContext = makeExecutionContext();
        try (final SafeCloseable ignored = executionContext.open()) {
            final Table keyTable = TableTools.newTable(stringCol("Key", "Apple"));

            final Table toPartition =
                    TableTools.newTable(stringCol("Key", "Apple", "Banana", "Carrot"), intCol("Sentinel", 10, 20, 30));
            final PartitionedTable partitionedTable = toPartition.partitionBy("Key");

            // simple case, no duplicates, one key
            final Table apple = service.getConstituents(getTableRequestSingle, keyTable, partitionedTable);
            final Table expectedApple = toPartition.where("Key=`Apple`");
            TstUtils.assertTableEquals(expectedApple, apple);

            // simple case, no duplicates, two keys
            final Table keyTable2 = TableTools.newTable(stringCol("Key", "Apple", "Carrot"));
            final Table appleAndCarrot = service.getConstituents(getTableRequestMerged, keyTable2, partitionedTable);
            final Table expectedAppleAndCarrot = toPartition.where("Key in `Apple`, `Carrot`");
            TstUtils.assertTableEquals(expectedAppleAndCarrot, appleAndCarrot);

            // but if you don't set the keys to permit duplicates, it fails
            final StatusRuntimeException e = Assert.assertThrows(StatusRuntimeException.class,
                    () -> service.getConstituents(getTableRequestSingle, keyTable2, partitionedTable));
            assertEquals("INVALID_ARGUMENT: Provided key table does not have one row, instead has 2", e.getMessage());

            final PartitionedTable duplicatedPartitionedTable = PartitionedTableFactory.of(
                    merge(partitionedTable.table(), partitionedTable.table()), partitionedTable.keyColumnNames(), false,
                    partitionedTable.constituentColumnName(), partitionedTable.constituentDefinition(), false);

            // single key, with duplicates, produces an error
            final StatusRuntimeException e2 = Assert.assertThrows(StatusRuntimeException.class,
                    () -> service.getConstituents(getTableRequestSingle, keyTable, duplicatedPartitionedTable));
            assertEquals(
                    "INVALID_ARGUMENT: Filtered PartitionedTable has more than one constituent, 2 constituents found",
                    e2.getMessage());

            // with the merged option, it works
            final Table doubleApple =
                    service.getConstituents(getTableRequestMerged, keyTable, duplicatedPartitionedTable);
            TstUtils.assertTableEquals(merge(expectedApple, expectedApple), doubleApple);

            // now combine duplicate keys and results
            final Table doubleAppleAndCarrot =
                    service.getConstituents(getTableRequestMerged, keyTable2, duplicatedPartitionedTable);
            TstUtils.assertTableEquals(merge(expectedAppleAndCarrot, expectedAppleAndCarrot), doubleAppleAndCarrot);
        }
    }

    private static ExecutionContext makeExecutionContext() {
        final OperationInitializer initializer = OperationInitializer.NON_PARALLELIZABLE;
        final ControlledUpdateGraph updateGraph = new ControlledUpdateGraph(initializer);
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        final ExecutionContext executionContext = ExecutionContext.newBuilder().setOperationInitializer(initializer)
                .setUpdateGraph(updateGraph).newQueryScope().newQueryLibrary().build()
                .withAuthContext(new AuthContext.Anonymous());
        return executionContext;
    }

    @Test

    public void testGetTableTicking() {
        final PartitionedTableServiceGrpcImpl service = makeService();

        final GetTableRequest getTableRequestSingle = GetTableRequest.newBuilder().build();
        final GetTableRequest getTableRequestMerged = GetTableRequest.newBuilder()
                .setUniqueBehavior(GetTableRequest.UniqueBehavior.PERMIT_MULTIPLE_KEYS).build();

        final ExecutionContext executionContext = makeExecutionContext();
        final ControlledUpdateGraph updateGraph = executionContext.getUpdateGraph().cast();
        try (final SafeCloseable ignored = executionContext.open()) {
            final QueryTable keyTable = TstUtils.testRefreshingTable(stringCol("Key", "Apple"));

            final Table toPartition =
                    TstUtils.testRefreshingTable(stringCol("Key", "Apple", "Banana", "Carrot"),
                            intCol("Sentinel", 10, 20, 30));
            final PartitionedTable partitionedTable = toPartition.partitionBy("Key");

            // simple case, no duplicates, one key
            final Table apple = service.lockAndGetConstituents(getTableRequestSingle, keyTable, partitionedTable);
            final Table expectedApple = toPartition.where("Key=`Apple`");
            TstUtils.assertTableEquals(expectedApple, apple);

            final Table appleTicking =
                    service.lockAndGetConstituents(getTableRequestMerged, keyTable, partitionedTable);
            TstUtils.assertTableEquals(expectedApple, appleTicking);

            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(keyTable, i(1), stringCol("Key", "Carrot"));
                keyTable.notifyListeners(i(1), i(), i());
            });

            final Table expectedAppleAndCarrot = toPartition.where("Key in `Apple`, `Carrot`");
            TstUtils.assertTableEquals(expectedAppleAndCarrot, appleTicking);
            TstUtils.assertTableEquals(expectedApple, apple);
        }

        updateGraph.resetForUnitTests(true);
    }

    public void testCrossGraphs() {
        final PartitionedTableServiceGrpcImpl service = makeService();

        final GetTableRequest getTableRequestMerged = GetTableRequest.newBuilder()
                .setUniqueBehavior(GetTableRequest.UniqueBehavior.PERMIT_MULTIPLE_KEYS).build();

        final ExecutionContext executionContext1 = makeExecutionContext();
        final ControlledUpdateGraph updateGraph1 = executionContext1.getUpdateGraph().cast();

        final ExecutionContext executionContext2 = makeExecutionContext();
        final ControlledUpdateGraph updateGraph2 = executionContext2.getUpdateGraph().cast();

        final Table keyTable;

        try (final SafeCloseable ignored = executionContext1.open()) {
            keyTable = TstUtils.testRefreshingTable(stringCol("Key", "Apple"));
        }

        try (final SafeCloseable ignored = executionContext2.open()) {
            final Table toPartition =
                    TstUtils.testRefreshingTable(stringCol("Key", "Apple", "Banana", "Carrot"),
                            intCol("Sentinel", 10, 20, 30));
            final PartitionedTable partitionedTable = toPartition.partitionBy("Key");

            final StatusRuntimeException e = Assert.assertThrows(StatusRuntimeException.class,
                    () -> service.lockAndGetConstituents(getTableRequestMerged, keyTable, partitionedTable));
            assertEquals(
                    "INVALID_ARGUMENT: Provided key table UpdateGraph is inconsistent with PartitionedTable UpdateGraph",
                    e.getMessage());
        }
    }

    private @NotNull PartitionedTableServiceGrpcImpl makeService() {
        final TicketRouter ticketRouter = mock(TicketRouter.class);
        final SessionService sessionService = mock(SessionService.class);
        final AuthorizationProvider authorizationProvider = mock(AuthorizationProvider.class);
        final NoopTicketResolverAuthorization ticketResolverAuthorization = new NoopTicketResolverAuthorization();
        checking(new Expectations() {
            {
                oneOf(authorizationProvider).getTicketResolverAuthorization();
                will(returnValue(ticketResolverAuthorization));
            }
        });
        final PartitionedTableServiceContextualAuthWiring authWiring =
                mock(PartitionedTableServiceContextualAuthWiring.class);


        return new PartitionedTableServiceGrpcImpl(ticketRouter, sessionService, authorizationProvider, authWiring);
    }
}
