package io.deephaven.db.v2.locations;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.utils.TableManagementTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.file.TrackedFileHandleFactory;
import io.deephaven.db.v2.NestedPartitionedDiskBackedTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.locations.local.NestedPartitionedLocalTableLocationScanner;
import io.deephaven.db.v2.locations.local.ReadOnlyLocalTableLocationProvider;
import io.deephaven.db.v2.locations.util.TableDataRefreshService;
import io.deephaven.db.v2.sources.regioned.RegionedTableComponentFactoryImpl;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link DeferredLegacyMetadataGroupingProvider} and {@link ParallelDeferredGroupingProvider}.
 */
public class TestGroupingProviders {

    private File dataDirectory;

    @Before
    public void setUp() throws Exception {
        dataDirectory = Files.createTempDirectory(Paths.get(""), "TestChunkedRegionedOperations-").toFile();
        dataDirectory.deleteOnExit();
    }

    @After
    public void tearDown() throws Exception {
        QueryLibrary.resetLibrary();

        if (dataDirectory.exists()) {
            TrackedFileHandleFactory.getInstance().closeAll();
            int tries = 0;
            boolean success = false;
            do {
                try {
                    FileUtils.deleteRecursively(dataDirectory);
                    success = true;
                } catch (Exception e) {
                    System.gc();
                    tries++;
                }
            } while (!success && tries < 10);
            TestCase.assertTrue(success);
        }
    }

    @Test
    public void testSerialOrdering() {
        final boolean oldSetting = GroupingProvider.getParallelizeRetrieval();
        try {
            GroupingProvider.setParallelizeRetrieval(false);
            doTest(false);
        } finally {
            GroupingProvider.setParallelizeRetrieval(oldSetting);
        }
    }

    @Test
    public void testParallelOrdering() {
        final boolean oldSetting = GroupingProvider.getParallelizeRetrieval();
        try {
            GroupingProvider.setParallelizeRetrieval(true);
            doTest(false);
        } finally {
            GroupingProvider.setParallelizeRetrieval(oldSetting);
        }
    }

    @Test
    public void testSerialMissingGroups() {
        final boolean oldSetting = GroupingProvider.getParallelizeRetrieval();
        try {
            GroupingProvider.setParallelizeRetrieval(false);
            doTest(true);
        } finally {
            GroupingProvider.setParallelizeRetrieval(oldSetting);
        }
    }

    @Test
    public void testParallelMissingGroups() {
        final boolean oldSetting = GroupingProvider.getParallelizeRetrieval();
        try {
            GroupingProvider.setParallelizeRetrieval(true);
            doTest(true);
        } finally {
            GroupingProvider.setParallelizeRetrieval(oldSetting);
        }
    }

    private void doTest(final boolean missingGroups) {
        final Table raw = TableTools.emptyTable(26 * 10 * 1000).update("Part=String.format(`%04d`, (long)(ii/1000))", "Sym=(char)('A' + ii % 26)", "Other=ii");
        final Table[] partitions = raw.byExternal("Part").transformTables(rp -> rp.by("Sym").ungroup()).values().toArray(Table.ZERO_LENGTH_TABLE_ARRAY);

        if (!missingGroups) {
            // Create a pair of partitions without the grouping column
            partitions[2] = partitions[2].dropColumns("Sym");
            partitions[3] = partitions[3].dropColumns("Sym");
        }

        final TableDefinition partitionedDataDefinition = TableDefinition.of(
            ColumnDefinition.ofString("Part").withPartitioning(),
            ColumnDefinition.ofChar("Sym").withGrouping(),
            ColumnDefinition.ofLong("Other"));
        partitionedDataDefinition.setNamespace("TestNamespace");
        partitionedDataDefinition.setName("TestTable");
        partitionedDataDefinition.setStorageType(TableDefinition.STORAGETYPE_NESTEDPARTITIONEDONDISK);

        final TableDefinition partitionedMissingDataDefinition;
        if (missingGroups) {
            partitionedMissingDataDefinition = TableDefinition.of(
                ColumnDefinition.ofString("Part").withPartitioning(),
                ColumnDefinition.ofChar("Sym"),
                ColumnDefinition.ofLong("Other"));
            partitionedMissingDataDefinition.setNamespace("TestNamespace");
            partitionedMissingDataDefinition.setName("TestTable");
            partitionedMissingDataDefinition.setStorageType(TableDefinition.STORAGETYPE_NESTEDPARTITIONEDONDISK);
        } else {
            partitionedMissingDataDefinition = TableDefinition.of(
                ColumnDefinition.ofString("Part").withPartitioning(),
                ColumnDefinition.ofLong("Other"));
            partitionedMissingDataDefinition.setNamespace("TestNamespace");
            partitionedMissingDataDefinition.setName("TestTable");
            partitionedMissingDataDefinition.setStorageType(TableDefinition.STORAGETYPE_NESTEDPARTITIONEDONDISK);
        }

        final TableKey tableKey = new TableLookupKey.Immutable(partitionedDataDefinition.getNamespace(), partitionedDataDefinition.getName(), TableType.STANDALONE_SPLAYED);

        TableManagementTools.writeTable(partitions[0], partitionedDataDefinition, new File(dataDirectory, "IP" + File.separator + "0000" + File.separator + tableKey.getTableName()), TableManagementTools.StorageFormat.Parquet);
        TableManagementTools.writeTable(partitions[1], partitionedDataDefinition, new File(dataDirectory, "IP" + File.separator + "0001" + File.separator + tableKey.getTableName()), TableManagementTools.StorageFormat.Parquet);
        TableManagementTools.writeTable(partitions[2], partitionedMissingDataDefinition, new File(dataDirectory, "IP" + File.separator + "0002" + File.separator + tableKey.getTableName()), TableManagementTools.StorageFormat.Parquet);
        TableManagementTools.writeTable(partitions[3], partitionedMissingDataDefinition, new File(dataDirectory, "IP" + File.separator + "0003" + File.separator + tableKey.getTableName()), TableManagementTools.StorageFormat.Parquet);
        TableManagementTools.writeTables(
                Arrays.copyOfRange(partitions, 4, partitions.length),
                partitionedDataDefinition,
                IntStream.range(4, 260)
                        .mapToObj(pcv -> new File(dataDirectory, "IP" + File.separator + String.format("%04d", pcv) + File.separator + tableKey.getTableName()))
                        .toArray(File[]::new)
        );
        // TODO (deephaven/deephaven-core/issues/321): Re-add this part of the test when the parquet bug is fixed
//        TableManagementTools.writeTable(TableTools.emptyTable(0).updateView("Sym=NULL_CHAR", "Other=NULL_LONG"), partitionedDataDefinition,
//                new File(dataDirectory, "IP" + File.separator + "XXXX" + File.separator + tableKey.getTableName()), TableManagementTools.StorageFormat.Parquet);

        if (!missingGroups) {
            // Put Sym back on for the partitions that dropped it.
            partitions[2] = partitions[2].updateView("Sym = NULL_CHAR");
            partitions[3] = partitions[3].updateView("Sym = NULL_CHAR");
        }
        final Table expected = TableTools.merge(partitions).view("Part", "Sym", "Other"); // Column ordering was changed by by()/ungroup() above, restore it here.

        final Table actual = new NestedPartitionedDiskBackedTable(
                partitionedDataDefinition,
                RegionedTableComponentFactoryImpl.INSTANCE,
                new ReadOnlyLocalTableLocationProvider(
                        tableKey,
                        new NestedPartitionedLocalTableLocationScanner(dataDirectory),
                        false,
                        TableDataRefreshService.Null.INSTANCE
                ),
                null,
                Collections.emptySet()
        ).coalesce();

        TstUtils.assertTableEquals(expected, actual);

        TestCase.assertEquals(missingGroups, actual.getColumnSource("Sym").getGroupToRange() == null);

        TstUtils.assertTableEquals(expected.by("Sym").ungroup(), actual.by("Sym").ungroup());
    }

    @Test
    public void testParallelCollection() {
        final List<Integer> observedOrder = Collections.synchronizedList(new ArrayList<>());
        final int[] intArray = IntStream.range(0, 10000).parallel().peek(observedOrder::add).toArray();
        for (int ii = 1; ii < intArray.length; ++ii) {
            TestCase.assertTrue(intArray[ii - 1] < intArray[ii]);
        }
        System.out.println("Out of order observed: " + IntStream.range(1, intArray.length).anyMatch(ii -> observedOrder.get(ii - 1) > observedOrder.get(ii)));
        observedOrder.clear();

        final List<Integer> integerList = Arrays.stream(intArray).boxed().parallel().peek(observedOrder::add)
                .collect(Collectors.toList());
        for (int ii = 0; ii < integerList.size(); ++ii) {
            TestCase.assertEquals(intArray[ii], integerList.get(ii).intValue());
        }
        System.out.println("Out of order observed: " + IntStream.range(1, intArray.length).anyMatch(ii -> observedOrder.get(ii - 1) > observedOrder.get(ii)));
        observedOrder.clear();

        final LinkedHashMap<Integer, Integer> integerMap = integerList.parallelStream().peek(observedOrder::add)
                .collect(Collectors.toMap(Function.identity(), Function.identity(), Assert::neverInvoked, LinkedHashMap::new));
        System.out.println("Out of order observed: " + IntStream.range(1, intArray.length).anyMatch(ii -> observedOrder.get(ii - 1) > observedOrder.get(ii)));
        observedOrder.clear();

        final LinkedHashMap<String, String> stringMap = integerMap.entrySet().parallelStream().peek(e -> observedOrder.add(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString(), Assert::neverInvoked, LinkedHashMap::new));
        System.out.println("Out of order observed: " + IntStream.range(1, intArray.length).anyMatch(ii -> observedOrder.get(ii - 1) > observedOrder.get(ii)));
        observedOrder.clear();

        final int[] outputArray = stringMap.values().parallelStream().mapToInt(Integer::parseInt).toArray();
        for (int ii = 0; ii < outputArray.length; ++ii) {
            TestCase.assertEquals(intArray[ii], outputArray[ii]);
        }
    }
}
