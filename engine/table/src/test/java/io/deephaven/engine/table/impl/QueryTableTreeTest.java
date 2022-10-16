/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.test.types.OutOfBandTest;

import java.util.*;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.*;

/**
 * Test of Tree Tables and rollups.
 */
@Category(OutOfBandTest.class)
public class QueryTableTreeTest extends QueryTableTestBase {

    public void testTreeTableNotImplemented() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/64): Delete this, uncomment and fix the rest
        try {
            emptyTable(10).tree("ABC", "DEF");
            fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }

    public void testRollupNotImplemented() {
        // TODO (https://github.com/deephaven/deephaven-core/issues/65): Delete this, uncomment and fix the rest
        try {
            emptyTable(10).rollup(List.of(), "ABC", "DEF");
            fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }

    // private final ExecutorService pool = Executors.newFixedThreadPool(1);
    //
    // public void testMemoize() {
    // final Random random = new Random(0);
    // final ParentChildGenerator parentChildGenerator = new ParentChildGenerator(0.25, 0);
    // final QueryTable table = getTable(1000, random,
    // initColumnInfos(new String[] {"IDPair", "Sentinel", "Sentinel2", "Sym"},
    // parentChildGenerator,
    // new IntGenerator(0, 1_000_000_000),
    // new IntGenerator(0, 1_000_000_000),
    // new SetGenerator<>("AAPL", "TSLA", "VXX", "SPY")));
    //
    // final Table prepared = table.update("ID=IDPair.getId()", "Parent=IDPair.getParent()").dropColumns("IDPair");
    // final Table tree = prepared.tree("ID", "Parent");
    //
    // final boolean old = QueryTable.setMemoizeResults(true);
    // try {
    // testMemoize(tree, t -> TreeTableFilter.rawFilterTree(t, "Sym in `AAPL`, `TSLA`"));
    // testMemoize(tree, t -> TreeTableFilter.rawFilterTree(t, "Sym in `AAPL`, `TSLA`", "Sentinel == 500000000"));
    // testNoMemoize(tree, t -> TreeTableFilter.rawFilterTree(t, "Sentinel > Sentinel2/4"));
    // testNoMemoize(tree, t -> TreeTableFilter.rawFilterTree(t, "Sym in `AAPL`, `TSLA`"),
    // t -> TreeTableFilter.rawFilterTree(t, "Sym in `AAPL`"));
    // } finally {
    // QueryTable.setMemoizeResults(old);
    // }
    // }
    //
    // private void testMemoize(Table source, Function.Unary<Table, Table> op) {
    // final Table result = op.call(source);
    // final Table result2 = op.call(source);
    // Assert.assertSame(result, result2);
    // }
    //
    // private void testNoMemoize(Table source, Function.Unary<Table, Table> op1, Function.Unary<Table, Table> op2) {
    // final Table result = op1.call(source);
    // final Table result2 = op2.call(source);
    // Assert.assertNotSame(result, result2);
    // }
    //
    // private void testNoMemoize(Table source, Function.Unary<Table, Table> op) {
    // final Table result = op.call(source);
    // final Table result2 = op.call(source);
    // Assert.assertNotSame(result, result2);
    // }
    //
    // public void testTreeTableSimple() {
    // final Table source = TableTools.newTable(col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2));
    //
    // final Table treed =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> source.tree("Sentinel", "Parent"));
    // final String hierarchicalColumnName = getHierarchicalColumnName(treed);
    // TableTools.showWithRowSet(treed);
    //
    // assertEquals(2, treed.size());
    //
    // assertTrue(Arrays.equals(new int[] {NULL_INT, NULL_INT}, (int[]) treed.getColumn("Parent").getDirect()));
    //
    // final Table child1 = getChildTable(treed, treed, hierarchicalColumnName, 0);
    // assertNotNull(child1);
    //
    // TableTools.showWithRowSet(child1);
    // assertEquals(2, child1.size());
    // final Table gc1 = getChildTable(treed, child1, hierarchicalColumnName, 0);
    // TableTools.showWithRowSet(gc1);
    //
    // assertNull(getChildTable(treed, child1, hierarchicalColumnName, 1));
    //
    // final Table child2 = getChildTable(treed, treed, hierarchicalColumnName, 1);
    // assertNotNull(child2);
    // TableTools.showWithRowSet(child2);
    // assertEquals(2, child2.size());
    //
    // assertNull(getChildTable(treed, child2, hierarchicalColumnName, 1));
    // final Table gc2 = getChildTable(treed, child2, hierarchicalColumnName, 0);
    // TableTools.showWithRowSet(gc2);
    // }
    //
    // public void testConcurrentInstantiation() throws ExecutionException, InterruptedException {
    // final Logger log = new StreamLoggerImpl(System.out, LogLevel.DEBUG);
    // final boolean oldMemoize = QueryTable.setMemoizeResults(false);
    //
    // try {
    //
    // final QueryTable source = TstUtils.testRefreshingTable(
    // RowSetFactory.flat(10).toTracking(),
    // col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2),
    // col("Extra", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
    // final QueryTable source2 = TstUtils.testRefreshingTable(
    // RowSetFactory.flat(11).toTracking(),
    // col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2, NULL_INT),
    // col("Extra", "aa", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"));
    // final QueryTable source3 = TstUtils.testRefreshingTable(
    // RowSetFactory.flat(12).toTracking(),
    // col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2, NULL_INT, 11),
    // col("Extra", "aa", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"));
    //
    // final Table rootExpected1 = source.where("isNull(Parent)");
    // final Table rootExpected2 = source2.where("isNull(Parent)");
    // final Table rootExpected3 = source3.where("isNull(Parent)");
    //
    // final Supplier<Table> doTree = () -> source.tree("Sentinel", "Parent");
    // final Table expect = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(doTree::get);
    // final Table expectOriginal = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> makeStatic(source).tree("Sentinel", "Parent"));
    // final Table expect2 = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> source2.tree("Sentinel", "Parent"));
    //
    // final String hierarchicalColumnName = getHierarchicalColumnName(expect);
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    //
    // final Table treed1 = pool.submit(doTree::get).get();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expect, 0, 10, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // System.out.println("ORIGINAL TREED1");
    // dumpRollup(treed1, hierarchicalColumnName);
    //
    // TstUtils.addToTable(source, i(0, 11), c("Sentinel", 1, 11), c("Parent", NULL_INT, NULL_INT),
    // c("Extra", "aa", "k"));
    //
    // final Table treed2 = pool.submit(doTree::get).get();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // final TableMap map1 = (TableMap) treed1.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // final TableMap map2 = (TableMap) treed2.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    //
    // assertNotNull(map1);
    // assertNotNull(map2);
    //
    // final Table root1 = map1.get(null);
    // final Table root2 = map2.get(null);
    //
    // final Table sortedRoot1 = pool.submit(() -> root1.sortDescending("Sentinel")).get();
    // final Table sortedRoot2 = pool.submit(() -> root2.sortDescending("Sentinel")).get();
    //
    // final Table sortedSortedRoot1 = pool.submit(() -> sortedRoot1.sort("Extra")).get();
    // final Table sortedSortedRoot2 = pool.submit(() -> sortedRoot2.sort("Extra")).get();
    //
    // assertTableEquals(rootExpected1.sortDescending("Sentinel"), sortedRoot1);
    // assertTableEquals(rootExpected1.sortDescending("Sentinel"), sortedRoot2);
    // assertTableEquals(rootExpected1.sortDescending("Sentinel").sort("Extra"), sortedSortedRoot1);
    // assertTableEquals(rootExpected1.sortDescending("Sentinel").sort("Extra"), sortedSortedRoot2);
    //
    // source.notifyListeners(i(11), i(), i());
    //
    // final Table treed3 = pool.submit(doTree::get).get();
    //
    // final TableMap map3 = (TableMap) treed3.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // final Table root3 = map3.get(null);
    // final Table sortedRoot3 = pool.submit(() -> root3.sortDescending("Sentinel")).get();
    // final Table sortedSortedRoot3 = pool.submit(() -> sortedRoot3.sort("Extra")).get();
    // assertTableEquals(rootExpected2.sortDescending("Sentinel"), sortedRoot3);
    // assertTableEquals(rootExpected2.sort("Extra"), sortedSortedRoot3);
    //
    // TableTools.show(treed3);
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect, treed1, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect, treed2, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed3, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed1, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed2, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed3, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // final Table expectedFinalSort = rootExpected2.sortDescending("Sentinel");
    // assertTableEquals(expectedFinalSort, sortedRoot1);
    // assertTableEquals(expectedFinalSort, sortedRoot1);
    // assertTableEquals(expectedFinalSort, sortedRoot3);
    // assertTableEquals(expectedFinalSort.sort("Extra"), sortedSortedRoot1);
    // assertTableEquals(expectedFinalSort.sort("Extra"), sortedSortedRoot1);
    // assertTableEquals(expectedFinalSort.sort("Extra"), sortedSortedRoot3);
    //
    // final Table eleven1a = map1.get(11);
    // assertNull(eleven1a);
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    //
    // final Table backwards1 =
    // pool.submit(() -> TreeTableFilter.rawFilterTree(treed1, "!isNull(Extra)").sortDescending("Extra"))
    // .get();
    // final Table backwardsTree1a = pool.submit(() -> backwards1.tree("Sentinel", "Parent")).get();
    //
    // final Table treed4 = pool.submit(doTree::get).get();
    //
    // TstUtils.addToTable(source, i(12), c("Sentinel", 12), c("Parent", 11), c("Extra", "l"));
    //
    // final Table backwards2 =
    // pool.submit(() -> TreeTableFilter.rawFilterTree(treed1, "!isNull(Extra)").sortDescending("Extra"))
    // .get();
    // final Table backwardsTree1b = pool.submit(() -> backwards1.tree("Sentinel", "Parent")).get();
    // final Table backwardsTree2a = pool.submit(() -> backwards2.tree("Sentinel", "Parent")).get();
    //
    // final Table treed5 = pool.submit(doTree::get).get();
    //
    // int ii = 1;
    // for (Table treed : Arrays.asList(treed1, treed2, treed3, treed4, treed5)) {
    // doCompareWithChildrenForTrees("testConcurrentInstantiation" + ii++, expect, treed, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // }
    //
    // final Table eleven1b = map1.get(11);
    // assertNull(eleven1b);
    //
    // source.notifyListeners(i(12), i(), i());
    //
    // final Table treed6 = pool.submit(doTree::get).get();
    // UpdateGraphProcessor.DEFAULT.flushAllNormalNotificationsForUnitTests();
    //
    // final Table backwardsTree1c = pool.submit(() -> backwards1.tree("Sentinel", "Parent")).get();
    // final Table backwardsTree2b = pool.submit(() -> backwards2.tree("Sentinel", "Parent")).get();
    // final Table backwards3 =
    // pool.submit(() -> TreeTableFilter.rawFilterTree(treed1, "!isNull(Extra)").sortDescending("Extra"))
    // .get();
    // final Table backwardsTree3 = pool.submit(() -> backwards3.tree("Sentinel", "Parent")).get();
    //
    // final Table root1a = map1.get(null);
    // final Table root2a = map2.get(null);
    // final Table root3a = map3.get(null);
    // assertTableEquals(root1a, rootExpected3);
    // assertTableEquals(root2a, rootExpected3);
    // assertTableEquals(root3a, rootExpected3);
    //
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // final Table eleven1c = map1.get(11);
    // assertNotNull(eleven1c);
    //
    // final TableMap map4 = (TableMap) treed4.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // final TableMap map5 = (TableMap) treed5.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // final TableMap map6 = (TableMap) treed6.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // assertNotNull(map4.get(11));
    // assertNotNull(map5.get(11));
    // assertNotNull(map6.get(11));
    // assertEquals(1, map4.get(11).size());
    // assertEquals(1, map5.get(11).size());
    // assertEquals(1, map6.get(11).size());
    //
    // ii = 1;
    // for (Table treed : Arrays.asList(treed1, treed2, treed3, treed4, treed5, treed6)) {
    // doCompareWithChildrenForTrees("testConcurrentInstantiation" + ii++, expect, treed, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // }
    //
    // final Table backwardsExpected = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> source.sortDescending("Extra").tree("Sentinel", "Parent"));
    // ii = 1;
    // for (Table treed : Arrays.asList(backwardsTree1a, backwardsTree1b, backwardsTree1c, backwardsTree2a,
    // backwardsTree2b, backwardsTree3)) {
    // doCompareWithChildrenForTrees("testConcurrentInstantiationBackward" + ii++, backwardsExpected, treed, 0,
    // 4, hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // }
    // } finally {
    // QueryTable.setMemoizeResults(oldMemoize);
    // }
    // }
    //
    // public void testConcurrentInstantiationOfSort() throws ExecutionException, InterruptedException {
    // final Logger log = new StreamLoggerImpl(System.out, LogLevel.DEBUG);
    // final boolean oldMemoize = QueryTable.setMemoizeResults(false);
    //
    // try {
    //
    // final QueryTable source = TstUtils.testRefreshingTable(
    // RowSetFactory.flat(10).toTracking(),
    // col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2),
    // col("Extra", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
    // final QueryTable source2 = TstUtils.testRefreshingTable(
    // RowSetFactory.flat(11).toTracking(),
    // col("Sentinel", 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
    // col("Parent", NULL_INT, 1, 1, 2, 3, 5, 5, 3, 2, NULL_INT, 11),
    // col("Extra", "bb", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"));
    //
    // final java.util.function.Function<Table, Table> doSort = t -> t.sortDescending("Extra");
    // final java.util.function.Function<Table, Table> doTree = t -> t.tree("Sentinel", "Parent");
    // final java.util.function.Function<Table, Table> doSortAndTree = doSort.andThen(doTree);
    //
    // final Table expect =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> doSortAndTree.apply(source));
    // final Table expectOriginal = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> doSortAndTree.apply(makeStatic(source)));
    // final Table expect2 = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> doSortAndTree.apply(makeStatic(source2)));
    //
    // final String hierarchicalColumnName = getHierarchicalColumnName(expect);
    //
    // final Table sorted0 = doSort.apply(source);
    // final Table sorted0Original = doSort.apply(makeStatic(source));
    // final Table sorted2 = doSort.apply(makeStatic(source2));
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    //
    // final Table treed1 = pool.submit(() -> doSortAndTree.apply(source)).get();
    // final Table sorted1 = pool.submit(() -> doSort.apply(source)).get();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expect, 0, 10, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // TstUtils.removeRows(source, i(0));
    // TstUtils.addToTable(source, i(1, 11, 12), c("Sentinel", 2, 11, 12), c("Parent", NULL_INT, NULL_INT, 11),
    // c("Extra", "bb", "k", "l"));
    //
    // final Table treed2a = pool.submit(() -> doSortAndTree.apply(source)).get();
    // final Table treed2b = pool.submit(() -> doTree.apply(sorted0)).get();
    // final Table treed2c = pool.submit(() -> doTree.apply(sorted1)).get();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2a, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2b, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2c, expectOriginal, true, false, 0, 10,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // source.notifyListeners(i(11, 12), i(0), i(1));
    //
    // UpdateGraphProcessor.DEFAULT.flushAllNormalNotificationsForUnitTests();
    //
    // // everything should have current values now
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2a, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2b, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2c, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // // but still have a previous value for things that are old
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed1, expectOriginal, true, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2a, expectOriginal, true, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2b, expectOriginal, true, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed2c, expectOriginal, true, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // // we now initialize things after the notification is complete
    // final Table treed3a = pool.submit(() -> doSortAndTree.apply(source)).get();
    // final Table treed3b = pool.submit(() -> doTree.apply(sorted0)).get();
    // final Table treed3c = pool.submit(() -> doTree.apply(sorted1)).get();
    //
    // System.out.println("Tree3a");
    // dumpRollup(treed3a, hierarchicalColumnName);
    // System.out.println("Tree3b");
    // dumpRollup(treed3b, hierarchicalColumnName);
    // System.out.println("Tree3c");
    // dumpRollup(treed3c, hierarchicalColumnName);
    //
    // // everything should have current values now
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed3a, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed3b, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", treed3c, expect2, false, false, 0, 4,
    // hierarchicalColumnName, CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // // Note that previous is not defined to be the starting value, now that redirectToGet has been discontinued.
    // assertTableEquals(sorted0Original, prevTable(sorted0));
    // assertTableEquals(sorted0Original, prevTable(sorted1));
    // assertTableEquals(sorted2, sorted0);
    // assertTableEquals(sorted2, sorted1);
    //
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed1, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed2a, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed2b, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed2c, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed3a, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed3b, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("testConcurrentInstantiation", expect2, treed3c, 0, 4, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    //
    // } finally {
    // QueryTable.setMemoizeResults(oldMemoize);
    // }
    // }
    //
    // private Table makeStatic(QueryTable source) {
    // return source.silent().select();
    // }
    //
    // public void testTreeTableStaticFilter() {
    // final Table source = TableTools.newTable(intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 6));
    //
    // final Table treed =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> source.tree("Sentinel", "Parent"));
    // TableTools.showWithRowSet(treed);
    //
    // final String hierarchicalColumnName = getHierarchicalColumnName(treed);
    // assertEquals(2, treed.size());
    //
    // final Table filtered = TreeTableFilter.filterTree(treed, "Sentinel in 6, 11, 14");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(1, filtered.size());
    //
    // assertTrue(Arrays.equals(new int[] {NULL_INT, NULL_INT}, (int[]) treed.getColumn("Parent").getDirect()));
    // final Table child1 = getChildTable(filtered, filtered, hierarchicalColumnName, 0);
    // assertNotNull(child1);
    //
    // TableTools.showWithRowSet(child1);
    // assertEquals(1, child1.size());
    // }
    //
    // public void testTreeTableSimpleFilter() {
    // final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(10).toTracking(),
    // col("Sentinel", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
    // col("Parent", NULL_INT, NULL_INT, 1, 1, 2, 3, 5, 5, 3, 6));
    //
    // final Table treed =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> source.tree("Sentinel", "Parent"));
    // TableTools.showWithRowSet(treed);
    //
    // final String hierarchicalColumnName = getHierarchicalColumnName(treed);
    // assertEquals(2, treed.size());
    //
    // System.out.println("Filtered.");
    // final Table filtered = TreeTableFilter.filterTree(treed, "Sentinel in 6, 11, 14");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(1, filtered.size());
    //
    // assertTrue(Arrays.equals(new int[] {NULL_INT, NULL_INT}, (int[]) treed.getColumn("Parent").getDirect()));
    // final Table child1 = getChildTable(filtered, filtered, hierarchicalColumnName, 0);
    // assertNotNull(child1);
    //
    // TableTools.showWithRowSet(child1);
    // assertEquals(1, child1.size());
    //
    // final Table child2 = getChildTable(filtered, child1, hierarchicalColumnName, 0);
    // assertNotNull(child2);
    // TableTools.showWithRowSet(child2);
    // assertEquals(1, child1.size());
    //
    // assertNull(getChildTable(filtered, child2, hierarchicalColumnName, 0));
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(10), col("Sentinel", 11), col("Parent", 2));
    // source.notifyListeners(i(10), i(), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // System.out.println("Modified.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(2, filtered.size());
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(10), col("Sentinel", 12), col("Parent", 2));
    // source.notifyListeners(i(), i(), i(10));
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // System.out.println("Modified.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(1, filtered.size());
    //
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(10, 11), col("Sentinel", 12, 11), col("Parent", 2, 12));
    // source.notifyListeners(i(11), i(), i(10));
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // System.out.println("Grand parent.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(2, filtered.size());
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(11), col("Sentinel", 13), col("Parent", 12));
    // source.notifyListeners(i(), i(), i(11));
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // System.out.println("Grand parent disappear.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(1, filtered.size());
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(12), col("Sentinel", 14), col("Parent", 13));
    // source.notifyListeners(i(12), i(), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(source, 15);
    // System.out.println("Great grand parent appear.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(2, filtered.size());
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // removeRows(source, i(1));
    // source.notifyListeners(i(), i(1), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(source, 15);
    // System.out.println("2 removed.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(1, filtered.size());
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(1), col("Sentinel", 2), col("Parent", NULL_INT));
    // source.notifyListeners(i(1), i(), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(source, 15);
    // System.out.println("2 resurrected.");
    // TableTools.showWithRowSet(filtered);
    // assertEquals(2, filtered.size());
    // }
    //
    // public void testOrphanPromoterSimple() {
    // final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(4).toTracking(),
    // col("Sentinel", 1, 2, 3, 4), col("Parent", NULL_INT, NULL_INT, 1, 5));
    //
    // final Table treed = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> TreeTableOrphanPromoter
    // .promoteOrphans(source, "Sentinel", "Parent").tree("Sentinel", "Parent"));
    // TableTools.showWithRowSet(treed);
    // assertEquals(3, treed.size());
    //
    // // add a parent, which will make something not an orphan
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(5), col("Sentinel", 5), col("Parent", 1));
    // source.notifyListeners(i(5), i(), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(treed);
    // assertEquals(2, treed.size());
    //
    // // swap two things
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(source, i(0, 1), col("Sentinel", 2, 1), col("Parent", NULL_INT, NULL_INT));
    // source.notifyListeners(i(), i(), i(0, 1));
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(treed);
    // assertEquals(2, treed.size());
    //
    // // now remove a parent
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // removeRows(source, i(0, 1));
    // source.notifyListeners(i(), i(0, 1), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    // TableTools.showWithRowSet(treed);
    // assertEquals(2, treed.size());
    // }
    //
    // private static String getHierarchicalColumnName(Table treed) {
    // final HierarchicalTableInfo info =
    // (HierarchicalTableInfo) treed.getAttribute(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
    // return info.getHierarchicalColumnName();
    // }
    //
    // private Table getChildTable(Table root, Table treed, String hierarchicalColumnName, long index) {
    // final Object childKey1 = treed.getColumn(hierarchicalColumnName).get(index);
    // final Table table =
    // ((TableMap) root.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)).get(childKey1);
    // if (table == null || table.isEmpty()) {
    // return null;
    // }
    // return table;
    // }
    //
    // public void testTreeTableEdgeCases() {
    // final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(4).toTracking(),
    // col("Sentinel", 0, 1, 2, 3),
    // col("Filter", 0, 0, 0, 0),
    // col("Parent", NULL_INT, NULL_INT, NULL_INT, NULL_INT));
    //
    // final EvalNugget[] en = new EvalNugget[] {
    // new EvalNugget() {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> {
    // final Table treed = source.tree("Sentinel", "Parent");
    // return TreeTableFilter.rawFilterTree(treed, "Filter in 1");
    // });
    // }
    // },
    // };
    //
    // TstUtils.validate(en);
    // Assert.assertEquals(0, en[0].originalValue.size());
    //
    // // modify child to have parent
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.addToTable(source, i(0), c("Sentinel", 0), c("Filter", 1), c("Parent", 1));
    // source.notifyListeners(i(), i(), i(0));
    // });
    // Assert.assertEquals(i(0, 1), en[0].originalValue.getRowSet());
    //
    // // modify parent to have grandparent
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.addToTable(source, i(1), c("Sentinel", 1), c("Filter", 0), c("Parent", 2));
    // source.notifyListeners(i(), i(), i(1));
    // });
    // Assert.assertEquals(i(0, 1, 2), en[0].originalValue.getRowSet());
    //
    // // modify parent's id to orphan child
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.addToTable(source, i(1), c("Sentinel", -1), c("Filter", 0), c("Parent", 2));
    // source.notifyListeners(i(), i(), i(1));
    // });
    // Assert.assertEquals(i(0), en[0].originalValue.getRowSet());
    //
    // // revert parent's id and adopt child
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.addToTable(source, i(1), c("Sentinel", 1), c("Filter", 0), c("Parent", 2));
    // source.notifyListeners(i(), i(), i(1));
    // });
    // Assert.assertEquals(i(0, 1, 2), en[0].originalValue.getRowSet());
    //
    // // remove child, resurrect parent
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.removeRows(source, i(0));
    // TstUtils.addToTable(source, i(3), c("Sentinel", 3), c("Filter", 1), c("Parent", 1));
    // source.notifyListeners(i(), i(0), i(3));
    // });
    // Assert.assertEquals(i(1, 2, 3), en[0].originalValue.getRowSet());
    // }
    //
    // @TableToolsShowControl(getWidth = 40)
    // public static class IdParentPair {
    // final int id;
    // final int parent;
    //
    // private IdParentPair(int id, int parent) {
    // this.id = id;
    // this.parent = parent;
    // }
    //
    // public int getId() {
    // return id;
    // }
    //
    // // used within query strings below
    // @SuppressWarnings("unused")
    // public int getParent() {
    // return parent;
    // }
    //
    // @Override
    // public String toString() {
    // return "{id=" + id + ", parent=" + parent + '}';
    // }
    // }
    //
    // public static class ParentChildGenerator implements TstUtils.Generator<IdParentPair, IdParentPair> {
    // final double rootFraction;
    // final double createAsOrphanFraction;
    //
    // final Map<Integer, Set<Integer>> parentToChild = new HashMap<>();
    // final Map<Integer, Set<Integer>> orphans = new HashMap<>();
    // final List<Integer> usedIds = new ArrayList<>();
    //
    // public ParentChildGenerator(double rootFraction, double createAsOrphanFraction) {
    // this.rootFraction = rootFraction;
    // this.createAsOrphanFraction = createAsOrphanFraction;
    // }
    //
    // @Override
    // public TreeMap<Long, IdParentPair> populateMap(TreeMap<Long, IdParentPair> values, RowSet toAdd,
    // Random random) {
    // final TreeMap<Long, IdParentPair> result = new TreeMap<>();
    //
    // for (final RowSet.Iterator it = toAdd.iterator(); it.hasNext();) {
    // add(random, values, result, it.nextLong());
    // }
    //
    // values.putAll(result);
    //
    // return result;
    // }
    //
    // private void add(Random random, TreeMap<Long, IdParentPair> values, TreeMap<Long, IdParentPair> result,
    // long key) {
    // if (values.containsKey(key)) {
    // // this is a modification, for now let's keep it actually the same, because otherwise it is hard
    // final IdParentPair existing = values.get(key);
    //
    // // final boolean isOrphan = orphans.containsKey(existing.id);
    // // final boolean isActive = parentToChild.containsKey(existing.id);
    //
    // result.put(key, existing);
    //
    // return;
    // }
    //
    // int id;
    // do {
    // id = random.nextInt();
    // } while (parentToChild.containsKey(id));
    //
    // final boolean asOrphan = random.nextDouble() < createAsOrphanFraction;
    // final boolean canSatisfy = !(asOrphan ? orphans : parentToChild).isEmpty();
    //
    // parentToChild.put(id, new HashSet<>());
    //
    // int parent;
    // if (usedIds.isEmpty() || random.nextDouble() < rootFraction || !canSatisfy) {
    // parent = NULL_INT;
    // } else {
    // boolean satisfied;
    // final int startIdx = random.nextInt(usedIds.size());
    // int nextIdx = startIdx;
    // do {
    // // we need to find an appropriate parent, from one of the existing elements
    // parent = usedIds.get(nextIdx);
    // if (asOrphan) {
    // satisfied = !orphans.isEmpty() && orphans.keySet().contains(parent);
    // } else {
    // satisfied = !parentToChild.isEmpty() && parentToChild.keySet().contains(parent);
    // }
    // nextIdx = (nextIdx + 1) % usedIds.size();
    // } while (!satisfied && nextIdx != startIdx);
    //
    // if (!satisfied) {
    // // oops couldn't find a parent without creating a cycle
    // parent = NULL_INT;
    // }
    // }
    // usedIds.add(id);
    //
    // final IdParentPair pair = new IdParentPair(id, parent);
    // result.put(key, pair);
    // }
    //
    // @Override
    // public void onRemove(long key, IdParentPair remove) {
    // final int parentToOrphan = remove.id;
    // doOrphan(parentToOrphan);
    // }
    //
    // private void doOrphan(int parentToOrphan) {
    // final Set<Integer> orphanKeys =
    // Require.neqNull(parentToChild.remove(parentToOrphan), Integer.toString(parentToOrphan));
    // orphans.put(parentToOrphan, orphanKeys);
    // orphanKeys.forEach(this::doOrphan);
    // }
    //
    // @Override
    // public Class<IdParentPair> getType() {
    // return IdParentPair.class;
    // }
    //
    // @Override
    // public Class<IdParentPair> getColumnType() {
    // return IdParentPair.class;
    // }
    //
    // void show() {
    // System.out.println("Parents: " + parentToChild.keySet());
    // System.out.println("Orphans: " + orphans.keySet());
    // }
    // }
    //
    // static abstract class HierarchicalTableEvalNugget extends EvalNugget {
    // final Supplier<Integer> maxLevels;
    // final String[] sortColumns;
    //
    // HierarchicalTableEvalNugget(Table prepared) {
    // this.maxLevels = prepared::intSize;
    // this.sortColumns = CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
    // }
    //
    // HierarchicalTableEvalNugget(int maxLevels, String... sortColumns) {
    // this.maxLevels = () -> maxLevels;
    // this.sortColumns = sortColumns;
    // }
    //
    // @Override
    // void checkDifferences(String msg, Table recomputed) {
    // compareWithChildren(msg, originalValue, recomputed, getHierarchicalColumnName(recomputed));
    // }
    //
    // abstract void compareWithChildren(String msg, Table originalValue, Table recomputed,
    // String hierarchicalColumnName);
    //
    // @Override
    // void showResult(String label, Table e) {
    // System.out.println(label);
    // dumpRollup(e, getHierarchicalColumnName(e));
    // }
    // }
    //
    // static abstract class TreeTableEvalNugget extends HierarchicalTableEvalNugget {
    //
    // TreeTableEvalNugget(Table prepared) {
    // super(prepared);
    // }
    //
    // TreeTableEvalNugget(int maxLevels, String... sortColumns) {
    // super(maxLevels, sortColumns);
    // }
    //
    // void compareWithChildren(String msg, Table originalValue, Table recomputed, String hierarchicalColumnName) {
    // doCompareWithChildrenForTrees(msg, originalValue, recomputed, 0, maxLevels.get(), hierarchicalColumnName,
    // sortColumns);
    // }
    // }
    //
    // static abstract class RollupEvalNugget extends HierarchicalTableEvalNugget {
    //
    // RollupEvalNugget(Table prepared) {
    // super(prepared);
    // }
    //
    // RollupEvalNugget(int maxLevels, String... sortColumns) {
    // super(maxLevels, sortColumns);
    // }
    //
    // void compareWithChildren(String msg, Table originalValue, Table recomputed, String hierarchicalColumnName) {
    // doCompareWithChildrenForRollups(msg, originalValue, recomputed, 0, maxLevels.get(), hierarchicalColumnName,
    // sortColumns);
    // }
    // }
    //
    // static private void doCompareWithChildrenForTrees(String msg, Table actualValue, Table expectedValue, int levels,
    // int maxLevels, String hierarchicalColumnName, String[] sortColumns) {
    // doCompareWithChildrenForTrees(msg, actualValue, expectedValue, false, false, levels, maxLevels,
    // hierarchicalColumnName, sortColumns);
    // }
    //
    // static private void doCompareWithChildrenForTrees(String msg, Table actualValue, Table expectedValue,
    // boolean actualPrev, boolean expectedPrev, int levels, int maxLevels, String hierarchicalColumnName,
    // String[] sortColumns) {
    // doCompareWithChildren(
    // t -> (TableMap) actualValue.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE),
    // t -> (TableMap) expectedValue.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE),
    // msg, actualValue, expectedValue, actualPrev, expectedPrev, levels, maxLevels, hierarchicalColumnName,
    // sortColumns);
    // }
    //
    // static private void doCompareWithChildrenForRollups(String msg, Table originalValue, Table recomputed, int
    // levels,
    // int maxLevels, String hierarchicalColumnName, String[] sortColumns) {
    // doCompareWithChildren(
    // t -> (TableMap) t.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE),
    // t -> (TableMap) t.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE),
    // msg, originalValue, recomputed, false, false, levels, maxLevels, hierarchicalColumnName, sortColumns);
    // }
    //
    // static private void doCompareWithChildren(Function.Unary<TableMap, Table> actualMapSource,
    // Function.Unary<TableMap, Table> expectedMapSource,
    // String msg,
    // Table actualValueIn,
    // Table expectedValueIn,
    // boolean actualPrev,
    // boolean expectedPrev,
    // int levels,
    // int maxLevels,
    // String hierarchicalColumnName,
    // String[] sortColumns) {
    // if (levels > maxLevels) {
    // throw new IllegalStateException("Refusing to validate levels " + levels + ", to prevent infinite looping!");
    // }
    //
    // Table actualValue = getDiffableTable(actualValueIn);
    // Table expectedValue = getDiffableTable(expectedValueIn);
    //
    // if (sortColumns.length > 0) {
    // actualValue = actualValue.sort(sortColumns);
    // expectedValue = expectedValue.sort(sortColumns);
    // }
    //
    // assertTableEquals(maybePrev(expectedValue.dropColumns(hierarchicalColumnName), expectedPrev),
    // maybePrev(actualValue.dropColumns(hierarchicalColumnName), actualPrev),
    // TableDiff.DiffItems.DoublesExact);
    //
    // final ColumnSource actualChildren = columnOrPrev(actualValue, hierarchicalColumnName, actualPrev);
    // final ColumnSource expectedChildren = columnOrPrev(expectedValue, hierarchicalColumnName, expectedPrev);
    //
    // final TableMap actualMap = actualMapSource.call(actualValue);
    // final TableMap expectedMap = expectedMapSource.call(expectedValue);
    //
    // final RowSet actualRowSet = indexOrPrev(actualValue, actualPrev);
    // final RowSet expectedRowSet = indexOrPrev(expectedValue, expectedPrev);
    // assertEquals(expectedRowSet.size(), actualRowSet.size());
    //
    // final RowSet.Iterator oit = actualRowSet.iterator();
    // final RowSet.Iterator rit = expectedRowSet.iterator();
    //
    // for (; oit.hasNext();) {
    // assertTrue(rit.hasNext());
    // final long originalRow = oit.nextLong();
    // final long recomputedRow = rit.nextLong();
    //
    // final Object aKey = actualChildren.get(originalRow);
    // final Object eKey = expectedChildren.get(recomputedRow);
    //
    // assertEquals(aKey == null, eKey == null);
    //
    // final Table ac = aKey == null ? null : actualMap.get(aKey);
    // final Table ec = eKey == null ? null : expectedMap.get(eKey);
    //
    // assertEquals(ac == null || ac.size() == 0, ec == null || ec.size() == 0);
    //
    // if ((ac != null && ac.size() > 0) && (ec != null && ec.size() > 0)) {
    // doCompareWithChildren(actualMapSource, expectedMapSource, msg, ac, ec, actualPrev, expectedPrev,
    // levels + 1, maxLevels, hierarchicalColumnName, sortColumns);
    // }
    // }
    // }
    //
    // public void testTreeTableIncremental() {
    // testTreeTableIncremental(10, 0, new MutableInt(100));
    // testTreeTableIncremental(100, 0, new MutableInt(100));
    // }
    //
    // private static Table maybePrev(Table table, boolean usePrev) {
    // return usePrev ? prevTable(table) : table;
    // }
    //
    // private static ColumnSource columnOrPrev(Table table, String columnName, boolean usePrev) {
    // // noinspection unchecked
    // return usePrev ? new PrevColumnSource(table.getColumnSource(columnName)) : table.getColumnSource(columnName);
    // }
    //
    // private static RowSet indexOrPrev(Table table, boolean usePrev) {
    // return usePrev ? table.getRowSet().copyPrev() : table.getRowSet();
    // }
    //
    // private void testTreeTableIncremental(final int size, final long seed, final MutableInt numSteps) {
    // final Random random = new Random(seed);
    // final ParentChildGenerator parentChildGenerator = new ParentChildGenerator(0.25, 0);
    //
    // final TstUtils.ColumnInfo[] columnInfo = initColumnInfos(new String[] {"IDPair", "Sentinel", "Sym"},
    // parentChildGenerator,
    // new IntGenerator(0, 1_000_000_000),
    // new SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"));
    // // noinspection unchecked
    // final QueryTable table = getTable(size, random, columnInfo);
    //
    // if (RefreshingTableTestCase.printTableUpdates) {
    // System.out.println("Original:");
    // TableTools.showWithRowSet(table);
    // }
    //
    // final Table prepared = table.update("ID=IDPair.getId()", "Parent=IDPair.getParent()").dropColumns("IDPair");
    //
    // if (RefreshingTableTestCase.printTableUpdates) {
    // System.out.println("Original Prepared:");
    // TableTools.showWithRowSet(prepared);
    // }
    //
    // final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
    // new EvalNuggetInterface() {
    // @Override
    // public void validate(String msg) {}
    //
    // @Override
    // public void show() {
    // TableTools.showWithRowSet(prepared);
    // }
    // },
    // new TreeTableEvalNugget(prepared) {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> prepared.tree("ID", "Parent"));
    // }
    // },
    // new TreeTableEvalNugget(prepared) {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> prepared.sort("Sym").tree("ID", "Parent"));
    // }
    // },
    // new TreeTableEvalNugget(prepared) {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> prepared.sort("Sentinel").tree("ID", "Parent"));
    // }
    // },
    // new TreeTableEvalNugget(prepared) {
    // @Override
    // protected Table e() {
    // return TreeTableFilter.filterTree(prepared.tree("ID", "Parent"), "Sentinel % 2 == 1");
    // }
    // },
    // EvalNugget.from(
    // () -> TreeTableFilter.rawFilterTree(prepared.tree("ID", "Parent"), "Sentinel % 2 == 1")),
    // };
    //
    // final int maxSteps = numSteps.intValue();
    // for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
    // if (RefreshingTableTestCase.printTableUpdates) {
    // System.out.println("Step = " + numSteps.intValue());
    // }
    // simulateShiftAwareStep(size, random, table, columnInfo, en);
    // }
    // }
    //
    // public void testOrphanPromoter() throws IOException {
    // testOrphanPromoter(10, 0, new MutableInt(100));
    // testOrphanPromoter(100, 0, new MutableInt(100));
    // }
    //
    // private void testOrphanPromoter(final int size, int seed, MutableInt numSteps) {
    // final int maxSteps = numSteps.intValue();
    // final Random random = new Random(seed);
    //
    // final ColumnInfo[] columnInfo;
    // final ParentChildGenerator parentChildGenerator = new ParentChildGenerator(0.25, 0.25);
    // final QueryTable table = getTable(size, random,
    // columnInfo = initColumnInfos(new String[] {"IDPair", "Sentinel", "Sym"},
    // parentChildGenerator,
    // new TstUtils.IntGenerator(0, 1_000_000_000),
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY")));
    //
    // final Table prepared = table.update("ID=IDPair.getId()", "Parent=IDPair.getParent()").dropColumns("IDPair");
    //
    // final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
    // new EvalNuggetInterface() {
    // @Override
    // public void validate(String msg) {}
    //
    // @Override
    // public void show() {
    // TableTools.showWithRowSet(prepared);
    // }
    // },
    // new EvalNugget() {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(
    // () -> TreeTableOrphanPromoter.promoteOrphans((QueryTable) prepared, "ID", "Parent"));
    // }
    // },
    // new EvalNugget() {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> TreeTableOrphanPromoter
    // .promoteOrphans((QueryTable) prepared.where("Sentinel % 2 == 0"), "ID", "Parent"));
    // }
    // },
    // new TreeTableEvalNugget(prepared) {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> TreeTableOrphanPromoter.promoteOrphans((QueryTable) prepared
    // .where("Sentinel % 2 == 0"), "ID", "Parent").tree("ID", "Parent"));
    // }
    // },
    // };
    //
    // for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
    // if (RefreshingTableTestCase.printTableUpdates) {
    // System.out.println("Step = " + numSteps.intValue());
    // }
    // simulateShiftAwareStep(size, random, table, columnInfo, en);
    // }
    // }
    //
    // private static void dumpRollup(Table root, String hierarchicalColumnName, String... labelColumns) {
    // dumpRollup(root, false, hierarchicalColumnName, labelColumns);
    // }
    //
    // private static void dumpRollup(Table root, boolean usePrev, String hierarchicalColumnName, String...
    // labelColumns) {
    // final HierarchicalTableInfo info =
    // (HierarchicalTableInfo) root.getAttribute(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
    //
    // final TableMap map = info instanceof TreeTableInfo
    // ? (TableMap) root.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)
    // : null;
    //
    // dumpRollup(root, map, usePrev, hierarchicalColumnName, labelColumns);
    // }
    //
    // private static void dumpRollup(Table root, TableMap childMap, boolean usePrev, String hierarchicalColumnName,
    // String... labelColumns) {
    // TableTools.showWithRowSet(usePrev ? prevTable(root) : root, 101);
    //
    // final List<ColumnSource> labelSource =
    // Arrays.stream(labelColumns).map(root::getColumnSource).collect(Collectors.toList());
    // final ColumnSource children = columnOrPrev(root, hierarchicalColumnName, usePrev);
    // final TableMap map =
    // childMap == null ? (TableMap) root.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)
    // : childMap;
    // final ReverseLookup reverseLookup = (ReverseLookup) root.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE);
    // if (reverseLookup != null) {
    // System.out.println("Reverse Lookup is set.");
    // } else {
    // System.out.println("No Reverse Lookup.");
    // }
    //
    // if (map == null) {
    // System.out.println("No child map.");
    // return;
    // }
    //
    // for (final RowSet.Iterator it = indexOrPrev(root, usePrev).iterator(); it.hasNext();) {
    // final long key = it.nextLong();
    // Object childKey = children.get(key);
    // if (childKey == null) {
    // childKey = new SmartKey();
    // }
    //
    // final Table childTable = map.get(childKey);
    // if (childTable == null || childTable.size() == 0) {
    // continue;
    // }
    //
    // System.out.println(
    // "Label: " + labelSource.stream().map(x -> (String) x.get(key)).collect(Collectors.joining(", ")));
    // dumpRollup(childTable, childMap, usePrev, hierarchicalColumnName, labelColumns);
    // }
    // }
    //
    // public void testRollupMinMax() {
    // testSimpleRollup(List.of(AggMin("IntCol", "DoubleCol")));
    // testSimpleRollup(List.of(AggMax("IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupSum() {
    // testSimpleRollup(List.of(AggSum("IntCol", "DoubleCol", "BigIntCol", "BigDecCol")));
    // }
    //
    // public void testRollupWSum() {
    // // TODO: BigDecimal, BigInteger
    // testSimpleRollup(List.of(AggWSum("DoubleCol", "IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupReverseLookup() {
    // final Collection<? extends Aggregation> comboAgg = List.of(Aggregation.AggSum("IntCol", "DoubleCol"));
    // final Random random = new Random(0);
    //
    // final int size = 100;
    // final QueryTable table = getTable(size, random,
    // initColumnInfos(
    // new String[] {"USym", "DateTime", "IntCol", "DoubleCol", "BoolCol", "BigIntCol", "BigDecCol"},
    // new SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new SetGenerator<>(DateTimeUtils.convertDateTime("2020-01-01T00:00:00 NY"), null,
    // DateTimeUtils.convertDateTime("2020-02-28T14:30:00 NY")),
    // new IntGenerator(0, 1_000_000),
    // new DoubleGenerator(-100, 100),
    // new BooleanGenerator(0.4, 0.1),
    // new BigIntegerGenerator(BigInteger.ZERO, BigInteger.valueOf(100), 0.1),
    // new BigDecimalGenerator(BigInteger.valueOf(-1000), BigInteger.valueOf(1000), 5, 0.1)));
    //
    // System.out.println("Source Data:");
    // TableTools.showWithRowSet(table);
    //
    // final Table rollup = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(comboAgg, "USym", "DateTime", "BoolCol", "BigIntCol", "BigDecCol"));
    // verifyReverseLookup(rollup);
    //
    // verifyReverseLookup(
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> table.rollup(comboAgg, "USym")));
    // verifyReverseLookup(
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> table.rollup(comboAgg, "DateTime")));
    // verifyReverseLookup(
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> table.rollup(comboAgg, "BoolCol")));
    // }
    //
    // private void verifyReverseLookup(Table rollup) {
    // final String columnName =
    // ((HierarchicalTableInfo) rollup.getAttribute(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE))
    // .getHierarchicalColumnName();
    // verifyReverseLookup(rollup, columnName);
    // }
    //
    // private void verifyReverseLookup(Table rollup, String columnName) {
    // final ReverseLookup rl = (ReverseLookup) rollup.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE);
    // Assert.assertNotNull("rl", rl);
    // final Set<Object> children = new LinkedHashSet<>();
    // final ColumnSource childSource = rollup.getColumnSource(columnName);
    //
    // TableTools.showWithRowSet(rollup);
    //
    // rollup.columnIterator(columnName).forEachRemaining(key -> {
    // if (key == null) {
    // return;
    // }
    // children.add(key);
    // final long idx = rl.get(key);
    // final Object fromColumn = childSource.get(idx);
    // TestCase.assertEquals(key, fromColumn);
    // });
    //
    // final TableMap childMap = (TableMap) rollup.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // for (final Object childKey : children) {
    // verifyReverseLookup(childMap.get(childKey), columnName);
    // }
    // }
    //
    // public void testRollupAbsSum() {
    // testSimpleRollup(List.of(AggAbsSum("IntCol", "DoubleCol", "BigIntCol", "BigDecCol")));
    // }
    //
    // public void testRollupAverage() {
    // testSimpleRollup(
    // List.of(AggAvg("IntCol", "DoubleCol", "BigIntCol", "BigDecCol", "DoubleNanCol", "FloatNullCol")));
    // }
    //
    // public void testRollupStd() {
    // testSimpleRollup(List.of(AggStd("IntCol", "DoubleCol", "BigIntCol", "BigDecCol")));
    // }
    //
    // public void testRollupVar() {
    // testSimpleRollup(List.of(AggVar("IntCol", "DoubleCol", "BigIntCol", "BigDecCol")));
    // }
    //
    // public void testRollupFirst() {
    // testSimpleRollup(List.of(AggFirst("IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupLast() {
    // testSimpleRollup(List.of(AggLast("IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupSortedLast() {
    // testSimpleRollup(List.of(AggSortedLast("IntCol", "IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupSortedFirst() {
    // testSimpleRollup(List.of(AggSortedFirst("IntCol", "IntCol", "DoubleCol")));
    // }
    //
    // public void testRollupCountDistinct() {
    // testSimpleRollup(List.of(AggCountDistinct("IntCol", "DoubleCol", "FloatNullCol", "StringCol", "BoolCol")));
    // testSimpleRollup(
    // List.of(AggCountDistinct(true, "IntCol", "DoubleCol", "FloatNullCol", "StringCol", "BoolCol")));
    // }
    //
    // public void testRollupDistinct() {
    // testSimpleRollup(List.of(AggDistinct("IntCol", "DoubleCol", "FloatNullCol", "StringCol", "BoolCol")));
    // testSimpleRollup(List.of(AggDistinct(true, "IntCol", "DoubleCol", "FloatNullCol", "StringCol", "BoolCol")));
    // }
    //
    // public void testRollupUnique() {
    // testSimpleRollup(List.of(AggUnique("IntCol", "DoubleCol", "FloatNullCol", "StringCol", "BoolCol")));
    // testSimpleRollup(List.of(AggUnique(true, Sentinel(), "IntCol", "DoubleCol", "FloatNullCol", "StringCol",
    // "BoolCol")));
    // }
    //
    // private void testSimpleRollup(Collection<? extends Aggregation> comboAgg) {
    // final Random random = new Random(0);
    //
    // final int size = 10;
    // final QueryTable table = getTable(size, random,
    // initColumnInfos(
    // new String[] {"USym", "Group", "IntCol", "DoubleCol", "DoubleNanCol", "FloatNullCol",
    // "StringCol", "BoolCol", "BigIntCol", "BigDecCol"},
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new TstUtils.SetGenerator<>("Terran", "Vulcan", "Klingon", "Romulan"),
    // new TstUtils.IntGenerator(0, 1_000_000),
    // new TstUtils.DoubleGenerator(-100, 100),
    // new TstUtils.DoubleGenerator(-100, 100, 0.0, 0.1),
    // new TstUtils.FloatGenerator(-100, 100, 0.1, 0.0),
    // new TstUtils.SetGenerator<>("A", "B", "C", "D"),
    // new TstUtils.BooleanGenerator(.5, .1),
    // new TstUtils.BigIntegerGenerator(BigInteger.ZERO, BigInteger.valueOf(100), 0.1),
    // new TstUtils.BigDecimalGenerator(BigInteger.valueOf(-1000), BigInteger.valueOf(1000), 5, 0.1)));
    //
    // System.out.println("Source Data:");
    // TableTools.showWithRowSet(table);
    //
    // final Table rollup =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(comboAgg, "USym", "Group"));
    //
    // final Table fullBy = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> table.aggBy(comboAgg));
    // System.out.println("Full By:");
    // TableTools.showWithRowSet(fullBy);
    //
    // System.out.println("Rollup Meta");
    // TableTools.show(rollup.getMeta());
    //
    // System.out.println("Rollup:");
    // dumpRollup(rollup, getHierarchicalColumnName(rollup), "USym", "Group");
    //
    // final List<String> viewCols = new ArrayList<>(Arrays.asList("IntCol", "DoubleCol"));
    // for (final String maybeColumn : new String[] {"BigIntCol", "BigDecCol", "DoubleNanCol", "FloatNullCol",
    // "StringCol", "BoolCol"}) {
    // if (fullBy.hasColumns(maybeColumn)) {
    // viewCols.add(maybeColumn);
    // }
    // }
    //
    // final Table rollupClean = getDiffableTable(rollup).view(Selectable.from(viewCols));
    //
    // final String diff = TableTools.diff(fullBy, rollupClean, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
    //
    // assertEquals("", diff);
    //
    // }
    //
    // public void testRollupScope() {
    // final Random random = new Random(0);
    //
    // final int size = 10;
    // final QueryTable table = getTable(size, random,
    // initColumnInfos(new String[] {"USym", "Group", "IntCol", "DoubleCol"},
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new TstUtils.SetGenerator<>("Terran", "Vulcan", "Klingon", "Romulan"),
    // new TstUtils.IntGenerator(0, 1_000_000),
    // new TstUtils.DoubleGenerator(-100, 100)));
    //
    // final SafeCloseable scopeCloseable = LivenessScopeStack.open();
    //
    // final Table rollup = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(List.of(AggSum("IntCol", "DoubleCol")), "USym", "Group"));
    // final TableMap rootMap = (TableMap) rollup.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // final Table nextLevel = rootMap.get(SmartKey.EMPTY);
    // assertNotNull(nextLevel);
    //
    // // TableTools.show(rollup.getMeta());
    // // dumpRollup(rollup, getHierarchicalColumnName(rollup), "USym", "Group");
    //
    // final SingletonLivenessManager rollupManager = new SingletonLivenessManager(rollup);
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);
    //
    // Assert.assertTrue(rollup.tryRetainReference());
    // Assert.assertTrue(rootMap.tryRetainReference());
    // Assert.assertTrue(nextLevel.tryRetainReference());
    //
    // rollup.dropReference();
    // rootMap.dropReference();
    // nextLevel.dropReference();
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(rollupManager::release);
    //
    // // we should not be able to retainReference the rollup, because closing the scope should have decremented it to
    // // zero
    // Assert.assertFalse(rollup.tryRetainReference());
    //
    // // we should not be able to retainReference the tablemap, because closing the scope should have decremented it
    // // to zero
    // Assert.assertFalse(rootMap.tryRetainReference());
    //
    // Assert.assertFalse(nextLevel.tryRetainReference());
    // }
    //
    // public void testTreeTableScope() {
    // final Random random = new Random(0);
    //
    // final int size = 10;
    // final QueryTable table = getTable(size, random,
    // initColumnInfos(new String[] {"USym", "Group", "IntCol", "DoubleCol", "ParentCol"},
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new TstUtils.SetGenerator<>("Terran", "Vulcan", "Klingon", "Romulan"),
    // new TstUtils.IntGenerator(1, 1_000_000),
    // new TstUtils.DoubleGenerator(-100, 100),
    // new TstUtils.IntGenerator(0, 0, 1.0)));
    //
    // final SafeCloseable scopeCloseable = LivenessScopeStack.open();
    //
    // final Table promoted = TreeTableOrphanPromoter.promoteOrphans(table, "IntCol", "ParentCol");
    // final Table treed = promoted.tree("IntCol", "ParentCol");
    //
    // final SingletonLivenessManager treeManager = new SingletonLivenessManager(treed);
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);
    //
    // Assert.assertTrue(treed.tryRetainReference());
    // Assert.assertTrue(promoted.tryRetainReference());
    // treed.dropReference();
    // promoted.dropReference();
    //
    // System.gc();
    //
    // assertTableEquals(table, treed);
    //
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // final long key = table.getRowSet().firstRowKey();
    // table.getRowSet().writableCast().remove(key);
    // TstUtils.removeRows(table, i(key));
    // table.notifyListeners(i(), i(key), i());
    // });
    //
    // assertTableEquals(table, treed);
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(treeManager::release);
    //
    // // we should not be able to retainReference the tree table, because closing the scope should have decremented it
    // // to zero
    // Assert.assertFalse(treed.tryRetainReference());
    // Assert.assertFalse(promoted.tryRetainReference());
    // }
    //
    // public void testRollupScope2() {
    // final QueryTable table = TstUtils.testRefreshingTable(i().toTracking(),
    // col("USym", CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
    // col("Group", CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
    // intCol("IntCol"), doubleCol("DoubleCol"));
    //
    // final SafeCloseable scopeCloseable = LivenessScopeStack.open();
    //
    // final Table rollup = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(List.of(AggSum("IntCol", "DoubleCol")), "USym", "Group"));
    // final TableMap rootMap = (TableMap) rollup.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    //
    // final SingletonLivenessManager rollupManager = new SingletonLivenessManager(rollup);
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(scopeCloseable::close);
    //
    // // dumpRollup(rollup, getHierarchicalColumnName(rollup), "USym", "Group");
    //
    // Assert.assertTrue(rollup.tryRetainReference());
    // Assert.assertTrue(rootMap.tryRetainReference());
    //
    // rollup.dropReference();
    // rootMap.dropReference();
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // addToTable(table, i(0, 1), col("USym", "AAPL", "TSLA"), col("Group", "Terran", "Vulcan"),
    // intCol("IntCol", 1, 2), doubleCol("DoubleCol", .1, .2));
    // table.notifyListeners(i(0, 1), i(), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // final SafeCloseable getScope = LivenessScopeStack.open();
    // final Table nextLevel = rootMap.get(SmartKey.EMPTY);
    // assertNotNull(nextLevel);
    //
    // Assert.assertTrue(rollup.tryRetainReference());
    // Assert.assertTrue(rootMap.tryRetainReference());
    // Assert.assertTrue(nextLevel.tryRetainReference());
    //
    // rollup.dropReference();
    // rootMap.dropReference();
    // nextLevel.dropReference();
    //
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(getScope::close);
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(rollupManager::release);
    //
    // // we should not be able to retainReference the rollup, because closing the scope should have decremented it to
    // // zero
    // Assert.assertFalse(rollup.tryRetainReference());
    //
    // // we should not be able to retainReference the tablemap, because closing the scope should have decremented it
    // // to zero
    // Assert.assertFalse(rootMap.tryRetainReference());
    //
    // Assert.assertFalse(nextLevel.tryRetainReference());
    // }
    //
    // public void testNullTypes() {
    // final Random random = new Random(0);
    //
    // final int size = 10;
    // final QueryTable table = getTable(size, random,
    // initColumnInfos(new String[] {"USym", "Group", "IntCol", "DoubleCol", "StringCol"},
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new TstUtils.SetGenerator<>("Terran", "Vulcan", "Klingon", "Romulan"),
    // new TstUtils.IntGenerator(0, 1_000_000),
    // new TstUtils.DoubleGenerator(-100, 100),
    // new TstUtils.SetGenerator<>("A", "B", "C", "D")));
    //
    // final Table rollup = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(
    // () -> table.rollup(List.of(AggSum("DoubleCol"), AggFirst("StringCol")), "USym", "Group", "IntCol"));
    // TestCase.assertEquals(String.class, rollup.getColumnSource("USym").getType());
    // TestCase.assertEquals(String.class, rollup.getColumnSource("Group").getType());
    // TestCase.assertEquals(int.class, rollup.getColumnSource("IntCol").getType());
    // }
    //
    // public void testSumIncrementalSimple() {
    // testIncrementalSimple(AggSum("IntCol"));
    // }
    //
    // public void testAvgIncrementalSimple() {
    // testIncrementalSimple(AggAvg("IntCol"));
    // }
    //
    // public void testStdIncrementalSimple() {
    // testIncrementalSimple(AggStd("IntCol"));
    // }
    //
    // public void testVarIncrementalSimple() {
    // testIncrementalSimple(AggVar("IntCol"));
    // }
    //
    // public void testRollupCountDistinctIncremental() {
    // testIncrementalSimple(AggCountDistinct("IntCol"));
    // testIncrementalSimple(AggCountDistinct(true, "IntCol"));
    // }
    //
    // public void testRollupDistinctIncremental() {
    // testIncrementalSimple(AggDistinct("IntCol"));
    // testIncrementalSimple(AggDistinct(true, "IntCol"));
    // }
    //
    // public void testRollupUniqueIncremental() {
    // testIncrementalSimple(AggUnique("IntCol"));
    // testIncrementalSimple(AggUnique(true, Sentinel(), "IntCol"));
    // // TODO (https://github.com/deephaven/deephaven-core/issues/991): Re-enable these sub-tests
    // // testIncrementalSimple(AggUnique(false, -1, -2, "IntCol"));
    // // testIncrementalSimple(AggUnique(true, -1, -2, "IntCol"));
    // }
    //
    // private void testIncrementalSimple(Aggregation aggregation) {
    // final QueryTable table =
    // TstUtils.testRefreshingTable(RowSetFactory.flat(6).toTracking(),
    // col("G1", "A", "A", "A", "B", "B", "B"),
    // col("G2", "C", "C", "D", "D", "E", "E"),
    // col("IntCol", 1, 2, 3, 4, 5, 6));
    //
    // final Table rollup = UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(List.of(aggregation), "G1", "G2"));
    //
    // dumpRollup(rollup, "G1", "G2");
    //
    // final Table fullBy =
    // UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> table.aggBy(List.of(aggregation)));
    //
    // final Table rollupClean = getDiffableTable(rollup).view("IntCol");
    //
    // final String diff = TableTools.diff(fullBy, rollupClean, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
    //
    // assertEquals("", diff);
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // removeRows(table, i(2));
    // table.notifyListeners(i(), i(2), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // System.out.println("Removed Row 2, Rollup:");
    // dumpRollup(rollup, "G1", "G2");
    // System.out.println("Expected:");
    // TableTools.showWithRowSet(fullBy);
    //
    // final String diff2 = TableTools.diff(fullBy, rollupClean, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
    // assertEquals("", diff2);
    //
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    // removeRows(table, i(0, 1));
    // table.notifyListeners(i(), i(0, 1), i());
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // dumpRollup(rollup, "G1", "G2");
    //
    // final String diff3 = TableTools.diff(fullBy, rollupClean, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
    // assertEquals("", diff3);
    // }
    //
    // public void testDuplicateAgg() {
    // final Table simpleTable = TableTools.emptyTable(10).update(
    // "MyString=new String(`a`+i)",
    // "MyInt=new Integer(i)",
    // "MyLong=new Long(i)",
    // "MyDouble=new Double(i+i/10)",
    // "MyFloat=new Float(i+i/10)",
    // "MyBoolean=new Boolean(i%2==0)",
    // "MyChar= new Character((char) ((i%26)+97))",
    // "MyShort=new Short(Integer.toString(i%32767))",
    // "MyByte= new java.lang.Byte(Integer.toString(i%127))");
    //
    //
    // try {
    // simpleTable.rollup(List.of(AggCount("MyString"), AggMin("MyString")), "MyDouble");
    // TestCase.fail("No exception generated with duplicate output column names.");
    // } catch (IllegalArgumentException iae) {
    // assertEquals("Duplicate output columns found: MyString used 2 times", iae.getMessage());
    // }
    // }
    //
    // public void testRollupIncremental() {
    // for (int seed = 0; seed < 1; ++seed) {
    // System.out.println("Seed = " + seed);
    // testRollupIncremental(seed);
    // }
    // }
    //
    // private void testRollupIncremental(int seed) {
    // final Random random = new Random(seed);
    // final TstUtils.ColumnInfo[] columnInfo;
    //
    // final int size = 100;
    // final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {
    // "USym", "Group", "IntCol", "DoubleCol", "StringCol", "StringNulls", "BoolCol", "DateTime",
    // "IntSet", "LongSet", "DoubleSet", "FloatSet", "CharSet", "ShortSet", "ByteSet"},
    //
    // new TstUtils.SetGenerator<>("AAPL", "TSLA", "VXX", "SPY"),
    // new TstUtils.SetGenerator<>("Terran", "Vulcan", "Klingon", "Romulan"),
    // new TstUtils.IntGenerator(0, 1_000_000),
    // new TstUtils.DoubleGenerator(-100, 100),
    // new TstUtils.SetGenerator<>("A", "B", "C", "D"),
    // new TstUtils.SetGenerator<>("A", "B", "C", "D", null),
    // new TstUtils.BooleanGenerator(.5, .1),
    // new TstUtils.UnsortedDateTimeGenerator(DateTimeUtils.convertDateTime("2020-03-17T09:30:00 NY"),
    // DateTimeUtils.convertDateTime("2020-03-17T16:00:00 NY")),
    // new TstUtils.SetGenerator<>(0, 1, 2, 3, 4, 5, NULL_INT),
    // new TstUtils.SetGenerator<>(0L, 1L, 2L, 3L, 4L, 5L, NULL_LONG),
    // new TstUtils.SetGenerator<>(0.0D, 1.1D, 2.2D, 3.3D, 4.4D, 5.5D, NULL_DOUBLE),
    // new TstUtils.SetGenerator<>(0.0f, 1.1f, 2.2f, 3.3f, 4.4f, 5.5f, NULL_FLOAT),
    // new TstUtils.SetGenerator<>('a', 'b', 'c', 'd', 'e', NULL_CHAR),
    // new TstUtils.SetGenerator<>((short) 0, (short) 1, (short) 2, (short) 3, (short) 4, (short) 5,
    // NULL_SHORT),
    // new TstUtils.SetGenerator<>((byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, NULL_BYTE)));
    //
    // final Collection<? extends Aggregation> rollupDefinition = List.of(
    // AggSum("IntCol", "DoubleCol"),
    // AggMin("MinInt=IntCol", "MinDT=DateTime"),
    // AggMax("MaxDouble=DoubleCol", "MaxDT=DateTime"),
    // AggAvg("IntAvg=IntCol", "DoubleAvg=DoubleCol"),
    // AggStd("IntStd=IntCol", "DoubleStd=DoubleCol"),
    // AggVar("IntVar=IntCol", "DoubleVar=DoubleCol"),
    // AggFirst("IntFirst=IntCol", "DoubleFirst=DoubleCol"),
    // AggLast("IntLast=IntCol", "DoubleLast=DoubleCol"),
    // AggCount("Count"),
    // AggCountDistinct("SCDistinct=StringCol", "CDBoolCol=BoolCol", "DTCDistinct=DateTime",
    // "CDIntCol=IntSet", "CDLongCol=LongSet", "CDDoubleCol=DoubleSet",
    // "CDFloatCol=FloatSet", "CDCharCol=CharSet", "CDShortCol=ShortSet", "CDByteCol=ByteSet"),
    // AggDistinct("SDistinct=StringCol", "DistinctBoolCol=BoolCol", "DTDistinct=DateTime",
    // "DIntCol=IntSet", "DLongCol=LongSet", "DDoubleCol=DoubleSet",
    // "DFloatCol=FloatSet", "DCharCol=CharSet", "DShortCol=ShortSet", "DByteCol=ByteSet"),
    // AggUnique("SUnique=StringCol", "UniqueBoolCol=BoolCol",
    // "UIntCol=IntSet", "ULongCol=LongSet", "UDoubleCol=DoubleSet",
    // "UFloatCol=FloatSet", "UCharCol=CharSet", "UShortCol=ShortSet", "UByteCol=ByteSet"),
    // AggCountDistinct(true, "SCDistinctN=StringNulls", "CDBoolColN=BoolCol",
    // "CDNIntCol=IntSet", "CDNLongCol=LongSet", "CDNDoubleCol=DoubleSet",
    // "CDNFloatCol=FloatSet", "CDNCharCol=CharSet", "CDNShortCol=ShortSet", "CDNByteCol=ByteSet"),
    // AggDistinct(true, "SDistinctN=StringNulls", "DistinctBoolColN=BoolCol",
    // "DNIntCol=IntSet", "DNLongCol=LongSet", "DNDoubleCol=DoubleSet",
    // "DNFloatCol=FloatSet", "DNCharCol=CharSet", "DNShortCol=ShortSet", "DNByteCol=ByteSet"),
    // AggUnique(true, Sentinel(), "SUniqueN=StringNulls", "UniqueBoolColN=BoolCol",
    // "UNIntCol=IntSet", "UNLongCol=LongSet", "UNDoubleCol=DoubleSet",
    // "UNFloatCol=FloatSet", "UNCharCol=CharSet", "UNShortCol=ShortSet", "UNByteCol=ByteSet"));
    // final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
    // new EvalNuggetInterface() {
    // @Override
    // public void validate(String msg) {}
    //
    // @Override
    // public void show() {
    // System.out.println("Table:");
    // TableTools.showWithRowSet(table);
    // }
    // },
    // new RollupEvalNugget(3, "USym", "Group") {
    // @Override
    // protected Table e() {
    // return UpdateGraphProcessor.DEFAULT.exclusiveLock()
    // .computeLocked(() -> table.rollup(rollupDefinition, "USym", "Group"));
    // }
    //
    // @Override
    // void showResult(String label, Table e) {
    // System.out.println(label);
    // dumpRollup(e, "USym", "Group");
    // }
    // },
    // new TableComparator(
    // getDiffableTable(table.rollup(rollupDefinition)).dropColumns(RollupInfo.ROLLUP_COLUMN),
    // table.aggBy(rollupDefinition))
    // };
    //
    // for (int step = 0; step < 100; step++) {
    // if (printTableUpdates) {
    // System.out.println("Step = " + step);
    // }
    // simulateShiftAwareStep(size, random, table, columnInfo, en);
    // }
    // }
    //
    // @ReflexiveUse(referrers = "QueryTableTreeTest")
    // static public SmartKey getPrefix(String id, String pos) {
    // if (pos == null || pos.isEmpty()) {
    // return null;
    // }
    //
    // final int liof = pos.lastIndexOf(".");
    // if (liof < 0) {
    // return null;
    // }
    //
    // return new io.deephaven.datastructures.util.SmartKey(id, pos.substring(0, liof));
    // }
    //
    // public void testOrderTreeTable() {
    // final Random random = new Random(0);
    //
    // int maxLevel = 1;
    //
    // int nextHid = 11;
    // long index = 2;
    //
    // final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(1).toTracking(),
    // longCol("Sentinel", 1), stringCol("hid", "a"), stringCol("hpos", "1"),
    // col("open", true), doubleCol("rand", 1.0));
    //
    // final List<String> openHid = new ArrayList<>();
    // openHid.add("a");
    // final Map<String, List<String>> hidToPos = new HashMap<>();
    // hidToPos.put("a", new ArrayList<>(Collections.singletonList("1")));
    //
    //
    // final Table orders = source
    // .lastBy("hpos", "hid")
    // .where("open")
    // .update("treeid=new io.deephaven.datastructures.util.SmartKey(hid, hpos)",
    // "parent=io.deephaven.engine.table.impl.QueryTableTreeTest.getPrefix(hid, hpos)");
    //
    // final Table ordersTree = orders.tree("treeid", "parent");
    // final Table ordersFiltered = TreeTableFilter.filterTree(ordersTree, "rand > 0.8");
    // final Table ordersFiltered2 = TreeTableFilter.filterTree(ordersTree, "rand > 0.1");
    //
    // for (int step = 0; step < 100; ++step) {
    // System.out.println("step = " + step);
    // UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
    //
    // final int numChanges = random.nextInt(100);
    // final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
    //
    // for (int count = 0; count < numChanges; ++count) {
    // assertEquals(openHid.size(), hidToPos.size());
    //
    // final double which = random.nextDouble();
    // if (which < 0.3 || openHid.isEmpty()) {
    // // add an order
    // final String hid;
    // if (openHid.isEmpty() || random.nextDouble() < 0.3) {
    // // new hid
    // hid = Integer.toHexString(nextHid++);
    // openHid.add(hid);
    // } else {
    // // existing hid
    // hid = openHid.get(random.nextInt(openHid.size()));
    // }
    //
    // final List<String> hpos = hidToPos.computeIfAbsent(hid, (key) -> new ArrayList<>());
    // final String newHpos;
    // if (hpos.isEmpty()) {
    // // newHpos = random.nextBoolean() ? "1" : "1.1";
    // newHpos = "1";
    // } else {
    // final String parentHpos = hpos.get(random.nextInt(hpos.size()));
    // final int next = hpos.stream().filter(s -> s.startsWith(parentHpos + "."))
    // .map(s -> s.substring(parentHpos.length() + 1).split("\\.")[0])
    // .mapToInt(Integer::parseInt).max().orElse(0) + 1;
    // newHpos = parentHpos + "." + next;
    // maxLevel = Math.max(maxLevel, 1 + StringUtils.countMatches(newHpos, "."));
    // }
    // hpos.add(newHpos);
    //
    // final long newIndex = ++index;
    // addToTable(source, i(newIndex), longCol("Sentinel", newIndex), col("hid", hid),
    // col("hpos", newHpos), col("open", true), col("rand", random.nextDouble()));
    // builder.appendKey(newIndex);
    // } else if (which < 0.1) {
    // // close an order
    // final String hid = openHid.get(random.nextInt(openHid.size()));
    // final List<String> validHpos = hidToPos.get(hid);
    // final String hpos = validHpos.get(random.nextInt(validHpos.size()));
    // validHpos.remove(hpos);
    // if (validHpos.isEmpty()) {
    // openHid.remove(hid);
    // hidToPos.remove(hid);
    // }
    //
    // final long newIndex = ++index;
    // addToTable(source, i(newIndex), longCol("Sentinel", newIndex), col("hid", hid), col("hpos", hpos),
    // col("open", false), col("rand", random.nextDouble()));
    // builder.appendKey(newIndex);
    // } else {
    // // modify an order
    // final String hid = openHid.get(random.nextInt(openHid.size()));
    // final List<String> validHpos = hidToPos.get(hid);
    // if (validHpos.isEmpty()) {
    // System.out.println(validHpos.size());
    // throw new IllegalStateException();
    // }
    // final String hpos = validHpos.get(random.nextInt(validHpos.size()));
    //
    // final long newIndex = ++index;
    // addToTable(source, i(newIndex), longCol("Sentinel", newIndex), col("hid", hid), col("hpos", hpos),
    // col("open", true), col("rand", random.nextDouble()));
    // builder.appendKey(newIndex);
    // }
    //
    // assertEquals(openHid.size(), hidToPos.size());
    // }
    //
    // final RowSet newRowSet = builder.build();
    // source.notifyListeners(newRowSet, i(), i());
    //
    // // TableTools.showWithRowSet(source.getSubTable(newRowSet));
    //
    // UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    //
    // final String hierarchicalColumnName = getHierarchicalColumnName(ordersFiltered);
    // doCompareWithChildrenForTrees("step = " + step, ordersFiltered,
    // TreeTableFilter.filterTree(ordersTree, "rand > 0.8"), 0, maxLevel, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // doCompareWithChildrenForTrees("step = " + step, ordersFiltered2,
    // TreeTableFilter.filterTree(ordersTree, "rand > 0.1"), 0, maxLevel, hierarchicalColumnName,
    // CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    // }
    // }
    //
    // public void testIds6262() {
    // final QueryTable table = TstUtils.testRefreshingTable(i(1).toTracking(),
    // col("Sym", "A"), col("BigI", new BigInteger[] {null}), col("BigD", new BigDecimal[] {null}));
    //
    // final Table rollup = table.rollup(List.of(AggVar("BigI", "BigD")), "Sym");
    //
    // dumpRollup(rollup, getHierarchicalColumnName(rollup), "Sym");
    //
    // assertNull(rollup.getColumn("BigI").get(0));
    // assertNull(rollup.getColumn("BigD").get(0));
    //
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(table, i(2, 3), col("Sym", "A", "A"), col("BigI", BigInteger.ZERO, BigInteger.ZERO),
    // col("BigD", BigDecimal.ZERO, BigDecimal.ZERO));
    // table.notifyListeners(i(2, 3), i(), i());
    // });
    //
    // dumpRollup(rollup, getHierarchicalColumnName(rollup), "Sym");
    //
    // assertEquals(BigDecimal.ZERO, rollup.getColumn("BigI").get(0));
    // assertEquals(BigDecimal.ZERO, rollup.getColumn("BigD").get(0));
    // }
    //
    // public void testIds7773() {
    // final QueryTable dataTable = TstUtils.testRefreshingTable(
    // stringCol("USym", "A"),
    // doubleCol("Value", NULL_DOUBLE),
    // byteCol("BValue", NULL_BYTE),
    // shortCol("SValue", NULL_SHORT),
    // intCol("IValue", NULL_INT),
    // longCol("LValue", NULL_LONG),
    // floatCol("FValue", NULL_FLOAT));
    //
    // final Table rolledUp =
    // dataTable.rollup(List.of(AggAvg("Value", "BValue", "SValue", "IValue", "LValue", "FValue")), "USym");
    // final TableMap rollupMap = (TableMap) rolledUp.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // assertNotNull(rollupMap);
    //
    // final Table aTable = rollupMap.get(SmartKey.EMPTY);
    // assertNotNull(aTable);
    //
    // // Start with Nulls and make sure we get NaN
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(1, 2),
    // stringCol("USym", "A", "A"),
    // doubleCol("Value", NULL_DOUBLE, NULL_DOUBLE),
    // byteCol("BValue", NULL_BYTE, NULL_BYTE),
    // shortCol("SValue", NULL_SHORT, NULL_SHORT),
    // intCol("IValue", NULL_INT, NULL_INT),
    // longCol("LValue", NULL_LONG, NULL_LONG),
    // floatCol("FValue", NULL_FLOAT, NULL_FLOAT));
    //
    // dataTable.notifyListeners(i(1, 2), i(), i());
    // });
    //
    // assertEquals(Double.NaN, rolledUp.getColumn("Value").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("FValue").getDouble(0));
    // assertEquals(NULL_DOUBLE, rolledUp.getColumn("BValue").getDouble(0));
    // assertEquals(NULL_DOUBLE, rolledUp.getColumn("SValue").getDouble(0));
    // assertEquals(NULL_DOUBLE, rolledUp.getColumn("IValue").getDouble(0));
    // assertEquals(NULL_DOUBLE, rolledUp.getColumn("LValue").getDouble(0));
    //
    // assertEquals(Double.NaN, aTable.getColumn("Value").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("FValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("BValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("SValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("IValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("LValue").getDouble(0));
    //
    // // Add a real value 0, which used to be broken because the default value was 0 and resulted in a no change
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(3),
    // stringCol("USym", "A"),
    // doubleCol("Value", 0.0d),
    // byteCol("BValue", (byte) 0),
    // shortCol("SValue", (short) 0),
    // intCol("IValue", 0),
    // longCol("LValue", 0),
    // floatCol("FValue", 0.0f));
    //
    // dataTable.notifyListeners(i(3), i(), i());
    // });
    //
    // assertEquals(0.0d, rolledUp.getColumn("Value").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("FValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("BValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("SValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("IValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("LValue").getDouble(0));
    //
    // assertEquals(0.0d, aTable.getColumn("Value").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("FValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("BValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("SValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("IValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("LValue").getDouble(0));
    //
    // // Delete the real value to make sure we go back to NaN
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // removeRows(dataTable, i(3));
    //
    // dataTable.notifyListeners(i(), i(3), i());
    // });
    //
    // assertEquals(Double.NaN, rolledUp.getColumn("Value").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("FValue").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("BValue").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("SValue").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("IValue").getDouble(0));
    // assertEquals(Double.NaN, rolledUp.getColumn("LValue").getDouble(0));
    //
    // assertEquals(Double.NaN, aTable.getColumn("Value").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("FValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("BValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("SValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("IValue").getDouble(0));
    // assertEquals(Double.NaN, aTable.getColumn("LValue").getDouble(0));
    //
    // // Add a couple of real 0's and make sure we get a 0
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(3, 4, 5),
    // stringCol("USym", "A", "A", "A"),
    // doubleCol("Value", 0.0d, 0.0d, 0.0d),
    // byteCol("BValue", (byte) 0, (byte) 0, (byte) 0),
    // shortCol("SValue", (short) 0, (short) 0, (short) 0),
    // intCol("IValue", 0, 0, 0),
    // longCol("LValue", 0, 0, 0),
    // floatCol("FValue", 0.0f, 0.0f, 0.0f));
    //
    // dataTable.notifyListeners(i(3, 4, 5), i(), i());
    // });
    //
    // assertEquals(0.0d, rolledUp.getColumn("Value").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("FValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("BValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("SValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("IValue").getDouble(0));
    // assertEquals(0.0d, rolledUp.getColumn("LValue").getDouble(0));
    //
    // assertEquals(0.0d, aTable.getColumn("Value").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("FValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("BValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("SValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("IValue").getDouble(0));
    // assertEquals(0.0d, aTable.getColumn("LValue").getDouble(0));
    //
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(6),
    // stringCol("USym", "A"),
    // doubleCol("Value", 1.0d),
    // byteCol("BValue", (byte) 1),
    // shortCol("SValue", (short) 1),
    // intCol("IValue", 1),
    // longCol("LValue", 1),
    // floatCol("FValue", 1.0f));
    //
    // dataTable.notifyListeners(i(6), i(), i());
    // });
    //
    // assertEquals(0.25d, rolledUp.getColumn("Value").getDouble(0));
    // assertEquals(0.25d, rolledUp.getColumn("FValue").getDouble(0));
    // assertEquals(0.25d, rolledUp.getColumn("BValue").getDouble(0));
    // assertEquals(0.25d, rolledUp.getColumn("SValue").getDouble(0));
    // assertEquals(0.25d, rolledUp.getColumn("IValue").getDouble(0));
    // assertEquals(0.25d, rolledUp.getColumn("LValue").getDouble(0));
    //
    // assertEquals(0.25d, aTable.getColumn("Value").getDouble(0));
    // assertEquals(0.25d, aTable.getColumn("FValue").getDouble(0));
    // assertEquals(0.25d, aTable.getColumn("BValue").getDouble(0));
    // assertEquals(0.25d, aTable.getColumn("SValue").getDouble(0));
    // assertEquals(0.25d, aTable.getColumn("IValue").getDouble(0));
    // assertEquals(0.25d, aTable.getColumn("LValue").getDouble(0));
    // }
    //
    // private static Table getDiffableTable(Table table) {
    // if (table instanceof HierarchicalTable) {
    // return ((HierarchicalTable) table).getRootTable();
    // }
    //
    // return table;
    // }
    //
    // public void testRollupStdSlotOutOfOrder() {
    // final QueryTable source =
    // TstUtils.testRefreshingTable(col("G1", "a", "b", "c", "d", "e", "f", "g", "A", "A", "B", "B", "A"),
    // col("G2", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a"),
    // intCol("Val", 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5));
    // final Table rollup = source.rollup(List.of(AggVar("Val")), "G1", "G2");
    // checkVar(source, rollup);
    //
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.addToTable(source, i(9, 11), col("G1", "B", "A"), col("G2", "a", "a"), intCol("Val", 6, 7));
    // final TableUpdate update =
    // new TableUpdateImpl(i(), i(), i(9, 11), RowSetShiftData.EMPTY, source.newModifiedColumnSet("Val"));
    // source.notifyListeners(update);
    // });
    // checkVar(source, rollup);
    // }
    //
    // private void checkVar(QueryTable source, Table rollup) {
    // System.out.println("Source:");
    // TableTools.showWithRowSet(source, 20);
    //
    // System.out.println("Total variance");
    // final Table totalExpect = source.view("Val").varBy();
    // TableTools.show(totalExpect);
    // System.out.println("A variance");
    // final Table aExpect = source.view("G1", "Val").varBy("G1").where("G1 in `A`");
    // TableTools.show(aExpect);
    // System.out.println("B variance");
    // final Table bExpect = source.view("G1", "Val").varBy("G1").where("G1 in `B`");
    // TableTools.show(bExpect);
    //
    // dumpRollup(rollup, "G1", "G2");
    //
    // assertTableEquals(totalExpect, getDiffableTable(rollup).view("Val"));
    //
    // final Table rootTable =
    // ((TableMap) rollup.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)).get(new SmartKey());
    //
    // final Table a = ((TableMap) rootTable.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)).get("A");
    // final Table b = ((TableMap) rootTable.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE)).get("B");
    //
    // assertTableEquals(aExpect, a.view("G1", "Val"));
    // assertTableEquals(bExpect, b.view("G1", "Val"));
    // }
}
