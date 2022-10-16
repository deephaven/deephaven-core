/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.engine.testutil.QueryTableTestBase;

import java.util.*;

import static io.deephaven.engine.util.TableTools.emptyTable;
import static org.junit.Assert.assertArrayEquals;

public class TreeSnapshotQueryTest extends QueryTableTestBase {

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

    // private TreeTableClientTableManager.Client mockClient;
    //
    // /**
    // * Since we're not using a remote client we need to provide TSQ with a way to map Table Ids to tables so we'll
    // just
    // * assign them and pass this thing in as the ExportedObjectClient
    // */
    // private final TIntObjectMap<Table> tableIdMap = new TIntObjectHashMap<>();
    // private final TObjectIntHashMap<Table> reverseTableIdMap = new TObjectIntHashMap<>();
    // private int nextId = 0;
    // private final ExecutorService pool = Executors.newFixedThreadPool(1);
    //
    // // region Tree state and management
    // private void addIdForTable(Table t) {
    // final int theId = nextId++;
    // tableIdMap.put(theId, t);
    // reverseTableIdMap.put(t, theId);
    // }
    //
    // private Table getTableById(int id) {
    // return tableIdMap.get(id);
    // }
    //
    // private int getIdForTable(Table t) {
    // return reverseTableIdMap.get(t);
    // }
    //
    // @Override
    // protected void setUp() throws Exception {
    // super.setUp();
    // tableIdMap.clear();
    // reverseTableIdMap.clear();
    // nextId = 0;
    //
    // mockClient = mock(TreeTableClientTableManager.Client.class);
    // checking(new Expectations() {
    // {
    // allowing(mockClient).addDisconnectHandler(with(anything()));
    // }
    // });
    // }
    //
    // private Map<Object, TableDetails> makeDetailsMap(Collection<TableDetails> details) {
    // return details.stream()
    // .map(TableDetails::copy)
    // .collect(Collectors.toMap(TableDetails::getKey, java.util.function.Function.identity(),
    // (u, v) -> u));
    // }
    //
    // private class TTState {
    // /** The tree we're using to test with */
    // final HierarchicalTable theTree;
    //
    // final boolean rollup;
    //
    // TreeSnapshotResult result;
    // Table snapshot;
    //
    // Table forComparisons;
    //
    // final String hierarchicalColumn;
    //
    // /** We need to do simple expansion tracking. */
    // Map<Object, TableDetails> expansionMap = new HashMap<>();
    //
    // EnumSet<TreeSnapshotQuery.Operation> ops = EnumSet.noneOf(TreeSnapshotQuery.Operation.class);
    // Map<String, ColumnSource> constituentSources = new HashMap<>();
    //
    // TTState(Table theTree) {
    // this.theTree = (HierarchicalTable) theTree;
    // expansionMap.put(ROOT_TABLE_KEY, new TableDetails(ROOT_TABLE_KEY, Collections.emptySet()));
    //
    // final TableMap sourceMap = (TableMap) theTree.getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // if (sourceMap != null) {
    // for (final Table t : sourceMap.values()) {
    // addIdForTable(t);
    // }
    // }
    //
    // final HierarchicalTableInfo info =
    // (HierarchicalTableInfo) theTree.getAttribute(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
    // rollup = info instanceof RollupInfo;
    // this.hierarchicalColumn = info.getHierarchicalColumnName();
    // }
    //
    // void applyTsq(BitSet columns, long start, long end, WhereFilter[] filters, List<SortDirective> sorts) {
    // if (filters.length > 0) {
    // ops.add(TreeSnapshotQuery.Operation.FilterChanged);
    // }
    //
    // if (!sorts.isEmpty()) {
    // ops.add(TreeSnapshotQuery.Operation.SortChanged);
    // }
    //
    // final TreeSnapshotQuery<?> tsq = new TreeSnapshotQuery(getIdForTable(theTree),
    // makeDetailsMap(expansionMap.values()), start, end, columns, filters, sorts, mockClient, ops);
    //
    // result = theTree.apply(tsq);
    // snapshot = result.asTable(theTree);
    //
    // constituentSources.clear();
    // if (result.getConstituentData() != null && result.getConstituentData().length > 0) {
    // Arrays.stream(result.getConstituentData())
    // .forEach(p -> constituentSources.put(p.getFirst(),
    // makeConstituentColumnSource(p.getFirst(), p.getSecond())));
    // }
    //
    // expansionMap =
    // result.getTableData().stream().collect(Collectors.toMap(TableDetails::getKey, Function.identity()));
    // ops.clear();
    // }
    //
    // private ColumnSource<?> makeConstituentColumnSource(String name, Object array) {
    // final ColumnDefinition<?> colDef = theTree.getSourceTable().getDefinition().getColumn(name);
    // // noinspection unchecked
    // return InMemoryColumnSource.getImmutableMemoryColumnSource(array, colDef.getDataType(),
    // colDef.getComponentType());
    // }
    //
    // void setCompareToTable(Table compareTo) {
    // this.forComparisons = compareTo;
    // }
    //
    // void addExpanded(Object parentKey, Object childKey) {
    // expansionMap.get(parentKey).getChildren().add(childKey);
    // expansionMap.computeIfAbsent(childKey, k -> new TableDetails(k, Collections.emptySet()));
    // ops.add(TreeSnapshotQuery.Operation.Expand);
    // }
    //
    // void updateTableIds() {
    // expansionMap = expansionMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
    // td -> new TableDetails(td.getValue().getKey(), td.getValue().getChildren())));
    // }
    //
    // TableMap getTableMap(Table table) {
    // return (TableMap) (rollup ? table : forComparisons != null ? forComparisons : theTree)
    // .getAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE);
    // }
    // }
    //
    // public static class StaticHolder {
    // @ReflexiveUse(referrers = "QueryLibrary")
    // public static List<String> removeEmpty(String... components) {
    // return Arrays.stream(components).filter(s -> s != null && !s.isEmpty()).collect(Collectors.toList());
    // }
    // }
    //
    // private static Table getRawNyMunis() throws CsvReaderException {
    // QueryLibrary.importStatic(StaticHolder.class);
    //
    // final BaseTable base =
    // (BaseTable) CsvTools.readCsv(TreeSnapshotQueryTest.class.getResourceAsStream("nymunis.csv"));
    // base.setRefreshing(true);
    // return base.update("Path=(List<String>)removeEmpty(County_Name, City_Name, Town_Name, Village_Name)")
    // .update("Direct = Path.size() == 1 ? null : new ArrayList(Path.subList(0, Path.size() - 1))")
    // .update("boolCol=(boolean)(i%2==0)",
    // "byteCol = (byte)(i & 0xFF)",
    // "charCol=(char)(isNull(Town_Name) ? null : Town_Name.charAt(0))",
    // "doubleCol=(double)i/33.2",
    // "floatCol=(float)(i/22.1)",
    // "longCol=(long)i",
    // "shortCol=(short)i",
    // "timestamp='2018-12-10 NY'")
    // .lastBy("Path");
    // }
    //
    // private static Table makeNyMunisTreeTable() throws CsvReaderException {
    // return makeNyMunisTreeTableFrom(getRawNyMunis());
    // }
    //
    // private static Table makeNyMunisTreeTableFrom(Table t) {
    // return t.tree("Path", "Direct");
    // }
    //
    // private static List<String> munisKey(String... path) {
    // return new ArrayList<>(Arrays.asList(path));
    // }
    //
    // // endregion
    //
    // // region Actual Tests
    //
    // public void testTsq() throws CsvReaderException {
    // final Table t = makeNyMunisTreeTable();
    // final TTState state = new TTState(t);
    // final BitSet allColumns = new BitSet(t.numColumns());
    // allColumns.set(0, t.numColumns());
    //
    // final long halfTableSize = t.size() / 2;
    //
    // // Make sure 0-n works
    // testViewport(state, 0, halfTableSize, allColumns, false);
    //
    // // Try something in the middle
    // final long startQuarter = halfTableSize / 2;
    // testViewport(state, startQuarter, startQuarter + halfTableSize, allColumns, false);
    //
    // // Try something at the end
    // testViewport(state, halfTableSize, t.size() - 1, allColumns, false);
    //
    // // Expand a row so we can check structured
    // state.addExpanded(ROOT_TABLE_KEY, munisKey("Chenango"));
    //
    // // First check when the viewport begins with the root, but ends within a child.
    // testViewport(state, 0, halfTableSize, allColumns, false);
    //
    // // Begins with root, ends in root, has a child in the middle
    // testViewport(state, 0, halfTableSize + 10, allColumns, false);
    //
    // // Begin at the node just before the child end in the child
    // testViewport(state, 7, 27, allColumns, false);
    //
    // // Begin at the node just before the child end at the last row of child
    // testViewport(state, 7, 28, allColumns, false);
    //
    // // Begin at the node just before the child end one row after the last row of child
    // testViewport(state, 7, 29, allColumns, false);
    //
    // // Same as above, but end in parent
    // testViewport(state, 7, 31, allColumns, false);
    //
    // // Start and end within the child table
    // testViewport(state, 8, 25, allColumns, false);
    //
    // // Start in the first row of the child, end on the last row
    // testViewport(state, 8, 28, allColumns, false);
    //
    // // Start in the first row of the child, end in the next parent row
    // testViewport(state, 8, 29, allColumns, false);
    //
    // // Start in the first row of the child, end in the parent
    // testViewport(state, 8, 31, allColumns, false);
    //
    // // Start in the middle of the child, end in the parent
    // testViewport(state, 11, 31, allColumns, false);
    //
    // // Expand another row
    // state.addExpanded(ROOT_TABLE_KEY, munisKey("Fulton"));
    //
    // // Root, to inside second child
    // testViewport(state, 0, 47, allColumns, false);
    //
    // // Root, to end of second child
    // testViewport(state, 0, 48, allColumns, false);
    //
    // // Root, to first parent after
    // testViewport(state, 0, 49, allColumns, false);
    //
    // // Root, to elsewhere in parent
    // testViewport(state, 0, 51, allColumns, false);
    //
    // // just before first child, to inside second
    // testViewport(state, 7, 47, allColumns, false);
    //
    // // just before first child, to end of second
    // testViewport(state, 7, 48, allColumns, false);
    //
    // // just before first child, to first parent after
    // testViewport(state, 7, 49, allColumns, false);
    //
    // // just before first child, to elsewhere in parent
    // testViewport(state, 7, 51, allColumns, true);
    //
    // // inside first child to before second
    // testViewport(state, 8, 37, allColumns, false);
    //
    // // inside first child to first inside second
    // testViewport(state, 8, 38, allColumns, false);
    //
    // // inside first child to inside second
    // testViewport(state, 8, 41, allColumns, false);
    //
    // // inside first child to first last of second
    // testViewport(state, 8, 48, allColumns, false);
    //
    // // inside first child to first after second
    // testViewport(state, 8, 49, allColumns, false);
    //
    // // inside first child to inside root
    // testViewport(state, 8, 51, allColumns, false);
    //
    // // After first, to before second
    // testViewport(state, 29, 37, allColumns, false);
    //
    // // After first, to first of second
    // testViewport(state, 29, 38, allColumns, false);
    //
    // // After first, to into second
    // testViewport(state, 29, 41, allColumns, false);
    //
    // // After first, to end of second
    // testViewport(state, 29, 48, allColumns, false);
    //
    // // After first, to first after second
    // testViewport(state, 29, 49, allColumns, false);
    //
    // // After first, to into root
    // testViewport(state, 29, 51, allColumns, false);
    //
    // // Just before second to first second
    // testViewport(state, 37, 38, allColumns, false);
    //
    // // Just before second into second
    // testViewport(state, 37, 48, allColumns, false);
    //
    // // Just before second last of second
    // testViewport(state, 37, 49, allColumns, false);
    //
    // // Just before second into root
    // testViewport(state, 37, 51, allColumns, false);
    //
    // // first of second into second
    // testViewport(state, 38, 41, allColumns, false);
    //
    // // first of second to last of second
    // testViewport(state, 38, 48, allColumns, false);
    //
    // // first of second first after second
    // testViewport(state, 38, 49, allColumns, false);
    //
    // // first of second into root
    // testViewport(state, 38, 51, allColumns, false);
    //
    // // Last of second first after
    // testViewport(state, 48, 49, allColumns, false);
    //
    // // last of second root
    // testViewport(state, 48, 51, allColumns, false);
    //
    // // Expand a nested child (chenago is 8-30)
    // state.addExpanded(munisKey("Chenango"), munisKey("Chenango", "Sherburne"));
    //
    // testViewport(state, 0, 32, allColumns, false);
    //
    // // At this point we're done expanding stuff, lets set the table IDs to test those paths in TSQ
    // state.updateTableIds();
    //
    // // Root, to first of subchild
    // testViewport(state, 0, 27, allColumns, false);
    // // Root and whole subchild
    // testViewport(state, 0, 28, allColumns, false);
    //
    // // Root first after subchild in child
    // testViewport(state, 0, 29, allColumns, false);
    //
    // // child to into subchild
    // testViewport(state, 8, 27, allColumns, false);
    // testViewport(state, 8, 28, allColumns, false);
    // testViewport(state, 8, 29, allColumns, false);
    // testViewport(state, 8, 31, allColumns, false);
    //
    // // Child into second child
    // testViewport(state, 8, 42, allColumns, false);
    //
    // // Child into somewhere in root after second child
    // testViewport(state, 8, 71, allColumns, false);
    //
    // // subchild to child
    // testViewport(state, 27, 29, allColumns, false);
    //
    // // subchild to root before second child
    // testViewport(state, 27, 32, allColumns, false);
    //
    // // subchild to second child
    // testViewport(state, 27, 42, allColumns, false);
    //
    // // subchild to somewhere in root
    // testViewport(state, 27, 71, allColumns, false);
    //
    // // after subchild to elsewhere
    // testViewport(state, 29, 71, allColumns, false);
    //
    // final List<String> fultonKey = munisKey("Fulton");
    // final List<String> mayfieldKey = munisKey("Fulton", "Mayfield");
    // state.addExpanded(fultonKey, mayfieldKey);
    //
    // // root to root, encompassing all
    // testViewport(state, 6, 53, allColumns, false);
    //
    // // first\ child to second subchild
    // testViewport(state, 9, 47, allColumns, false);
    //
    // testViewport(state, 9, 49, allColumns, false);
    //
    // testViewport(state, 9, 53, allColumns, false);
    //
    // testViewport(state, 80, state.result.getTreeSize() - 1, allColumns, false);
    //
    // // Lets do some bogus expansions
    // final List<String> foodvilleKey = munisKey("Foodville");
    // state.addExpanded(ROOT_TABLE_KEY, foodvilleKey);
    // testViewport(state, 9, 53, allColumns, false);
    // assertFalse(state.expansionMap.get(ROOT_TABLE_KEY).getChildren().contains(foodvilleKey));
    // assertFalse(state.expansionMap.containsKey(foodvilleKey));
    //
    // final List<String> bacontownKey = munisKey("Foodville", "Bacontown");
    // state.addExpanded(ROOT_TABLE_KEY, foodvilleKey);
    // state.addExpanded(foodvilleKey, bacontownKey);
    //
    // // Add a null expanded key to eliminate -- Tests for regression of IDS-3659
    // state.addExpanded(ROOT_TABLE_KEY, null);
    //
    // testViewport(state, 9, 53, allColumns, false);
    // assertFalse(state.expansionMap.get(ROOT_TABLE_KEY).getChildren().contains(foodvilleKey));
    // assertFalse(state.expansionMap.containsKey(foodvilleKey));
    // assertFalse(state.expansionMap.containsKey(bacontownKey));
    //
    // // We'll delete a child key so that a child table becomes empty. TSQ should eliminate it from the set.
    // final QueryTable source = (QueryTable) t.getAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE);
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // TstUtils.removeRows(source, i(467));
    // source.notifyListeners(i(), i(467), i());
    // });
    //
    // // Did it go away?
    // testViewport(state, 9, 53, allColumns, false);
    // assertFalse(state.expansionMap.get(fultonKey).getChildren().contains(mayfieldKey));
    // assertFalse(state.expansionMap.containsKey(mayfieldKey));
    // }
    //
    // public void testSortandFilter() throws CsvReaderException {
    // final Table t = makeNyMunisTreeTable();
    // final TTState state = new TTState(t);
    // final BitSet allColumns = new BitSet(t.numColumns());
    // allColumns.set(0, t.numColumns());
    //
    // state.addExpanded(ROOT_TABLE_KEY, munisKey("Chenango"));
    // state.addExpanded(ROOT_TABLE_KEY, munisKey("Fulton"));
    // state.addExpanded(munisKey("Chenango"), munisKey("Chenango", "Sherburne"));
    // state.addExpanded(munisKey("Fulton"), munisKey("Fulton", "Mayfield"));
    //
    // final Table sortThenTree = getRawNyMunis()
    // .sort("Town_Name")
    // .sortDescending("Type")
    // .tree("Path", "Direct");
    //
    // state.setCompareToTable(sortThenTree);
    // List<SortDirective> directives = Arrays.asList(
    // new SortDirective("Type", SortDirective.DESCENDING, false),
    // new SortDirective("Town_Name", SortDirective.ASCENDING, false));
    //
    // testViewportAgainst(sortThenTree, state, 7, 51, allColumns, directives,
    // WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY, true);
    //
    // final Table filterThenTree = TreeTableFilter.filterTree(makeNyMunisTreeTable(),
    // "!isNull(Website) && Website.contains(`ny`)");
    //
    // state.setCompareToTable(filterThenTree);
    // testViewportAgainst(filterThenTree, state, 0, 30, allColumns, Collections.emptyList(),
    // WhereFilterFactory.getExpressions("!isNull(Website) && Website.contains(`ny`)"), true);
    //
    // // Deliberately sorting first than filtering, it will produce the same result and is a different order than TSQ
    // // does,
    // // so validates things nicely.
    // final Table filterAndSortThenTree = TreeTableFilter.filterTree(getRawNyMunis()
    // .sortDescending("County_Name", "Town_Name")
    // .tree("Path", "Direct"),
    // "!isNull(Website) && Website.contains(`ny`)");
    //
    // state.setCompareToTable(filterAndSortThenTree);
    // directives = Arrays.asList(
    // new SortDirective("County_Name", SortDirective.DESCENDING, false),
    // new SortDirective("Town_Name", SortDirective.DESCENDING, false));
    //
    // testViewportAgainst(filterAndSortThenTree, state, 37, 63, allColumns, directives,
    // WhereFilterFactory.getExpressions("!isNull(Website) && Website.contains(`ny`)"), true);
    // state.setCompareToTable(null);
    // }
    //
    // public void testRollupTsq() {
    // final Table t = TableTools.emptyTable(100)
    // .update("I=i", "Test=i%12", "Dtest=44.6*i/2", "Bagel= i%2==0");
    //
    // final Table rollup = t.rollup(List.of(AggLast("Dtest"), AggSum("I")), "Bagel", "Test");
    //
    // final TTState state = new TTState(rollup);
    // final BitSet allColumns = new BitSet(rollup.numColumns());
    // allColumns.set(0, rollup.numColumns());
    //
    // final SmartKey nullSmartKey = new SmartKey();
    // state.addExpanded(ROOT_TABLE_KEY, nullSmartKey);
    // state.addExpanded(nullSmartKey, false);
    // state.addExpanded(nullSmartKey, true);
    // testViewport(state, 0, 14, allColumns, true);
    //
    // final Table filtered = t.where("Bagel").rollup(List.of(AggLast("Dtest"), AggSum("I")), "Bagel", "Test");
    // testViewportAgainst(filtered, state, 0, 7, allColumns, Collections.emptyList(),
    // WhereFilterFactory.getExpressions("Bagel"), true);
    //
    // final List<SortDirective> directives = Arrays.asList(
    // new SortDirective("Test", SortDirective.DESCENDING, false),
    // new SortDirective("Bagel", SortDirective.ASCENDING, false));
    //
    // state.addExpanded(nullSmartKey, false);
    // testViewportAgainst(rollup, state, 0, 14, allColumns, directives, WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY,
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    //
    // final Table filtered2 = t.where("Bagel").rollup(List.of(AggLast("Dtest"), AggSum("I")), "Bagel", "Test");
    //
    // testViewportAgainst(filtered2, state, 0, 7, allColumns, directives, WhereFilterFactory.getExpressions("Bagel"),
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    // }
    //
    // public void testRollupConstituentsTsq() {
    // final Table t = TableTools.emptyTable(100)
    // .update("I=i", "Test=i%12", "Dtest=44.6*i/2", "Bagel= i%2==0");
    //
    // final Table rollup = t.rollup(List.of(AggLast("Dtest"), AggSum("I")), true, "Bagel", "Test");
    //
    // final TTState state = new TTState(rollup);
    // final BitSet allColumns = new BitSet(rollup.numColumns());
    // allColumns.set(0, rollup.getColumns().length);
    //
    // final SmartKey nullSmartKey = new SmartKey();
    // state.addExpanded(ROOT_TABLE_KEY, nullSmartKey);
    // state.addExpanded(nullSmartKey, false);
    // state.addExpanded(nullSmartKey, true);
    // testViewport(state, 0, 14, allColumns, true);
    //
    // final Table filtered = t.where("Bagel").rollup(List.of(AggLast("Dtest"), AggSum("I")), true, "Bagel", "Test");
    // testViewportAgainst(filtered, state, 0, 7, allColumns, Collections.emptyList(),
    // WhereFilterFactory.getExpressions("Bagel"), true);
    //
    // final List<SortDirective> directives = Arrays.asList(
    // new SortDirective("Test", SortDirective.DESCENDING, false),
    // new SortDirective("Bagel", SortDirective.ASCENDING, false));
    //
    // state.addExpanded(nullSmartKey, false);
    // testViewportAgainst(rollup, state, 0, 14, allColumns, directives, WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY,
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    //
    // testViewportAgainst(filtered, state, 0, 7, allColumns, directives, WhereFilterFactory.getExpressions("Bagel"),
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    //
    // state.addExpanded(true, new SmartKey(true, 0));
    // testViewportAgainst(rollup, state, 0, 17, allColumns, directives, WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY,
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    //
    // state.addExpanded(true, new SmartKey(true, 6));
    // testViewportAgainst(filtered, state, 0, 24, allColumns, directives, WhereFilterFactory.getExpressions("Bagel"),
    // ct -> {
    // final Table sortTarget =
    // ct instanceof HierarchicalTable ? ((HierarchicalTable) ct).getRootTable() : ct;
    // return sortTarget.sort("Bagel").sortDescending("Test");
    // }, true);
    // }
    //
    // public void testGetPrev() throws Exception {
    // final QueryTable raw = (QueryTable) getRawNyMunis();
    // final Table t = makeNyMunisTreeTableFrom(raw);
    // final TTState state = new TTState(t);
    // final BitSet allColumns = new BitSet(t.getColumns().length);
    // allColumns.set(0, t.getColumns().length);
    //
    // final long halfTableSize = t.size() / 2;
    // final Object fultonKey = munisKey("Fulton");
    //
    // // Expand another row
    // state.addExpanded(ROOT_TABLE_KEY, fultonKey);
    //
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // // Fetch previous data
    // pool.submit(() -> testViewport(state, 0, halfTableSize, allColumns, true)).get();
    // assertNotNull(state.expansionMap.get(fultonKey));
    //
    // // Remove the "Fulton" row
    // TstUtils.removeRows(raw, i(475));
    // raw.notifyListeners(i(), i(475), i());
    //
    // // Fetch current data while it is concurrently updating
    // final Future<?> currentFetch = pool.submit(() -> testViewport(state, 0, halfTableSize, allColumns, true));
    //
    // // Flush the changes
    // final AtomicBoolean done = new AtomicBoolean(false);
    // final Runnable awaitFlushJob =
    // UpdateGraphProcessor.DEFAULT.flushAllNormalNotificationsForUnitTests(done::get, 60_000L);
    //
    // // Fetch current data
    // currentFetch.get();
    //
    // // Tell the flush to stop busy-waiting and wait for it
    // done.set(true);
    // UpdateGraphProcessor.DEFAULT.wakeRefreshThreadForUnitTests();
    // awaitFlushJob.run();
    //
    // // Check that we don't have "Fulton" anymore
    // assertNull(state.expansionMap.get(fultonKey));
    // });
    // }
    //
    // /**
    // * This test verifies that reparenting an arbitrary expanded node does not break TSQ
    // */
    // public void testArbitraryReparent_IDS6404() {
    // // Build a simple tree based upon a directory hierarchy
    // final QueryTable dataTable = TstUtils.testRefreshingTable(
    // c("ID", "Root", "0", "1", "2", "0-0", "1-0", "2-0", "0-0-0", "1-0-0", "2-0-0"),
    // c("Parent", null, "Root", "Root", "Root", "0", "1", "2", "0-0", "1-0", "2-0"),
    // c("Name", "Root", "0", "1", "2", "0-0", "1-0", "2-0", "0-0-0", "1-0-0", "2-0-0"));
    //
    // final Table lastBy = dataTable.lastBy("ID");
    // final Table tree = lastBy.tree("ID", "Parent");
    //
    // final BitSet allCols = new BitSet(3);
    // allCols.set(0, 3);
    // final TTState treeState = new TTState(tree);
    // treeState.addExpanded(ROOT_TABLE_KEY, "Root");
    // testViewport(treeState, 0, 3, allCols, true);
    //
    // // Expand errything
    // treeState.addExpanded("Root", "0");
    // treeState.addExpanded("0", "0-0");
    // treeState.addExpanded("Root", "1");
    // treeState.addExpanded("1", "1-0");
    // treeState.addExpanded("Root", "2");
    // treeState.addExpanded("2", "2-0");
    //
    // testViewport(treeState, 0, 9, allCols, true);
    //
    // // Reparent 1-0 to root
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(10), c("ID", "1-0"), c("Parent", "Root"), c("Name", "1-0"));
    // dataTable.notifyListeners(i(10), i(), i());
    // });
    //
    // testViewport(treeState, 0, 8, allCols, true);
    //
    // treeState.addExpanded("Root", "1-0");
    // testViewport(treeState, 0, 9, allCols, true);
    //
    // // Reparent 1-0 to 1
    // UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
    // addToTable(dataTable, i(11), c("ID", "1-0"), c("Parent", "1"), c("Name", "1-0"));
    // dataTable.notifyListeners(i(11), i(), i());
    // });
    //
    // // This is where the break occurs.
    // testViewport(treeState, 0, 7, allCols, true);
    //
    // treeState.addExpanded("Root", "1");
    // treeState.addExpanded("1", "1-0");
    // testViewport(treeState, 0, 9, allCols, true);
    // }
    // // region
    //
    // // region Hierarchical verification
    //
    // private static void testViewport(TTState state, long start, long end, BitSet columns, boolean showAfter) {
    // testViewportAgainst(state.theTree, state, start, end, columns, Collections.emptyList(),
    // WhereFilter.ZERO_LENGTH_SELECT_FILTER_ARRAY, showAfter);
    // }
    //
    // private static void testViewportAgainst(Table against, TTState state, long start, long end, BitSet columns,
    // List<SortDirective> sorts, WhereFilter[] filters, boolean showAfter) {
    // testViewportAgainst(against, state, start, end, columns, sorts, filters, Function.identity(), showAfter);
    // }
    //
    // private static void testViewportAgainst(Table against, TTState state, long start, long end, BitSet columns,
    // List<SortDirective> sorts, WhereFilter[] filters, Function<Table, Table> childMutator, boolean showAfter) {
    // state.applyTsq(columns, start, end, filters, sorts);
    //
    // if (showAfter) {
    // System.out.println();
    // TableTools.show(state.snapshot, state.snapshot.size());
    // }
    //
    // checkSnapshotAgainst(state, against, start, end, childMutator);
    // }
    //
    // private static class TraversalState {
    // long traversed;
    // long consumed;
    // }
    //
    // private static void checkSnapshotAgainst(TTState state, Table against, long start, long end,
    // Function<Table, Table> childMutator) {
    // assertEquals(state.result.getSnapshotStart(), start);
    // assertEquals(state.result.getSnapshotEnd(), end);
    //
    // final TraversalState ts = new TraversalState();
    // checkSnapshotAgainst(state, childMutator.apply(against), ROOT_TABLE_KEY, start, end, ts, childMutator);
    // assertEquals(ts.traversed, state.result.getTreeSize());
    // assertEquals(ts.consumed, end - start + 1);
    // }
    //
    // private static void checkSnapshotAgainst(TTState state, Table currentTable, Object tableKey, long start, long
    // end,
    // TraversalState ts, Function<Table, Table> childMutator) {
    // final boolean usePrev = LogicalClock.DEFAULT.currentState() == LogicalClock.State.Updating
    // && ((NotificationStepSource) state.theTree.getSourceTable())
    // .getLastNotificationStep() != LogicalClock.DEFAULT.currentStep();
    //
    // for (int rowNo = 0; rowNo < currentTable.size(); rowNo++) {
    // final long tableRow =
    // usePrev ? currentTable.getRowSet().getPrev(rowNo) : currentTable.getRowSet().get(rowNo);
    // final ColumnSource childSource = currentTable.getColumnSource(state.hierarchicalColumn);
    // final Object childKey = usePrev ? childSource.getPrev(tableRow) : childSource.get(tableRow);
    // final TableMap childMap = state.getTableMap(currentTable);
    //
    // if (ts.traversed >= start && ts.traversed <= end) {
    // final Object[] record =
    // state.snapshot.getRecord(ts.consumed, currentTable.getDefinition().getColumnNamesArray());
    // final Pair<String, Object>[] constituentData = state.result.getConstituentData();
    // if (constituentData != null && currentTable.hasAttribute(Table.ROLLUP_LEAF_ATTRIBUTE)) {
    // for (int i = 0; i < constituentData.length; i++) {
    // final int colIndex =
    // currentTable.getDefinition().getColumnNames().indexOf(constituentData[i].first);
    // record[colIndex] = state.constituentSources.get(constituentData[i].first).get(ts.consumed);
    // }
    // }
    //
    // assertArrayEquals(getRecord(currentTable, tableRow, usePrev), record);
    // assertEquals(childKey != null && childMap.get(childKey) != null && !childMap.get(childKey).isEmpty(),
    // state.snapshot.getColumnSource(CHILD_PRESENCE_COLUMN).get(ts.consumed));
    // assertEquals(tableKey, state.snapshot.getColumnSource(TABLE_KEY_COLUMN).get(ts.consumed));
    // ts.consumed++;
    // }
    //
    // ts.traversed++;
    //
    // if (childKey != null) {
    // final TableDetails childDetails = state.expansionMap.get(childKey);
    // if (childDetails != null) {
    // checkSnapshotAgainst(state, childMutator.apply(childMap.get(childKey)), childKey, start, end, ts,
    // childMutator);
    // }
    // }
    // }
    // }
    //
    // private static Object[] getRecord(Table source, long rowNo, boolean usePrev) {
    // final Collection<? extends ColumnSource> sources = source.getColumnSources();
    // final Object[] record = new Object[sources.size()];
    //
    // final Iterator<? extends ColumnSource> it = sources.iterator();
    // for (int i = 0; i < sources.size(); i++) {
    // final ColumnSource cs = it.next();
    // record[i] = usePrev ? cs.getPrev(rowNo) : cs.get(rowNo);
    // }
    //
    // return record;
    // }
    //
    // // endregion
}
