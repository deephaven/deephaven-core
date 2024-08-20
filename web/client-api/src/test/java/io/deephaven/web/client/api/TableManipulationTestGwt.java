//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.gwt.junit.DoNotRunWith;
import com.google.gwt.junit.Platform;
import elemental2.core.JsArray;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import jsinterop.base.Js;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@DoNotRunWith(Platform.HtmlUnitBug)
public class TableManipulationTestGwt extends AbstractAsyncGwtTestCase {
    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table")
            .script("truthtable",
                    "empty_table(8).update([\"I=i\", \"four=(int)(i/4)%2\", \"two=(int)(i/2)%2\", \"one=i%2\"])")
            .script("strings", "empty_table(1).update([\"str=`abcdefg`\", \"nostr=(String)null\"])")
            .script("threedays",
                    "empty_table(3).update([\"I=i\", \"Timestamp=now() - (i * 24 * 60 * 60 * 1000 * 1000000l)\"])");

    public void testChangingFilters() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // filter one column
                    table.applyFilter(
                            new FilterCondition[] {table.findColumn("one").filter().eq(FilterValue.ofNumber(0.0))});

                    // before setting viewport, test the size of the table
                    return waitForEvent(table, JsTable.EVENT_SIZECHANGED, e -> {
                        assertEquals(4., table.getSize(), 0);
                        assertEquals(8., table.getTotalSize(), 0);
                    }, 2014);
                })
                .then(table -> {
                    // then set the viewport, confirm we get those items back
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 2, 4, 6);
                })
                .then(table -> {
                    // filter another too
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(0.0)),
                            table.findColumn("two").filter().eq(FilterValue.ofNumber(1.0)),
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 2, 6);
                })
                .then(table -> {
                    // check full table size from the last filter
                    assertEquals(2., table.getSize(), 0);


                    // remove the first filter
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("two").filter().eq(FilterValue.ofNumber(1.0)),
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 2, 3, 6, 7);
                })
                .then(table -> {
                    // check full table size from the last filter
                    assertEquals(4., table.getSize(), 0);

                    // clear all filters
                    table.applyFilter(new FilterCondition[] {});
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 1, 2, 3, 4, 5, 6, 7);
                })
                .then(table -> {
                    // check full table size from the last filter
                    assertEquals(8., table.getSize(), 0);
                    assertEquals(8., table.getTotalSize(), 0);

                    return Promise.resolve(table);
                })

                .then(this::finish).catch_(this::report);
    }

    public void testStackingSorts() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // simple sort
                    table.applySort(new Sort[] {table.findColumn("one").sort().asc()});
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 2, 4, 6, 1, 3, 5, 7);
                })
                .then(table -> {
                    // check full table size from the last operation
                    assertEquals(8., table.getSize(), 0);
                    assertEquals(8., table.getTotalSize(), 0);

                    // toggle it
                    table.applySort(new Sort[] {table.findColumn("one").sort().desc()});
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 1, 3, 5, 7, 0, 2, 4, 6);
                })
                .then(table -> {
                    // check full table size from the last operation
                    assertEquals(8., table.getSize(), 0);

                    // add another sort
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc(),
                            table.findColumn("two").sort().asc(),
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 1, 5, 3, 7, 0, 4, 2, 6);
                })
                .then(table -> {
                    // toggle second sort
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc(),
                            table.findColumn("two").sort().desc(),
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 3, 7, 1, 5, 2, 6, 0, 4);
                })
                .then(table -> {
                    // remove first sort
                    table.applySort(new Sort[] {
                            table.findColumn("two").sort().desc(),
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 2, 3, 6, 7, 0, 1, 4, 5);
                })
                .then(table -> {
                    // clear all sorts
                    table.applySort(new Sort[] {});
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 1, 2, 3, 4, 5, 6, 7);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testSerialFilterAndSort() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // first, filter, as soon as that is resolved on the server, sort
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("two").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    // no viewport, since we're going to make another change
                    return waitForEvent(table, JsTable.EVENT_FILTERCHANGED, e -> {
                    }, 2015);
                })
                .then(table -> {
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc()
                    });
                    // set a viewport once this is complete
                    table.setViewport(0, 6, null);
                    return waitForEvent(table, JsTable.EVENT_SORTCHANGED, e -> {
                    }, 2016);
                })
                .then(table -> {
                    return assertNextViewportIs(table, 1, 5, 0, 4);
                })// this looks confusing, remember it sorts "one" descending, and is a stable sort
                .then(table -> {
                    // check full table size from the last filter
                    assertEquals(4., table.getSize(), 0);

                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }


    public void testRapidFilterAndSort() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // filter and sort, and set a viewport, wait for results
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("two").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc()
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 1, 5, 0, 4);
                })
                .then(table -> {
                    // replace sort then replace filter then set viewport, wait for results
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.applySort(new Sort[] {
                            table.findColumn("two").sort().asc()
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 4, 2, 6);
                })
                .then(table -> {
                    // remove both then re-add both in reverse order, wait for results
                    table.applySort(new Sort[] {
                            table.findColumn("two").sort().asc()
                    });
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 4, 2, 6);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testSwappingFilterAndSort() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // filter one way, set a sort, then change the filter back again before setting a viewport
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(1.0))
                    });
                    table.applySort(new Sort[] {
                            table.findColumn("two").sort().asc()
                    });
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 4, 2, 6);
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testClearingFilter() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("I").filter().lessThan(FilterValue.ofNumber(3.5))
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 1, 2, 3);
                })
                .then(table -> {
                    table.applyFilter(new FilterCondition[0]);
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 1, 2, 3, 4, 5, 6, 7);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testFilterOutAllItems() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(2012);
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("one").filter().eq(FilterValue.ofNumber(100.0))
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table);
                })
                .then(table -> {
                    // check full table size from the last filter
                    assertEquals(0., table.getSize(), 0);

                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testReplaceSort() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);
                    // sort, get results, remove sort, get results
                    table.applySort(new Sort[] {
                            table.findColumn("I").sort().desc()
                    });
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, 7, 6, 5, 4, 3, 2, 1, 0);
                })
                .then(table -> {
                    table.applySort(new Sort[] {});
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, 0, 1, 2, 3, 4, 5, 6, 7);
                })
                // .then(table -> {
                // //sort, set viewport, then right away switch sort
                // table.applySort(new Sort[]{
                // table.findColumn("one").sort().desc()
                // });
                // table.setViewport(0, 7, null);
                // table.applySort(new Sort[]{
                // table.findColumn("one").sort().asc()
                // });
                // table.setViewport(0, 7, null);
                //
                // return assertNextViewportIs(table, 0, 2, 4, 6, 1, 3, 5, 7);
                // })
                .then(table -> {
                    // sort, set viewport, wait a moment, then do the same
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc()
                    });
                    table.setViewport(0, 7, null);
                    return Promise.resolve(table);
                })
                .then(table -> {
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().asc()
                    });
                    table.setViewport(0, 7, null);
                    return assertNextViewportIs(table, 0, 2, 4, 6, 1, 3, 5, 7);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testChangingColumns() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(5000);

                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("a=\"\" + I")));
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"),
                            new String[] {"0", "1", "2", "3", "4", "5", "6", "7"});
                })
                .then(table -> {

                    table.applyCustomColumns(JsArray.of(
                            JsTable.CustomColumnArgUnionType.of("a=\"\" + I"),
                            JsTable.CustomColumnArgUnionType.of("b=\"x\" + I")));
                    table.setViewport(0, 7, null);

                    @SuppressWarnings("unchecked")
                    Promise<Object[]> assertion = Promise.all(new IThenable[] {
                            assertNextViewportIs(table, t -> t.findColumn("a"),
                                    new String[] {"0", "1", "2", "3", "4", "5", "6", "7"}),
                            assertNextViewportIs(table, t -> t.findColumn("b"),
                                    new String[] {"x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7"})
                    });
                    // we don't want the array of results, just the success/failure and the same table we were working
                    // with before
                    return assertion.then(ignore -> Promise.resolve(table));
                })
                .then(table -> {

                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("b=\"x\" + I")));
                    table.setViewport(0, 7, null);

                    return assertUpdateReceived(table, viewportData -> {
                        // make sure we see the one column, but not the other
                        String[] expected = new String[] {"x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7"};
                        String[] actual = Js.uncheckedCast(getColumnData(viewportData, table.findColumn("b")));
                        assertTrue("Expected " + Arrays.toString(expected) + ", found " + Arrays.toString(actual)
                                + " in table " + table, Arrays.equals(expected, actual));
                        for (int i = 0; i < table.getColumns().length; i++) {
                            assertFalse("a".equals(table.getColumns().getAt(i).getName()));
                        }
                        assertEquals(5, viewportData.getColumns().length);
                    }, 2001);
                })
                .then(table -> {

                    table.applyCustomColumns(new JsArray<>());
                    table.setViewport(0, 7, null);

                    return assertUpdateReceived(table, viewportData -> {
                        // verify the absence of "a" and "b" in data and table
                        for (int i = 0; i < table.getColumns().length; i++) {
                            Column col = table.getColumns().getAt(i);
                            assertFalse("a".equals(col.getName()));
                            assertFalse("b".equals(col.getName()));
                        }
                        assertEquals(4, viewportData.getColumns().length);
                    }, 2002);
                })
                .then(this::finish).catch_(this::report);
    }

    // TODO: https://deephaven.atlassian.net/browse/DH-11196
    public void ignore_testColumnsFiltersAndSorts() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(10000);
                    // add a column
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("a=\"\" + I")));
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"),
                            new String[] {"0", "1", "2", "3", "4", "5", "6", "7"});
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // apply a filter
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("two").filter().eq(FilterValue.ofNumber(0.0))
                    });
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"), new String[] {"0", "1", "4", "5"});
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // apply a sort
                    table.applySort(new Sort[] {
                            table.findColumn("one").sort().desc()
                    });
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"), new String[] {"1", "5", "0", "4"});
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // remove the column
                    table.applyCustomColumns(new JsArray<>());
                    table.setViewport(0, 7, null);

                    @SuppressWarnings("unchecked")
                    Promise<Object[]> assertion = Promise.all(new IThenable[] {
                            assertUpdateReceived(table, viewportData -> {
                                // make sure we don't see the column
                                for (int i = 0; i < table.getColumns().length; i++) {
                                    assertFalse("a".equals(table.getColumns().getAt(i).getName()));
                                }
                                assertEquals(4, viewportData.getColumns().length);
                            }, 2001),
                            assertNextViewportIs(table, 1, 5, 0, 4)
                    });
                    return assertion.then(ignore -> Promise.resolve(table));
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // put the column back
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("a=\"\" + I")));
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"), new String[] {"1", "5", "0", "4"});
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // remove the filter (column and sort, no filter)
                    table.applyFilter(new FilterCondition[] {});
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, t -> t.findColumn("a"),
                            new String[] {"1", "3", "5", "7", "0", "2", "4", "6"});
                })
                .then(table -> {
                    delayTestFinish(10000);
                    // remove the column (sorted, no column)
                    table.applyCustomColumns(new JsArray<>());
                    table.setViewport(0, 7, null);

                    return assertNextViewportIs(table, 1, 3, 5, 7, 0, 2, 4, 6);
                })
                .then(this::finish).catch_(this::report);

    }

    public void testCustomColumnsReferencingOtherCustomColumns() {
        connect(tables)
                .then(table("truthtable"))
                .then(table -> {
                    delayTestFinish(2001);
                    table.applyCustomColumns(JsArray.of(JsTable.CustomColumnArgUnionType.of("y=`z`"),
                            JsTable.CustomColumnArgUnionType.of("z=y")));
                    table.setViewport(0, 7, null);

                    String[] expected = {"z", "z", "z", "z", "z", "z", "z", "z"};
                    // noinspection unchecked
                    return Promise.all(new IThenable[] {
                            assertNextViewportIs(table, t -> t.findColumn("y"), expected),
                            assertNextViewportIs(table, t -> t.findColumn("z"), expected)
                    });
                }).then(this::finish).catch_(this::report);
    }

    public void testDateTimeInFilters() {
        List<DateWrapper> dates = new ArrayList<>();
        connect(tables)
                .then(table("threedays"))
                .then(table -> {
                    delayTestFinish(2002);
                    // grab the three days in the db
                    table.setViewport(0, 2, null);

                    return assertUpdateReceived(table, viewportData -> {
                        viewportData.getRows().forEach((row, index) -> {
                            dates.add(row.get(table.findColumn("Timestamp")).cast());
                            return null;
                        });
                    }, 2003);
                })
                .then(table -> {
                    // take the first date, filter it out, confirm we see the other two
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("Timestamp").filter()
                                    .notIn(new FilterValue[] {
                                            FilterValue.ofNumber(FilterValue.OfNumberUnionParam.of(dates.get(0)))})
                    });
                    table.setViewport(0, 2, null);
                    return assertUpdateReceived(table, viewportData -> {
                        assertEquals(2, viewportData.getRows().length);
                        // this looks shady with the toString(), but they are both LongWrapper values
                        assertEquals(dates.get(1).toString(),
                                viewportData.getRows().getAt(0).get(table.findColumn("Timestamp")).toString());
                        assertEquals(dates.get(2).toString(),
                                viewportData.getRows().getAt(1).get(table.findColumn("Timestamp")).toString());
                    }, 2004);
                })
                .then(table -> {
                    // take the first date, filter for it, confirm we see only it
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("Timestamp").filter()
                                    .in(new FilterValue[] {
                                            FilterValue.ofNumber(FilterValue.OfNumberUnionParam.of(dates.get(0)))})
                    });
                    table.setViewport(0, 2, null);
                    return assertUpdateReceived(table, viewportData -> {
                        assertEquals(1, viewportData.getRows().length);
                        assertEquals(dates.get(0).toString(),
                                viewportData.getRows().getAt(0).get(table.findColumn("Timestamp")).toString());

                    }, 2005);
                })
                .then(this::finish).catch_(this::report);
    }

    public void testIcaseEqualFilters() {
        connect(tables)
                .then(table("strings"))
                .then(table -> {
                    delayTestFinish(5000);
                    // ==icase with match
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("str").filter().eqIgnoreCase(FilterValue.ofString("ABCdefg"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 1, 2006);
                })
                .then(table -> {
                    // ==icase with no match
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("str").filter().eqIgnoreCase(FilterValue.ofString("xyz"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 0, 2007);
                })
                .then(table -> {
                    // !=icase with match (i.e. don't show anything)
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("str").filter().notEqIgnoreCase(FilterValue.ofString("ABCdefg"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 0, 2008);
                })
                .then(table -> {
                    // !=icase with no match (i.e. show row)
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("nostr").filter().notEqIgnoreCase(FilterValue.ofString("ABC"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 1, 2009);
                })
                .then(table -> {
                    // 0=icase with value check against null (i.e. find nothing)
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("nostr").filter().eqIgnoreCase(FilterValue.ofString("ABC"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 0, 2010);
                })
                .then(table -> {
                    // !=icase with value check against null (i.e. find a row)
                    table.applyFilter(new FilterCondition[] {
                            table.findColumn("nostr").filter().notEqIgnoreCase(FilterValue.ofString("ABC"))
                    });
                    table.setViewport(0, 0, null);
                    return assertUpdateReceived(table, 1, 2011);
                })
                .then(this::finish).catch_(this::report);
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}
