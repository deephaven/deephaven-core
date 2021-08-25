/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.EvalNuggetInterface;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.EvalNugget;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.UpdateValidatorNugget;
import io.deephaven.db.v2.sources.ColumnSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class TestHashSetBackedTableFactory extends LiveTableTestCase {
    public void testSimple() {
        final HashSet<SmartKey> set = new HashSet<>();
        set.add(new SmartKey("Raylan", "USMS"));
        set.add(new SmartKey("Bowd", "Crowder"));
        set.add(new SmartKey("Dewey", "Crow"));
        set.add(new SmartKey("Darryl", "Crow"));
        set.add(new SmartKey("Art", "USMS"));
        set.add(new SmartKey("Earl", "Crow"));

        final Table result = HashSetBackedTableFactory.create(() -> set, 0, "Name", "Faction");

        io.deephaven.db.tables.utils.TableTools.show(result);

        assertEquals(result.size(), set.size());

        final HashSet<SmartKey> tableAsSet = tableToSet(result);

        assertEquals(set, tableAsSet);
    }

    public void testIterative() {
        final HashSet<SmartKey> set = new HashSet<>();

        final Table result = HashSetBackedTableFactory.create(() -> set, 0, "Arg");

        final TstUtils.StringGenerator generator = new TstUtils.StringGenerator();
        final Random random = new Random();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                            .computeLocked(() -> result.update("Arg0=Arg.substring(0, 1)"));
                    }
                },
                new UpdateValidatorNugget(result),
        };


        for (int ii = 0; ii < 1000; ++ii) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                final int additions = random.nextInt(4);
                final int removals = random.nextInt(4);
                for (int jj = 0; jj < removals; ++jj) {
                    if (!set.isEmpty()) {
                        int element = random.nextInt(set.size());
                        final Iterator<SmartKey> it = set.iterator();
                        do {
                            if (it.hasNext())
                                it.next();
                            element--;
                        } while (element > 0);
                        it.remove();
                    }
                }
                for (int jj = 0; jj < additions; ++jj) {
                    set.add(new SmartKey(generator.nextValue(null, 0, random)));
                }

                ((LiveTable) result).refresh();
            });

            final HashSet<SmartKey> tableAsSet = tableToSet(result);
            assertEquals(set, tableAsSet);

            TstUtils.validate("ii=" + ii, en);
        }
    }

    private HashSet<SmartKey> tableToSet(Table result) {
        final HashSet<SmartKey> set = new HashSet<>();

        assertTrue(result instanceof QueryTable);

        final QueryTable queryTable = (QueryTable) result;

        final Map<String, ColumnSource> map = queryTable.getColumnSourceMap();

        // noinspection unchecked
        final ColumnSource<String>[] columnSources =
            (ColumnSource<String>[]) new ColumnSource[map.size()];
        int ii = 0;
        for (ColumnSource cs : map.values()) {
            // noinspection unchecked
            columnSources[ii++] = cs;
        }

        for (final Index.Iterator it = queryTable.getIndex().iterator(); it.hasNext();) {
            final long idx = it.nextLong();
            final String[] values = new String[columnSources.length];
            for (ii = 0; ii < columnSources.length; ++ii) {
                values[ii] = columnSources[ii].get(idx);
            }
            set.add(new SmartKey((Object[]) values));
        }

        return set;
    }
}
