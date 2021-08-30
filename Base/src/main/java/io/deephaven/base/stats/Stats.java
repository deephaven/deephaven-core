/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import io.deephaven.base.Function;

import java.util.ArrayList;

public class Stats {

    /** A non-static interface to the Stats component. */
    public interface Maker {
        <V extends Value> Item<V> makeItem(String groupName, String itemName, Function.Unary<V, Long> valueFactory);

        Maker DEFAULT = new Maker() {
            @Override
            public <V extends Value> Item<V> makeItem(String groupName, String itemName,
                    Function.Unary<V, Long> valueFactory) {
                return Stats.makeItem(groupName, itemName, valueFactory);
            }
        };
    }

    public interface TimeSource {
        public long currentTimeMillis();
    }

    private static TimeSource timeSource = new TimeSource() {
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    };

    public static void setTimeSource(TimeSource ts) {
        timeSource = ts;
    }

    /** top-level groups */
    private static ArrayList<Group> groups = new ArrayList<Group>();
    private static Group[] groupsArray;

    /** get or create a named, top-level group */
    public static synchronized Group makeGroup(String name, String description) {
        for (Group g : groups) {
            if (g.getName().equals(name)) {
                if (description != null) {
                    g.setDescription(description);
                }
                return g;
            }
        }
        if (description == null) {
            description = "The description of the group " + name + " should go here";
        }
        Group g = new Group(name, description);
        groups.add(g);
        groupsArray = null;
        return g;
    }

    public static final String UNKNOWN_DESCRIPTION = "Please describe this stats item";

    /** get or create a new item */
    public static synchronized <V extends Value> Item<V> makeItem(String groupName, String itemName,
            Function.Unary<V, Long> valueFactory) {
        return makeItem(groupName, itemName, valueFactory, UNKNOWN_DESCRIPTION, timeSource.currentTimeMillis());
    }

    /** get or create a new item */
    public static synchronized <V extends Value> Item<V> makeItem(String groupName, String itemName,
            Function.Unary<V, Long> valueFactory, long now) {
        return makeItem(groupName, itemName, valueFactory, UNKNOWN_DESCRIPTION, now);
    }

    /** get or create a new item */
    public static synchronized <V extends Value> Item<V> makeItem(String groupName, String itemName,
            Function.Unary<V, Long> valueFactory, String description) {
        return makeItem(groupName, itemName, valueFactory, description, timeSource.currentTimeMillis());
    }

    /** get or create a new item */
    public static synchronized <V extends Value> Item<V> makeItem(String groupName, String itemName,
            Function.Unary<V, Long> valueFactory, String description, long now) {
        Group g = makeGroup(groupName, null);
        return g.makeItem(itemName, valueFactory, description, now);
    }


    /** get or create a new item with a one-argument factory */
    public static synchronized <V extends Value, Arg> Item<V> makeItem(String groupName, String itemName,
            Function.Binary<V, Long, Arg> valueFactory, Arg arg) {
        return makeItem(groupName, itemName, valueFactory, UNKNOWN_DESCRIPTION, timeSource.currentTimeMillis(), arg);
    }

    /** get or create a new item with a one-argument factory */
    public static synchronized <V extends Value, Arg> Item<V> makeItem(String groupName, String itemName,
            Function.Binary<V, Long, Arg> valueFactory, long now, Arg arg) {
        return makeItem(groupName, itemName, valueFactory, UNKNOWN_DESCRIPTION, now, arg);
    }

    /** get or create a new item with a one-argument factory */
    public static synchronized <V extends Value, Arg> Item<V> makeItem(String groupName, String itemName,
            Function.Binary<V, Long, Arg> valueFactory, String description, Arg arg) {
        return makeItem(groupName, itemName, valueFactory, description, timeSource.currentTimeMillis(), arg);
    }

    /** get or create a new item with a one-argument factory */
    public static synchronized <V extends Value, Arg> Item<V> makeItem(String groupName, String itemName,
            Function.Binary<V, Long, Arg> valueFactory, String description, long now, Arg arg) {
        Group g = makeGroup(groupName, null);
        return g.makeItem(itemName, valueFactory, description, now, arg);
    }

    /** get or create a new histogrammed item */
    public static synchronized <V extends Value> Item<HistogramState> makeHistogram(String groupName, String itemName,
            long rangeMin, long rangeMax, int numBuckets) {
        return makeHistogram(groupName, itemName, UNKNOWN_DESCRIPTION, timeSource.currentTimeMillis(), rangeMin,
                rangeMax, numBuckets);
    }

    /** get or create a new histogrammed item */
    public static synchronized <V extends Value> Item<HistogramState> makeHistogram(String groupName, String itemName,
            long now, long rangeMin, long rangeMax, int numBuckets) {
        return makeHistogram(groupName, itemName, UNKNOWN_DESCRIPTION, now, rangeMin, rangeMax, numBuckets);
    }

    /** get or create a new histogrammed item */
    public static synchronized <V extends Value> Item<HistogramState> makeHistogram(String groupName, String itemName,
            String description, long rangeMin, long rangeMax, int numBuckets) {
        return makeHistogram(groupName, itemName, description, timeSource.currentTimeMillis(), rangeMin, rangeMax,
                numBuckets);
    }

    /** get or create a new histogrammed item */
    public static synchronized <V extends Value> Item<HistogramState> makeHistogram(String groupName, String itemName,
            String description, long now, long rangeMin, long rangeMax, int numBuckets) {
        return Stats.makeItem(groupName, itemName, HistogramState.FACTORY, description, now,
                new HistogramState.Spec(groupName, itemName, rangeMin, rangeMax, numBuckets));
    }

    /** return an array of all groups */
    public synchronized static Group[] getGroups() {
        if (null == groupsArray) {
            groupsArray = groups.toArray(new Group[groups.size()]);
        }
        return groupsArray;
    }

    /**
     * Return a specific group by name.
     */
    public synchronized static Group getGroup(String name) {
        // TODO: shouldn't use O(n) here!
        for (Group g : groups) {
            if (g.getName().equals(name)) {
                return g;
            }
        }
        return null;
    }

    /**
     * Update the histories of all items in all groups, logging all updated intervals >= logInterval.
     */
    public synchronized static void update(ItemUpdateListener listener, long now, long appNow, long logInterval) {
        for (Group g : groups) {
            g.update(listener, logInterval, now, appNow);
        }
    }

    /**
     * Throw away all state
     */
    public synchronized static void clearAll() {
        groups = new ArrayList<Group>();
        groupsArray = null;
    }
}
