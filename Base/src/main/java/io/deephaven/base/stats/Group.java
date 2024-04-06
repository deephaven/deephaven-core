//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.stats;

import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.LongFunction;

public class Group {
    private String name;
    private String description;
    private ArrayList<Item> items;

    /**
     * Constructs a new group with the given id and an empty item list.
     */
    Group(String name, String description) {
        this.name = name;
        this.description = description;
        this.items = new ArrayList<Item>();
    }

    /**
     * Set the description for this group.
     */
    void setDescription(String description) {
        this.description = description;
    }

    /** get or create a named, top-level item */
    synchronized <V extends Value> Item<V> makeItem(String name, LongFunction<V> valueFactory,
            String description, long now) {
        for (Item i : items) {
            if (i.getName().equals(name)) {
                return i;
            }
        }
        Item i = new Item<V>(this, name, valueFactory.apply(now), description);
        addItem(i);
        return i;
    }

    /** get or create a named, top-level item */
    synchronized <V extends Value, Arg> Item<V> makeItem(String name, BiFunction<Long, Arg, V> valueFactory,
            String description, long now, Arg arg) {
        for (Item i : items) {
            if (i.getName().equals(name)) {
                return i;
            }
        }
        Item i = new Item<V>(this, name, valueFactory.apply(now, arg), description);
        addItem(i);
        return i;
    }

    /**
     * Get an item from this group by name
     */
    public synchronized Item getItem(String id) {
        for (Item i : items) {
            if (i.name.equals(id)) {
                return i;
            }
        }
        return null;
    }

    /**
     * Add an item to this group
     */
    Item[] itemsArray = new Item[0];

    public synchronized void addItem(Item i) {
        items.add(i);
        itemsArray = items.toArray(itemsArray);
    }

    /**
     * @return the group's identifier
     */
    public synchronized String getName() {
        return name;
    }

    /**
     * @return the group's description
     */
    public synchronized String getDescription() {
        return description;
    }

    public Item[] getItems() {
        return itemsArray;
    }

    /**
     * Update the histories of all items in this group, logging all updated intervals &gt;= logInterval.
     */
    public void update(ItemUpdateListener listener, long logInterval, long now, long appNow) {
        Item[] arr = itemsArray;
        // DO NOT USE FOREACH HERE AS IT CREATES AN ITERATOR -> No Allocation changes
        for (int i = 0; i < arr.length; i++) {
            arr[i].update(listener, logInterval, now, appNow);
        }
    }
}
