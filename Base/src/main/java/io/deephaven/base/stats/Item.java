/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

public class Item<V extends Value> {

    /** The item's name */
    protected final String name;

    /** The item's description */
    protected final String description;

    /** The group to which the item belongs */
    protected final Group group;

    /** The value associated with this item */
    protected final V value;

    /** Creating this for intraday so we don't have to allocate or change the existing schema **/
    protected final String compactName;

    /**
     * Constructs a new Item, recording its group and the associated value.
     */
    Item(Group group, String name, V value, String description) {
        this.name = name;
        this.description = description;
        this.group = group;
        this.value = value;
        this.compactName = group.getName() + "." + name;
    }

    /**
     * @return the item's name, which is unique within its group
     */
    public String getName() {
        return name;
    }

    /**
     * @return the item's description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return the item's group
     */
    public Group getGroup() {
        return group;
    }

    /**
     * @return the name of the item's group (convenience method)
     */
    public String getGroupName() {
        return group.getName();
    }

    /**
     * @return the value associated with this item.
     */
    public V getValue() {
        return value;
    }

    public String getCompactName() {
        return compactName;
    }

    /**
     * Update the history intervals for this item's value, logging updated intervals >= logInterval
     */
    public void update(ItemUpdateListener listener, long logInterval, long now, long appNow) {
        value.update(this, listener, logInterval, now, appNow);
    }
}
