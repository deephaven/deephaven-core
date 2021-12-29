package io.deephaven.engine.updategraph;

/**
 * Interface for dynamic nodes in a query's directed acyclic graph.
 */
public interface DynamicNode {

    /**
     * Is the node updating?
     *
     * @return true if the node is updating; false otherwise.
     */
    boolean isRefreshing();

    /**
     * Change the node's run mode.
     *
     * @param refreshing true to cause the node to update; false otherwise.
     * @return new refreshing state
     */
    boolean setRefreshing(boolean refreshing);

    /**
     * Called on a dependent node to ensure that a strong reference is maintained to any parent object that is required
     * for the proper maintenance and functioning of the dependent.
     *
     * In the most common case, the parent object is a child listener to a parent node. The parent node only keeps a
     * weak reference to its child listener, but the listener maintains a strong reference to the parent node. In this
     * scenario, the only strong reference to the listener (and thus indirectly to the parent node itself) is the
     * reference kept by the dependent node.
     *
     * @param parent A parent of this node
     */
    void addParentReference(Object parent);

    /**
     * Determine if an object is a refreshing {@link DynamicNode}.
     *
     * @param object The object
     * @return True if the object is a {@link DynamicNode} and its {@link DynamicNode#isRefreshing()} returns true,
     *         false otherwise
     */
    static boolean isDynamicAndIsRefreshing(final Object object) {
        return object instanceof DynamicNode && ((DynamicNode) object).isRefreshing();
    }

    /**
     * Determine if an object is a {@link DynamicNode} but is not refreshing.
     *
     * @param object The object
     * @return True if the object is a {@link DynamicNode} and its {@link DynamicNode#isRefreshing()} returns true,
     *         false otherwise
     */
    static boolean isDynamicAndNotRefreshing(final Object object) {
        return object instanceof DynamicNode && !((DynamicNode) object).isRefreshing();
    }

    /**
     * Determine if an object is not a refreshing {@link DynamicNode}.
     *
     * @param object The object
     * @return True if the object is not a {@link DynamicNode} or its {@link DynamicNode#isRefreshing()} returns false,
     *         false otherwise
     */
    static boolean notDynamicOrNotRefreshing(final Object object) {
        return !(object instanceof DynamicNode) || !((DynamicNode) object).isRefreshing();
    }

    /**
     * Determine if an object is either not a {@link DynamicNode}, or is a refreshing {@link DynamicNode}.
     *
     * @param object The object
     * @return True if the object is not a {@link DynamicNode} or if its {@link DynamicNode#isRefreshing()} returns
     *         true, false otherwise
     */
    static boolean notDynamicOrIsRefreshing(final Object object) {
        return !(object instanceof DynamicNode) || ((DynamicNode) object).isRefreshing();
    }
}
