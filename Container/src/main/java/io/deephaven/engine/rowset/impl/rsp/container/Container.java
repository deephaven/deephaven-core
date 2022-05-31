/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 *
 * The code in this file is a heavily modified version of the original in the RoaringBitmap library; please see
 * https://roaringbitmap.org/
 *
 */

package io.deephaven.engine.rowset.impl.rsp.container;

import java.util.NoSuchElementException;

/**
 * Base container class.
 */
public abstract class Container {

    public static final boolean DEBUG =
            Boolean.getBoolean("io.deephaven.engine.rowset.impl.rsp.container.Container.DEBUG");

    /**
     * The maximum possible cardinality of a container, as an int. Also the maximum possible exclusive end for a range
     * that can be stored in a container.
     */
    public static final int MAX_RANGE = (1 << 16);

    /**
     * The maxumum possible value that can be stored in a container, as an int.
     */
    public static final int MAX_VALUE = MAX_RANGE - 1;

    // Sizing of a short array object in a 64 bit JVM (Hotspot) uses
    // 12 bytes of object overhead (including array.length), plus
    // the space for the short elements payload, plus padding to round
    // objects to 8 bytes boundaries.
    // So an array of 4 elements uses: 12 + 2*4 + 4 padding = 24 bytes = 3*8 bytes.
    // The 4 bytes of padding are wasted and can be used for another 2 short elements.
    static int shortArraySizeRounding(final int sizeToRound) {
        final int sz = 12 + 2 * sizeToRound;
        final int szMod8 = sz & 7;
        final int padding = szMod8 > 0 ? 8 - szMod8 : 0;
        return sizeToRound + padding / 2;
    }

    static int runsShortArraySizeRounding(final int nruns) {
        return 2 * runsSizeRounding(nruns);
    }

    static int runsSizeRounding(final int nruns) {
        final int sz = 12 + 4 * nruns;
        final int szMod8 = sz & 7;
        final int padding = szMod8 > 0 ? 8 - szMod8 : 0;
        return nruns + padding / 4;
    }

    protected static final ThreadLocal<short[]> threadLocalBuf = ThreadLocal.withInitial(() -> new short[256 - 12]);

    public final void ifDebugValidate() {
        if (DEBUG) {
            validate();
        }
    }

    public Container check() {
        validate();
        return this;
    }

    public abstract void validate();

    private static boolean smallContainersDisabled() {
        return !ImmutableContainer.ENABLED;
    }

    public static Container singleton(final short v) {
        if (smallContainersDisabled()) {
            final short[] vs = new short[shortArraySizeRounding(1)];
            vs[0] = v;
            return new ArrayContainer(vs, 1);
        }
        return new SingletonContainer(v);
    }

    public static Container singleRange(final int start, final int end) {
        if (DEBUG) {
            if (end <= start || start < 0 || end > MAX_RANGE) {
                throw new IllegalArgumentException("start=" + start + ", end=" + end);
            }
        }
        return rangeOfOnes(start, end);
    }

    public static Container full() {
        if (smallContainersDisabled()) {
            return new RunContainer(0, MAX_RANGE);
        }
        return new SingleRangeContainer(0, MAX_RANGE);
    }

    /**
     * Create a container initialized with a range of consecutive values
     *
     * @param start first index
     * @param end last index (range is exclusive)
     * @return a new container initialized with the specified values
     */
    public static Container rangeOfOnes(final int start, final int end) {
        if (DEBUG && (start < 0 || end > MAX_RANGE)) {
            throw new IllegalArgumentException("start=" + start + ", end=" + end);
        }
        if (end <= start) {
            return Container.empty();
        }
        if (end - start == 1) {
            return Container.singleton(ContainerUtil.lowbits(start));
        }
        if (smallContainersDisabled()) {
            return new RunContainer(start, end);
        }
        return new SingleRangeContainer(start, end);
    }

    public static Container twoValues(final short v1, final short v2) {
        final int iv1 = ContainerUtil.toIntUnsigned(v1);
        final int iv2 = ContainerUtil.toIntUnsigned(v2);
        if (DEBUG) {
            if (iv2 <= iv1) {
                throw new IllegalStateException("iv1=" + iv1 + " && iv2=" + iv2);
            }
        }
        if (smallContainersDisabled()) {
            final short[] vs = new short[shortArraySizeRounding(2)];
            vs[0] = v1;
            vs[1] = v2;
            return new ArrayContainer(vs, 2);
        }
        if (iv2 - 1 == iv1) {
            return Container.singleRange(iv1, iv2 + 1);
        }
        return new TwoValuesContainer(v1, v2);
    }

    /**
     * Returns a new Container containing the two provided ranges. The ranges provided should be nonempty, disjoint and
     * provided in appearance order, ie, start2 > start1.
     *
     * @param start1 start of first range, inclusive.
     * @param end1 end of first range, exclusive.
     * @param start2 start of second range, inclusive.
     * @param end2 end of second range, exclusive.
     * @return A new Container containing the provided ranges.
     */
    public static Container twoRanges(final int start1, final int end1, final int start2, final int end2) {
        if (DEBUG) {
            if (end1 <= start1 || end2 <= start2 ||
                    start1 < 0 || start2 < 0 ||
                    end1 > MAX_RANGE || end2 > MAX_RANGE ||
                    start2 <= end1) {
                throw new IllegalArgumentException(
                        "start1=" + start1 + ", end1=" + end1 + ", start2=" + start2 + ", end2=" + end2);
            }
        }
        if (smallContainersDisabled()) {
            return new RunContainer(start1, end1, start2, end2);
        }
        if (end1 - start1 == 1 && end2 - start2 == 1) {
            return new TwoValuesContainer(ContainerUtil.lowbits(start1), ContainerUtil.lowbits(start2));
        }
        return new RunContainer(start1, end1, start2, end2);
    }

    static Container makeSingletonContainer(final short v) {
        if (smallContainersDisabled()) {
            return new ArrayContainer(v);
        }
        return new SingletonContainer(v);
    }

    static Container makeTwoValuesContainer(final short v1, final short v2) {
        if (smallContainersDisabled()) {
            return new ArrayContainer(v1, v2);
        }
        return new TwoValuesContainer(v1, v2);
    }

    static Container makeSingleRangeContainer(final int start, final int end) {
        if (smallContainersDisabled()) {
            return new RunContainer(start, end);
        }
        return new SingleRangeContainer(start, end);
    }

    /**
     * @return An empty container.
     */
    public static Container empty() {
        if (smallContainersDisabled()) {
            return new ArrayContainer();
        }
        return EmptyContainer.instance;
    }

    /**
     * Return a new container with all shorts in [begin,end) added using an unsigned interpretation.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract Container add(int begin, int end);

    /**
     * Insert a short to the container. May generate a new container.
     *
     * @param x short to be added
     * @return the resulting container
     */
    public abstract Container iset(short x);

    /**
     * Return a new container with the short given as parameter added.
     *
     * @param x a short to be added
     * @return a new container with x added
     */
    public abstract Container set(final short x);

    private String myType() {
        return getClass().getSimpleName();
    }

    /**
     * Computes the bitwise AND of this container with another (intersection). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container and(ArrayContainer x);

    /**
     * Computes the bitwise AND of this container with another (intersection). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container and(BitmapContainer x);

    /**
     * Computes the bitwise AND of this container with another (intersection). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container and(RunContainer x);

    private Container and(final SingletonContainer sc) {
        return contains(sc.value) ? sc : Container.empty();
    }

    private Container and(final SingleRangeContainer sr) {
        return andRange(sr.first(), sr.last() + 1);
    }

    private Container and(final TwoValuesContainer tv) {
        final boolean has1 = contains(tv.v1);
        final boolean has2 = contains(tv.v2);
        if (has1) {
            if (has2) {
                return tv.cowRef();
            }
            return Container.singleton(tv.v1);
        }
        if (has2) {
            return Container.singleton(tv.v2);
        }
        return Container.empty();
    }

    /**
     * Computes the bitwise AND of this container with another (intersection). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container and(final Container x) {
        if (isEmpty() || x.isEmpty()) {
            return Container.empty();
        }
        if (x instanceof SingletonContainer) {
            return and((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return and((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return and((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return and((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return and((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return and((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }


    /**
     * Calculate the intersection of this container and a range, in a new container. The existing container is not
     * modified.
     *
     * @param start start of range
     * @param end end of range, exclusive.
     * @return a new Container containing the intersction of this container and the given range.
     */
    public abstract Container andRange(int start, int end);

    /**
     * Calculate the intersection of this container and a range; may overwrite the existing container or return a new
     * one.
     *
     * @param start start of range
     * @param end end of range, exclusive.
     * @return a Container containing the intersction of this container and the given range.
     */
    public abstract Container iandRange(int start, int end);

    /**
     * Computes the bitwise ANDNOT of this container with another (difference). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container andNot(ArrayContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another (difference). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container andNot(BitmapContainer x);

    /**
     * Computes the bitwise ANDNOT of this container with another (difference). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container andNot(RunContainer x);

    private Container andNot(final SingletonContainer sc) {
        return unset(sc.value);
    }

    private Container andNot(final SingleRangeContainer sr) {
        return remove(sr.first(), sr.last() + 1);
    }

    private Container andNot(final TwoValuesContainer tv) {
        final PositionHint hint = new PositionHint();
        return unset(tv.v1, hint).iunset(tv.v2, hint);
    }

    /**
     * Computes the bitwise ANDNOT of this container with another (difference). This container as well as the provided
     * container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container andNot(final Container x) {
        if (isEmpty()) {
            return Container.empty();
        }
        if (x.isEmpty()) {
            return cowRef();
        }
        if (x instanceof SingletonContainer) {
            return andNot((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return andNot((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return andNot((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return andNot((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return andNot((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return andNot((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Get a full deep copy of the container in a new container object.
     *
     * @return A copy of the container as a new object.
     */
    public abstract Container deepCopy();

    /**
     * Get a shared, copy-on-write copy of an existing container. Mutations on the returned container will always return
     * a copy and leave the original container unchanged.
     * <p>
     * This operation allows for cheap read-only references to the same values, at the cost of an additional copy for
     * any first mutation.
     *
     * @return A copy-on-write reference to the container.
     */
    public abstract Container cowRef();

    /**
     * Checks whether the container is empty or not.
     *
     * @return true if the container is empty.
     */
    public abstract boolean isEmpty();

    /**
     * Checks whether the container spans the full 2^16 range (ie, contains every short value) This is an O(1) operation
     * in all container types (some do not cache cardinality).
     *
     * @return true if the container does not miss any single short value.
     */
    public abstract boolean isAllOnes();

    /**
     * Checks whether the container has exactly one element (meaningful since cardinality may not be cached in some
     * Container types, eg, Run).
     *
     * @return true if the container contains exactly one element, false otherwise.
     */
    public boolean isSingleElement() {
        return getCardinality() == 1;
    }

    /**
     * Checks whether the container spans the full 2^16 range (ie, contains every short value) This is an O(1) operation
     * in all container types (some do not cache cardinality).
     *
     * @return true if the container does not miss any single short value. This method is deprecated, prefer isAllOnes
     *         instead.
     */
    @Deprecated
    public final boolean isFull() {
        return isAllOnes();
    }

    /**
     * Checks whether the contain contains the provided value
     *
     * @param x value to check
     * @return whether the value is in the container
     */
    public abstract boolean contains(short x);

    /**
     * Checks whether the container contains the entire range
     *
     * @param rangeStart the inclusive lower bound of the range
     * @param rangeEnd the exclusive upper bound of the range
     * @return true if the container contains the range
     */
    public abstract boolean contains(int rangeStart, int rangeEnd);

    protected abstract boolean contains(ArrayContainer arrayContainer);

    protected abstract boolean contains(BitmapContainer bitmapContainer);

    protected abstract boolean contains(RunContainer runContainer);

    /**
     * Checks whether the container is a subset of this container or not
     *
     * @param subset the container to be tested
     * @return true if the parameter is a subset of this container
     */
    public boolean contains(final Container subset) {
        if (subset.isEmpty()) {
            return true;
        }
        if (isEmpty()) {
            return false;
        }
        if (subset instanceof SingletonContainer) {
            final SingletonContainer sc = (SingletonContainer) subset;
            return contains(sc.value);
        }
        if (subset instanceof SingleRangeContainer) {
            final SingleRangeContainer sr = (SingleRangeContainer) subset;
            return contains(sr.first(), sr.last() + 1);
        }
        if (subset instanceof TwoValuesContainer) {
            final TwoValuesContainer tv = (TwoValuesContainer) subset;
            return contains(tv.v1) && contains(tv.v2);
        }
        if (subset instanceof ArrayContainer) {
            return contains((ArrayContainer) subset);
        }
        if (subset instanceof BitmapContainer) {
            return contains((BitmapContainer) subset);
        }
        if (subset instanceof RunContainer) {
            return contains((RunContainer) subset);
        }
        throw new IllegalStateException(subset.myType());
    }

    /**
     * Add a short to the container if it is not present, otherwise remove it. May generate a new container.
     *
     * @param x short to be added
     * @return the new container
     */
    public abstract Container iflip(short x);

    /**
     * Computes the distinct number of short values in the container. Can be expected to run in constant time.
     *
     * @return the cardinality
     */
    public abstract int getCardinality();

    /**
     * Get the name of this container.
     *
     * @return name of the container
     */
    public String getContainerName() {
        if (this instanceof EmptyContainer) {
            return ContainerNames[0];
        }
        if (this instanceof SingletonContainer) {
            return ContainerNames[1];
        }
        if (this instanceof SingleRangeContainer) {
            return ContainerNames[2];
        }
        if (this instanceof TwoValuesContainer) {
            return ContainerNames[3];
        }
        if (this instanceof ArrayContainer) {
            return ContainerNames[4];
        }
        if (this instanceof BitmapContainer) {
            return ContainerNames[5];
        }
        if (this instanceof RunContainer) {
            return ContainerNames[6];
        }
        throw new IllegalStateException(myType());
    }

    /**
     * Name of the various possible containers
     */
    public static final String[] ContainerNames = {
            "empty", "singleton", "singlerange", "twovalues", "array", "bitmap", "run"};

    /**
     * Iterate through the values of this container in order and pass them along to the ShortConsumer.
     *
     * @param sc a shortConsumer
     * @return false if the consumer returned false at some point, true otherwise.
     */
    public abstract boolean forEach(ShortConsumer sc);

    /**
     * Like forEach, but skipping the first rankOffset elements.
     *
     * @param rankOffset the position (rank) offset of the element where to start
     * @param sc a ShortConsumer
     * @return false if the consumer returned false at some point, true otherwise.
     */
    public abstract boolean forEach(int rankOffset, ShortConsumer sc);

    public abstract boolean forEachRange(int rankOffset, ShortRangeConsumer sc);

    /**
     * Iterator to visit the short values in the container in descending order.
     *
     * @return iterator
     */
    public abstract ShortAdvanceIterator getReverseShortIterator();

    /**
     * Iterator to visit the short values in the container in ascending order.
     *
     * @return iterator
     */
    public abstract ShortIterator getShortIterator();

    /**
     * Gets an iterator to visit the contents of the container in batches of short values
     *
     * @param skipFromStartCount number of elements to skip from the start of the container.
     * @return iterator
     */
    public abstract ContainerShortBatchIterator getShortBatchIterator(int skipFromStartCount);

    /**
     * Iterator to visit the short values in container in [start, end) ranges, in increasing order of start values.
     *
     * @return iterator
     */
    public abstract SearchRangeIterator getShortRangeIterator(int skipFromStartCount);

    /**
     * Add all shorts in [begin,end) using an unsigned interpretation. May generate a new container.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract Container iadd(int begin, int end);

    /**
     * Add all shorts in [begin,end) using an unsigned interpretation. May generate a new container. The beginning of
     * the range should be strictly greater than the last value already present in the container, if there is one.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract Container iappend(int begin, int end);

    /**
     * Computes the in-place bitwise AND of this container with another (intersection). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iand(ArrayContainer x);


    /**
     * Computes the in-place bitwise AND of this container with another (intersection). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iand(BitmapContainer x);

    /**
     * Computes the in-place bitwise AND of this container with another (intersection). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iand(RunContainer x);

    private Container iand(final SingleRangeContainer sr) {
        return iandRange(sr.first(), sr.last() + 1);
    }

    /**
     * Computes the in-place bitwise AND of this container with another (intersection). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container iand(final Container x) {
        if (isEmpty() || x.isEmpty()) {
            return Container.empty();
        }
        if (x instanceof SingletonContainer) {
            return and((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return iand((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return and((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return iand((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return iand((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return iand((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Computes the in-place bitwise ANDNOT of this container with another (difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iandNot(ArrayContainer x);


    /**
     * Computes the in-place bitwise ANDNOT of this container with another (difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iandNot(BitmapContainer x);

    /**
     * Computes the in-place bitwise ANDNOT of this container with another (difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container iandNot(RunContainer x);

    private Container iandNot(final SingletonContainer sc) {
        return iunset(sc.value);
    }

    private Container iandNot(final SingleRangeContainer sr) {
        return iremove(sr.first(), sr.last() + 1);
    }

    private Container iandNot(final TwoValuesContainer tv) {
        final PositionHint hint = new PositionHint();
        return iunset(tv.v1, hint).iunset(tv.v2, hint);
    }

    /**
     * Computes the in-place bitwise ANDNOT of this container with another (difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container iandNot(final Container x) {
        if (isEmpty()) {
            return Container.empty();
        }
        if (x.isEmpty()) {
            return this;
        }
        if (x instanceof SingletonContainer) {
            return iandNot((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return iandNot((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return iandNot((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return iandNot((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return iandNot((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return iandNot((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Computes the in-place bitwise NOT of this container (complement). Only those bits within the range are affected.
     * The current container is generally modified. May generate a new container.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this container.
     * @param rangeEnd ending of range (exclusive)
     * @return (partially) complemented container
     */
    public abstract Container inot(int rangeStart, int rangeEnd);

    /**
     * Computes the in-place bitwise OR of this container with another (union). The current container is generally
     * modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ior(ArrayContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another (union). The current container is generally
     * modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ior(BitmapContainer x);

    /**
     * Computes the in-place bitwise OR of this container with another (union). The current container is generally
     * modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ior(RunContainer x);

    private Container ior(final SingletonContainer sc) {
        return iset(sc.value);
    }

    private Container ior(final SingleRangeContainer sr) {
        return iadd(sr.first(), sr.last() + 1);
    }

    private Container ior(final TwoValuesContainer tv) {
        final PositionHint hint = new PositionHint();
        final Container ans = iset(tv.v1, hint);
        return ans.iset(tv.v2, hint);
    }

    /**
     * Computes the in-place bitwise OR of this container with another (union). The current container is generally
     * modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container ior(final Container x) {
        if (isEmpty()) {
            if (x.isEmpty()) {
                return Container.empty();
            }
            return x.cowRef();
        }
        if (x.isEmpty()) {
            return this;
        }
        if (x instanceof SingletonContainer) {
            return ior((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return ior((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return ior((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return ior((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return ior((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return ior((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Remove shorts in [begin,end) using an unsigned interpretation. May generate a new container.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract Container iremove(int begin, int end);

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ixor(ArrayContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ixor(BitmapContainer x);

    /**
     * Computes the in-place bitwise XOR of this container with another (symmetric difference). The current container is
     * generally modified, whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container ixor(RunContainer x);

    private Container ixor(final SingletonContainer sc) {
        return iflip(sc.value);
    }

    private Container ixor(final SingleRangeContainer sr) {
        return inot(sr.first(), sr.last() + 1);
    }

    private Container ixor(final TwoValuesContainer tv) {
        return iflip(tv.v1).iflip(tv.v2);
    }

    /**
     * Computes the in-place bitwise XOR of this container with another. The current container is generally modified,
     * whereas the provided container (x) is unaffected. May generate a new container.
     *
     * @param x Another container
     * @return xor result as a new container reference
     */
    public Container ixor(final Container x) {
        if (isEmpty()) {
            return x.cowRef();
        }
        if (x.isEmpty()) {
            return this;
        }
        if (x instanceof SingletonContainer) {
            return ixor((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return ixor((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return ixor((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return ixor((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return ixor((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return ixor((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Computes the bitwise NOT of this container (complement). Only those bits within the range are affected. This is
     * equivalent to an xor with a range of ones for the given range. The current container is left unaffected.
     *
     * @param rangeStart beginning of range (inclusive); 0 is beginning of this container.
     * @param rangeEnd ending of range (exclusive)
     * @return (partially) complemented container
     */
    public abstract Container not(int rangeStart, int rangeEnd);

    abstract int numberOfRuns(); // exact

    /**
     * Computes the number of ranges that a RangeIterator would provide on this container.
     *
     * @return The number of ranges that a RangeIterator would provide on this container.
     */
    @SuppressWarnings("unused")
    public int numberOfRanges() {
        return numberOfRuns();
    }

    /**
     * Computes the bitwise OR of this container with another (union). This container as well as the provided container
     * are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container or(ArrayContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This container as well as the provided container
     * are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container or(BitmapContainer x);

    /**
     * Computes the bitwise OR of this container with another (union). This container as well as the provided container
     * are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container or(RunContainer x);

    private Container or(final SingletonContainer sc) {
        return set(sc.value);
    }

    private Container or(final SingleRangeContainer sr) {
        return add(sr.first(), sr.last() + 1);
    }

    private Container or(final TwoValuesContainer tv) {
        final PositionHint hint = new PositionHint();
        final Container ans = set(tv.v1, hint);
        return ans.iset(tv.v2, hint);
    }

    /**
     * Computes the bitwise OR of this container with another (union). This container as well as the provided container
     * are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public Container or(final Container x) {
        if (isEmpty()) {
            if (x.isEmpty()) {
                return Container.empty();
            }
            return x.deepCopy();
        }
        if (x.isEmpty()) {
            return deepCopy();
        }
        if (x instanceof SingletonContainer) {
            return or((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return or((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return or((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return or((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return or((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return or((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Rank returns the number of integers that are smaller or equal to x (Rank(infinity) would be GetCardinality()).
     *
     * @param lowbits upper limit
     * @return the rank
     */
    public abstract int rank(short lowbits);

    /**
     * Return a new container with all shorts in [begin,end) remove using an unsigned interpretation.
     *
     * @param begin start of range (inclusive)
     * @param end end of range (exclusive)
     * @return the new container
     */
    public abstract Container remove(int begin, int end);

    /**
     * Remove the short from this container. May create a new container. Note this legacy method does not respect the
     * naming convention of an i prefix for inplace operations; prefer iunset.
     *
     * @param x to be removed
     * @return resulting container.
     */
    @Deprecated
    public final Container remove(short x) {
        return iunset(x);
    }

    /**
     * Remove the short from this container. May create a new container.
     *
     * @param x to be removed
     * @return resulting container.
     */
    public abstract Container unset(short x);

    /**
     * Create a new container with the short removed.
     *
     * @param x to be removed
     * @return New container without x.
     */
    public abstract Container iunset(short x);

    /**
     * Convert to RunContainers, when the result is smaller. Overridden by RunContainer to possibility switch from
     * RunContainer to a smaller alternative. Overridden by BitmapContainer with a more efficient approach.
     *
     * @return the new container
     */
    public abstract Container runOptimize();

    /**
     * Return the jth value
     *
     * @param j index of the value
     * @return the value
     */
    public abstract short select(int j);

    /**
     * Returns a new container with all values between ranks startPos and endPos.
     *
     * @param startRank rank for the start of the range
     * @param endRank rank for the end of the range, exclusive
     * @return a new Container with all the values between ranks [startPos, endPos)
     */
    public abstract Container select(int startRank, int endRank);

    /**
     * Searches for the specified short value
     *
     * @param x value to search for
     * @return Relative position of the value in the sorted set of elements in this container, in the range [0 ..
     *         cardinality - 1]. If not present, (-(insertion point) - 1) similar to Array.binarySearch.
     *         <p>
     *         For values of x that {@link io.deephaven.engine.rowset.impl.rsp.container.Container#contains} returns
     *         true, this method returns the same value as
     *         {@link io.deephaven.engine.rowset.impl.rsp.container.Container#rank}.
     */
    public abstract int find(short x);

    /**
     * As select but for all the positions in a range.
     *
     * @param outValues accept is called in this consumer for each resulting range.
     * @param inPositions input iterator that provides the position ranges.
     */
    public abstract void selectRanges(RangeConsumer outValues, RangeIterator inPositions);

    /**
     * As find but for all the values in a range.
     *
     * @param outPositions accept is called in this consumer for each resulting position range.
     * @param inValues input iterator that provides the key ranges; these must each exist in the container.
     * @param maxPos maximum position to add to outPositions; values of position > maxPos are not added.
     * @return true if maxPos was reached, false otherwise.
     */
    public abstract boolean findRanges(RangeConsumer outPositions, RangeIterator inValues, int maxPos);

    /**
     * If possible, recover wasted memory.
     */
    public abstract void trim();

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This container as well as the
     * provided container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container xor(ArrayContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This container as well as the
     * provided container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container xor(BitmapContainer x);

    /**
     * Computes the bitwise XOR of this container with another (symmetric difference). This container as well as the
     * provided container are left unaffected.
     *
     * @param x Another container
     * @return aggregated container
     */
    public abstract Container xor(RunContainer x);

    private Container xor(final SingletonContainer sc) {
        return cowRef().iflip(sc.value);
    }

    private Container xor(final SingleRangeContainer sr) {
        return not(sr.first(), sr.last() + 1);
    }

    private Container xor(final TwoValuesContainer tv) {
        return cowRef().iflip(tv.v1).iflip(tv.v2);
    }

    /**
     * Computes the bitwise OR of this container with another (symmetric difference). This container as well as the
     * provided container are left unaffected.
     *
     * @param x other parameter
     * @return aggregated container
     */
    public Container xor(final Container x) {
        if (isEmpty()) {
            if (x.isEmpty()) {
                return Container.empty();
            }
            return x.cowRef();
        }
        if (x.isEmpty()) {
            return cowRef();
        }
        if (x instanceof SingletonContainer) {
            return xor((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return xor((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return xor((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return xor((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return xor((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return xor((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * Convert the current container to a BitmapContainer, if a conversion is needed. If the container is already a
     * bitmap, the container is returned unchanged.
     * <p>
     * When multiple container "merge" operations are done it might be more efficient to convert to bitmap first, and
     * then at the end convert to the efficient container type, to avoid multiple container type conversions, since
     * bitmap can always stay a bitmap.
     *
     * @return a bitmap container
     */
    public abstract BitmapContainer toBitmapContainer();

    // For tests.
    abstract Container toLargeContainer();

    /**
     * Gets the first value greater than or equal to the lower bound, or -1 if no such value exists.
     *
     * @param fromValue the lower bound (inclusive)
     * @return the next value
     */
    public abstract int nextValue(short fromValue);

    /**
     * Get the first integer held in the container
     *
     * @return the first integer in the container
     * @throws NoSuchElementException if empty
     */
    public abstract int first();

    /**
     * Get the last integer held in the container
     *
     * @return the last integer in the container
     * @throws NoSuchElementException if empty
     */
    public abstract int last();

    /**
     * @param x Another container
     * @return true if every key in this container is also a key in x.
     */
    public abstract boolean subsetOf(ArrayContainer x);

    /**
     * @param x Another container
     * @return true if every key in this container is also a key in x.
     */
    public abstract boolean subsetOf(BitmapContainer x);

    /**
     * @param x Another container
     * @return true if every key in this container is also a key in x.
     */
    public abstract boolean subsetOf(RunContainer x);

    private boolean subsetOf(final SingletonContainer sc) {
        return getCardinality() == 1 && first() == sc.intValue();
    }

    private boolean subsetOf(final SingleRangeContainer sr) {
        return sr.first() <= first() && last() <= sr.last();
    }

    private boolean subsetOf(final TwoValuesContainer tv) {
        final int card = getCardinality();
        if (card > 2) {
            return false;
        }
        if (card == 2) {
            return first() == tv.first() && last() == tv.last();
        }
        final int v = first();
        return v == tv.v1AsInt() || v == tv.v2AsInt();
    }

    /**
     * @param x Another container
     * @return true if every key in this container is also a key in x.
     */
    public boolean subsetOf(final Container x) {
        if (isEmpty()) {
            return true;
        }
        if (x.isEmpty()) {
            return false;
        }
        if (x instanceof SingletonContainer) {
            return subsetOf((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return subsetOf((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return subsetOf((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return subsetOf((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return subsetOf((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return subsetOf((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /**
     * @param x Another container
     * @return true if some key in this container is also a key in x.
     */
    public final boolean intersects(final Container x) {
        return overlaps(x);
    }

    /**
     * @param start the beginning of the range, as an int.
     * @param end the end of the range (exclusive), as an int.
     * @return true if there is any element in this container in the range provided.
     */
    public final boolean intersects(final int start, final int end) {
        return overlapsRange(start, end);
    }

    /**
     * @param x Another container
     * @return true if at least one key in this container is also a key in x.
     */
    public abstract boolean overlaps(ArrayContainer x);

    /**
     * @param x Another container
     * @return true if at least one key in this container is also a key in x.
     */
    public abstract boolean overlaps(BitmapContainer x);

    /**
     * @param x Another container
     * @return true if at least one key in this container is also a key in x.
     */
    public abstract boolean overlaps(RunContainer x);

    /**
     * @param start the beginning of the range, as an int.
     * @param end the end of the range (exclusive), as an int.
     * @return true if there is any element in this container in the range provided.
     */
    public abstract boolean overlapsRange(int start, int end);

    private boolean overlaps(final SingletonContainer sc) {
        return contains(sc.value);
    }

    private boolean overlaps(final SingleRangeContainer sr) {
        return overlapsRange(sr.first(), sr.last() + 1);
    }

    private boolean overlaps(final TwoValuesContainer tv) {
        return contains(tv.v1) || contains(tv.v2);
    }

    /**
     * @param x Another container
     * @return true if some key in this container is also a key in x.
     */
    public boolean overlaps(final Container x) {
        if (isEmpty() || x.isEmpty()) {
            return false;
        }
        if (x instanceof SingletonContainer) {
            return overlaps((SingletonContainer) x);
        }
        if (x instanceof SingleRangeContainer) {
            return overlaps((SingleRangeContainer) x);
        }
        if (x instanceof TwoValuesContainer) {
            return overlaps((TwoValuesContainer) x);
        }
        if (x instanceof ArrayContainer) {
            return overlaps((ArrayContainer) x);
        }
        if (x instanceof BitmapContainer) {
            return overlaps((BitmapContainer) x);
        }
        if (x instanceof RunContainer) {
            return overlaps((RunContainer) x);
        }
        throw new IllegalStateException(x.myType());
    }

    /*
     * Instruct this container to never modify itself with mutation operations, and instead always return a new
     * container.
     */
    public abstract void setCopyOnWrite();

    /**
     * @return The allocated size in bytes of the underlying array backing store used by this container.
     */
    public abstract int bytesAllocated();

    /**
     * @return The size in bytes of the used portion out of the total allocated bytes for the underlying array backing
     *         store used by this container.
     */
    @SuppressWarnings("unused")
    public abstract int bytesUsed();

    /**
     * Insert a value in the current container. May modify the existing container or return a new one. If positionHint
     * is greater or equal than zero, it is taken to be a container-specific position hint to help speed up the
     * insertion; this can be obtained from previous calls to any method taking a hint to help speedup a sequence of
     * operations done in increasing value order. Before returning, the method stores in positionHint a valid value for
     * a subsequent calls to methods taking a hint, which can be used on the returned container for a value greater than
     * the one provided in this call.
     *
     * @param x the value to insert
     * @param positionHint a position hint to speed up insertion specific to a container, returned from a previous call
     *        to a hint taking method, or -1 if none available. Updated to a valid hint for a subsequent call to a hint
     *        taking method on the returned container; if that subsequent call uses the hint it should be for an
     *        argument bigger than x provided in this call.
     * @return A container with the value to be inserted added; this container may or may not be a modification on the
     *         object on which the call was performed; the value in positionHint after return would be valid on the
     *         returned container.
     */
    abstract Container iset(short x, PositionHint positionHint);

    /**
     * Return a new container container everything in the existing container plus the provided value; does not modify
     * the existing container. If positionHint is greater or equal than zero, it is taken to be a container-specific
     * position hint to help speed up the insertion; this can be obtained from previous calls to any method taking a
     * hint to help speedup a sequence of operations done in increasing value order. Before returning, the method stores
     * in positionHint a valid value for a subsequent calls to methods taking a hint, which can be used on the returned
     * container for a value greater than the one provided in this call.
     *
     * @param x the value to insert
     * @param positionHint a position hint to speed up insertion specific to a container, returned from a previous call
     *        to a hint taking method, or -1 if none available. Updated to a valid hint for a subsequent call to a hint
     *        taking method on the returned container; if that subsequent call uses the hint it should be for an
     *        argument bigger than x provided in this call.
     * @return A new container with the value to be inserted added; the value in positionHint after return would be
     *         valid on the returned container.
     */
    abstract Container set(short x, PositionHint positionHint);

    /**
     * Remove a value in the current container. May modify the existing container or return a new one. If positionHint
     * is greater or equal than zero, it is taken to be a container-specific position hint to help speed up the removal;
     * this can be obtained from previous calls to any method taking a hint to help speedup a sequence of operations
     * done in increasing value order. Before returning, the method stores in positionHint a valid value for a
     * subsequent calls to methods taking a hint, which can be used on the returned container for a value greater than
     * the one provided in this call.
     *
     * @param x the value to remove
     * @param positionHint a position hint to speed up removal specific to a container, returned from a previous call to
     *        a hint taking method, or -1 if none available. Updated to a valid hint for a subsequent call to a hint
     *        taking method on the returned container; if that subsequent call uses the hint it should be for an
     *        argument bigger than x provided in this call.
     * @return A container with the value removed; this container may or may not be a modification on the object on
     *         which the call was performed; the value in positionHint after return would be valid on the returned
     *         container.
     */
    abstract Container iunset(short x, PositionHint positionHint);

    /**
     * Return a new container with every value in the existing container except for the provided argument; the existing
     * container is not modified. If positionHint is greater or equal than zero, it is taken to be a container-specific
     * position hint to help speed up the removal; this can be obtained from previous calls to any method taking a hint
     * to help speedup a sequence of operations done in increasing value order. Before returning, the method stores in
     * positionHint a valid value for a subsequent calls to methods taking a hint, which can be used on the returned
     * container for a value greater than the one provided in this call.
     *
     * @param x the value to remove
     * @param positionHint a position hint to speed up removal specific to a container, returned from a previous call to
     *        a hint taking method, or -1 if none available. Updated to a valid hint for a subsequent call to a hint
     *        taking method on the returned container; if that subsequent call uses the hint it should be for an
     *        argument bigger than x provided in this call.
     * @return A new container with the value removed; this container may or may not be a modification on the object on
     *         which the call was performed; the value in positionHint after return would be valid on the returned
     *         container.
     */
    abstract Container unset(short x, PositionHint positionHint);

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final RangeIterator rit = this.getShortRangeIterator(0);
        final int maxRanges = 99;
        int range = 0;
        sb.append("{ ");
        while (rit.hasNext() && range < maxRanges) {
            rit.next();
            final int first = rit.start();
            final int last = rit.end() - 1;
            if (range > 0) {
                sb.append(",");
            }
            sb.append(first);
            if (last != first) {
                sb.append("-").append(last);
            }
            ++range;
        }
        if (rit.hasNext()) {
            int prevStart = -1;
            int prevLast = -1;
            int prevRange = range;
            while (rit.hasNext()) {
                rit.next();
                prevStart = rit.start();
                prevLast = rit.end() - 1;
                ++prevRange;
            }
            if (prevRange > range + 1) {
                sb.append(", ... ");
            }
            sb.append(",");
            sb.append(prevStart);
            if (prevLast != prevStart) {
                sb.append("-").append(prevLast);
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    public abstract boolean isShared();

    // @VisibleForTesting
    final boolean sameContents(final Container other) {
        if (other.getCardinality() != getCardinality()) {
            return false; // should be a frequent branch if they differ
        }
        return subsetOf(other);
    }
}
