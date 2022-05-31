package io.deephaven.engine.rowset.impl.rsp.container;


public abstract class ImmutableContainer extends Container {
    public static final boolean ENABLED =
            !Boolean.getBoolean("io.deephaven.engine.rowset.impl.rsp.container.ImmutableContainer.DISABLED");

    @Override
    public final Container deepCopy() {
        return this;
    }

    @Override
    public final Container cowRef() {
        return this;
    }

    @Override
    public final Container iset(final short x) {
        return set(x);
    }

    @Override
    public final Container iunset(final short x) {
        return unset(x);
    }

    @Override
    public final Container iadd(final int begin, final int end) {
        return add(begin, end);
    }

    @Override
    public final Container iappend(final int begin, final int end) {
        return add(begin, end);
    }

    @Override
    public final Container iand(final ArrayContainer x) {
        return and(x);
    }

    @Override
    public final Container iand(final BitmapContainer x) {
        return and(x);
    }

    @Override
    public final Container iand(final RunContainer x) {
        return and(x);
    }

    @Override
    public final Container iandNot(final ArrayContainer x) {
        return andNot(x);
    }

    @Override
    public final Container iandNot(final BitmapContainer x) {
        return andNot(x);
    }

    @Override
    public final Container iandNot(final RunContainer x) {
        return andNot(x);
    }

    @Override
    public final Container inot(final int rangeStart, final int rangeEnd) {
        return not(rangeStart, rangeEnd);
    }

    @Override
    public final Container ior(final ArrayContainer x) {
        return or(x);
    }

    @Override
    public final Container ior(final BitmapContainer x) {
        return or(x);
    }

    @Override
    public final Container ior(final RunContainer x) {
        return or(x);
    }

    @Override
    public final Container iremove(final int begin, final int end) {
        return remove(begin, end);
    }

    @Override
    public final Container ixor(final ArrayContainer x) {
        return xor(x);
    }

    @Override
    public final Container ixor(final BitmapContainer x) {
        return xor(x);
    }

    @Override
    public final Container ixor(final RunContainer x) {
        return xor(x);
    }

    @Override
    public final Container iandRange(final int start, final int end) {
        return andRange(start, end);
    }

    @Override
    public final boolean isShared() {
        return false;
    }

    @Override
    final Container iset(final short x, final PositionHint positionHint) {
        return set(x, positionHint);
    }

    @Override
    final Container iunset(short x, PositionHint positionHint) {
        return unset(x, positionHint);
    }
}
