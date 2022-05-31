package io.deephaven.engine.rowset.impl.rsp.container;

final class BitmapContainerRangeIterator implements SearchRangeIterator {
    private final long[] bitmap;

    private int x; // position in the bitmap array, for our next value.
    private long w; // remaining bits not returned by the iterator yet from the word at position x.
    private int start; // start of the current range.
    private int end; // end of the current range, exclusive.
    private int offset; // bit offset in bitmap[x] where w begins.
    private boolean hasNext;

    public BitmapContainerRangeIterator(final BitmapContainer p) {
        this(p, 0);
    }

    public BitmapContainerRangeIterator(final BitmapContainer p, int initialSkipCount) {
        bitmap = p.bitmap;
        x = 0;
        offset = 0;
        w = bitmap[0];
        int remaining = initialSkipCount;
        while (true) {
            final int bitCount = Long.bitCount(w);
            if (remaining < bitCount) {
                for (int i = 0; i < remaining; ++i) {
                    int shift = lowestBit(w) + 1;
                    offset += shift;
                    w = w >>> shift;
                }
                break;
            }
            remaining -= bitCount;
            if (x >= 1023) {
                w = 0;
                break;
            }
            ++x;
            w = bitmap[x];
        }
        start = end = -1;
        hasNext = eatZeroes();
    }

    // move x and shift w until the first bit set is found, or we run out of bitmap.
    // return whether "hasNext".
    private boolean eatZeroes() {
        if (x >= bitmap.length) {
            return false;
        }
        // Advance x until the word at x has some bit set;
        // set offset to point to that first bit set if there is one.
        // If not, we ran out of ranges.
        if (w == 0) {
            offset = 0;
            while (++x < bitmap.length) {
                w = bitmap[x];
                if (w != 0) {
                    break;
                }
            }
            if (w == 0) {
                return false;
            }
        }
        // w != 0
        int zbits = lowestBit(w);
        offset += zbits;
        w = w >>> zbits;
        return true;
    }

    BitmapContainerRangeIterator(final BitmapContainerRangeIterator other) {
        this.bitmap = other.bitmap;
        this.x = other.x;
        this.w = other.w;
        this.start = other.start;
        this.end = other.end;
        this.offset = other.offset;
        this.hasNext = other.hasNext;
    }

    public BitmapContainerRangeIterator copy() {
        return new BitmapContainerRangeIterator(this);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int start() {
        return start;
    }

    @Override
    public int end() {
        return end;
    }

    @Override
    public void next() {
        // given invariants, at this point `(w & 1) != 0`.
        start = 64 * x + offset;
        int oneBits = lowestBit(w ^ ~0L); // oneBits is at least 1.
        offset += oneBits;
        w = w >>> oneBits;
        if (offset == 64) {
            offset = 0;
            do {
                ++x;
                if (x >= bitmap.length) {
                    end = 0xFFFF + 1;
                    hasNext = false;
                    return;
                }
                w = bitmap[x];
            } while (w == ~0L);
            // w has /some/ zero bit.
            oneBits = lowestBit(w ^ ~0L);
            offset += oneBits;
            w = w >>> oneBits;
        }
        end = 64 * x + offset;
        hasNext = eatZeroes();
    }

    @Override
    public boolean advance(final int v) {
        if (start == -1) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        if (v < end) {
            if (start < v) {
                start = v;
            }
            return true;
        }
        final int p = v >> 6; // v / 64.
        final int poffset = v & 63; // v % 64.
        x = p;
        if (x >= bitmap.length) {
            return false;
        }
        offset = poffset;
        w = bitmap[x] >>> offset;
        hasNext = eatZeroes();
        if (!hasNext()) {
            return false;
        }
        next();
        if (start < v) {
            start = v;
        }
        return true;
    }

    private void setTo(final int i, final int ioffset) {
        x = i;
        w = bitmap[x];
        offset = ioffset;
        w = w >>> offset;
        next();
    }

    private static int lowestBit(final long v) {
        return Long.numberOfTrailingZeros(v);
    }

    private static int highestBit(final long v) {
        return 63 - Long.numberOfLeadingZeros(v);
    }

    private static long maskForAllBitsSetFromOffsetToHigher(final int offset) {
        return ~0L << offset;
    }

    @Override
    public boolean search(final ContainerUtil.TargetComparator comp) {
        if (start == -1) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        int c = comp.directionFrom(start);
        if (c <= 0) {
            return (c == 0);
        }
        int j = bitmap.length - 1;
        while (j > 0 && bitmap[j] == 0) {
            --j; // seek backward
        }
        // (j,joffset) will be our cursor to the higher words with invariant directionToTarget < 0.
        int joffset = highestBit(bitmap[j]);
        int jv = 64 * j + joffset;
        if (comp.directionFrom(jv) >= 0) {
            x = bitmap.length;
            start = jv;
            end = start + 1;
            hasNext = false;
            return true;
        }
        // (i,ioffset) will be our cursor to the lower words with invariant directionToTarget >= 0.
        int i = start >>> 6; // v / 64.
        int ioffset = start & 63; // v % 64.
        int iv = start;
        while (true) {
            int mv = (iv + jv) / 2;
            int m = mv >>> 6; // mv / 64.
            int moffset = mv & 63; // mv % 64.
            boolean tryLowerBits = false;
            final long mask = maskForAllBitsSetFromOffsetToHigher(moffset);
            long masked = bitmap[m] & mask;
            if (masked == 0) {
                // We will try towards the higher words first. Don't clobber m as we may realize we need to go towards
                // lower words from m instead.
                int m2 = m;
                do {
                    ++m2;
                    // by construction, bitmap[j] != 0 on entry, and m <= j, so this loop will end before
                    // mm > bitmap.length - 1.
                } while (bitmap[m2] == 0);
                // since we are moving towards higher words, the lowest bit is the one closer to the original
                // (m,moffset) target.
                int m2offset = lowestBit(bitmap[m2]);
                if (m2 == j && m2offset == joffset) {
                    // Going towards higher words we ended up in the same place we started, and there are no more lower
                    // bits.
                    // Try indexes from m towards the lower words instead.
                    tryLowerBits = true;
                } else {
                    m = m2;
                    moffset = m2offset;
                    mv = 64 * m + moffset;
                }
            } else {
                moffset = lowestBit(masked);
                if (m == j && moffset == joffset) {
                    tryLowerBits = true;
                } else {
                    mv = 64 * m + moffset;
                }
            }
            if (tryLowerBits) {
                masked = bitmap[m] & ~mask;
                if (masked != 0) {
                    moffset = highestBit(masked);
                    mv = 64 * m + moffset;
                    // We already found (j,joffset) to the highest words. If this happens to be (i,ioffset)
                    // we are done. That check will happen a bit later.
                } else {
                    do {
                        --m;
                        // by construction, bitmap[i] != 0 on entry, and i <= m, so this loop will end before m < 0.
                    } while (bitmap[m] == 0);
                    moffset = highestBit(bitmap[m]);
                    mv = 64 * m + moffset;
                    // We already found (j,joffset) to the highest words. If this happens to be (i,ioffset)
                    // we are done. That check will happen a bit later.
                }
            }
            if (mv <= iv || mv >= jv) {
                setTo(i, ioffset);
                return true;
            }
            c = comp.directionFrom(mv);
            if (c < 0) {
                jv = mv;
                j = m;
                joffset = moffset;
                continue;
            }
            if (c > 0) {
                iv = mv;
                i = m;
                ioffset = moffset;
                continue;
            }
            // c == 0.
            setTo(m, moffset);
            return true;
        }
    }
}
