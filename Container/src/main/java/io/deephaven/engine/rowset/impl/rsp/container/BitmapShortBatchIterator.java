package io.deephaven.engine.rowset.impl.rsp.container;

import static java.lang.Long.numberOfTrailingZeros;

public class BitmapShortBatchIterator implements ContainerShortBatchIterator {
    private final BitmapContainer bitmap;

    private int wordIndex;
    private long word;

    public BitmapShortBatchIterator(final BitmapContainer bitmap, final int initialSkipCount) {
        this.bitmap = bitmap;
        wordIndex = 0;
        word = bitmap.bitmap[0];
        int remaining = initialSkipCount;
        while (true) {
            final int bitCount = Long.bitCount(word);
            if (remaining < bitCount) {
                for (int i = 0; i < remaining; ++i) {
                    word &= (word - 1);
                }
                break;
            }
            remaining -= bitCount;
            if (wordIndex >= 1023) {
                word = 0;
                break;
            }
            ++wordIndex;
            word = bitmap.bitmap[wordIndex];
        }
    }

    @Override
    public int next(final short[] buffer, final int offset, final int maxCount) {
        int count = 0;
        while (count < maxCount) {
            while (word == 0) {
                ++wordIndex;
                if (wordIndex == 1024) {
                    return count;
                }
                word = bitmap.bitmap[wordIndex];
            }
            buffer[offset + count] = (short) ((64 * wordIndex) + numberOfTrailingZeros(word));
            ++count;
            word &= (word - 1);
        }
        return count;
    }

    @Override
    public boolean hasNext() {
        if (word != 0) {
            return true;
        }
        // word is zero, will need to increment.
        if (wordIndex >= 1023) {
            return false;
        }
        do {
            ++wordIndex;
            word = bitmap.bitmap[wordIndex];
            if (word != 0) {
                return true;
            }
        } while (wordIndex < 1023);
        return false;
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        boolean wantMore;
        while (true) {
            while (word == 0) {
                ++wordIndex;
                if (wordIndex == 1024) {
                    return true;
                }
                word = bitmap.bitmap[wordIndex];
            }
            final short v = (short) ((64 * wordIndex) + numberOfTrailingZeros(word));
            wantMore = sc.accept(v);
            word &= (word - 1);
            if (!wantMore) {
                return false;
            }
        }
    }
}
