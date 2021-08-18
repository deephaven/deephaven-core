package io.deephaven.integrations.numpy;

import io.deephaven.dbtypes.DbImage;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Arrays;

/**
 * Utilities for copying data into numpy arrays.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class Java2NumpyCopy {

    /**
     * Type of table.
     */
    public enum TableType {
        /**
         * Table with only numerical types.
         */
        NUMBER,
        /**
         * Table with only images.
         */
        IMAGE
    }

    /**
     * Determines the type of table.
     *
     * @param t table.
     * @return table type.
     */
    public static TableType tableType(final Table t) {
        if (isImageTable(t)) {
            return TableType.IMAGE;
        } else if (isNumberTable(t)) {
            return TableType.NUMBER;
        } else {
            throw new UnsupportedOperationException("Unsupported table type");
        }
    }

    private static boolean isImageTable(final Table t) {
        return t.getColumns().length == 1 && DbImage.class.isAssignableFrom(t.getColumn(0).getType());
    }

    private static boolean isNumberTable(final Table t) {
        return Arrays.stream(t.getColumns()).allMatch(c -> TypeUtils.isNumeric(c.getType()));
    }

    private static void assertCopySliceArgs(final Table t, final long rowStart, final int dataLength, final int nRow, final int nCol) {
        if (t == null) {
            throw new IllegalArgumentException("t must not be null");
        }

        if (t.isLive()) {
            throw new UnsupportedOperationException("Live tables are not supported");
        }

        if (dataLength < 0) {
            throw new IllegalArgumentException("data must not be null");
        }

        if (nRow <= 0) {
            throw new IllegalArgumentException(nRow + " = nRow <= 0");
        }

        if (nCol <= 0) {
            throw new IllegalArgumentException(nCol + " = nCol <= 0");
        }

        if (dataLength != nRow * nCol) {
            throw new IllegalArgumentException("data is expected to be of length nRow*nCol.  length=" + dataLength + " nRow=" + nRow + " nCol=" + nCol);
        }

        final int nc = t.getColumns().length;
        if (nc != nCol) {
            throw new IllegalArgumentException("Number of table columns does not match the number of output columns: table=" + nc + " nCol=" + nCol);
        }

        final long rowEnd = rowStart + nRow;
        if (rowStart < 0 || rowEnd > t.size()) {
            throw new IllegalArgumentException("Selected rows that are not within the table.  table=[0," + t.size() + "] rowStart=" + rowStart + " rowEnd=" + rowEnd);
        }
    }

    @FunctionalInterface
    private interface CopySetter {
        /**
         * Sets the value.
         *
         * @param cs  column source
         * @param k   column source index
         * @param idx output array index
         */
        void set(final ColumnSource cs, final long k, final int idx);
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     * @param type     type of output data
     * @param cast     string used to cast to the output type
     * @param setter   setter used to assign data
     */
    private static void copySlice(final Table t, final long rowStart, final int nRow, final int nCol, final Class type, final String cast, final CopySetter setter) {
        final Table tt = t.view(Arrays.stream(t.getColumns()).map(c -> c.getType() == type ? c.getName() : c.getName() + " = " + cast + " " + c.getName()).toArray(String[]::new));
        final Index index = tt.getIndex().subindexByPos(rowStart, rowStart + nRow);

        for (int i = 0; i < nCol; i++) {
            final DataColumn c = tt.getColumn(i);
            final ColumnSource cs = tt.getColumnSource(c.getName());

            int j = 0;

            for (Index.Iterator it = index.iterator(); it.hasNext(); ) {
                final long k = it.nextLong();
                final int idx = j * nCol + i;
                setter.set(cs, k, idx);
                j++;
            }
        }
    }


    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final double[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, double.class, "(double)", (cs, k, idx) -> data[idx] = cs.getDouble(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final float[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, float.class, "(float)", (cs, k, idx) -> data[idx] = cs.getFloat(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final byte[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, byte.class, "(byte)", (cs, k, idx) -> data[idx] = cs.getByte(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final short[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, short.class, "(short)", (cs, k, idx) -> data[idx] = cs.getShort(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final int[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, int.class, "(int)", (cs, k, idx) -> data[idx] = cs.getInt(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final long[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, long.class, "(long)", (cs, k, idx) -> data[idx] = cs.getLong(k));
    }

    /**
     * Casts data to the desired type and copies a slice of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol     number of table columns; also the number of columns in <code>data</code>.
     */
    public static void copySlice(final Table t, final long rowStart, final boolean[] data, final int nRow, final int nCol) {
        assertCopySliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, nCol);
        assert data != null;
        copySlice(t, rowStart, nRow, nCol, boolean.class, "(boolean)", (cs, k, idx) -> data[idx] = cs.getBoolean(k));
    }
    
    

    private static class Slice {
        final long[] data;
        final int start;
        final int size;

        private Slice(final long[] data, final int start, final int size) {
            this.data = data;
            this.start = start;
            this.size = size;
        }

        void set(int idx, long val) {
            data[start + idx] = val;
        }

        final int size() {
            return size;
        }
    }

    /**
     * Randomly fills the slice with vaules in the range <code>[0,tSize-1]</code> using a
     * reservoir sampling algorithm.  No rows will be repeated.
     * The slice must be smaller than <code>tSize</code>
     *
     * @param slice slice to fill.
     * @param tSize table size.
     */
    private static void reservoirSample(final Slice slice, final long tSize) {
        // https://en.wikipedia.org/wiki/Reservoir_sampling
        // A-Chao algorithm

        final int k = slice.size();

        if (k > tSize) {
            throw new IllegalArgumentException("Requesting more items than are available.  slice.size()=" + slice.size() + " tSize=" + tSize);
        }

        final RandomDataGenerator rnd = new RandomDataGenerator();

        // fill the reservoir array
        for (int i = 0; i < k; i++) {
            slice.set(i, i);
        }

        for (long i = k; i < tSize; i++) {
            final double p = (double) (k) / (double) (i + 1);
            final double j = rnd.nextUniform(0, 1);

            if (j <= p) {
                slice.set(rnd.nextInt(0, k - 1), i);
            }
        }
    }

    /**
     * Generates a list of random table rows.
     *
     * @param nRow  number of random rows.
     * @param tSize table size.
     * @return indices of random table rows.
     */
    public static long[] randRows(final int nRow, final long tSize) {

        final long[] R = new long[nRow];

        for (int start = 0; start < nRow; start += tSize) {
            final long size = start + tSize <= nRow ? tSize : nRow - start;

            if (size > Integer.MAX_VALUE) {
                throw new IllegalStateException("size > Integer.MAX_VALUE");
            }

            final Slice s = new Slice(R, start, (int) size);
            reservoirSample(s, tSize);
        }

        return R;
    }

    private static void assertCopyRandArgs(final Table t, final long dataLength, final int nRow, final int nCol, final long[] rows) {
        if (t == null) {
            throw new IllegalArgumentException("t must not be null");
        }

        if (t.isLive()) {
            throw new UnsupportedOperationException("Live tables are not supported");
        }

        if (dataLength < 0) {
            throw new IllegalArgumentException("data must not be null");
        }

        if (nRow <= 0) {
            throw new IllegalArgumentException(nRow + " = nRow <= 0");
        }

        if (nCol <= 0) {
            throw new IllegalArgumentException(nCol + " = nCol <= 0");
        }

        if (dataLength != nRow * nCol) {
            throw new IllegalArgumentException("data is expected to be of length nRow*nCol.  length=" + dataLength + " nRow=" + nRow + " nCol=" + nCol);
        }

        final int nc = t.getColumns().length;
        if (nc != nCol) {
            throw new IllegalArgumentException("Number of table columns does not match the number of output columns: table=" + nc + " nCol=" + nCol);
        }

        if (rows != null && rows.length != nRow) {
            throw new IllegalArgumentException("Length of rows does not match nRow.  rows.length=" + rows.length + " nRow=" + nRow);
        }
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into arrays.
     *
     * @param t      table to copy data from
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol   number of table columns; also the number of columns in <code>data</code>.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     * @param type   type of output data
     * @param cast   string used to cast to the output type
     * @param setter setter used to assign data
     */
    public static void copyRand(final Table t, final int nRow, final int nCol, final long[] rows, final Class type, final String cast, final CopySetter setter) {

        final long s = t.size();
        final long[] tidxs = rows == null ? randRows(nRow, s) : rows;
        final Table tt = t.view(Arrays.stream(t.getColumns()).map(c -> c.getType() == type ? c.getName() : c.getName() + " = " + cast + " " + c.getName()).toArray(String[]::new));
        final Index index = tt.getIndex();

        for (int i = 0; i < nCol; i++) {
            final DataColumn c = tt.getColumn(i);
            final ColumnSource cs = tt.getColumnSource(c.getName());

            for (int j = 0; j < nRow; j++) {
                final int idx = j * nCol + i;
                final long tIdx = tidxs[j];

                if (tIdx < 0 || tIdx >= s) {
                    throw new IllegalArgumentException("Table index out of range.  range=[0," + (s - 1) + "] idx=" + tIdx);
                }

                final long k = index.get(tIdx);

                setter.set(cs, k, idx);
            }
        }
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final double[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, double.class, "(double)", (cs, k, idx) -> data[idx] = cs.getDouble(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final float[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, float.class, "(float)", (cs, k, idx) -> data[idx] = cs.getFloat(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final byte[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, byte.class, "(byte)", (cs, k, idx) -> data[idx] = cs.getByte(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final short[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, short.class, "(short)", (cs, k, idx) -> data[idx] = cs.getShort(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final int[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, int.class, "(int)", (cs, k, idx) -> data[idx] = cs.getInt(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final long[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, long.class, "(long)", (cs, k, idx) -> data[idx] = cs.getLong(k));
    }

    /**
     * Casts data to the desired type and copies a random selection of rows into a flattened 2D array.
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t    table to copy data from
     * @param data array to copy data into
     * @param nRow number of rows to copy; also the number of rows in <code>data</code>.
     * @param nCol number of table columns; also the number of columns in <code>data</code>.
     * @param rows indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyRand(final Table t, final boolean[] data, final int nRow, final int nCol, final long[] rows) {
        assertCopyRandArgs(t, data == null ? -1 : data.length, nRow, nCol, rows);
        assert data != null;
        copyRand(t, nRow, nCol, rows, boolean.class, "(boolean)", (cs, k, idx) -> data[idx] = cs.getBoolean(k));
    }

    private static void assertCopyImageSliceArgs(final Table t, final long rowStart, final int dataLength, final int nRow, final int width, final int height, final boolean color) {
        if (t == null) {
            throw new IllegalArgumentException("t must not be null");
        }

        if (t.isLive()) {
            throw new UnsupportedOperationException("Live tables are not supported");
        }

        if (t.getColumns().length != 1) {
            throw new IllegalArgumentException("t must contain one column");
        }

        if (dataLength < 0) {
            throw new IllegalArgumentException("data must not be null");
        }

        if (nRow <= 0) {
            throw new IllegalArgumentException(nRow + " = nRow <= 0");
        }

        if (color) {
            if (dataLength != nRow * height * width * 3) {
                throw new IllegalArgumentException("data is expected to be of length nRow*height*width*3.  length=" + dataLength + " nRow=" + nRow + " width=" + width + " height=" + height);
            }
        } else {
            if (dataLength != nRow * height * width) {
                throw new IllegalArgumentException("data is expected to be of length nRow*height*width.  length=" + dataLength + " nRow=" + nRow + " width=" + width + " height=" + height);
            }
        }

        final long rowEnd = rowStart + nRow;
        if (rowStart < 0 || rowEnd > t.size()) {
            throw new IllegalArgumentException("Selected rows that are not within the table.  table=[0," + t.size() + "] rowStart=" + rowStart + " rowEnd=" + rowEnd);
        }
    }

    private static DbImage getImage(final ColumnSource c, final long k, final int width, final int height, final boolean resize) {
        final DbImage img = (DbImage) c.get(k);

        if (img == null) {
            throw new NullPointerException("Null image.  index=" + k);
        }

        if (resize) {
            if (img.getWidth() != width || img.getHeight() != height) {
                return img.resize(width, height);
            } else {
                return img;
            }
        } else {
            if (img.getWidth() != width || img.getHeight() != height) {
                throw new IllegalArgumentException("Image size does not match expected size.  index=" + k + " image=(" + img.getWidth() + "," + img.getHeight() + ") expected=(" + width + "," + height + ")");
            } else {
                return img;
            }
        }
    }

    @FunctionalInterface
    private interface IntValSetter {
        /**
         * Sets an int value in an array.
         *
         * @param idx array index.
         * @param val value to set.
         */
        void set(final int idx, final int val);
    }

    /**
     * Sets the values in an image into an array.
     *
     * @param img    image
     * @param row    table row
     * @param width  width of the image
     * @param height height of the image
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param setter setter used to assign data
     */
    private static void setImage(final DbImage img, final int row, final int width, final int height, final boolean color, final IntValSetter setter) {
        if (color) {
            for (int h = 0; h < height; h++) {
                for (int w = 0; w < width; w++) {
                    // tensorflow indexes images as [row,height,width,channel]
                    final int idx = row * height * width * 3 + h * width * 3 + w * 3;
                    setter.set(idx, img.getRed(w, h));
                    setter.set(idx + 1, img.getGreen(w, h));
                    setter.set(idx + 2, img.getBlue(w, h));
                }
            }
        } else {
            for (int h = 0; h < height; h++) {
                for (int w = 0; w < width; w++) {
                    // tensorflow indexes images as [row,height,width,channel]
                    final int idx = row * height * width + h * width + w;
                    setter.set(idx, img.getGray(w, h));
                }
            }
        }
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     * @param setter   setter used to assign data
     */
    public static void copyImageSlice(final Table t, final long rowStart, final int nRow, final int width, final int height, final boolean resize, final boolean color, final IntValSetter setter) {

        final DataColumn c = t.getColumn(0);
        final ColumnSource cs = t.getColumnSource(c.getName());
        final Index index = t.getIndex().subindexByPos(rowStart, rowStart + nRow);

        int r = 0;
        for (final Index.Iterator it = index.iterator(); it.hasNext(); r++) {
            final long k = it.nextLong();
            final DbImage img = getImage(cs, k, width, height, resize);
            setImage(img, r, width, height, color, setter);
        }
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     */
    public static void copyImageSlice(final Table t, final long rowStart, final short[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color) {
        assertCopyImageSliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, width, height, color);
        assert data != null;
        copyImageSlice(t, rowStart, nRow, width, height, resize, color, (idx, v) -> data[idx] = (short) v);
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     */
    public static void copyImageSlice(final Table t, final long rowStart, final int[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color) {
        assertCopyImageSliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, width, height, color);
        assert data != null;
        copyImageSlice(t, rowStart, nRow, width, height, resize, color, (idx, v) -> data[idx] = v);
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     */
    public static void copyImageSlice(final Table t, final long rowStart, final long[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color) {
        assertCopyImageSliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, width, height, color);
        assert data != null;
        copyImageSlice(t, rowStart, nRow, width, height, resize, color, (idx, v) -> data[idx] = (long) v);
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     */
    public static void copyImageSlice(final Table t, final long rowStart, final float[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color) {
        assertCopyImageSliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, width, height, color);
        assert data != null;
        copyImageSlice(t, rowStart, nRow, width, height, resize, color, (idx, v) -> data[idx] = (float) v);
    }

    /**
     * Copies a slice of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table images directly into numpy arrays.
     *
     * @param t        table to copy data from
     * @param rowStart first row of data to copy
     * @param data     array to copy data into
     * @param nRow     number of rows to copy; also the number of rows in <code>data</code>.
     * @param width    width of the image
     * @param height   height of the image
     * @param resize   true to resize the image to the target size; false otherwise.
     * @param color    true to return a color image; false to return a gray-scale image.
     */
    public static void copyImageSlice(final Table t, final long rowStart, final double[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color) {
        assertCopyImageSliceArgs(t, rowStart, data == null ? -1 : data.length, nRow, width, height, color);
        assert data != null;
        copyImageSlice(t, rowStart, nRow, width, height, resize, color, (idx, v) -> data[idx] = (double) v);
    }


    private static void assertCopyImageRandArgs(final Table t, final int dataLength, final int nRow, final int width, final int height, final boolean color, final long[] rows) {
        if (t == null) {
            throw new IllegalArgumentException("t must not be null");
        }

        if (t.isLive()) {
            throw new UnsupportedOperationException("Live tables are not supported");
        }

        if (t.getColumns().length != 1) {
            throw new IllegalArgumentException("t must contain one column");
        }

        if (dataLength < 0) {
            throw new IllegalArgumentException("data must not be null");
        }

        if (nRow <= 0) {
            throw new IllegalArgumentException(nRow + " = nRow <= 0");
        }

        if (color) {
            if (dataLength != nRow * height * width * 3) {
                throw new IllegalArgumentException("data is expected to be of length nRow*height*width*3.  length=" + dataLength + " nRow=" + nRow + " width=" + width + " height=" + height);
            }
        } else {
            if (dataLength != nRow * height * width) {
                throw new IllegalArgumentException("data is expected to be of length nRow*height*width.  length=" + dataLength + " nRow=" + nRow + " width=" + width + " height=" + height);
            }
        }

        if (rows != null && rows.length != nRow) {
            throw new IllegalArgumentException("Length of rows does not match nRow.  rows.length=" + rows.length + " nRow=" + nRow);
        }
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into arrays.
     *
     * @param t      table to copy data from
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     * @param setter setter used to assign data
     */
    public static void copyImageRand(final Table t, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows, final IntValSetter setter) {

        final long s = t.size();
        final long[] tidxs = rows == null ? randRows(nRow, s) : rows;

        final DataColumn c = t.getColumn(0);
        final ColumnSource cs = t.getColumnSource(c.getName());
        final Index index = t.getIndex();

        for (int r = 0; r < nRow; r++) {
            final long ridx = tidxs[r];

            if (ridx < 0 || ridx >= s) {
                throw new IllegalArgumentException("Table index out of range.  range=[0," + (s - 1) + "] idx=" + ridx);
            }

            final long k = index.get(r);
            final DbImage img = getImage(cs, k, width, height, resize);
            setImage(img, r, width, height, color, setter);
        }
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t      table to copy data from
     * @param data   array to copy data into
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyImageRand(final Table t, final short[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows) {
        assertCopyImageRandArgs(t, data == null ? -1 : data.length, nRow, width, height, color, rows);
        assert data != null;
        copyImageRand(t, nRow, width, height, resize, color, rows, (idx, val) -> data[idx] = (short) val);
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t      table to copy data from
     * @param data   array to copy data into
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyImageRand(final Table t, final int[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows) {
        assertCopyImageRandArgs(t, data == null ? -1 : data.length, nRow, width, height, color, rows);
        assert data != null;
        copyImageRand(t, nRow, width, height, resize, color, rows, (idx, val) -> data[idx] = val);
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t      table to copy data from
     * @param data   array to copy data into
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyImageRand(final Table t, final long[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows) {
        assertCopyImageRandArgs(t, data == null ? -1 : data.length, nRow, width, height, color, rows);
        assert data != null;
        copyImageRand(t, nRow, width, height, resize, color, rows, (idx, val) -> data[idx] = (long) val);
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t      table to copy data from
     * @param data   array to copy data into
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyImageRand(final Table t, final float[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows) {
        assertCopyImageRandArgs(t, data == null ? -1 : data.length, nRow, width, height, color, rows);
        assert data != null;
        copyImageRand(t, nRow, width, height, resize, color, rows, (idx, val) -> data[idx] = (float) val);
    }

    /**
     * Copies a random selection of image rows into a flattened array.
     * For color, the array is indexed as [row, height, width, channel], where channel is red, green, blue.
     * For grayscale, the array is indexed as [row, height, width].
     * <p>
     * This is useful for copying table data directly into numpy arrays.
     *
     * @param t      table to copy data from
     * @param data   array to copy data into
     * @param nRow   number of rows to copy; also the number of rows in <code>data</code>.
     * @param width  width of the image
     * @param height height of the image
     * @param resize true to resize the image to the target size; false otherwise.
     * @param color  true to return a color image; false to return a gray-scale image.
     * @param rows   indices of rows to copy.  Null causes rows to be randomly generated.
     */
    public static void copyImageRand(final Table t, final double[] data, final int nRow, final int width, final int height, final boolean resize, final boolean color, final long[] rows) {
        assertCopyImageRandArgs(t, data == null ? -1 : data.length, nRow, width, height, color, rows);
        assert data != null;
        copyImageRand(t, nRow, width, height, resize, color, rows, (idx, val) -> data[idx] = (double) val);
    }

}
