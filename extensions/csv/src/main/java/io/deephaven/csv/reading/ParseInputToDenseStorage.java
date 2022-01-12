package io.deephaven.csv.reading;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.densestorage.DenseStorageReader;
import io.deephaven.csv.densestorage.DenseStorageWriter;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.nio.charset.StandardCharsets;

/**
 * The job of this class is to take the input text, parse the CSV format (dealing with quoting, escaping, field
 * delimiters, and line delimiters) in order to break it into columns of cells (where a cell just contains uninterpreted
 * text... we haven't yet tried to parse into types yet), and to feed each of those columns of cells into its own
 * {@link DenseStorageWriter}. On the reading side, there is a {@link DenseStorageReader} paired with every
 * {@link DenseStorageWriter} and its job is to pull the data back out and have it processed by the
 * {@link ParseDenseStorageToColumn} class. The job of that class is to do pick the most appropriate parser, typically
 * by doing type inference, and parse the text into typed data. The reason for all this separation is that the
 * {@link DenseStorageReader} and {@link ParseDenseStorageToColumn} classes can run concurrently for each column.
 */
public class ParseInputToDenseStorage {
    /**
     * Take cell text (parsed by the {@link CellGrabber}), and feed them to the various {@link DenseStorageWriter}
     * classes.
     * 
     * @param optionalFirstDataRow If not null, this is the first row of data from the file, which the caller had to
     *        peek at in order to know the number of columns in the file.
     * @param nullValueLiteral The configured null value literal. This is used for providing the null value literal to
     *        the downstream processing code (namely the {@link ParseDenseStorageToColumn} code).
     * @param grabber The {@link CellGrabber} which does all the CSV format handling (delimiters, quotes, etc).
     * @param dsws The array of {@link DenseStorageWriter}s, one for each column. As a special case, if a given
     *        {@link DenseStorageWriter} is null, then instead of passing data to it, we confirm that the data is the
     *        empty string and then just drop the data. This is used to handle input files that have a trailing empty
     *        column on the right.
     * @return The number of rows in the input.
     */
    public static long doit(final byte[][] optionalFirstDataRow, final String nullValueLiteral,
            final long startingRowNum, final CellGrabber grabber,
            final DenseStorageWriter[] dsws) throws CsvReaderException {
        final ByteSlice slice = new ByteSlice();
        final int numCols = dsws.length;

        // If a "short row" is encountered (one with a fewer-than-expected number of columns) we will treat
        // it as if the missing cells contained the nullValueLiteral.
        final byte[] nullValueBytes = nullValueLiteral.getBytes(StandardCharsets.UTF_8);
        final ByteSlice nullSlice = new ByteSlice(nullValueBytes, 0, nullValueBytes.length);

        // Zero-based row number.
        long rowNum = startingRowNum;
        // There is a case (namely when the file has no headers and the client hasn't specified
        // them either) where the CsvReader was forced to read the first row of data from the file
        // in order to determine the number of columns. If this happened, optionalFirstDataRow will
        // be non-null and we can process it as data here. Then the rest of the processing can
        // proceed as normal.
        if (optionalFirstDataRow != null) {
            if (optionalFirstDataRow.length != numCols) {
                throw new CsvReaderException(String.format("Expected %d columns but optionalFirstRow had %d",
                        numCols, optionalFirstDataRow.length));
            }
            for (int ii = 0; ii < optionalFirstDataRow.length; ++ii) {
                final byte[] temp = optionalFirstDataRow[ii];
                slice.reset(temp, 0, temp.length);
                appendToDenseStorageWriter(dsws[ii], slice);
            }
            ++rowNum;
        }

        // Grab the remaining lines and store them.
        // The outer while is the "row" iteration.
        final MutableBoolean lastInRow = new MutableBoolean();
        OUTER: while (true) {
            // Zero-based column number.
            int colNum = 0;

            try {
                // The inner while is the "column" iteration
                while (true) {
                    if (!grabber.grabNext(slice, lastInRow)) {
                        if (colNum == 0) {
                            break OUTER;
                        }
                        // Can't get here. If there is any data at all in the last row, and *then* the file ends,
                        // grabNext() will return true, with lastInRow set.
                        throw new RuntimeException("Logic error: uncaught short last row");
                    }
                    appendToDenseStorageWriter(dsws[colNum], slice);
                    ++colNum;
                    if (colNum == numCols) {
                        if (!lastInRow.booleanValue()) {
                            throw new CsvReaderException(
                                    String.format("Row %d has too many columns (expected %d)", rowNum + 1, numCols));
                        }
                        break;
                    }
                    if (lastInRow.booleanValue()) {
                        // Short rows are padded with null
                        while (colNum != numCols) {
                            appendToDenseStorageWriter(dsws[colNum], nullSlice);
                            ++colNum;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                final String message = String.format("While processing row %d, column %d:",
                        rowNum + 1, colNum + 1);
                throw new CsvReaderException(message, e);
            }
            ++rowNum;
        }
        for (DenseStorageWriter dsw : dsws) {
            if (dsw != null) {
                dsw.finish();
            }
        }

        return rowNum;
    }

    private static void appendToDenseStorageWriter(final DenseStorageWriter dsw, final ByteSlice bs)
            throws CsvReaderException {
        if (dsw != null) {
            dsw.append(bs);
            return;
        }
        if (bs.size() != 0) {
            throw new CsvReaderException("Column assumed empty but contains data");
        }

    }
}
