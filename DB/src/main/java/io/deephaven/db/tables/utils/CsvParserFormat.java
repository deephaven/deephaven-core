package io.deephaven.db.tables.utils;

import org.apache.commons.csv.CSVFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Returns an Apache CSVFormat based on the fileFormat String.
 */
public class CsvParserFormat {
    private static final String FORMAT_DEFAULT = "DEFAULT";
    private static final String FORMAT_TRIM = "TRIM";
    private static final String FORMAT_EXCEL = "EXCEL";
    private static final String FORMAT_TDF = "TDF";
    private static final String FORMAT_MYSQL = "MYSQL";
    private static final String FORMAT_RFC4180 = "RFC4180";
    public static final String FORMAT_BPIPE = "BPIPE";

    public static String[] getFormatNames() {
        return new String[] {FORMAT_DEFAULT, FORMAT_TRIM, FORMAT_EXCEL, FORMAT_TDF, FORMAT_MYSQL,
                FORMAT_RFC4180, FORMAT_BPIPE};
    }

    /**
     * Returns an Apache CSVFormat based on the fileFormat String.
     * 
     * @param fileFormat The string for which a format should be created
     * @param noHeader Indicates when the CSV does not include a row of column names
     * @return A CSVFormat object matching the passed String
     * @throws RuntimeException if fileFormat is unrecognized
     */
    private static CSVFormat getCsvFormat(@NotNull final String fileFormat, final boolean noHeader,
        final List<String> columnNames) {
        CSVFormat result;
        switch (fileFormat) {
            case FORMAT_TRIM:
                result = CSVFormat.DEFAULT.withIgnoreSurroundingSpaces();
                break;
            case FORMAT_DEFAULT:
                result = CSVFormat.DEFAULT;
                break;
            case FORMAT_EXCEL:
                result = CSVFormat.EXCEL;
                break;
            case FORMAT_TDF:
                result = CSVFormat.TDF;
                break;
            case FORMAT_MYSQL:
                result = CSVFormat.MYSQL;
                break;
            case FORMAT_RFC4180:
                result = CSVFormat.RFC4180;
                break;
            default:
                throw new RuntimeException("Unrecognized file format: " + fileFormat);
        }
        if (!noHeader) {
            result = result.withFirstRecordAsHeader();
        } else if (columnNames != null && columnNames.size() > 0) {
            result = result.withHeader(columnNames.toArray(new String[columnNames.size()]));
        }
        return result;
    }

    /**
     * Returns an Apache CSVFormat based on the fileFormat String.
     * 
     * @param fileFormat The string for which a format should be created
     * @param delimiter A single character to use as a delimiter - comma when format will be
     *        controlled by fileFormat
     * @param trim Whether to trim white space within delimiters
     * @param noHeader Indicates when the CSV does not include a row of column names
     * @param columnNames A List of column names to use as a header
     * @return A CSVFormat object matching the passed String and trim option
     * @throws RuntimeException if fileFormat is unrecognized
     */
    public static CSVFormat getCsvFormat(final String fileFormat, final char delimiter,
        final boolean trim, final boolean noHeader, @Nullable final List<String> columnNames) {
        // First figure out the CSVFormat
        final CSVFormat csvFormat;
        if (fileFormat == null || delimiter != ',') {
            if (!noHeader) {
                csvFormat = CSVFormat.newFormat(delimiter).withTrim(trim).withFirstRecordAsHeader()
                    .withQuote('"');
            } else if (columnNames != null && columnNames.size() > 0) {
                csvFormat = CSVFormat.newFormat(delimiter).withTrim(trim)
                    .withHeader(columnNames.toArray(new String[0])).withQuote('"');
            } else {
                csvFormat = CSVFormat.newFormat(delimiter).withTrim(trim).withQuote('"');
            }
        } else {
            csvFormat = getCsvFormat(fileFormat, noHeader, columnNames).withTrim(trim);
        }

        return csvFormat;
    }

    /**
     * For the specified file format, return the required delimiter if there is one
     * 
     * @param fileFormat the file format
     * @return the required delimiter, or null if it is not specified
     */
    public static String getRequiredDelimiter(final String fileFormat) {
        if (FORMAT_BPIPE.equals(fileFormat)) {
            return "|";
        } else {
            return null;
        }
    }
}
