//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.deephaven.parquet.base.ColumnChunkReaderImpl.mayHaveDictionaryPage;
import static io.deephaven.parquet.base.ColumnChunkReaderImpl.usesDictionaryOnEveryPage;
import static org.assertj.core.api.Assertions.assertThat;

class ColumnChunkReaderImplTest {

    private static PageEncodingStats stats(final PageType pageType, final Encoding encoding) {
        return new PageEncodingStats(pageType, encoding, 1);
    }

    private static final PageEncodingStats DICTIONARY_PAGE = stats(PageType.DICTIONARY_PAGE, Encoding.PLAIN);
    private static final PageEncodingStats DICTIONARY_DATA_PAGE = stats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY);
    private static final PageEncodingStats DICTIONARY_DATA_PAGE_V2 =
            stats(PageType.DATA_PAGE_V2, Encoding.RLE_DICTIONARY);
    private static final PageEncodingStats LEGACY_DICTIONARY_DATA_PAGE =
            stats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY);
    private static final PageEncodingStats PLAIN_DATA_PAGE = stats(PageType.DATA_PAGE, Encoding.PLAIN);

    @Test
    void noStatisticsIsNoEvidence() {
        assertThat(usesDictionaryOnEveryPage(null)).isFalse();
        assertThat(usesDictionaryOnEveryPage(List.of())).isFalse();
    }

    @Test
    void statisticsWithoutDataPagesAreNoEvidence() {
        assertThat(usesDictionaryOnEveryPage(List.of(DICTIONARY_PAGE))).isFalse();
    }

    @Test
    void allDictionaryEncoded() {
        assertThat(usesDictionaryOnEveryPage(List.of(DICTIONARY_PAGE, DICTIONARY_DATA_PAGE))).isTrue();
        assertThat(usesDictionaryOnEveryPage(List.of(DICTIONARY_PAGE, DICTIONARY_DATA_PAGE_V2))).isTrue();
        assertThat(usesDictionaryOnEveryPage(List.of(DICTIONARY_PAGE, LEGACY_DICTIONARY_DATA_PAGE))).isTrue();
    }

    @Test
    void anyPlainDataPage() {
        assertThat(usesDictionaryOnEveryPage(List.of(DICTIONARY_PAGE, DICTIONARY_DATA_PAGE, PLAIN_DATA_PAGE)))
                .isFalse();
        assertThat(usesDictionaryOnEveryPage(List.of(PLAIN_DATA_PAGE))).isFalse();
    }

    private static ColumnMetaData columnMeta(final Long dictionaryPageOffset, final Encoding... encodings) {
        final ColumnMetaData columnMeta = new ColumnMetaData();
        columnMeta.setEncodings(List.of(encodings));
        if (dictionaryPageOffset != null) {
            columnMeta.setDictionary_page_offset(dictionaryPageOffset);
        }
        return columnMeta;
    }

    @Test
    void dictionaryPageOffsetMeansMaybe() {
        assertThat(mayHaveDictionaryPage(columnMeta(4L, Encoding.PLAIN, Encoding.RLE_DICTIONARY))).isTrue();
    }

    @Test
    void dictionaryEncodingWithoutOffsetMeansMaybe() {
        // Some writers leave the dictionary page offset unset even though there is a dictionary page
        assertThat(mayHaveDictionaryPage(columnMeta(null, Encoding.RLE, Encoding.RLE_DICTIONARY))).isTrue();
        assertThat(mayHaveDictionaryPage(columnMeta(null, Encoding.PLAIN_DICTIONARY))).isTrue();
    }

    @Test
    void noDictionaryEncodingOrOffsetMeansNone() {
        assertThat(mayHaveDictionaryPage(columnMeta(null, Encoding.PLAIN, Encoding.RLE))).isFalse();
    }
}
