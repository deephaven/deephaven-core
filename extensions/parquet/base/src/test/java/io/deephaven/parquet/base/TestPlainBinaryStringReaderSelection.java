//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.materializers.BlobMaterializer;
import io.deephaven.parquet.base.materializers.PlainBinaryStringMaterializer;
import io.deephaven.parquet.base.materializers.PlainBinaryStringValuesReader;
import io.deephaven.parquet.base.materializers.StringMaterializer;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two halves of the selection rule for {@code PlainBinaryStringValuesReader}: whether the page may be offered at
 * all, and whether a factory wants it. Verified here rather than through a parquet read because class loading happens
 * whether or not the branch is taken, so it is not evidence the path fired.
 */
class TestPlainBinaryStringReaderSelection {

    private static final ByteBuffer HEAP = ByteBuffer.allocate(64);
    private static final ByteBuffer DIRECT = ByteBuffer.allocateDirect(64);

    /** The real factory opts in; the integration tests below use a recording stub, so this is the only check. */
    @Test
    void selectedForPlainBinaryStrings() {
        assertThat(StringMaterializer.FACTORY.maybeMakePlainBinaryValuesReader(HEAP))
                .isInstanceOf(PlainBinaryStringValuesReader.class);
    }

    /**
     * Declining is a correctness requirement, not a tuning choice: PlainBinaryStringValuesReader implements only bulk
     * String decoding, so any other BINARY consumer handed one would throw from readBytes().
     */
    @Test
    void declinedByOtherBinaryMaterializers() {
        assertThat(BlobMaterializer.FACTORY.maybeMakePlainBinaryValuesReader(HEAP)).isNull();
        assertThat(PageMaterializerFactory.NULL_FACTORY.maybeMakePlainBinaryValuesReader(HEAP)).isNull();
    }

    /** A direct page buffer has no backing array, so the factory must decline and let parquet's reader handle it. */
    @Test
    void declinedForDirectBuffers() {
        assertThat(StringMaterializer.FACTORY.maybeMakePlainBinaryValuesReader(DIRECT)).isNull();
    }

    /** Records whether the hook was called, and hands back a reader that is trivially identifiable. */
    private static final class RecordingFactory implements PageMaterializerFactory {
        private final ValuesReader supplied;
        private int calls;

        private RecordingFactory(final ValuesReader supplied) {
            this.supplied = supplied;
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValuesReader maybeMakePlainBinaryValuesReader(final ByteBuffer in) {
            ++calls;
            return supplied;
        }
    }

    private static ColumnPageReaderImpl readerFor(
            final PrimitiveTypeName type, final PageMaterializerFactory factory) {
        final ColumnDescriptor descriptor = new ColumnDescriptor(
                new String[] {"c"}, Types.required(type).named("c"), 0, 0);
        return new ColumnPageReaderImpl("c", null, null, ctx -> ColumnChunkReader.NULL_DICTIONARY, factory,
                descriptor, URI.create("file:///test"), List.of(), 0L,
                new PageHeader(PageType.DATA_PAGE, 0, 0), 0);
    }

    /**
     * The dispatch block in {@code getDataReader} is what makes the optimization reachable; deleting it would leave
     * every other test here passing while silently reverting to parquet's reader.
     */
    @Test
    void getDataReaderConsultsTheFactory() {
        final ValuesReader supplied = new PlainBinaryStringValuesReader(HEAP);
        final RecordingFactory factory = new RecordingFactory(supplied);

        assertThat(readerFor(PrimitiveTypeName.BINARY, factory).getDataReader(Encoding.PLAIN, HEAP, 0, null))
                .isSameAs(supplied);
        assertThat(factory.calls).isEqualTo(1);
    }

    /** A factory that declines must leave the page to parquet, not break the read. */
    @Test
    void getDataReaderFallsBackWhenFactoryDeclines() {
        final RecordingFactory factory = new RecordingFactory(null);

        assertThat(readerFor(PrimitiveTypeName.BINARY, factory).getDataReader(Encoding.PLAIN, HEAP, 0, null))
                .isInstanceOf(BinaryPlainValuesReader.class);
        assertThat(factory.calls).isEqualTo(1);
    }

    /**
     * Dictionary pages must reach the dictionary path without the factory ever being offered them. Encoding is per
     * page, so a chunk holding a dictionary page still reaches here with PLAIN pages; offering a dictionary page would
     * hand the reader RLE bytes to parse as PLAIN.
     */
    @Test
    void getDataReaderSkipsTheFactoryForDictionaryPages() {
        // noinspection deprecation
        for (final Encoding encoding : List.of(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY)) {
            final RecordingFactory factory = new RecordingFactory(new PlainBinaryStringValuesReader(HEAP));
            final ColumnPageReaderImpl reader = readerFor(PrimitiveTypeName.BINARY, factory);

            // NULL_DICTIONARY means "no dictionary was loaded", so reaching that branch is itself the assertion.
            assertThatThrownBy(() -> reader.getDataReader(encoding, HEAP, 0, null))
                    .as("encoding %s", encoding)
                    .isInstanceOf(ParquetDecodingException.class);
            assertThat(factory.calls).as("encoding %s", encoding).isZero();
        }
    }

    /** Non-BINARY columns must never be offered to the factory, whatever it would return. */
    @Test
    void getDataReaderSkipsTheFactoryForNonBinaryColumns() {
        for (final PrimitiveTypeName type : List.of(PrimitiveTypeName.INT32, PrimitiveTypeName.DOUBLE)) {
            final RecordingFactory factory = new RecordingFactory(new PlainBinaryStringValuesReader(HEAP));

            readerFor(type, factory).getDataReader(Encoding.PLAIN, HEAP, 0, null);
            assertThat(factory.calls).as("type %s", type).isZero();
        }
    }

    /**
     * The factory, not the materializer, picks the decode strategy. Falling through to {@link StringMaterializer} would
     * still produce correct values, just slower, so only a type assertion catches it.
     */
    @Test
    void factoryDispatchesOnReaderType() {
        final PlainBinaryStringValuesReader fast = new PlainBinaryStringValuesReader(HEAP);
        final BinaryPlainValuesReader stock = new BinaryPlainValuesReader();

        assertThat(StringMaterializer.FACTORY.makeMaterializerNonNull(fast, 1))
                .isInstanceOf(PlainBinaryStringMaterializer.class);
        assertThat(StringMaterializer.FACTORY.makeMaterializerWithNulls(fast, null, 1))
                .isInstanceOf(PlainBinaryStringMaterializer.class);

        assertThat(StringMaterializer.FACTORY.makeMaterializerNonNull(stock, 1))
                .isInstanceOf(StringMaterializer.class);
        assertThat(StringMaterializer.FACTORY.makeMaterializerWithNulls(stock, null, 1))
                .isInstanceOf(StringMaterializer.class);
    }
}
